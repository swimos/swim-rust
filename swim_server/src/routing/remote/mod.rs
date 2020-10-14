// Copyright 2015-2020 SWIM.AI inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::select_biased;
use futures::stream::FuturesUnordered;
use futures::stream::{poll_fn, FusedStream};
use futures::{FutureExt, StreamExt};
use tokio::io;
use tokio::net::{lookup_host, TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tracing::{event, Level};
use url::Url;

use swim_common::request::Request;
use swim_common::routing::RoutingError;
use swim_common::sink::item::{ItemSink, MpscSend};
use swim_common::warp::envelope::Envelope;
use swim_common::ws::error::{ConnectionError, WebSocketError};
use swim_common::ws::WsMessage;
use utilities::future::retryable::strategy::RetryStrategy;
use utilities::sync::{promise, trigger};
use utilities::task::Spawner;
use utilities::uri::RelativeUri;

use crate::plane::error::{ResolutionError, Unresolvable};
use crate::routing::ws::{JoinedStreamSink, WsConnections};
use crate::routing::{error, Route, RoutingAddr, ServerRouter, TaggedEnvelope};

mod task;

#[derive(Debug, Clone)]
pub enum ConnectionDropped {
    Closed,
    TimedOut(Duration),
    Failed(ConnectionError),
    AgentFailed,
    Unknown,
}

impl ConnectionDropped {
    pub fn is_recoverable(&self) -> bool {
        match self {
            ConnectionDropped::TimedOut(_) => true,
            ConnectionDropped::Failed(err) => err.is_transient(),
            ConnectionDropped::AgentFailed => true,
            _ => false,
        }
    }
}

type EndpointRequest = Request<Result<Route<mpsc::Sender<TaggedEnvelope>>, Unresolvable>>;
type ResolutionRequest = Request<Result<RoutingAddr, ConnectionError>>;

enum RoutingRequest {
    /// Get channel to route messages to a specified routing address.
    Endpoint {
        addr: RoutingAddr,
        request: EndpointRequest,
    },
    /// Resolve the routing address for a host.
    ResolveUrl {
        host: Url,
        request: ResolutionRequest,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct ConnectionConfig {
    buffer_size: NonZeroUsize,
    activity_timeout: Duration,
    connection_retries: RetryStrategy,
}

#[derive(Debug)]
pub struct RemoteConnections<Ws, Router, Sp> {
    requests_tx: mpsc::Sender<RoutingRequest>,
    requests_rx: mpsc::Receiver<RoutingRequest>,
    listener: TcpListener,
    websockets: Ws,
    delegate_router: Router,
    stop_trigger: trigger::Receiver,
    spawner: Sp,
    configuration: ConnectionConfig,
}

enum DeferredResult<Snk, I> {
    ServerHandshake {
        result: Result<Snk, ConnectionError>,
        sock_addr: SocketAddr,
    },
    ClientHandshake {
        result: Result<(Snk, SocketAddr), ConnectionError>,
        request: ResolutionRequest,
        host: Option<String>,
    },
    Dns {
        result: io::Result<I>,
        host: HostAndPort,
        request: ResolutionRequest,
    },
}

impl<Snk, I> DeferredResult<Snk, I> {
    fn incoming_handshake(result: Result<Snk, ConnectionError>, sock_addr: SocketAddr) -> Self {
        DeferredResult::ServerHandshake { result, sock_addr }
    }

    fn outgoing_handshake(
        result: Result<(Snk, SocketAddr), ConnectionError>,
        request: ResolutionRequest,
        host: Option<String>,
    ) -> Self {
        DeferredResult::ClientHandshake {
            result,
            request,
            host,
        }
    }

    fn dns(result: io::Result<I>, host: HostAndPort, request: ResolutionRequest) -> Self {
        DeferredResult::Dns {
            result,
            host,
            request,
        }
    }
}

struct TaskFactory {
    request_tx: mpsc::Sender<RoutingRequest>,
    stop_trigger: trigger::Receiver,
    configuration: ConnectionConfig,
}

impl TaskFactory {
    fn new(
        request_tx: mpsc::Sender<RoutingRequest>,
        stop_trigger: trigger::Receiver,
        configuration: ConnectionConfig,
    ) -> Self {
        TaskFactory {
            request_tx,
            stop_trigger,
            configuration,
        }
    }

    fn spawn_connection_task<Str, Sp>(
        &self,
        ws_stream: Str,
        tag: RoutingAddr,
        spawner: &Sp,
    ) -> mpsc::Sender<TaggedEnvelope>
    where
        Str: JoinedStreamSink<WsMessage, ConnectionError> + Send + Unpin + 'static,
        Sp: Spawner<BoxFuture<'static, ConnectionDropped>>,
    {
        let TaskFactory {
            request_tx,
            stop_trigger,
            configuration,
        } = self;
        let (msg_tx, msg_rx) = mpsc::channel(configuration.buffer_size.get());
        let task = task::ConnectionTask::new(
            ws_stream,
            RemoteRouter::new(tag, request_tx.clone()),
            msg_rx,
            stop_trigger.clone(),
            configuration.activity_timeout,
            configuration.connection_retries,
        );
        spawner.add(task.run().boxed());
        msg_tx
    }
}

const REQUEST_DROPPED: &'static str =
    "The receiver of a routing request was dropped before it completed.";
const FAILED_SERVER_CONN: &'static str = "Failed to establish a server connection.";
const FAILED_CLIENT_CONN: &'static str = "Failed to establish a client connection.";

impl<Ws, Router, Sp> RemoteConnections<Ws, Router, Sp>
where
    Ws: WsConnections + Send + Sync + 'static,
    Router: ServerRouter + Clone + Send + Sync + 'static,
    Sp: Spawner<BoxFuture<'static, ConnectionDropped>> + Send,
{
    pub fn new(
        req_buffer_size: NonZeroUsize,
        listener: TcpListener,
        websockets: Ws,
        delegate_router: Router,
        stop_trigger: trigger::Receiver,
        spawner: Sp,
        configuration: ConnectionConfig,
    ) -> Self {
        let (requests_tx, requests_rx) = mpsc::channel(req_buffer_size.get());
        RemoteConnections {
            requests_tx,
            requests_rx,
            listener,
            websockets,
            delegate_router,
            stop_trigger,
            spawner,
            configuration,
        }
    }

    pub async fn run(self) {
        let RemoteConnections {
            requests_tx,
            requests_rx,
            mut listener,
            websockets,
            delegate_router: _,
            stop_trigger,
            spawner,
            configuration,
        } = self;

        let mut listener = poll_fn(move |cx| listener.poll_accept(cx).map(Some)).fuse();
        let mut requests = requests_rx.take_until(stop_trigger.clone());

        let mut table = RoutingTable::default();

        let mut deferred_actions = FuturesUnordered::new();

        let websockets = &websockets;

        let mut closing = false;

        let tasks = TaskFactory::new(requests_tx.clone(), stop_trigger.clone(), configuration);

        let mut addresses = RemoteRoutingAddresses::default();

        let _err = loop {
            let next = select_next(
                &mut listener,
                &mut requests,
                &mut deferred_actions,
                &mut closing,
            )
            .await;
            match next {
                Some(Event::Incoming(Ok((stream, peer_addr)))) => {
                    deferred_actions.push(
                        async move {
                            let result = do_handshake(true, stream, websockets).await;
                            DeferredResult::incoming_handshake(result, peer_addr)
                        }
                        .boxed(),
                    );
                }
                Some(Event::Incoming(Err(conn_err))) => {
                    break Some(conn_err);
                }
                Some(Event::Request(RoutingRequest::Endpoint { addr, request })) => {
                    let result = if let Some(tx) = table.resolve(addr) {
                        Ok(tx)
                    } else {
                        Err(Unresolvable(addr))
                    };
                    request.send_debug(result, REQUEST_DROPPED);
                }
                Some(Event::Request(RoutingRequest::ResolveUrl { host, request })) => {
                    match unpack_url(&host) {
                        Ok(target) => {
                            if let Some(addr) = table.try_resolve(&target) {
                                request.send_ok_debug(addr, REQUEST_DROPPED);
                            } else {
                                deferred_actions.push(
                                    async move {
                                        let resolved = lookup_host(target.to_string()).await;
                                        DeferredResult::dns(resolved, target, request)
                                    }
                                    .boxed(),
                                );
                            }
                        }
                        _ => {
                            request.send_err_debug(
                                ConnectionError::SocketError(WebSocketError::Url(
                                    host.into_string(),
                                )),
                                REQUEST_DROPPED,
                            );
                        }
                    }
                }
                Some(Event::Deferred(DeferredResult::ServerHandshake {
                    result: Ok(ws_stream),
                    sock_addr,
                })) => {
                    let addr = addresses.next().expect("Counter overflow.");
                    let msg_tx = tasks.spawn_connection_task(ws_stream, addr, &spawner);
                    table.insert(addr, None, sock_addr, msg_tx);
                }
                Some(Event::Deferred(DeferredResult::ServerHandshake {
                    result: Err(error),
                    ..
                })) => {
                    event!(Level::ERROR, FAILED_SERVER_CONN, ?error);
                }
                Some(Event::Deferred(DeferredResult::ClientHandshake {
                    result: Ok((ws_stream, sock_addr)),
                    request,
                    host,
                })) => {
                    let addr = addresses.next().expect("Counter overflow.");
                    let msg_tx = tasks.spawn_connection_task(ws_stream, addr, &spawner);
                    table.insert(addr, host, sock_addr, msg_tx.clone());
                    request.send_ok_debug(addr, REQUEST_DROPPED);
                }
                Some(Event::Deferred(DeferredResult::ClientHandshake {
                    result: Err(error),
                    request,
                    ..
                })) => {
                    event!(Level::ERROR, FAILED_CLIENT_CONN, ?error);
                    request.send_err_debug(error, REQUEST_DROPPED);
                }
                Some(Event::Deferred(DeferredResult::Dns {
                    result: Err(_err),
                    request,
                    ..
                })) => {
                    request.send_err_debug(ConnectionError::ConnectError, REQUEST_DROPPED);
                }
                Some(Event::Deferred(DeferredResult::Dns {
                    result: Ok(addrs),
                    host,
                    request,
                })) => {
                    let HostAndPort(host, _) = host;
                    deferred_actions.push(
                        async move {
                            let result = connect_and_handshake(addrs, websockets).await;
                            DeferredResult::outgoing_handshake(result, request, Some(host))
                        }
                        .boxed(),
                    );
                }
                _ => {
                    break None;
                }
            }
        };
    }
}

#[derive(Debug, Default)]
struct RemoteRoutingAddresses(u32);

impl Iterator for RemoteRoutingAddresses {
    type Item = RoutingAddr;

    fn next(&mut self) -> Option<Self::Item> {
        let RemoteRoutingAddresses(count) = self;
        let addr = RoutingAddr::remote(*count);
        if let Some(next) = count.checked_add(1) {
            *count = next;
            Some(addr)
        } else {
            None
        }
    }
}

enum Event<Snk, I> {
    Incoming(io::Result<(TcpStream, SocketAddr)>),
    Request(RoutingRequest),
    Deferred(DeferredResult<Snk, I>),
}

async fn select_next<'a, Listener, Requests, Snk, I>(
    listener: &mut Listener,
    requests: &mut Requests,
    deferred: &mut FuturesUnordered<BoxFuture<'a, DeferredResult<Snk, I>>>,
    closing: &mut bool,
) -> Option<Event<Snk, I>>
where
    Listener: FusedStream<Item = io::Result<(TcpStream, SocketAddr)>> + Unpin,
    Requests: FusedStream<Item = RoutingRequest> + Unpin,
{
    loop {
        if *closing {
            return if deferred.is_empty() {
                None
            } else {
                deferred.next().await.map(Event::Deferred)
            };
        } else {
            let next = if deferred.is_empty() {
                select_biased! {
                    incoming = listener.next() => incoming.map(Event::Incoming),
                    request = requests.next() => request.map(Event::Request),
                }
            } else {
                select_biased! {
                    incoming = listener.next() => incoming.map(Event::Incoming),
                    request = requests.next() => request.map(Event::Request),
                    def_complete = deferred.next() => def_complete.map(Event::Deferred),
                }
            };
            if next.is_none() {
                *closing = true;
            } else {
                return next;
            }
        }
    }
}

async fn do_handshake<Ws>(
    server: bool,
    socket: TcpStream,
    websockets: &Ws,
) -> Result<Ws::StreamSink, ConnectionError>
where
    Ws: WsConnections,
{
    if server {
        websockets.accept_connection(socket).await
    } else {
        websockets.open_connection(socket).await
    }
}

async fn connect_and_handshake<Ws>(
    mut addrs: impl Iterator<Item = SocketAddr>,
    websockets: &Ws,
) -> Result<(Ws::StreamSink, SocketAddr), ConnectionError>
where
    Ws: WsConnections,
{
    let mut prev_err = None;
    loop {
        if let Some(addr) = addrs.next() {
            match connect_and_handshake_single(addr, websockets).await {
                Ok(str) => {
                    break Ok((str, addr));
                }
                Err(err) => {
                    prev_err = Some(err);
                }
            }
        } else {
            break Err(prev_err.unwrap_or(ConnectionError::ConnectError));
        }
    }
}

async fn connect_and_handshake_single<Ws>(
    addr: SocketAddr,
    websockets: &Ws,
) -> Result<Ws::StreamSink, ConnectionError>
where
    Ws: WsConnections,
{
    websockets
        .open_connection(TcpStream::connect(addr).await?)
        .await
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct HostAndPort(String, u16);

impl Display for HostAndPort {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let HostAndPort(host, port) = self;
        write!(f, "{}:{}", host, port)
    }
}

#[derive(Debug)]
struct Handle {
    tx: mpsc::Sender<TaggedEnvelope>,
    drop_tx: promise::Sender<ConnectionDropped>,
    drop_rx: promise::Receiver<ConnectionDropped>,
    peer: SocketAddr,
    bindings: HashSet<HostAndPort>,
}

impl Handle {
    fn new(
        tx: mpsc::Sender<TaggedEnvelope>,
        peer: SocketAddr,
        bindings: HashSet<HostAndPort>,
    ) -> Self {
        let (drop_tx, drop_rx) = promise::promise();
        Handle {
            tx,
            drop_tx,
            drop_rx,
            peer,
            bindings,
        }
    }
}

#[derive(Debug, Default)]
struct RoutingTable {
    open_sockets: HashMap<SocketAddr, RoutingAddr>,
    resolved_forward: HashMap<HostAndPort, RoutingAddr>,
    endpoints: HashMap<RoutingAddr, Handle>,
}

enum BadUrl {
    MissingPort,
    NoHost,
}

fn unpack_url(url: &Url) -> Result<HostAndPort, BadUrl> {
    match (url.host_str(), url.port()) {
        (Some(host_str), Some(port)) => Ok(HostAndPort(host_str.to_owned(), port)),
        (Some(host_str), _) => {
            if let Some(port) = default_port(url.scheme()) {
                Ok(HostAndPort(host_str.to_owned(), port))
            } else {
                Err(BadUrl::MissingPort)
            }
        }
        _ => Err(BadUrl::NoHost),
    }
}

fn default_port(scheme: &str) -> Option<u16> {
    match scheme {
        "ws" | "swim" | "warp" => Some(80),
        "wss" | "swims" | "warps" => Some(443),
        _ => None,
    }
}

impl RoutingTable {
    fn try_resolve(&self, target: &HostAndPort) -> Option<RoutingAddr> {
        self.resolved_forward.get(target).copied()
    }

    fn get_resolved(&self, target: &SocketAddr) -> Option<RoutingAddr> {
        self.open_sockets.get(target).copied()
    }

    fn resolve(&self, addr: RoutingAddr) -> Option<Route<mpsc::Sender<TaggedEnvelope>>> {
        self.endpoints
            .get(&addr)
            .map(|h| Route::new(h.tx.clone(), h.drop_rx.clone()))
    }

    fn insert(
        &mut self,
        addr: RoutingAddr,
        host: Option<String>,
        sock_addr: SocketAddr,
        tx: mpsc::Sender<TaggedEnvelope>,
    ) {
        let RoutingTable {
            open_sockets,
            resolved_forward,
            endpoints,
        } = self;
        debug_assert!(!open_sockets.contains_key(&sock_addr));

        open_sockets.insert(sock_addr, addr);
        let mut hosts = HashSet::new();
        if let Some(host) = host {
            resolved_forward.insert(HostAndPort(host.clone(), sock_addr.port()), addr);
            hosts.insert(HostAndPort(host, sock_addr.port()));
        }

        endpoints.insert(addr, Handle::new(tx, sock_addr, hosts));
    }

    fn add_host(&mut self, host: String, sock_addr: SocketAddr) -> Option<RoutingAddr> {
        let RoutingTable {
            open_sockets,
            resolved_forward,
            endpoints,
            ..
        } = self;

        if let Some(addr) = open_sockets.get(&sock_addr) {
            let host_and_port = HostAndPort(host, sock_addr.port());
            debug_assert!(!resolved_forward.contains_key(&host_and_port));
            resolved_forward.insert(host_and_port.clone(), *addr);
            let handle = endpoints.get_mut(&addr).expect("Inconsistent table.");
            handle.bindings.insert(host_and_port);
            Some(*addr)
        } else {
            None
        }
    }

    fn remove(&mut self, addr: RoutingAddr) -> Option<promise::Sender<ConnectionDropped>> {
        let RoutingTable {
            open_sockets,
            resolved_forward,
            endpoints,
            ..
        } = self;
        if let Some(Handle {
            peer,
            bindings,
            drop_tx,
            ..
        }) = endpoints.remove(&addr)
        {
            open_sockets.remove(&peer);
            bindings.iter().for_each(move |h| {
                resolved_forward.remove(h);
            });
            Some(drop_tx)
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct RemoteRouter {
    tag: RoutingAddr,
    request_tx: mpsc::Sender<RoutingRequest>,
}

impl RemoteRouter {
    fn new(tag: RoutingAddr, request_tx: mpsc::Sender<RoutingRequest>) -> Self {
        RemoteRouter { tag, request_tx }
    }
}

struct RemoteRouterSender {
    tag: RoutingAddr,
    inner: mpsc::Sender<TaggedEnvelope>,
}

impl RemoteRouterSender {
    fn new(tag: RoutingAddr, inner: mpsc::Sender<TaggedEnvelope>) -> Self {
        RemoteRouterSender { tag, inner }
    }
}

impl<'a> ItemSink<'a, Envelope> for RemoteRouterSender {
    type Error = error::SendError;
    type SendFuture = MpscSend<'a, TaggedEnvelope, error::SendError>;

    fn send_item(&'a mut self, envelope: Envelope) -> Self::SendFuture {
        let RemoteRouterSender { tag, inner } = self;
        MpscSend::new(inner, TaggedEnvelope(*tag, envelope))
    }
}

impl ServerRouter for RemoteRouter {
    type Sender = RemoteRouterSender;

    fn get_sender(
        &mut self,
        addr: RoutingAddr,
    ) -> BoxFuture<'_, Result<Route<Self::Sender>, RoutingError>> {
        async move {
            let RemoteRouter { tag, request_tx } = self;
            let (tx, rx) = oneshot::channel();
            let request = Request::new(tx);
            let routing_req = RoutingRequest::Endpoint { addr, request };
            if request_tx.send(routing_req).await.is_err() {
                Err(RoutingError::RouterDropped)
            } else {
                match rx.await {
                    Ok(Ok(Route { sender, on_drop })) => {
                        Ok(Route::new(RemoteRouterSender::new(*tag, sender), on_drop))
                    }
                    Ok(Err(_)) => Err(RoutingError::PoolError(ConnectionError::Closed)),
                    Err(_) => Err(RoutingError::RouterDropped),
                }
            }
        }
        .boxed()
    }

    fn resolve(
        &mut self,
        host: Option<Url>,
        _route: RelativeUri,
    ) -> BoxFuture<'_, Result<RoutingAddr, ResolutionError>> {
        async move {
            let RemoteRouter { request_tx, .. } = self;
            if let Some(url) = host {
                let (tx, rx) = oneshot::channel();
                let request = Request::new(tx);
                let routing_req = RoutingRequest::ResolveUrl { host: url, request };
                if request_tx.send(routing_req).await.is_err() {
                    Err(ResolutionError::NoRoute(RoutingError::RouterDropped))
                } else {
                    match rx.await {
                        Ok(Ok(addr)) => Ok(addr),
                        Ok(Err(err)) => Err(ResolutionError::NoRoute(RoutingError::PoolError(err))),
                        Err(_) => Err(ResolutionError::NoRoute(RoutingError::RouterDropped)),
                    }
                }
            } else {
                //TODO Add a more appropriate error variant or delegate.
                Err(ResolutionError::NoRoute(RoutingError::ConnectionError))
            }
        }
        .boxed()
    }
}
