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
use std::collections::hash_map::Entry;
use utilities::future::open_ended::OpenEndedFutures;

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
        host: HostAndPort,
    },
    FailedConnection {
        error: ConnectionError,
        remaining: I,
        host: HostAndPort,
    },
    Dns {
        result: io::Result<I>,
        host: HostAndPort,
    },
}

impl<Snk, I> DeferredResult<Snk, I> {
    fn incoming_handshake(result: Result<Snk, ConnectionError>, sock_addr: SocketAddr) -> Self {
        DeferredResult::ServerHandshake { result, sock_addr }
    }

    fn outgoing_handshake(
        result: Result<(Snk, SocketAddr), ConnectionError>,
        host: HostAndPort,
    ) -> Self {
        DeferredResult::ClientHandshake { result, host }
    }

    fn dns(result: io::Result<I>, host: HostAndPort) -> Self {
        DeferredResult::Dns { result, host }
    }

    fn failed_connection(error: ConnectionError, remaining: I, host: HostAndPort) -> Self {
        DeferredResult::FailedConnection {
            error,
            remaining,
            host,
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
        Sp: Spawner<BoxFuture<'static, (RoutingAddr, ConnectionDropped)>>,
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

        spawner.add(
            async move {
                let result = task.run().await;
                (tag, result)
            }
            .boxed(),
        );
        msg_tx
    }
}

#[derive(Debug, Default)]
struct PendingRequests(HashMap<HostAndPort, Vec<ResolutionRequest>>);

impl PendingRequests {
    fn add(&mut self, host: HostAndPort, request: ResolutionRequest) {
        let PendingRequests(map) = self;
        match map.entry(host) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().push(request);
            }
            Entry::Vacant(entry) => {
                entry.insert(vec![request]);
            }
        }
    }

    fn send_ok(&mut self, host: &HostAndPort, addr: RoutingAddr) {
        let PendingRequests(map) = self;
        if let Some(requests) = map.remove(host) {
            for request in requests.into_iter() {
                request.send_ok_debug(addr, REQUEST_DROPPED);
            }
        }
    }

    fn send_err(&mut self, host: &HostAndPort, err: ConnectionError) {
        let PendingRequests(map) = self;
        if let Some(mut requests) = map.remove(host) {
            let first = requests.pop();
            for request in requests.into_iter() {
                request.send_err_debug(err.clone(), REQUEST_DROPPED);
            }
            if let Some(first) = first {
                first.send_err_debug(err, REQUEST_DROPPED);
            }
        }
    }
}

const REQUEST_DROPPED: &str = "The receiver of a routing request was dropped before it completed.";
const FAILED_SERVER_CONN: &str = "Failed to establish a server connection.";
const FAILED_CLIENT_CONN: &str = "Failed to establish a client connection.";
const NOT_IN_TABLE: &str = "A connection closed that was not in the routing table.";
const CLOSED_NO_HANDLES: &str = "A connection closed with no handles remaining.";

impl<Ws, Router, Sp> RemoteConnections<Ws, Router, Sp>
where
    Ws: WsConnections + Send + Sync + 'static,
    Router: ServerRouter + Clone + Send + Sync + 'static,
    Sp: Spawner<BoxFuture<'static, (RoutingAddr, ConnectionDropped)>> + Send + Unpin,
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
            mut spawner,
            configuration,
        } = self;

        let mut listener = poll_fn(move |cx| listener.poll_accept(cx).map(Some)).fuse();
        let mut requests = requests_rx.take_until(stop_trigger.clone());

        let mut table = RoutingTable::default();

        let mut deferred_actions = OpenEndedFutures::new();

        let websockets = &websockets;
        let mut state = State::Running;
        let tasks = TaskFactory::new(requests_tx.clone(), stop_trigger.clone(), configuration);
        let mut addresses = RemoteRoutingAddresses::default();
        let mut pending = PendingRequests::default();

        let _err = loop {
            let next = select_next(
                &mut listener,
                &mut requests,
                &mut deferred_actions,
                &mut spawner,
                &mut state,
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
                                let target_cpy = target.clone();
                                deferred_actions.push(
                                    async move {
                                        let resolved = lookup_host(target_cpy.to_string()).await;
                                        DeferredResult::dns(resolved, target_cpy)
                                    }
                                    .boxed(),
                                );
                                pending.add(target, request);
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
                    host,
                })) => {
                    let addr = addresses.next().expect("Counter overflow.");
                    let msg_tx = tasks.spawn_connection_task(ws_stream, addr, &spawner);
                    pending.send_ok(&host, addr);
                    table.insert(addr, Some(host), sock_addr, msg_tx.clone());
                }
                Some(Event::Deferred(DeferredResult::ClientHandshake {
                    result: Err(error),
                    host,
                    ..
                })) => {
                    event!(Level::ERROR, FAILED_CLIENT_CONN, ?error);
                    pending.send_err(&host, error);
                }
                Some(Event::Deferred(DeferredResult::FailedConnection {
                    error,
                    mut remaining,
                    host,
                })) => {
                    if let Some(sock_addr) = remaining.next() {
                        if let Some(addr) = table.get_resolved(&sock_addr) {
                            pending.send_ok(&host, addr);
                            table.add_host(host, sock_addr);
                        } else {
                            deferred_actions.push(
                                connect_and_handshake(sock_addr, remaining, host, websockets)
                                    .boxed(),
                            );
                        }
                    } else {
                        pending.send_err(&host, error);
                    }
                }
                Some(Event::Deferred(DeferredResult::Dns {
                    result: Err(_err),
                    host,
                    ..
                })) => {
                    pending.send_err(&host, ConnectionError::ConnectError);
                }
                Some(Event::Deferred(DeferredResult::Dns {
                    result: Ok(mut addrs),
                    host,
                })) => {
                    if let Some(sock_addr) = addrs.next() {
                        if let Some(addr) = table.get_resolved(&sock_addr) {
                            pending.send_ok(&host, addr);
                            table.add_host(host, sock_addr);
                        } else {
                            deferred_actions.push(
                                connect_and_handshake(sock_addr, addrs, host, websockets).boxed(),
                            );
                        }
                    } else {
                        pending.send_err(&host, ConnectionError::ConnectError);
                    }
                }
                Some(Event::ConnectionClosed(addr, reason)) => {
                    if let Some(tx) = table.remove(addr) {
                        if let Err(reason) = tx.provide(reason) {
                            event!(Level::TRACE, CLOSED_NO_HANDLES, ?addr, ?reason);
                        }
                    } else {
                        event!(Level::ERROR, NOT_IN_TABLE, ?addr);
                    }
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
    ConnectionClosed(RoutingAddr, ConnectionDropped),
}

enum State {
    Running,
    ClosingConnections,
    ClearingDeferred,
}

async fn select_next<'a, Listener, Requests, Snk, I, Sp>(
    listener: &mut Listener,
    requests: &mut Requests,
    deferred: &mut OpenEndedFutures<BoxFuture<'a, DeferredResult<Snk, I>>>,
    spawner: &mut Sp,
    state: &mut State,
) -> Option<Event<Snk, I>>
where
    Listener: FusedStream<Item = io::Result<(TcpStream, SocketAddr)>> + Unpin,
    Requests: FusedStream<Item = RoutingRequest> + Unpin,
    Sp: Spawner<BoxFuture<'static, (RoutingAddr, ConnectionDropped)>> + Unpin,
{
    loop {
        match state {
            State::Running => {
                let result = select_biased! {
                    incoming = listener.next() => incoming.map(Event::Incoming),
                    request = requests.next() => request.map(Event::Request),
                    def_complete = deferred.next() => def_complete.map(Event::Deferred),
                    result = spawner.next() => result.map(|(addr, reason)| Event::ConnectionClosed(addr, reason)),
                };
                if result.is_none() {
                    spawner.stop();
                    *state = State::ClosingConnections;
                } else {
                    return result;
                }
            }
            State::ClosingConnections => {
                let result = select_biased! {
                    def_complete = deferred.next() => def_complete.map(Event::Deferred),
                    result = spawner.next() => result.map(|(addr, reason)| Event::ConnectionClosed(addr, reason)),
                };
                if result.is_none() {
                    OpenEndedFutures::stop(deferred);
                    *state = State::ClearingDeferred;
                } else {
                    return result;
                }
            }
            State::ClearingDeferred => {
                return deferred.next().await.map(Event::Deferred);
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

async fn connect_and_handshake<Ws, I>(
    sock_addr: SocketAddr,
    remaining: I,
    host: HostAndPort,
    websockets: &Ws,
) -> DeferredResult<Ws::StreamSink, I>
where
    Ws: WsConnections,
    I: Iterator<Item = SocketAddr>,
{
    match connect_and_handshake_single(sock_addr, websockets).await {
        Ok(str) => DeferredResult::outgoing_handshake(Ok((str, sock_addr)), host),
        Err(err) => DeferredResult::failed_connection(err, remaining, host),
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
        host: Option<HostAndPort>,
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
            resolved_forward.insert(host.clone(), addr);
            hosts.insert(host);
        }

        endpoints.insert(addr, Handle::new(tx, sock_addr, hosts));
    }

    fn add_host(&mut self, host: HostAndPort, sock_addr: SocketAddr) -> Option<RoutingAddr> {
        let RoutingTable {
            open_sockets,
            resolved_forward,
            endpoints,
            ..
        } = self;

        if let Some(addr) = open_sockets.get(&sock_addr) {
            debug_assert!(!resolved_forward.contains_key(&host));
            resolved_forward.insert(host.clone(), *addr);
            let handle = endpoints.get_mut(&addr).expect("Inconsistent table.");
            handle.bindings.insert(host);
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
