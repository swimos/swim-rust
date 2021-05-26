// Copyright 2015-2021 SWIM.AI inc.
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

mod addresses;
pub mod config;
mod pending;
pub(crate) mod router;
mod state;
pub mod table;
mod task;

#[cfg(not(target_arch = "wasm32"))]
pub mod net;

#[cfg(test)]
mod tests;

use std::net::SocketAddr;

use crate::request::Request;
use futures::future::BoxFuture;
use tokio::sync::mpsc;
use tracing::{event, Level};
use url::Url;
use utilities::sync::promise;

use crate::routing::{ConnectionError, ResolutionError};
use utilities::sync::trigger;
use utilities::task::Spawner;

use crate::routing::error::Unresolvable;
use crate::routing::error::{HttpError, ResolutionErrorKind};
use crate::routing::remote::config::ConnectionConfig;
use crate::routing::remote::state::{DeferredResult, Event, RemoteConnections, RemoteTasksState};
use crate::routing::remote::table::SchemeHostPort;
use crate::routing::ws::WsConnections;
use crate::routing::{ConnectionDropped, RouterFactory, RoutingAddr, TaggedEnvelope};
use futures::stream::FusedStream;
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use std::io;
use std::io::Error;
use tokio::net::TcpListener;

#[cfg(test)]
pub mod test_fixture;

#[derive(Clone, Debug)]
pub struct RawRoute {
    pub sender: mpsc::Sender<TaggedEnvelope>,
    pub on_drop: promise::Receiver<ConnectionDropped>,
}

impl RawRoute {
    pub fn new(
        sender: mpsc::Sender<TaggedEnvelope>,
        on_drop: promise::Receiver<ConnectionDropped>,
    ) -> Self {
        RawRoute { sender, on_drop }
    }
}

type EndpointRequest = Request<Result<RawRoute, Unresolvable>>;
type ResolutionRequest = Request<Result<RoutingAddr, ConnectionError>>;

/// Requests that are generated by the remote router to be serviced by the connection manager.
#[derive(Debug)]
pub enum RoutingRequest {
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

pub struct RemoteConnectionChannels {
    request_tx: mpsc::Sender<RoutingRequest>,
    request_rx: mpsc::Receiver<RoutingRequest>,
    stop_trigger: trigger::Receiver,
}

impl RemoteConnectionChannels {
    pub fn new(
        request_tx: mpsc::Sender<RoutingRequest>,
        request_rx: mpsc::Receiver<RoutingRequest>,
        stop_trigger: trigger::Receiver,
    ) -> RemoteConnectionChannels {
        RemoteConnectionChannels {
            request_tx,
            request_rx,
            stop_trigger,
        }
    }
}

#[derive(Debug)]
pub struct RemoteConnectionsTask<External: ExternalConnections, Ws, Router, Sp> {
    external: External,
    listener: Option<External::ListenerType>,
    websockets: Ws,
    delegate_router: Router,
    stop_trigger: trigger::Receiver,
    spawner: Sp,
    configuration: ConnectionConfig,
    remote_tx: mpsc::Sender<RoutingRequest>,
    remote_rx: mpsc::Receiver<RoutingRequest>,
}

type SchemeSocketAddrIt = std::vec::IntoIter<SchemeSocketAddr>;

const REQUEST_DROPPED: &str = "The receiver of a routing request was dropped before it completed.";
const FAILED_SERVER_CONN: &str = "Failed to establish a server connection.";
const FAILED_CLIENT_CONN: &str = "Failed to establish a client connection.";
const NOT_IN_TABLE: &str = "A connection closed that was not in the routing table.";
const CLOSED_NO_HANDLES: &str = "A connection closed with no handles remaining.";

/// An event loop that listens for incoming connections and routing requests and opens/accepts
/// remote connections accordingly.
///
/// # Type Parameters
///
/// * `External` - Provides the ability to open sockets.
/// * `Ws` - Negotiates a web socket connection on top of the sockets provided by `External`.
/// * `Sp` - Spawner to run the tasks that manage the connections opened by this state machine.
/// * `Routerfac` - Creates router instances to be provided to the connection management tasks.
impl<External, Ws, RouterFac, Sp> RemoteConnectionsTask<External, Ws, RouterFac, Sp>
where
    External: ExternalConnections,
    Ws: WsConnections<External::Socket> + Send + Sync + 'static,
    RouterFac: RouterFactory + 'static,
    Sp: Spawner<BoxFuture<'static, (RoutingAddr, ConnectionDropped)>> + Send + Unpin,
{
    pub async fn new_client_task(
        configuration: ConnectionConfig,
        external: External,
        websockets: Ws,
        delegate_router: RouterFac,
        spawner: Sp,
        channels: RemoteConnectionChannels,
    ) -> Self {
        let RemoteConnectionChannels {
            request_tx: remote_tx,
            request_rx: remote_rx,
            stop_trigger,
        } = channels;

        RemoteConnectionsTask {
            external,
            listener: None,
            websockets,
            delegate_router,
            stop_trigger,
            spawner,
            configuration,
            remote_tx,
            remote_rx,
        }
    }

    pub async fn new_server_task(
        configuration: ConnectionConfig,
        external: External,
        bind_addr: SocketAddr,
        websockets: Ws,
        delegate_router: RouterFac,
        spawner: Sp,
        channels: RemoteConnectionChannels,
    ) -> io::Result<Self> {
        let RemoteConnectionChannels {
            request_tx: remote_tx,
            request_rx: remote_rx,
            stop_trigger,
        } = channels;

        let listener = external.bind(bind_addr).await?;

        Ok(RemoteConnectionsTask {
            external,
            listener: Some(listener),
            websockets,
            delegate_router,
            stop_trigger,
            spawner,
            configuration,
            remote_tx,
            remote_rx,
        })
    }

    pub fn listener(&self) -> Option<&External::ListenerType> {
        self.listener.as_ref()
    }

    pub async fn run(self) -> Result<(), io::Error> {
        match self {
            RemoteConnectionsTask {
                external,
                listener,
                websockets,
                delegate_router,
                stop_trigger,
                spawner,
                configuration,
                remote_tx,
                remote_rx,
            } => {
                let state = RemoteConnections::new(
                    &websockets,
                    configuration,
                    spawner,
                    external,
                    listener,
                    delegate_router,
                    RemoteConnectionChannels {
                        request_tx: remote_tx,
                        request_rx: remote_rx,
                        stop_trigger,
                    },
                );

                RemoteConnectionsTask::run_loop(state, configuration).await
            }
        }
    }

    async fn run_loop(
        mut state: RemoteConnections<'_, External, Ws, Sp, RouterFac>,
        configuration: ConnectionConfig,
    ) -> Result<(), Error> {
        let mut overall_result = Ok(());
        let mut iteration_count: usize = 0;
        let yield_mod = configuration.yield_after.get();

        while let Some(event) = state.select_next().await {
            update_state(&mut state, &mut overall_result, event);

            iteration_count += 1;
            if iteration_count % yield_mod == 0 {
                tokio::task::yield_now().await;
            }
        }
        overall_result
    }
}

/// The state transition function for the state machine underlying the task.
fn update_state<State: RemoteTasksState>(
    state: &mut State,
    overall_result: &mut Result<(), io::Error>,
    next: Event<State::Socket, State::WebSocket>,
) {
    match next {
        Event::Incoming(Ok((stream, peer_addr))) => {
            state.defer_handshake(stream, peer_addr);
        }
        Event::Incoming(Err(conn_err)) => {
            *overall_result = Err(conn_err);
            state.stop();
        }
        Event::Request(RoutingRequest::Endpoint { addr, request }) => {
            let result = if let Some(tx) = state.table_resolve(addr) {
                Ok(tx)
            } else {
                Err(Unresolvable(addr))
            };
            request.send_debug(result, REQUEST_DROPPED);
        }
        Event::Request(RoutingRequest::ResolveUrl { host, request }) => match unpack_url(&host) {
            Ok(target) => {
                if let Some(addr) = state.table_try_resolve(&target) {
                    request.send_ok_debug(addr, REQUEST_DROPPED);
                } else {
                    state.defer_dns_lookup(target, request);
                }
            }
            _ => {
                request.send_err_debug(
                    ConnectionError::Http(HttpError::invalid_url(host.to_string(), None)),
                    REQUEST_DROPPED,
                );
            }
        },
        Event::Deferred(DeferredResult::ServerHandshake {
            result: Ok(ws_stream),
            sock_addr,
        }) => {
            state.spawn_task(sock_addr, ws_stream, None);
        }
        Event::Deferred(DeferredResult::ServerHandshake {
            result: Err(error), ..
        }) => {
            event!(Level::ERROR, FAILED_SERVER_CONN, ?error);
        }
        Event::Deferred(DeferredResult::ClientHandshake {
            result: Ok((ws_stream, sock_addr)),
            host,
        }) => {
            state.spawn_task(sock_addr, ws_stream, Some(host));
        }
        Event::Deferred(DeferredResult::ClientHandshake {
            result: Err(error),
            host,
            ..
        }) => {
            event!(Level::ERROR, FAILED_CLIENT_CONN, ?error);
            state.fail_connection(&host, error);
        }
        Event::Deferred(DeferredResult::FailedConnection {
            error,
            mut remaining,
            host,
        }) => {
            if let Some(sock_addr) = remaining.next() {
                state.defer_connect_and_handshake(host, sock_addr, remaining);
            } else {
                state.fail_connection(&host, error);
            }
        }
        Event::Deferred(DeferredResult::Dns {
            result: Err(err),
            host,
            ..
        }) => {
            state.fail_connection(&host, ConnectionError::Io(err.into()));
        }
        Event::Deferred(DeferredResult::Dns {
            result: Ok(mut addrs),
            host,
        }) => {
            if let Some(sock_addr) = addrs.next() {
                if let Err(host) = state.check_socket_addr(host, sock_addr.clone()) {
                    state.defer_connect_and_handshake(host, sock_addr, addrs);
                }
            } else {
                let host_err = host.to_string();

                state.fail_connection(
                    &host,
                    ConnectionError::Resolution(ResolutionError::new(
                        ResolutionErrorKind::Unresolvable,
                        Some(host_err),
                    )),
                );
            }
        }
        Event::ConnectionClosed(addr, reason) => {
            if let Some(tx) = state.table_remove(addr) {
                if let Err(reason) = tx.provide(reason) {
                    event!(Level::TRACE, CLOSED_NO_HANDLES, ?addr, ?reason);
                }
            } else {
                event!(Level::ERROR, NOT_IN_TABLE, ?addr);
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum BadUrl {
    BadScheme(String),
    NoHost,
}

fn unpack_url(url: &Url) -> Result<SchemeHostPort, BadUrl> {
    let scheme = Scheme::try_from(url.scheme())?;
    match (url.host_str(), url.port()) {
        (Some(host_str), Some(port)) => Ok(SchemeHostPort::new(scheme, host_str.to_owned(), port)),
        (Some(host_str), _) => {
            let default_port = scheme.get_default_port();
            Ok(SchemeHostPort::new(
                scheme,
                host_str.to_owned(),
                default_port,
            ))
        }
        _ => Err(BadUrl::NoHost),
    }
}

/// Supported websocket schemes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scheme {
    Ws,
    Wss,
}

impl TryFrom<&str> for Scheme {
    type Error = BadUrl;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "ws" | "swim" | "warp" => Ok(Scheme::Ws),
            "wss" | "swims" | "warps" => Ok(Scheme::Wss),
            _ => Err(BadUrl::BadScheme(value.to_owned())),
        }
    }
}

impl Scheme {
    /// Get the default port for the schemes.
    fn get_default_port(&self) -> u16 {
        match self {
            Scheme::Ws => 80,
            Scheme::Wss => 443,
        }
    }

    /// Return if the scheme is secure.
    fn is_secure(&self) -> bool {
        match self {
            Scheme::Ws => false,
            Scheme::Wss => true,
        }
    }
}

impl Display for Scheme {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Scheme::Ws => {
                write!(f, "ws")
            }
            Scheme::Wss => {
                write!(f, "wss")
            }
        }
    }
}

type IoResult<T> = io::Result<T>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemeSocketAddr {
    scheme: Scheme,
    addr: SocketAddr,
}

impl SchemeSocketAddr {
    fn new(scheme: Scheme, addr: SocketAddr) -> SchemeSocketAddr {
        SchemeSocketAddr { scheme, addr }
    }
}

impl Display for SchemeSocketAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}://{}/", self.scheme, self.addr)
    }
}

/// Trait for servers that listen for incoming remote connections. This is primarily used to
/// abstract over [`std::net::TcpListener`] for testing purposes.
pub trait Listener {
    type Socket: Unpin + Send + Sync + 'static;
    type AcceptStream: FusedStream<Item = IoResult<(Self::Socket, SchemeSocketAddr)>> + Unpin;

    fn into_stream(self) -> Self::AcceptStream;
}

/// Trait for types that can create remote network connections asynchronously. This is primarily
/// used to abstract over [`std::net::TcpListener`] and [`std::net::TcpStream`] for testing purposes.
pub trait ExternalConnections: Clone + Send + Sync + 'static {
    type Socket: Unpin + Send + Sync + 'static;
    type ListenerType: Listener<Socket = Self::Socket>;

    fn bind(&self, addr: SocketAddr) -> BoxFuture<'static, IoResult<Self::ListenerType>>;
    fn try_open(&self, addr: SocketAddr) -> BoxFuture<'static, IoResult<Self::Socket>>;
    fn lookup(&self, host: SchemeHostPort) -> BoxFuture<'static, IoResult<Vec<SchemeSocketAddr>>>;
}
