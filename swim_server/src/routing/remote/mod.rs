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

mod addresses;
pub mod config;
pub mod net;
mod pending;
mod router;
mod state;
mod table;
mod task;
#[cfg(test)]
mod tests;

use std::net::SocketAddr;
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::sync::mpsc;
use tracing::{event, Level};
use url::Url;

use swim_common::request::Request;
use swim_common::ws::error::WebSocketError;
use utilities::sync::trigger;
use utilities::task::Spawner;

use crate::routing::error::{ConnectionError, Unresolvable};
use crate::routing::remote::config::ConnectionConfig;
use crate::routing::remote::net::ExternalConnections;
use crate::routing::remote::state::{DeferredResult, Event, RemoteConnections, RemoteTasksState};
use crate::routing::remote::table::HostAndPort;
use crate::routing::ws::WsConnections;
use crate::routing::{Route, RoutingAddr, ServerRouterFactory, TaggedEnvelope};
use std::fmt::{Display, Formatter};
use std::io;
use utilities::errors::Recoverable;

#[cfg(test)]
pub mod test_fixture;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionDropped {
    Closed,
    TimedOut(Duration),
    Failed(ConnectionError),
    AgentFailed,
    Unknown,
}

impl Display for ConnectionDropped {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionDropped::Closed => write!(f, "The connection was explicitly closed."),
            ConnectionDropped::TimedOut(t) => write!(f, "The connection timed out after {:?}.", t),
            ConnectionDropped::Failed(err) => write!(f, "The connection failed: '{}'", err),
            ConnectionDropped::AgentFailed => write!(f, "The agent failed."),
            ConnectionDropped::Unknown => write!(f, "The reason could not be determined."),
        }
    }
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

#[derive(Debug)]
pub struct RemoteConnectionsTask<External: ExternalConnections, Ws, Router, Sp> {
    external: External,
    listener: External::ListenerType,
    websockets: Ws,
    delegate_router: Router,
    stop_trigger: trigger::Receiver,
    spawner: Sp,
    configuration: ConnectionConfig,
}

type SocketAddrIt = std::vec::IntoIter<SocketAddr>;

const REQUEST_DROPPED: &str = "The receiver of a routing request was dropped before it completed.";
const FAILED_SERVER_CONN: &str = "Failed to establish a server connection.";
const FAILED_CLIENT_CONN: &str = "Failed to establish a client connection.";
const NOT_IN_TABLE: &str = "A connection closed that was not in the routing table.";
const CLOSED_NO_HANDLES: &str = "A connection closed with no handles remaining.";

impl<External, Ws, RouterFac, Sp> RemoteConnectionsTask<External, Ws, RouterFac, Sp>
where
    External: ExternalConnections,
    Ws: WsConnections<External::Socket> + Send + Sync + 'static,
    RouterFac: ServerRouterFactory + 'static,
    Sp: Spawner<BoxFuture<'static, (RoutingAddr, ConnectionDropped)>> + Send + Unpin,
{
    pub async fn new(
        configuration: ConnectionConfig,
        external: External,
        bind_addr: SocketAddr,
        websockets: Ws,
        delegate_router: RouterFac,
        stop_trigger: trigger::Receiver,
        spawner: Sp,
    ) -> io::Result<Self> {
        let listener = external.bind(bind_addr).await?;
        Ok(RemoteConnectionsTask {
            external,
            listener,
            websockets,
            delegate_router,
            stop_trigger,
            spawner,
            configuration,
        })
    }

    pub async fn run(self) -> Result<(), io::Error> {
        let RemoteConnectionsTask {
            external,
            listener,
            websockets,
            delegate_router,
            stop_trigger,
            spawner,
            configuration,
        } = self;

        let mut state = RemoteConnections::new(
            &websockets,
            configuration,
            spawner,
            external,
            listener,
            stop_trigger,
            delegate_router,
        );

        let mut overall_result = Ok(());

        while let Some(event) = state.select_next().await {
            update_state(&mut state, &mut overall_result, event);
        }
        overall_result
    }
}

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
                    ConnectionError::Websocket(WebSocketError::Url(host.into_string())),
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
            state.spawn_task(sock_addr, ws_stream, Some(&host));
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
            state.fail_connection(&host, ConnectionError::Socket(err.kind()));
        }
        Event::Deferred(DeferredResult::Dns {
            result: Ok(mut addrs),
            host,
        }) => {
            if let Some(sock_addr) = addrs.next() {
                if let Err(host) = state.check_socket_addr(host, sock_addr) {
                    state.defer_connect_and_handshake(host, sock_addr, addrs);
                }
            } else {
                state.fail_connection(&host, ConnectionError::Resolution);
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

enum BadUrl {
    MissingPort,
    NoHost,
}

fn unpack_url(url: &Url) -> Result<HostAndPort, BadUrl> {
    match (url.host_str(), url.port()) {
        (Some(host_str), Some(port)) => Ok(HostAndPort::new(host_str.to_owned(), port)),
        (Some(host_str), _) => {
            if let Some(port) = default_port(url.scheme()) {
                Ok(HostAndPort::new(host_str.to_owned(), port))
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
