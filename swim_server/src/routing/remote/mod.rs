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
mod pending;
mod router;
mod state;
mod table;
mod task;

use std::net::SocketAddr;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::poll_fn;
use futures::StreamExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{event, Level};
use url::Url;

use swim_common::request::Request;
use swim_common::ws::error::{ConnectionError, WebSocketError};
use utilities::sync::trigger;
use utilities::task::Spawner;

use crate::plane::error::Unresolvable;
use crate::routing::remote::config::ConnectionConfig;
use crate::routing::remote::state::{DeferredResult, Event, RemoteConnections};
use crate::routing::remote::table::HostAndPort;
use crate::routing::ws::WsConnections;
use crate::routing::{Route, RoutingAddr, ServerRouter, TaggedEnvelope};
use std::io;

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
pub struct RemoteConnectionsTask<Ws, Router, Sp> {
    listener: TcpListener,
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

impl<Ws, Router, Sp> RemoteConnectionsTask<Ws, Router, Sp>
where
    Ws: WsConnections + Send + Sync + 'static,
    Router: ServerRouter + Clone + Send + Sync + 'static,
    Sp: Spawner<BoxFuture<'static, (RoutingAddr, ConnectionDropped)>> + Send + Unpin,
{
    pub fn new(
        configuration: ConnectionConfig,
        listener: TcpListener,
        websockets: Ws,
        delegate_router: Router,
        stop_trigger: trigger::Receiver,
        spawner: Sp,
    ) -> Self {
        RemoteConnectionsTask {
            listener,
            websockets,
            delegate_router,
            stop_trigger,
            spawner,
            configuration,
        }
    }

    pub async fn run(self) -> Result<(), io::Error> {
        let RemoteConnectionsTask {
            mut listener,
            websockets,
            delegate_router: _,
            stop_trigger,
            spawner,
            configuration,
        } = self;

        let listener = poll_fn(move |cx| listener.poll_accept(cx).map(Some)).fuse();

        let mut state =
            RemoteConnections::new(&websockets, configuration, spawner, listener, stop_trigger);

        let mut overall_result = Ok(());

        loop {
            let next = state.select_next().await;
            match next {
                Some(Event::Incoming(Ok((stream, peer_addr)))) => {
                    state.defer_handshake(stream, peer_addr);
                }
                Some(Event::Incoming(Err(conn_err))) => {
                    overall_result = Err(conn_err);
                    state.stop();
                }
                Some(Event::Request(RoutingRequest::Endpoint { addr, request })) => {
                    let result = if let Some(tx) = state.table().resolve(addr) {
                        Ok(tx)
                    } else {
                        Err(Unresolvable(addr))
                    };
                    request.send_debug(result, REQUEST_DROPPED);
                }
                Some(Event::Request(RoutingRequest::ResolveUrl { host, request })) => {
                    match unpack_url(&host) {
                        Ok(target) => {
                            if let Some(addr) = state.table().try_resolve(&target) {
                                request.send_ok_debug(addr, REQUEST_DROPPED);
                            } else {
                                state.defer_dns_lookup(target, request);
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
                    state.spawn_task(sock_addr, ws_stream, None);
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
                    state.spawn_task(sock_addr, ws_stream, Some(&host));
                }
                Some(Event::Deferred(DeferredResult::ClientHandshake {
                    result: Err(error),
                    host,
                    ..
                })) => {
                    event!(Level::ERROR, FAILED_CLIENT_CONN, ?error);
                    state.fail_connection(&host, error);
                }
                Some(Event::Deferred(DeferredResult::FailedConnection {
                    error,
                    mut remaining,
                    host,
                })) => {
                    if let Some(sock_addr) = remaining.next() {
                        state.defer_connect_and_handshake(host, sock_addr, remaining);
                    } else {
                        state.fail_connection(&host, error);
                    }
                }
                Some(Event::Deferred(DeferredResult::Dns {
                    result: Err(_err),
                    host,
                    ..
                })) => {
                    state.fail_connection(&host, ConnectionError::ConnectError);
                }
                Some(Event::Deferred(DeferredResult::Dns {
                    result: Ok(mut addrs),
                    host,
                })) => {
                    if let Some(sock_addr) = addrs.next() {
                        state.defer_connect_and_handshake(host, sock_addr, addrs);
                    } else {
                        state.fail_connection(&host, ConnectionError::ConnectError);
                    }
                }
                Some(Event::ConnectionClosed(addr, reason)) => {
                    if let Some(tx) = state.table_mut().remove(addr) {
                        if let Err(reason) = tx.provide(reason) {
                            event!(Level::TRACE, CLOSED_NO_HANDLES, ?addr, ?reason);
                        }
                    } else {
                        event!(Level::ERROR, NOT_IN_TABLE, ?addr);
                    }
                }
                _ => {
                    break;
                }
            }
        }
        overall_result
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
