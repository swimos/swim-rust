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

use crate::plane::error::Unresolvable;
use crate::routing::ws::WsConnections;
use crate::routing::{RoutingAddr, ServerRouter, TaggedEnvelope};
use either::Either;
use futures::future::BoxFuture;
use futures::select_biased;
use futures::stream::poll_fn;
use futures::StreamExt;
use futures_util::stream::FuturesUnordered;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::time::Duration;
use swim_common::request::Request;
use swim_common::ws::error::ConnectionError;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use url::Url;
use utilities::sync::trigger;
use utilities::task::Spawner;

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

type EndpointRequest = Request<Result<mpsc::Sender<TaggedEnvelope>, ConnectionError>>;
type ResolutionRequest = Request<Result<RoutingAddr, Unresolvable>>;

enum RoutingRequest {
    /// Get channel to route messages to a specified routing address.
    Endpoint {
        id: RoutingAddr,
        request: EndpointRequest,
    },
    /// Resolve the routing address for an host.
    Resolve {
        host: Url,
        request: ResolutionRequest,
    },
    QueryRemote {
        address: SocketAddr,
        request: ResolutionRequest,
    },
}

struct RemoteConnections<Ws, Router, Sp> {
    requests_tx: mpsc::Sender<RoutingRequest>,
    requests_rx: mpsc::Receiver<RoutingRequest>,
    listener: TcpListener,
    websockets: Ws,
    delegate_router: Router,
    stop_trigger: trigger::Receiver,
    spawner: Sp,
}

impl<Ws, Router, Sp> RemoteConnections<Ws, Router, Sp>
where
    Ws: WsConnections,
    Router: ServerRouter + Clone + Send + Sync + 'static,
    Sp: Spawner<BoxFuture<'static, Result<(), ConnectionDropped>>>,
{
    fn new(
        req_buffer_size: NonZeroUsize,
        listener: TcpListener,
        websockets: Ws,
        delegate_router: Router,
        stop_trigger: trigger::Receiver,
        spawner: Sp,
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
        }
    }

    async fn run(self) {
        let RemoteConnections {
            requests_tx,
            requests_rx,
            mut listener,
            websockets,
            delegate_router,
            stop_trigger,
            spawner,
        } = self;

        let mut listener = poll_fn(move |cx| listener.poll_accept(cx).map(Some)).fuse();
        let mut requests = requests_rx.take_until(stop_trigger.clone());

        let mut _open_sockets: HashMap<SocketAddr, RoutingAddr> = HashMap::new();
        let resolved_endpoints: HashMap<(String, u16), RoutingAddr> = HashMap::new();
        let endpoints: HashMap<RoutingAddr, mpsc::Sender<TaggedEnvelope>> = HashMap::new();

        let ws_handshakes = FuturesUnordered::new();

        let err = loop {
            let next: Option<Either<io::Result<(TcpStream, SocketAddr)>, RoutingRequest>> = select_biased! {
                incoming = listener.next() => incoming.map(Either::Left),
                request = requests.next() => request.map(Either::Right),
            };
            match next {
                Some(Either::Left(Ok((stream, peer_addr)))) => {
                    ws_handshakes.push(do_handshake(true, stream, &websockets, peer_addr));
                }
                Some(Either::Left(Err(conn_err))) => {
                    break Some(conn_err);
                }
                Some(Either::Right(RoutingRequest::Endpoint { .. })) => {}
                Some(Either::Right(RoutingRequest::Resolve { .. })) => {}
                Some(Either::Right(RoutingRequest::QueryRemote { .. })) => {}
                _ => {
                    break None;
                }
            }
        };
    }
}

async fn do_handshake<Ws>(
    server: bool,
    socket: TcpStream,
    websockets: &Ws,
    peer_addr: SocketAddr,
) -> (Result<Ws::StreamSink, ConnectionError>, SocketAddr)
where
    Ws: WsConnections,
{
    let stream = if server {
        websockets.accept_connection(socket).await
    } else {
        websockets.open_connection(socket).await
    };
    (stream, peer_addr)
}
