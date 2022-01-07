// Copyright 2015-2021 Swim Inc.
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

use futures::future::select;
use futures::future::BoxFuture;
use futures::future::Either;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use pin_utils::pin_mut;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use swim_model::Text;
use swim_runtime::error::{
    CloseError, ConnectionDropped, ConnectionError, ResolutionError, RouterError, Unresolvable,
};
use swim_runtime::remote::{AttachClientRequest, RawOutRoute, RemoteRoutingRequest};
use swim_runtime::routing::{
    ClientRoute, ClientRouteMonitor, CloseReceiver, Route, Router, RouterFactory, RoutingAddr,
    TaggedEnvelope, TaggedSender,
};
use swim_tracing::request::RequestExt;
use swim_utilities::future::request::Request;
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::trigger::promise;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, Level};
use url::Url;

use swim_warp::envelope::ResponseEnvelope;

#[cfg(test)]
pub(crate) mod tests;

/// The Router events are emitted by the connection streams of the router and indicate
/// messages or errors from the remote host.
#[derive(Debug, Clone, PartialEq)]
pub enum RouterEvent {
    // Incoming message from a remote host.
    Message(ResponseEnvelope),
    // There was an error in the connection. If a retry strategy exists this will trigger it.
    ConnectionClosed,
    /// The remote host is unreachable. This will not trigger the retry system.
    Unreachable(String),
    // The router is stopping.
    Stopping,
}

struct Entry {
    route: RawOutRoute,
    on_drop: Option<promise::Sender<ConnectionDropped>>,
}

impl Drop for Entry {
    fn drop(&mut self) {
        if let Some(on_drop) = self.on_drop.take() {
            let _ = on_drop.provide(ConnectionDropped::Closed);
        }
    }
}

struct ClientRoutingTable {
    local_client_routes: HashMap<RoutingAddr, Entry>,
    next_id: u32,
    channel_size: NonZeroUsize,
}

impl ClientRoutingTable {
    fn new(channel_size: NonZeroUsize) -> Self {
        ClientRoutingTable {
            local_client_routes: Default::default(),
            next_id: 1,
            channel_size,
        }
    }
}

impl ClientRoutingTable {
    fn new_route(
        &mut self,
    ) -> (
        RoutingAddr,
        mpsc::Receiver<TaggedEnvelope>,
        promise::Receiver<ConnectionDropped>,
    ) {
        let new_addr = self.new_addr();
        let ClientRoutingTable {
            local_client_routes,
            channel_size,
            ..
        } = self;
        let (tx, rx) = mpsc::channel(channel_size.get());
        let (drop_tx, drop_rx) = promise::promise();
        local_client_routes.insert(
            new_addr,
            Entry {
                route: RawOutRoute::new(tx, drop_rx.clone()),
                on_drop: Some(drop_tx),
            },
        );

        (new_addr, rx, drop_rx)
    }

    fn new_addr(&mut self) -> RoutingAddr {
        let ClientRoutingTable { next_id, .. } = self;
        let new_addr = RoutingAddr::client(*next_id);
        *next_id += 1;
        new_addr
    }
}

pub struct ClientEndpoint {
    endpoint_addr: RoutingAddr,
    receiver: mpsc::Receiver<TaggedEnvelope>,
    on_dropped: promise::Receiver<ConnectionDropped>,
    on_drop: ClientRouteMonitor,
}

pub enum ClientEndpointRequest {
    Get(RoutingAddr, Request<Result<RawOutRoute, Unresolvable>>),
    MakeRoutable(Request<ClientEndpoint>),
    MakeUnroutable(Request<RoutingAddr>),
}

pub struct ClientRouterTask {
    stop_trigger: CloseReceiver,
    requests: mpsc::Receiver<ClientEndpointRequest>,
    channel_size: NonZeroUsize,
    yield_after: NonZeroUsize,
}

const REQUEST_DROPPED: &str = "Client routing request dropped.";

impl ClientRouterTask {
    pub fn new(
        stop_trigger: CloseReceiver,
        requests: mpsc::Receiver<ClientEndpointRequest>,
        channel_size: NonZeroUsize,
        yield_after: NonZeroUsize,
    ) -> Self {
        ClientRouterTask {
            stop_trigger,
            requests,
            channel_size,
            yield_after,
        }
    }

    pub async fn run(self) {
        let ClientRouterTask {
            stop_trigger,
            requests,
            channel_size,
            yield_after,
        } = self;

        let mut iteration_count: usize = 0;
        let yield_mod = yield_after.get();

        let mut routing_table = ClientRoutingTable::new(channel_size);
        let endpoint_monitor = FuturesUnordered::new();
        pin_mut!(endpoint_monitor);

        let mut requests = ReceiverStream::new(requests).take_until(stop_trigger);

        loop {
            let next_event = if endpoint_monitor.is_empty() {
                Either::Left(requests.next().await)
            } else {
                match select(requests.next(), endpoint_monitor.next()).await {
                    Either::Left((maybe_req, _)) => Either::Left(maybe_req),
                    Either::Right((Some(closed_notification), _)) => {
                        Either::Right(closed_notification)
                    }
                    _ => unreachable!(), //Unreachable as we already checked that endpoint_monitior is non-empty.
                }
            };

            match next_event {
                Either::Left(Some(ClientEndpointRequest::Get(addr, request))) => {
                    let result = routing_table
                        .local_client_routes
                        .get(&addr)
                        .map(|Entry { route, .. }| route.clone())
                        .ok_or(Unresolvable(addr));
                    request.send_debug(result, REQUEST_DROPPED);
                }
                Either::Left(Some(ClientEndpointRequest::MakeUnroutable(request))) => {
                    let addr = routing_table.new_addr();
                    request.send_debug(addr, REQUEST_DROPPED);
                }
                Either::Left(Some(ClientEndpointRequest::MakeRoutable(request))) => {
                    let (endpoint_addr, receiver, on_dropped) = routing_table.new_route();
                    let (on_drop_tx, on_drop_rx) = promise::promise();
                    let endpoint = ClientEndpoint {
                        endpoint_addr,
                        receiver,
                        on_dropped,
                        on_drop: ClientRouteMonitor::new(on_drop_tx),
                    };
                    if request.send(endpoint).is_err() {
                        event!(Level::DEBUG, REQUEST_DROPPED);
                        routing_table.local_client_routes.remove(&endpoint_addr);
                    } else {
                        let fut = async move {
                            let _ = on_drop_rx.await;
                            endpoint_addr
                        };
                        endpoint_monitor.push(fut);
                    }
                }
                Either::Right(endpoint_addr) => {
                    routing_table.local_client_routes.remove(&endpoint_addr);
                }
                _ => {
                    break;
                }
            }

            iteration_count += 1;
            if iteration_count % yield_mod == 0 {
                tokio::task::yield_now().await;
            }
        }
    }
}

pub struct ClientRouter {
    tag: RoutingAddr,
    client: mpsc::Sender<ClientEndpointRequest>,
}

#[derive(Clone, Debug)]
pub struct ClientConnectionFactory<RF> {
    router_factory: RF,
    client: mpsc::Sender<ClientEndpointRequest>,
    remote: mpsc::Sender<RemoteRoutingRequest>,
}

impl ClientRouter {
    pub fn new(tag: RoutingAddr, client: mpsc::Sender<ClientEndpointRequest>) -> Self {
        ClientRouter { tag, client }
    }
}

impl<RF> ClientConnectionFactory<RF> {
    pub fn new(
        router_factory: RF,
        client: mpsc::Sender<ClientEndpointRequest>,
        remote: mpsc::Sender<RemoteRoutingRequest>,
    ) -> Self {
        ClientConnectionFactory {
            router_factory,
            client,
            remote,
        }
    }
}

impl Router for ClientRouter {
    fn resolve_sender(&mut self, addr: RoutingAddr) -> BoxFuture<Result<Route, ResolutionError>> {
        async move {
            if addr.is_client() {
                let (tx, rx) = oneshot::channel();
                if self
                    .client
                    .send(ClientEndpointRequest::Get(addr, Request::new(tx)))
                    .await
                    .is_err()
                {
                    Err(ResolutionError::router_dropped())
                } else {
                    match rx.await {
                        Ok(Ok(RawOutRoute { sender, on_drop })) => {
                            Ok(Route::new(TaggedSender::new(self.tag, sender), on_drop))
                        }
                        Ok(Err(Unresolvable(addr))) => Err(ResolutionError::Unresolvable(addr)),
                        Err(_) => Err(ResolutionError::router_dropped()),
                    }
                }
            } else {
                Err(ResolutionError::Unresolvable(addr))
            }
        }
        .boxed()
    }

    fn lookup(
        &mut self,
        _host: Option<Url>,
        _node_uri: RelativeUri,
    ) -> BoxFuture<Result<RoutingAddr, RouterError>> {
        futures::future::ready(Err(RouterError::ConnectionFailure(
            ConnectionError::Resolution("Client router cannot resolve endpoints.".to_string()),
        )))
        .boxed()
    }
}

impl<RF> ClientConnectionFactory<RF> {
    async fn new_unroutable(&mut self) -> Result<RoutingAddr, RouterError> {
        let (ep_addr_tx, ep_addr_rx) = oneshot::channel();
        if self
            .client
            .send(ClientEndpointRequest::MakeUnroutable(Request::new(
                ep_addr_tx,
            )))
            .await
            .is_err()
        {
            Err(RouterError::RouterDropped)
        } else {
            ep_addr_rx.await.map_err(|_| RouterError::RouterDropped)
        }
    }
}

impl<RF: RouterFactory> ClientConnectionFactory<RF> {
    pub async fn get_sender(
        &mut self,
        host: Option<Url>,
        node_uri: RelativeUri,
    ) -> Result<Route, RouterError> {
        let addr = self.router_factory.lookup(host, node_uri).await?;
        let endpoint_addr = self.new_unroutable().await?;
        let mut router = self.router_factory.create_for(endpoint_addr);
        match router.resolve_sender(addr).await {
            Ok(route) => Ok(route),
            Err(ResolutionError::RouterDropped) => Err(RouterError::RouterDropped),
            Err(ResolutionError::Unresolvable(_)) => Err(RouterError::ConnectionFailure(
                ConnectionError::Closed(CloseError::closed()),
            )),
        }
    }

    pub async fn create_endpoint(
        &mut self,
        host: Option<Url>,
        node_uri: RelativeUri,
        lane: Text,
    ) -> Result<ClientRoute, RouterError> {
        let addr = self.router_factory.lookup(host, node_uri.clone()).await?;
        if addr.is_remote() {
            let endpoint_addr = self.new_unroutable().await?;
            let (tx, rx) = oneshot::channel();
            let request = AttachClientRequest::new(addr, node_uri, lane, Request::new(tx));
            if self
                .remote
                .send(RemoteRoutingRequest::AttachClient { request })
                .await
                .is_err()
            {
                return Err(RouterError::RouterDropped);
            }
            match rx.await {
                Err(_) => Err(RouterError::RouterDropped),
                Ok(Err(_)) => Err(RouterError::ConnectionFailure(ConnectionError::Closed(
                    CloseError::closed(),
                ))),
                Ok(Ok(client_route)) => Ok(client_route.make_client(endpoint_addr)),
            }
        } else {
            let ClientConnectionFactory {
                router_factory,
                client,
                ..
            } = self;
            let (tx, rx) = oneshot::channel();
            let request = ClientEndpointRequest::MakeRoutable(Request::new(tx));
            if client.send(request).await.is_err() {
                return Err(RouterError::RouterDropped);
            }
            if let Ok(ClientEndpoint {
                endpoint_addr,
                receiver,
                on_dropped,
                on_drop,
            }) = rx.await
            {
                let mut router = router_factory.create_for(endpoint_addr);
                match router.resolve_sender(addr).await {
                    Ok(route) => {
                        let client_route =
                            ClientRoute::new(endpoint_addr, route, receiver, on_dropped, on_drop);
                        Ok(client_route)
                    }
                    Err(ResolutionError::RouterDropped) => Err(RouterError::RouterDropped),
                    _ => Err(RouterError::ConnectionFailure(ConnectionError::Closed(
                        CloseError::closed(),
                    ))),
                }
            } else {
                Err(RouterError::RouterDropped)
            }
        }
    }
}
