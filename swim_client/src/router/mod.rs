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

use crate::configuration::router::RouterParams;
use crate::connections::ConnectionChannel;
use crate::connections::ConnectionRegistrator;
use crate::connections::{ConnectionPool, ConnectionSender};
use either::Either;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{select_biased, FutureExt, StreamExt};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::num::NonZeroUsize;
use std::str::FromStr;
use swim_common::request::Request;
use swim_common::routing::error::{
    ConnectionError, ResolutionError, RouterError, RoutingError, Unresolvable,
};
use swim_common::routing::remote::table::SchemeHostPort;
use swim_common::routing::remote::{RawRoute, RemoteRoutingRequest};
use swim_common::routing::BidirectionalRoute;
use swim_common::routing::{CloseReceiver, ConnectionDropped, Origin, PlaneRoutingRequest};
use swim_common::routing::{
    Route, Router, RouterFactory, RoutingAddr, TaggedEnvelope, TaggedSender,
};
use swim_common::warp::envelope::{Envelope, IncomingLinkMessage};
use swim_common::warp::path::{AbsolutePath, Addressable, RelativePath};
use swim_runtime::task::*;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tracing::trace_span;
use tracing::{span, warn, Level};
use tracing_futures::Instrument;
use url::Url;
use utilities::errors::Recoverable;
use utilities::sync::promise;
use utilities::uri::RelativeUri;

#[cfg(test)]
mod tests;

//Todo dm
// mod incoming;
// mod outgoing;
// mod retry;

#[derive(Debug, Clone)]
pub(crate) struct TopLevelClientRouterFactory {
    client_sender: mpsc::Sender<ClientRoutingRequest<AbsolutePath>>,
    remote_sender: mpsc::Sender<RemoteRoutingRequest>,
}

impl TopLevelClientRouterFactory {
    pub(in crate) fn new(
        client_sender: mpsc::Sender<ClientRoutingRequest<AbsolutePath>>,
        remote_sender: mpsc::Sender<RemoteRoutingRequest>,
    ) -> Self {
        TopLevelClientRouterFactory {
            client_sender,
            remote_sender,
        }
    }
}

impl RouterFactory for TopLevelClientRouterFactory {
    type Router = TopLevelClientRouter;

    fn create_for(&self, addr: RoutingAddr) -> Self::Router {
        TopLevelClientRouter::new(addr, self.client_sender.clone(), self.remote_sender.clone())
    }
}

#[derive(Debug, Clone)]
pub struct TopLevelClientRouter {
    addr: RoutingAddr,
    client_sender: mpsc::Sender<ClientRoutingRequest<AbsolutePath>>,
    remote_sender: mpsc::Sender<RemoteRoutingRequest>,
}

impl TopLevelClientRouter {
    pub(crate) fn new(
        addr: RoutingAddr,
        client_sender: mpsc::Sender<ClientRoutingRequest<AbsolutePath>>,
        remote_sender: mpsc::Sender<RemoteRoutingRequest>,
    ) -> Self {
        TopLevelClientRouter {
            addr,
            client_sender,
            remote_sender,
        }
    }
}

impl Router for TopLevelClientRouter {
    fn resolve_sender(
        &mut self,
        addr: RoutingAddr,
    ) -> BoxFuture<'_, Result<Route, ResolutionError>> {
        async move {
            let TopLevelClientRouter {
                remote_sender,
                client_sender,
                addr: tag,
            } = self;

            if addr.is_remote() {
                let (tx, rx) = oneshot::channel();
                let request = Request::new(tx);
                if remote_sender
                    .send(RemoteRoutingRequest::Endpoint { addr, request })
                    .await
                    .is_err()
                {
                    Err(ResolutionError::router_dropped())
                } else {
                    match rx.await {
                        Ok(Ok(RawRoute { sender, on_drop })) => {
                            Ok(Route::new(TaggedSender::new(*tag, sender), on_drop))
                        }
                        Ok(Err(_)) => Err(ResolutionError::unresolvable(addr.to_string())),
                        Err(_) => Err(ResolutionError::router_dropped()),
                    }
                }
            } else {
                // Remote will always route through the bidirectional connection
                unimplemented!()
            }
        }
        .boxed()
    }

    fn resolve_bidirectional(
        &mut self,
        host: Url,
    ) -> BoxFuture<'_, Result<BidirectionalRoute, ResolutionError>> {
        async move {
            let TopLevelClientRouter { remote_sender, .. } = self;

            let (tx, rx) = oneshot::channel();
            if remote_sender
                .send(RemoteRoutingRequest::Bidirectional {
                    host: host.clone(),
                    request: Request::new(tx),
                })
                .await
                .is_err()
            {
                Err(ResolutionError::router_dropped())
            } else {
                match rx.await {
                    Ok(Ok(addr)) => Ok(addr),
                    Ok(Err(err)) => Err((ResolutionError::unresolvable(host.to_string()))),
                    Err(_) => Err(ResolutionError::router_dropped()),
                }
            }
        }
        .boxed()
    }

    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
    ) -> BoxFuture<'_, Result<RoutingAddr, RouterError>> {
        async move {
            let TopLevelClientRouter {
                addr,
                client_sender,
                remote_sender,
            } = self;

            match host {
                Some(host) => {
                    let (tx, rx) = oneshot::channel();
                    if remote_sender
                        .send(RemoteRoutingRequest::ResolveUrl {
                            host,
                            request: Request::new(tx),
                        })
                        .await
                        .is_err()
                    {
                        Err(RouterError::RouterDropped)
                    } else {
                        match rx.await {
                            Ok(Ok(addr)) => Ok(addr),
                            Ok(Err(err)) => Err(RouterError::ConnectionFailure(err)),
                            Err(_) => Err(RouterError::RouterDropped),
                        }
                    }
                }
                None => {
                    // Remote will always route through the bidirectional connection
                    unimplemented!()
                }
            }
        }
        .boxed()
    }
}

pub(crate) type Node = String;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum RoutingPath {
    Remote(SchemeHostPort),
    Local(Node),
}

impl<T: Addressable> From<T> for RoutingPath {
    fn from(path: T) -> RoutingPath {
        match path.host() {
            //Todo dm change this from unwrap
            Some(host) => {
                RoutingPath::Remote(SchemeHostPort::try_from(path.host().unwrap()).unwrap())
            }
            None => RoutingPath::Local(path.node().to_string()),
        }
    }
}

pub(crate) struct RoutingTable<Path: Addressable> {
    addresses: HashMap<RoutingPath, RoutingAddr>,
    endpoints: HashMap<RoutingAddr, ConnectionRegistrator<Path>>,
}

impl<Path: Addressable> RoutingTable<Path> {
    pub(crate) fn new() -> Self {
        RoutingTable {
            addresses: HashMap::new(),
            endpoints: HashMap::new(),
        }
    }

    pub(crate) fn try_resolve_addr(&self, routing_path: &RoutingPath) -> Option<RoutingAddr> {
        self.addresses.get(routing_path).copied()
    }

    pub(crate) fn try_resolve_endpoint(
        &self,
        routing_addr: &RoutingAddr,
    ) -> Option<ConnectionRegistrator<Path>> {
        self.endpoints.get(routing_addr).cloned()
    }

    pub(crate) fn add_registrator(
        &mut self,
        routing_path: RoutingPath,
        routing_addr: RoutingAddr,
        connection_registrator: ConnectionRegistrator<Path>,
    ) {
        self.addresses.insert(routing_path, routing_addr.clone());
        self.endpoints.insert(routing_addr, connection_registrator);
    }
}

#[derive(Debug, Clone)]
pub struct ClientRouterFactory<Path: Addressable, DelegateFac: RouterFactory> {
    client_request_sender: mpsc::Sender<ClientRoutingRequest<Path>>,
    delegate_fac: DelegateFac,
}

impl<Path: Addressable, DelegateFac: RouterFactory> ClientRouterFactory<Path, DelegateFac> {
    pub fn new(
        request_sender: mpsc::Sender<ClientRoutingRequest<Path>>,
        delegate_fac: DelegateFac,
    ) -> Self {
        ClientRouterFactory {
            client_request_sender: request_sender,
            delegate_fac,
        }
    }
}

impl<Path: Addressable, DelegateFac: RouterFactory> RouterFactory
    for ClientRouterFactory<Path, DelegateFac>
{
    type Router = ClientRouter<Path, DelegateFac::Router>;

    fn create_for(&self, addr: RoutingAddr) -> Self::Router {
        ClientRouter {
            tag: addr,
            request_sender: self.client_request_sender.clone(),
            delegate_router: self.delegate_fac.create_for(addr),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClientRouter<Path: Addressable, DelegateRouter: Router> {
    tag: RoutingAddr,
    request_sender: mpsc::Sender<ClientRoutingRequest<Path>>,
    delegate_router: DelegateRouter,
}

impl<Path: Addressable, DelegateRouter: Router> Router for ClientRouter<Path, DelegateRouter> {
    fn resolve_sender(
        &mut self,
        addr: RoutingAddr,
    ) -> BoxFuture<'_, Result<Route, ResolutionError>> {
        async move {
            let ClientRouter {
                tag,
                request_sender,
                delegate_router,
            } = self;

            delegate_router.resolve_sender(addr).await
        }
        .boxed()
    }

    fn resolve_bidirectional(
        &mut self,
        host: Url,
    ) -> BoxFuture<Result<BidirectionalRoute, ResolutionError>> {
        async move {
            let ClientRouter {
                tag,
                request_sender,
                delegate_router,
            } = self;

            delegate_router.resolve_bidirectional(host).await
        }
        .boxed()
    }

    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
    ) -> BoxFuture<'_, Result<RoutingAddr, RouterError>> {
        async move {
            let ClientRouter {
                delegate_router, ..
            } = self;

            delegate_router.lookup(host, route).await
        }
        .boxed()
    }
}

#[derive(Debug)]
pub enum ClientRoutingRequest<Path: Addressable> {
    /// Obtain a connection.
    Connect {
        target: Path,
        request: Request<Result<ConnectionChannel, ConnectionError>>,
    },
    /// Get channel to route messages to a specified routing address.
    Endpoint {
        addr: RoutingAddr,
        request: Request<Result<RawRoute, Unresolvable>>,
    },
}

// pub struct ClientConnectionsManager<Path: Addressable> {
//     router_request_rx: mpsc::Receiver<ClientRequest<Path>>,
//     remote_router_tx: mpsc::Sender<RemoteRoutingRequest>,
//     plane_router_tx: Option<mpsc::Sender<PlaneRoutingRequest>>,
//     buffer_size: NonZeroUsize,
//     close_rx: CloseReceiver,
// }
//
// type SubscriptionRequestSender = mpsc::Sender<(RawRoute, SubscriptionRequest)>;
//
// impl<Path: Addressable> ClientConnectionsManager<Path> {
//     pub fn new(
//         router_request_rx: mpsc::Receiver<ClientRequest<Path>>,
//         remote_router_tx: mpsc::Sender<RemoteRoutingRequest>,
//         plane_router_tx: Option<mpsc::Sender<PlaneRoutingRequest>>,
//         buffer_size: NonZeroUsize,
//         close_rx: CloseReceiver,
//     ) -> ClientConnectionsManager<Path> {
//         ClientConnectionsManager {
//             router_request_rx,
//             remote_router_tx,
//             plane_router_tx,
//             buffer_size,
//             close_rx,
//         }
//     }
//
//     pub async fn run(self) -> Result<(), ConnectionError> {
//         let ClientConnectionsManager {
//             router_request_rx,
//             remote_router_tx,
//             plane_router_tx,
//             buffer_size,
//             close_rx,
//         } = self;
//
//         let mut outgoing_managers: HashMap<
//             String,
//             (mpsc::Sender<TaggedEnvelope>, SubscriptionRequestSender),
//         > = HashMap::new();
//
//         let mut router_request_rx = ReceiverStream::new(router_request_rx).fuse();
//         let futures = FuturesUnordered::new();
//         let mut close_txs = Vec::new();
//         let mut close_trigger = close_rx.clone().fuse();
//         loop {
//             let next: Option<ClientRequest<Path>> = select_biased! {
//                 request = router_request_rx.next() => request,
//                 _stop = close_trigger => None,
//             };
//
//             match next {
//                 Some(ClientRequest::Connect { request, origin }) => {
//                     let (sender, _) = outgoing_managers
//                         .entry(origin.get_manager_key()?)
//                         .or_insert_with(|| {
//                             let (manager, sender, sub_tx) =
//                                 ClientConnectionManager::new(buffer_size, close_rx.clone());
//
//                             let handle = spawn(manager.run());
//                             futures.push(handle);
//                             (sender, sub_tx)
//                         })
//                         .clone();
//
//                     let (on_drop_tx, on_drop_rx) = promise::promise();
//                     close_txs.push(on_drop_tx);
//
//                     if request.send(Ok(RawRoute::new(sender, on_drop_rx))).is_err() {
//                         break Err(ConnectionError::Resolution(
//                             ResolutionError::router_dropped(),
//                         ));
//                     }
//                 }
//                 Some(ClientRequest::Subscribe {
//                     target,
//                     request: sub_req,
//                 }) => match target.host() {
//                     None => {
//                         let node = target.node();
//
//                         let (tx, rx) = oneshot::channel();
//                         let plane_tx = plane_router_tx.as_ref().unwrap();
//
//                         plane_tx
//                             .send(PlaneRoutingRequest::Resolve {
//                                 host: None,
//                                 name: RelativeUri::from_str(node.as_str()).unwrap(),
//                                 request: Request::new(tx),
//                             })
//                             .await
//                             .unwrap();
//
//                         let routing_addr = rx.await.unwrap().unwrap();
//
//                         let (tx, rx) = oneshot::channel();
//                         plane_tx
//                             .send(PlaneRoutingRequest::Endpoint {
//                                 id: routing_addr,
//                                 request: Request::new(tx),
//                             })
//                             .await
//                             .unwrap();
//
//                         let raw_route = rx.await.unwrap().unwrap();
//
//                         let (_, sub_sender) = outgoing_managers
//                             .entry(node.to_string())
//                             .or_insert_with(|| {
//                                 let (manager, sender, sub_tx) =
//                                     ClientConnectionManager::new(buffer_size, close_rx.clone());
//
//                                 let handle = spawn(manager.run());
//                                 futures.push(handle);
//                                 (sender, sub_tx)
//                             });
//
//                         if sub_sender.send((raw_route, sub_req)).await.is_err() {
//                             break Err(ConnectionError::Resolution(
//                                 ResolutionError::router_dropped(),
//                             ));
//                         }
//                     }
//                     Some(host) => {
//                         let (tx, rx) = oneshot::channel();
//
//                         if remote_router_tx
//                             .send(RemoteRoutingRequest::ResolveUrl {
//                                 host: host.clone(),
//                                 request: Request::new(tx),
//                             })
//                             .await
//                             .is_err()
//                         {
//                             if sub_req
//                                 .send(Err(ConnectionError::Resolution(
//                                     ResolutionError::router_dropped(),
//                                 )))
//                                 .is_err()
//                             {
//                                 break Err(ConnectionError::Resolution(
//                                     ResolutionError::router_dropped(),
//                                 ));
//                             }
//                             continue;
//                         }
//
//                         let routing_addr = match rx.await {
//                             Ok(result) => match result {
//                                 Ok(routing_addr) => routing_addr,
//                                 Err(err) => {
//                                     if sub_req.send(Err(err)).is_err() {
//                                         break Err(ConnectionError::Resolution(
//                                             ResolutionError::router_dropped(),
//                                         ));
//                                     }
//                                     continue;
//                                 }
//                             },
//                             Err(_) => {
//                                 if sub_req
//                                     .send(Err(ConnectionError::Resolution(
//                                         ResolutionError::router_dropped(),
//                                     )))
//                                     .is_err()
//                                 {
//                                     break Err(ConnectionError::Resolution(
//                                         ResolutionError::router_dropped(),
//                                     ));
//                                 }
//                                 continue;
//                             }
//                         };
//
//                         let (tx, rx) = oneshot::channel();
//                         if remote_router_tx
//                             .send(RemoteRoutingRequest::Endpoint {
//                                 addr: routing_addr,
//                                 request: Request::new(tx),
//                             })
//                             .await
//                             .is_err()
//                         {
//                             if sub_req
//                                 .send(Err(ConnectionError::Resolution(
//                                     ResolutionError::router_dropped(),
//                                 )))
//                                 .is_err()
//                             {
//                                 break Err(ConnectionError::Resolution(
//                                     ResolutionError::router_dropped(),
//                                 ));
//                             }
//                             continue;
//                         }
//
//                         let raw_route = match rx.await {
//                             Ok(result) => match result {
//                                 Ok(raw_route) => raw_route,
//                                 Err(_) => {
//                                     if sub_req
//                                         .send(Err(ConnectionError::Resolution(
//                                             ResolutionError::unresolvable(host.to_string()),
//                                         )))
//                                         .is_err()
//                                     {
//                                         break Err(ConnectionError::Resolution(
//                                             ResolutionError::router_dropped(),
//                                         ));
//                                     }
//                                     continue;
//                                 }
//                             },
//                             Err(_) => {
//                                 if sub_req
//                                     .send(Err(ConnectionError::Resolution(
//                                         ResolutionError::router_dropped(),
//                                     )))
//                                     .is_err()
//                                 {
//                                     break Err(ConnectionError::Resolution(
//                                         ResolutionError::router_dropped(),
//                                     ));
//                                 }
//                                 continue;
//                             }
//                         };
//
//                         let (_, sub_sender) = outgoing_managers
//                             .entry(host.to_string())
//                             .or_insert_with(|| {
//                                 let (manager, sender, sub_tx) =
//                                     ClientConnectionManager::new(buffer_size, close_rx.clone());
//
//                                 let handle = spawn(manager.run());
//                                 futures.push(handle);
//                                 (sender, sub_tx)
//                             });
//
//                         if sub_sender.send((raw_route, sub_req)).await.is_err() {
//                             break Err(ConnectionError::Resolution(
//                                 ResolutionError::router_dropped(),
//                             ));
//                         }
//                     }
//                 },
//                 _ => {
//                     close_txs.into_iter().for_each(|trigger| {
//                         if let Err(err) = trigger.provide(ConnectionDropped::Closed) {
//                             tracing::error!("{:?}", err);
//                         }
//                     });
//
//                     for result in futures.collect::<Vec<_>>().await {
//                         match result {
//                             Ok(res) => {
//                                 if let Err(err) = res {
//                                     tracing::error!("{:?}", err);
//                                 }
//                             }
//                             Err(err) => {
//                                 tracing::error!("{:?}", err);
//                             }
//                         }
//                     }
//
//                     break Ok(());
//                 }
//             }
//         }
//     }
// }
//
// pub(crate) struct ClientConnectionManager {
//     envelope_rx: mpsc::Receiver<TaggedEnvelope>,
//     sub_rx: mpsc::Receiver<(RawRoute, SubscriptionRequest)>,
//     buffer_size: NonZeroUsize,
//     close_rx: CloseReceiver,
// }
//
// impl ClientConnectionManager {
//     pub(crate) fn new(
//         buffer_size: NonZeroUsize,
//         close_rx: CloseReceiver,
//     ) -> (
//         ClientConnectionManager,
//         mpsc::Sender<TaggedEnvelope>,
//         mpsc::Sender<(RawRoute, SubscriptionRequest)>,
//     ) {
//         let (envelope_tx, envelope_rx) = mpsc::channel(buffer_size.get());
//         let (sub_tx, sub_rx) = mpsc::channel(buffer_size.get());
//         (
//             ClientConnectionManager {
//                 envelope_rx,
//                 sub_rx,
//                 buffer_size,
//                 close_rx,
//             },
//             envelope_tx,
//             sub_tx,
//         )
//     }
//
//     pub(crate) async fn run(self) -> Result<(), RoutingError> {
//         let ClientConnectionManager {
//             envelope_rx,
//             sub_rx,
//             buffer_size,
//             close_rx,
//         } = self;
//
//         let mut envelope_rx = ReceiverStream::new(envelope_rx).fuse();
//         let mut sub_rx = ReceiverStream::new(sub_rx).fuse();
//
//         let mut subs: Vec<mpsc::Sender<Envelope>> = Vec::new();
//         let mut close_trigger = close_rx.clone().fuse();
//
//         loop {
//             let next: Option<Either<TaggedEnvelope, (RawRoute, SubscriptionRequest)>> = select_biased! {
//                 envelope = envelope_rx.next() => envelope.map(Either::Left),
//                 sub = sub_rx.next() => sub.map(Either::Right),
//                 _stop = close_trigger => None,
//             };
//
//             match next {
//                 Some(Either::Left(envelope)) => match envelope.1.clone().into_incoming() {
//                     Ok(_) => {
//                         broadcast(&mut subs, envelope.1).await?;
//                     }
//                     Err(env) => {
//                         warn!("Unsupported message: {:?}", env);
//                     }
//                 },
//                 Some(Either::Right((raw_route, sub_request))) => {
//                     let (subscriber_tx, subscriber_rx) = mpsc::channel(buffer_size.get());
//                     subs.push(subscriber_tx);
//                     if sub_request.send(Ok((raw_route, subscriber_rx))).is_err() {
//                         break Err(RoutingError::RouterDropped);
//                     };
//                 }
//                 _ => break Ok(()),
//             }
//         }
//     }
// }
//
// pub(crate) enum ConnectionRequestMode {
//     Full(oneshot::Sender<ConnectionChannel>),
//     Outgoing(oneshot::Sender<mpsc::Sender<Envelope>>),
// }
//
// pub(crate) type RouterConnRequest<Path> = (Path, ConnectionRequestMode);
// type ConnectionChannel = (mpsc::Sender<Envelope>, mpsc::Receiver<RouterEvent>);
// type CloseResponseSender = mpsc::Sender<Result<(), RoutingError>>;
// type SubscriptionRequest = Request<Result<(RawRoute, mpsc::Receiver<Envelope>), ConnectionError>>;
//
// /// The Router events are emitted by the connection streams of the router and indicate
// /// messages or errors from the remote host.
// #[derive(Debug, Clone, PartialEq)]
// pub enum RouterEvent {
//     // Incoming message from a remote host.
//     Message(IncomingLinkMessage),
//     // There was an error in the connection. If a retry strategy exists this will trigger it.
//     ConnectionClosed,
//     /// The remote host is unreachable. This will not trigger the retry system.
//     Unreachable(String),
//     // The router is stopping.
//     Stopping,
// }
//
// /// Tasks that the router can handle.
// enum RouterTask<Path: Addressable> {
//     Connect(RouterConnRequest<Path>),
//     Close(Option<CloseResponseSender>),
// }
//
// type HostManagerHandle = (
//     mpsc::Sender<Envelope>,
//     mpsc::Sender<SubscriberRequest>,
//     TaskHandle<Result<(), RoutingError>>,
// );
//
// /// The task manager is the main task in the router. It is responsible for creating sub-tasks
// /// for each unique remote host. It can also handle direct messages by sending them directly
// /// to the appropriate sub-task.
// pub struct TaskManager<Pool: ConnectionPool<PathType = Path>, Path: Addressable> {
//     conn_request_rx: mpsc::Receiver<RouterConnRequest<Path>>,
//     connection_pool: Pool,
//     close_rx: CloseReceiver,
//     config: RouterParams,
// }
//
// impl<Pool: ConnectionPool<PathType = Path>, Path: Addressable> TaskManager<Pool, Path> {
//     pub(crate) fn new(
//         connection_pool: Pool,
//         close_rx: CloseReceiver,
//         config: RouterParams,
//     ) -> (Self, mpsc::Sender<RouterConnRequest<Path>>) {
//         let (conn_request_tx, conn_request_rx) = mpsc::channel(config.buffer_size().get());
//         (
//             TaskManager {
//                 conn_request_rx,
//                 connection_pool,
//                 close_rx,
//                 config,
//             },
//             conn_request_tx,
//         )
//     }
//
//     pub async fn run(self) -> Result<(), RoutingError> {
//         let TaskManager {
//             conn_request_rx,
//             connection_pool,
//             close_rx,
//             config,
//         } = self;
//
//         let mut conn_request_rx = ReceiverStream::new(conn_request_rx).fuse();
//         let mut close_trigger = close_rx.clone().fuse();
//
//         let mut host_managers: HashMap<String, HostManagerHandle> = HashMap::new();
//
//         loop {
//             let task = select_biased! {
//                 closed = &mut close_trigger => {
//                     match closed {
//                         Ok(tx) => Some(RouterTask::Close(Some((*tx).clone()))),
//                         _ => Some(RouterTask::Close(None)),
//                     }
//                 },
//                 maybe_req = conn_request_rx.next() => maybe_req.map(RouterTask::Connect),
//             }
//             .ok_or(RoutingError::ConnectionError)?;
//
//             match task {
//                 RouterTask::Connect((target, mode)) => match mode {
//                     ConnectionRequestMode::Full(response_tx) => {
//                         let (sink, stream_registrator, _) = get_host_manager(
//                             &mut host_managers,
//                             target.clone(),
//                             connection_pool.clone(),
//                             close_rx.clone(),
//                             config,
//                         );
//
//                         let (subscriber_tx, stream) = mpsc::channel(config.buffer_size().get());
//
//                         let relative_path = target.relative_path();
//
//                         stream_registrator
//                             .send(SubscriberRequest::new(relative_path, subscriber_tx))
//                             .await
//                             .map_err(|_| RoutingError::ConnectionError)?;
//
//                         response_tx
//                             .send((sink.clone(), stream))
//                             .map_err(|_| RoutingError::ConnectionError)?;
//                     }
//                     ConnectionRequestMode::Outgoing(response_tx) => {
//                         let (sink, _, _) = get_host_manager(
//                             &mut host_managers,
//                             target.clone(),
//                             connection_pool.clone(),
//                             close_rx.clone(),
//                             config,
//                         );
//
//                         response_tx
//                             .send(sink.clone())
//                             .map_err(|_| RoutingError::ConnectionError)?;
//                     }
//                 },
//
//                 RouterTask::Close(close_rx) => {
//                     if let Some(close_response_tx) = close_rx {
//                         let futures = FuturesUnordered::new();
//
//                         host_managers
//                             .iter_mut()
//                             .for_each(|(_, (_, _, handle))| futures.push(handle));
//
//                         for result in futures.collect::<Vec<_>>().await {
//                             close_response_tx
//                                 .send(result.unwrap_or(Err(RoutingError::CloseError)))
//                                 .await
//                                 .map_err(|_| RoutingError::CloseError)?;
//                         }
//
//                         break Ok(());
//                     }
//                 }
//             }
//         }
//     }
// }
//
// fn get_host_manager<Pool, Path>(
//     host_managers: &mut HashMap<String, HostManagerHandle>,
//     target: Path,
//     connection_pool: Pool,
//     close_rx: CloseReceiver,
//     config: RouterParams,
// ) -> &mut HostManagerHandle
// where
//     Pool: ConnectionPool<PathType = Path>,
//     Path: Addressable,
// {
//     host_managers.entry(target.to_string()).or_insert_with(|| {
//         let (host_manager, sink, stream_registrator) =
//             HostManager::new(target, connection_pool, close_rx, config);
//         (
//             sink,
//             stream_registrator,
//             spawn(
//                 host_manager
//                     .run()
//                     .instrument(trace_span!(HOST_MANAGER_TASK_NAME)),
//             ),
//         )
//     })
// }
//
// /// A connection request is used by the [`OutgoingHostTask`] to request a connection when
// /// it is trying to send a message.
// pub(crate) struct ConnectionRequest {
//     request_tx: oneshot::Sender<Result<ConnectionSender, RoutingError>>,
//     //If the connection should be recreated or returned from cache.
//     recreate: bool,
// }
//
// impl ConnectionRequest {
//     fn new(
//         request_tx: oneshot::Sender<Result<ConnectionSender, RoutingError>>,
//         recreate: bool,
//     ) -> Self {
//         ConnectionRequest {
//             request_tx,
//             recreate,
//         }
//     }
// }
//
// /// A subscriber request is sent to the [`IncomingHostTask`] to request for a new subscriber
// /// to receive all new messages for the given path.
// #[derive(Debug)]
// pub(crate) struct SubscriberRequest {
//     path: RelativePath,
//     subscriber_tx: mpsc::Sender<RouterEvent>,
// }
//
// impl SubscriberRequest {
//     fn new(path: RelativePath, subscriber_tx: mpsc::Sender<RouterEvent>) -> Self {
//         SubscriberRequest {
//             path,
//             subscriber_tx,
//         }
//     }
// }
//
// const INCOMING_TASK_NAME: &str = "incoming";
// const OUTGOING_TASK_NAME: &str = "outgoing";
// const HOST_MANAGER_TASK_NAME: &str = "host manager";
//
// /// Tasks that the host manager can handle.
// enum HostTask {
//     Connect(ConnectionRequest),
//     Subscribe(SubscriberRequest),
//     Close(Option<CloseResponseSender>),
// }
//
// /// The host manager is responsible for routing messages to a single host only.
// /// All host managers are sub-tasks of the task manager. The host manager is responsible for
// /// obtaining connections from the connection pool when needed and for registering new subscribers
// /// for the given host.
// ///
// /// Note: The host manager *DOES NOT* open connections by default when created.
// /// It will only open connections when required.
// struct HostManager<Pool: ConnectionPool<PathType = Path>, Path: Addressable> {
//     target: Path,
//     connection_pool: Pool,
//     sink_rx: mpsc::Receiver<Envelope>,
//     stream_registrator_rx: mpsc::Receiver<SubscriberRequest>,
//     close_rx: CloseReceiver,
//     config: RouterParams,
// }
//
// impl<Pool: ConnectionPool<PathType = Path>, Path: Addressable> HostManager<Pool, Path> {
//     fn new(
//         target: Path,
//         connection_pool: Pool,
//         close_rx: CloseReceiver,
//         config: RouterParams,
//     ) -> (
//         HostManager<Pool, Path>,
//         mpsc::Sender<Envelope>,
//         mpsc::Sender<SubscriberRequest>,
//     ) {
//         let (sink_tx, sink_rx) = mpsc::channel(config.buffer_size().get());
//         let (stream_registrator_tx, stream_registrator_rx) =
//             mpsc::channel(config.buffer_size().get());
//
//         (
//             HostManager {
//                 target,
//                 connection_pool,
//                 sink_rx,
//                 stream_registrator_rx,
//                 close_rx,
//                 config,
//             },
//             sink_tx,
//             stream_registrator_tx,
//         )
//     }
//
//     async fn run(self) -> Result<(), RoutingError> {
//         let HostManager {
//             target,
//             mut connection_pool,
//             sink_rx,
//             stream_registrator_rx,
//             close_rx,
//             config,
//         } = self;
//
//         let (connection_request_tx, connection_request_rx) =
//             mpsc::channel(config.buffer_size().get());
//
//         let (incoming_task, incoming_task_tx) =
//             IncomingHostTask::new(close_rx.clone(), config.buffer_size().get());
//         let outgoing_task =
//             OutgoingHostTask::new(sink_rx, connection_request_tx, close_rx.clone(), config);
//
//         let incoming_handle = spawn(
//             incoming_task
//                 .run()
//                 .instrument(span!(Level::TRACE, INCOMING_TASK_NAME)),
//         );
//         let outgoing_handle = spawn(
//             outgoing_task
//                 .run()
//                 .instrument(span!(Level::TRACE, OUTGOING_TASK_NAME)),
//         );
//
//         let mut close_trigger = close_rx.fuse();
//         let mut connection_request_rx = ReceiverStream::new(connection_request_rx).fuse();
//         let mut stream_registrator_rx = ReceiverStream::new(stream_registrator_rx).fuse();
//
//         loop {
//             let task = select_biased! {
//                 closed = &mut close_trigger => {
//                     match closed {
//                         Ok(tx) => Some(HostTask::Close(Some((*tx).clone()))),
//                         _ => Some(HostTask::Close(None)),
//                     }
//                 },
//                 maybe_req = connection_request_rx.next() => maybe_req.map(HostTask::Connect),
//                 maybe_reg = stream_registrator_rx.next() => maybe_reg.map(HostTask::Subscribe),
//             }
//             .ok_or(RoutingError::ConnectionError)?;
//
//             match task {
//                 HostTask::Connect(ConnectionRequest {
//                     request_tx: connection_response_tx,
//                     recreate,
//                 }) => {
//                     let conn_target = target.clone();
//
//                     let maybe_connection_channel = connection_pool
//                         .request_connection(conn_target, recreate)
//                         .await
//                         .map_err(|_| RoutingError::ConnectionError)?;
//
//                     match maybe_connection_channel {
//                         Ok((connection_tx, maybe_connection_rx)) => {
//                             connection_response_tx
//                                 .send(Ok(connection_tx))
//                                 .map_err(|_| RoutingError::ConnectionError)?;
//
//                             if let Some(connection_rx) = maybe_connection_rx {
//                                 incoming_task_tx
//                                     .send(IncomingRequest::Connection(connection_rx))
//                                     .await
//                                     .map_err(|_| RoutingError::ConnectionError)?;
//                             }
//                         }
//                         Err(connection_error) => match connection_error {
//                             e if e.is_transient() => {
//                                 let _ =
//                                     connection_response_tx.send(Err(RoutingError::PoolError(e)));
//                             }
//                             e => {
//                                 let _ =
//                                     connection_response_tx.send(Err(RoutingError::ConnectionError));
//                                 let msg = format!("{}", e);
//                                 let _ = incoming_task_tx
//                                     .send(IncomingRequest::Unreachable(msg.to_string()))
//                                     .await;
//                             }
//                         },
//                     }
//                 }
//                 HostTask::Subscribe(SubscriberRequest {
//                     path: relative_path,
//                     subscriber_tx: event_tx,
//                 }) => {
//                     incoming_task_tx
//                         .send(IncomingRequest::Subscribe(SubscriberRequest::new(
//                             relative_path,
//                             event_tx,
//                         )))
//                         .await
//                         .map_err(|_| RoutingError::ConnectionError)?;
//                 }
//                 HostTask::Close(close_rx) => {
//                     if let Some(close_response_tx) = close_rx {
//                         let futures = FuturesUnordered::new();
//
//                         futures.push(incoming_handle);
//                         futures.push(outgoing_handle);
//
//                         for result in futures.collect::<Vec<_>>().await {
//                             close_response_tx
//                                 .send(result.unwrap_or(Err(RoutingError::CloseError)))
//                                 .await
//                                 .map_err(|_| RoutingError::CloseError)?;
//                         }
//
//                         break Ok(());
//                     }
//                 }
//             }
//         }
//     }
// }
