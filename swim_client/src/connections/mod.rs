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

use crate::configuration::router::ConnectionPoolParams;
use crate::router::{
    AddressableWrapper, ClientRouter, ClientRouterFactory, DownlinkRoutingRequest, RouterEvent,
    RoutingPath, RoutingTable,
};
use futures::future::BoxFuture;
use futures::select;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use slab::Slab;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::sync::Arc;
use swim_common::request::request_future::RequestError;
use swim_common::request::Request;
use swim_common::routing::error::{
    CloseError, ConnectionError, ResolutionError, RouterError, Unresolvable,
};
use swim_common::routing::remote::RawRoute;
use swim_common::routing::{
    BidirectionalRoute, CloseReceiver, ConnectionDropped, Route, Router, RouterFactory,
    RoutingAddr, TaggedEnvelope, TaggedSender,
};
use swim_common::warp::path::{Addressable, RelativePath};
use swim_runtime::task::*;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tracing::instrument;
use tracing::{event, Level};
use url::Url;
use utilities::sync::promise;
use utilities::uri::RelativeUri;

#[cfg(test)]
mod tests;

/// Connection pool is responsible for managing the opening and closing of connections
/// to remote hosts.
pub trait ConnectionPool: Clone + Send + 'static {
    type PathType: Addressable;

    fn request_connection(
        &mut self,
        target: Self::PathType,
        conn_type: ConnectionType,
    ) -> BoxFuture<Result<Result<Connection, ConnectionError>, RequestError>>;
}

pub type Connection = (ConnectionSender, Option<ConnectionReceiver>);

pub(crate) type ConnectionReceiver = mpsc::Receiver<RouterEvent>;
pub(crate) type ConnectionSender = TaggedSender;

/// The connection pool is responsible for opening new connections to remote hosts and managing
/// them.
#[derive(Clone)]
pub struct SwimConnPool<Path: Addressable> {
    client_tx: mpsc::Sender<DownlinkRoutingRequest<Path>>,
}

impl<Path: Addressable> SwimConnPool<Path> {
    /// Creates a new connection pool for managing connections to remote hosts.
    ///
    /// # Arguments
    ///
    /// * `config`                 - The configuration for the connection pool.
    /// * `client_conn_request_tx` - A channel for requesting remote connections.
    #[instrument(skip(config))]
    pub fn new<DelegateFac: RouterFactory + Debug>(
        config: ConnectionPoolParams,
        client_channel: ClientChannel<Path>,
        client_router_factory: ClientRouterFactory<Path, DelegateFac>,
        stop_trigger: CloseReceiver,
    ) -> (SwimConnPool<Path>, PoolTask<Path, DelegateFac>) {
        let (client_tx, client_rx) = client_channel;

        (
            SwimConnPool { client_tx },
            PoolTask::new(
                client_rx,
                client_router_factory,
                config.buffer_size,
                config.yield_after,
                stop_trigger,
            ),
        )
    }
}

type ClientChannel<Path> = (
    mpsc::Sender<DownlinkRoutingRequest<Path>>,
    mpsc::Receiver<DownlinkRoutingRequest<Path>>,
);

impl<Path: Addressable> ConnectionPool for SwimConnPool<Path> {
    type PathType = Path;

    /// Sends an asynchronous request for a connection to a specific path.
    ///
    /// # Arguments
    ///
    /// * `target`                  - The path to which we want to connect.
    /// * `conn_type`               - Whether or not the connection is full or only partial.
    ///
    /// # Returns
    ///
    /// A `Result` containing either a `Connection` to the remote host or a `ConnectionError`.
    /// The `Connection` contains a `ConnectionSender` and an optional `ConnectionReceiver`.
    /// The `ConnectionReceiver` is returned when the type of the connection is `ConnectionType::Full`.
    fn request_connection(
        &mut self,
        target: Self::PathType,
        conn_type: ConnectionType,
    ) -> BoxFuture<Result<Result<Connection, ConnectionError>, RequestError>> {
        async move {
            let (tx, rx) = oneshot::channel();

            self.client_tx
                .send(DownlinkRoutingRequest::Connect {
                    target,
                    request: Request::new(tx),
                    conn_type,
                })
                .await?;
            Ok(rx.await?)
        }
        .boxed()
    }
}

pub type ConnectionChannel = (ConnectionSender, Option<ConnectionReceiver>);

const REQUEST_ERROR: &str = "The request channel was dropped.";

pub struct PoolTask<Path: Addressable, DelegateFac: RouterFactory> {
    client_rx: mpsc::Receiver<DownlinkRoutingRequest<Path>>,
    client_router_factory: ClientRouterFactory<Path, DelegateFac>,
    buffer_size: NonZeroUsize,
    yield_after: NonZeroUsize,
    stop_trigger: CloseReceiver,
}

impl<Path: Addressable, DelegateFac: RouterFactory> PoolTask<Path, DelegateFac> {
    fn new(
        client_rx: mpsc::Receiver<DownlinkRoutingRequest<Path>>,
        client_router_factory: ClientRouterFactory<Path, DelegateFac>,
        buffer_size: NonZeroUsize,
        yield_after: NonZeroUsize,
        stop_trigger: CloseReceiver,
    ) -> Self {
        PoolTask {
            client_rx,
            client_router_factory,
            buffer_size,
            yield_after,
            stop_trigger,
        }
    }

    pub async fn run(self) -> Result<(), ConnectionError> {
        let PoolTask {
            client_rx,
            client_router_factory,
            buffer_size,
            yield_after,
            stop_trigger,
        } = self;

        let mut routing_table = RoutingTable::new();
        let mut counter: u32 = 0;
        let registrator_handles = FuturesUnordered::new();

        let mut client_rx = ReceiverStream::new(client_rx).fuse();
        let mut stop_rx = stop_trigger.clone().fuse();
        let mut iteration_count: usize = 0;

        loop {
            let request: Option<DownlinkRoutingRequest<Path>> = select! {
                client_req = client_rx.next() => client_req,
                _ = stop_rx => None
            };

            if let Some(request) = request {
                match request {
                    DownlinkRoutingRequest::Connect {
                        target,
                        request,
                        conn_type,
                    } => {
                        let routing_path = RoutingPath::try_from(AddressableWrapper(
                            target.clone(),
                        ))
                        .map_err(|_| {
                            ConnectionError::Resolution(ResolutionError::unresolvable(
                                target.to_string(),
                            ))
                        })?;

                        let registrator = match routing_table.try_resolve_addr(&routing_path) {
                            Some((_, registrator)) => registrator,
                            None => {
                                let routing_address = RoutingAddr::client(counter);
                                counter += 1;

                                let client_router =
                                    client_router_factory.create_for(routing_address);
                                let (registrator, registrator_task) = ConnectionRegistrator::new(
                                    buffer_size,
                                    yield_after,
                                    target.clone(),
                                    client_router,
                                    stop_trigger.clone(),
                                );
                                registrator_handles.push(spawn(registrator_task.run()));

                                routing_table.add_registrator(
                                    routing_path,
                                    routing_address,
                                    registrator.clone(),
                                );

                                registrator
                            }
                        };

                        let connection = registrator.request_connection(target, conn_type).await;
                        if request.send(connection).is_err() {
                            event!(Level::ERROR, REQUEST_ERROR);
                        }
                    }
                    DownlinkRoutingRequest::Endpoint { addr, request } => {
                        match routing_table.try_resolve_endpoint(&addr) {
                            Some(registrator) => {
                                if let Err(err) = registrator
                                    .registrator_tx
                                    .send(RegistratorRequest::Resolve { request })
                                    .await
                                {
                                    if let RegistratorRequest::Resolve { request } = err.0 {
                                        if request.send(Err(Unresolvable(addr))).is_err() {
                                            event!(Level::ERROR, REQUEST_ERROR);
                                        }
                                    }
                                }
                            }
                            None => {
                                if request.send(Err(Unresolvable(addr))).is_err() {
                                    event!(Level::ERROR, REQUEST_ERROR);
                                }
                            }
                        }
                    }
                }
            } else {
                registrator_handles.collect::<Vec<_>>().await;

                return Ok(());
            }

            iteration_count += 1;
            if iteration_count % yield_after == 0 {
                tokio::task::yield_now().await;
            }
        }
    }
}

type ConnectionResult = Result<(ConnectionSender, Option<ConnectionReceiver>), ConnectionError>;

#[derive(Debug, Clone, Copy)]
pub enum ConnectionType {
    /// A connection type that can both send and receive messages.
    Full,
    /// A connection type that can only send messages.
    Outgoing,
}

enum RegistratorRequest<Path: Addressable> {
    Connect {
        tx: oneshot::Sender<ConnectionResult>,
        path: Path,
        conn_type: ConnectionType,
    },
    Resolve {
        request: Request<Result<RawRoute, Unresolvable>>,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct ConnectionRegistrator<Path: Addressable> {
    registrator_tx: mpsc::Sender<RegistratorRequest<Path>>,
}

impl<Path: Addressable> ConnectionRegistrator<Path> {
    fn new<DelegateRouter: Router>(
        buffer_size: NonZeroUsize,
        yield_after: NonZeroUsize,
        target: Path,
        client_router: ClientRouter<Path, DelegateRouter>,
        stop_trigger: CloseReceiver,
    ) -> (
        ConnectionRegistrator<Path>,
        ConnectionRegistratorTask<Path, DelegateRouter>,
    ) {
        let (registrator_tx, registrator_rx) = mpsc::channel(buffer_size.get());

        (
            ConnectionRegistrator { registrator_tx },
            ConnectionRegistratorTask::new(
                buffer_size,
                yield_after,
                target,
                registrator_rx,
                client_router,
                stop_trigger,
            ),
        )
    }

    async fn request_connection(&self, path: Path, conn_type: ConnectionType) -> ConnectionResult {
        let (tx, rx) = oneshot::channel();
        self.registrator_tx
            .send(RegistratorRequest::Connect {
                tx,
                path,
                conn_type,
            })
            .await
            .map_err(|_| ConnectionError::Resolution(ResolutionError::router_dropped()))?;
        rx.await
            .map_err(|_| ConnectionError::Resolution(ResolutionError::router_dropped()))?
    }
}

enum RegistrationTarget {
    Remote(Url),
    Local(String),
}

enum ConnectionRegistratorEvent<Path: Addressable> {
    Message(TaggedEnvelope),
    Request(RegistratorRequest<Path>),
    ConnectionDropped(Arc<ConnectionDropped>),
}

struct ConnectionRegistratorTask<Path: Addressable, DelegateRouter: Router> {
    buffer_size: NonZeroUsize,
    yield_after: NonZeroUsize,
    target: RegistrationTarget,
    registrator_rx: mpsc::Receiver<RegistratorRequest<Path>>,
    client_router: ClientRouter<Path, DelegateRouter>,
    stop_trigger: CloseReceiver,
}

impl<Path: Addressable, DelegateRouter: Router> ConnectionRegistratorTask<Path, DelegateRouter> {
    fn new(
        buffer_size: NonZeroUsize,
        yield_after: NonZeroUsize,
        target: Path,
        registrator_rx: mpsc::Receiver<RegistratorRequest<Path>>,
        client_router: ClientRouter<Path, DelegateRouter>,
        stop_trigger: CloseReceiver,
    ) -> Self {
        let target = match target.host() {
            Some(url) => RegistrationTarget::Remote(url),
            None => RegistrationTarget::Local(target.node().to_string()),
        };

        ConnectionRegistratorTask {
            buffer_size,
            yield_after,
            target,
            registrator_rx,
            client_router,
            stop_trigger,
        }
    }

    async fn run(self) -> Result<(), ConnectionError> {
        let ConnectionRegistratorTask {
            buffer_size,
            yield_after,
            target,
            registrator_rx,
            mut client_router,
            stop_trigger,
        } = self;

        let (sender, receiver, maybe_raw_route, remote_drop_rx, local_drop_tx) = match target {
            RegistrationTarget::Remote(target) => {
                //Todo dm implement retry
                let BidirectionalRoute {
                    sender,
                    receiver,
                    on_drop: remote_drop_rx,
                } = client_router.resolve_bidirectional(target).await?;

                (sender, receiver, None, remote_drop_rx, None)
            }
            RegistrationTarget::Local(target) => {
                let relative_uri = RelativeUri::try_from(target).map_err(|e| {
                    ConnectionError::Resolution(ResolutionError::unresolvable(e.to_string()))
                })?;

                //Todo dm implement retry
                let routing_addr = client_router
                    .lookup(None, relative_uri.clone())
                    .await
                    .map_err(|err| match err {
                        RouterError::RouterDropped => {
                            ConnectionError::Resolution(ResolutionError::router_dropped())
                        }
                        _ => ConnectionError::Resolution(ResolutionError::unresolvable(
                            relative_uri.to_string(),
                        )),
                    })?;
                let Route {
                    sender,
                    on_drop: remote_drop_rx,
                } = client_router.resolve_sender(routing_addr).await?;

                let (local_drop_tx, local_drop_rx) = promise::promise();
                let (envelope_sender, envelope_receiver) = mpsc::channel(buffer_size.get());
                let raw_route = RawRoute::new(envelope_sender, local_drop_rx);

                (
                    sender,
                    envelope_receiver,
                    Some(raw_route),
                    remote_drop_rx,
                    Some(local_drop_tx),
                )
            }
        };

        let mut receiver = ReceiverStream::new(receiver).fuse();
        let mut registrator_rx = ReceiverStream::new(registrator_rx).fuse();
        let mut remote_drop_rx = remote_drop_rx.fuse();
        let mut stop_rx = stop_trigger.fuse();
        let mut iteration_count: usize = 0;

        let mut subscribers: HashMap<RelativePath, Slab<mpsc::Sender<RouterEvent>>> =
            HashMap::new();

        loop {
            let request: Option<ConnectionRegistratorEvent<Path>> = select! {
                message = receiver.next() => message.map(ConnectionRegistratorEvent::Message),
                req = registrator_rx.next() => req.map(ConnectionRegistratorEvent::Request),
                conn_err = remote_drop_rx => {
                    match conn_err{
                        Ok(conn_err) => Some(ConnectionRegistratorEvent::ConnectionDropped(conn_err)),
                        Err(_) => None,
                    }
                }
                _ = stop_rx => None,
            };

            match request {
                Some(ConnectionRegistratorEvent::Message(envelope)) => {
                    if let Ok(incoming_message) = envelope.1.clone().into_incoming() {
                        if let Some(subscribers) = subscribers.get_mut(&incoming_message.path) {
                            let futures = FuturesUnordered::new();

                            for (idx, sub) in subscribers.iter() {
                                let msg = incoming_message.clone();

                                futures.push(async move {
                                    let result = sub.send(RouterEvent::Message(msg)).await;
                                    (idx, result)
                                });
                            }

                            let results = futures.collect::<Vec<_>>().await;

                            for result in results {
                                if let (idx, Err(_)) = result {
                                    subscribers.remove(idx);
                                }
                            }
                        }
                    }
                }
                Some(ConnectionRegistratorEvent::Request(RegistratorRequest::Connect {
                    tx,
                    path,
                    conn_type: ConnectionType::Full,
                })) => {
                    let receiver = match subscribers.entry(path.relative_path()) {
                        Entry::Occupied(mut entry) => {
                            let (tx, rx) = mpsc::channel(buffer_size.get());
                            entry.get_mut().insert(tx);
                            rx
                        }
                        Entry::Vacant(vacancy) => {
                            let (tx, rx) = mpsc::channel(buffer_size.get());
                            let mut slab = Slab::new();
                            slab.insert(tx);

                            vacancy.insert(slab);
                            rx
                        }
                    };

                    if tx.send(Ok((sender.clone(), Some(receiver)))).is_err() {
                        event!(Level::ERROR, REQUEST_ERROR);
                    }
                }
                Some(ConnectionRegistratorEvent::Request(RegistratorRequest::Connect {
                    tx,
                    conn_type: ConnectionType::Outgoing,
                    ..
                })) => {
                    if let Err(Err(err)) = tx.send(Ok((sender.clone(), None))) {
                        return Err(err);
                    }
                }
                Some(ConnectionRegistratorEvent::Request(RegistratorRequest::Resolve {
                    request,
                })) if maybe_raw_route.is_some() => {
                    if request
                        .send(Ok(maybe_raw_route.as_ref().unwrap().clone()))
                        .is_err()
                    {
                        event!(Level::ERROR, REQUEST_ERROR);
                    }
                }
                Some(ConnectionRegistratorEvent::ConnectionDropped(_connection_dropped)) => {
                    //Todo dm implement retry
                    unimplemented!()
                }
                _ => {
                    if let Some(local_drop_tx) = local_drop_tx {
                        local_drop_tx
                            .provide(ConnectionDropped::Closed)
                            .map_err(|_| ConnectionError::Closed(CloseError::closed()))?;
                    }

                    return Ok(());
                }
            }

            iteration_count += 1;
            if iteration_count % yield_after == 0 {
                tokio::task::yield_now().await;
            }
        }
    }
}
