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

use crate::configuration::downlink::ClientParams;
use crate::router::{
    AddressableWrapper, ClientRouter, ClientRouterFactory, DownlinkRoutingRequest, RouterEvent,
    RoutingPath, RoutingTable,
};
use futures::future::BoxFuture;
use futures::select_biased;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use slab::Slab;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
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
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tracing::instrument;
use tracing::{event, Level};
use url::Url;
use utilities::errors::Recoverable;
use utilities::future::retryable::strategy::RetryStrategy;
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
        config: ClientParams,
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
                config,
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
const SUBSCRIBER_ERROR: &str = "The subscriber channel was dropped.";

pub struct PoolTask<Path: Addressable, DelegateFac: RouterFactory> {
    client_rx: mpsc::Receiver<DownlinkRoutingRequest<Path>>,
    client_router_factory: ClientRouterFactory<Path, DelegateFac>,
    config: ClientParams,
    stop_trigger: CloseReceiver,
}

impl<Path: Addressable, DelegateFac: RouterFactory> PoolTask<Path, DelegateFac> {
    fn new(
        client_rx: mpsc::Receiver<DownlinkRoutingRequest<Path>>,
        client_router_factory: ClientRouterFactory<Path, DelegateFac>,
        config: ClientParams,
        stop_trigger: CloseReceiver,
    ) -> Self {
        PoolTask {
            client_rx,
            client_router_factory,
            config,
            stop_trigger,
        }
    }

    pub async fn run(self) -> Result<(), ConnectionError> {
        let PoolTask {
            client_rx,
            client_router_factory,
            config,
            stop_trigger,
        } = self;

        let mut routing_table = RoutingTable::new();
        let mut counter: u32 = 0;
        let registrator_handles = FuturesUnordered::new();

        let mut client_rx = ReceiverStream::new(client_rx).fuse();
        let mut stop_rx = stop_trigger.clone().fuse();
        let mut iteration_count: usize = 0;

        loop {
            let request: Option<DownlinkRoutingRequest<Path>> = select_biased! {
                 _ = stop_rx => None,
                client_req = client_rx.next() => client_req,
            };

            if let Some(ss) = request {
                match ss {
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
                                    config,
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
            if iteration_count % config.yield_after == 0 {
                tokio::task::yield_now().await;
            }
        }
    }
}

type ConnectionResult = Result<(ConnectionSender, Option<ConnectionReceiver>), ConnectionError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
        config: ClientParams,
        target: Path,
        client_router: ClientRouter<Path, DelegateRouter>,
        stop_trigger: CloseReceiver,
    ) -> (
        ConnectionRegistrator<Path>,
        ConnectionRegistratorTask<Path, DelegateRouter>,
    ) {
        let (registrator_tx, registrator_rx) =
            mpsc::channel(config.conn_pool_params.buffer_size().get());

        (
            ConnectionRegistrator { registrator_tx },
            ConnectionRegistratorTask::new(
                config,
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

#[derive(Debug, Clone)]
enum RegistrationTarget {
    Remote(Url),
    Local(String),
}

impl Display for RegistrationTarget {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistrationTarget::Remote(remote) => {
                write!(f, "{}", remote)
            }
            RegistrationTarget::Local(local) => {
                write!(f, "{}", local)
            }
        }
    }
}

enum ConnectionRegistratorEvent<Path: Addressable> {
    Message(TaggedEnvelope),
    Request(RegistratorRequest<Path>),
    ConnectionDropped(Arc<ConnectionDropped>),
}

struct ConnectionRegistratorTask<Path: Addressable, DelegateRouter: Router> {
    config: ClientParams,
    target: RegistrationTarget,
    registrator_rx: mpsc::Receiver<RegistratorRequest<Path>>,
    client_router: ClientRouter<Path, DelegateRouter>,
    stop_trigger: CloseReceiver,
}

impl<Path: Addressable, DelegateRouter: Router> ConnectionRegistratorTask<Path, DelegateRouter> {
    fn new(
        config: ClientParams,
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
            config,
            target,
            registrator_rx,
            client_router,
            stop_trigger,
        }
    }

    async fn run(self) -> Result<(), ConnectionError> {
        let ConnectionRegistratorTask {
            config,
            target,
            registrator_rx,
            mut client_router,
            stop_trigger,
        } = self;

        let (mut sender, receiver, remote_drop_rx) = open_connection(
            target.clone(),
            config.router_params.retry_strategy(),
            &mut client_router,
            sleep,
            stop_trigger.clone(),
        )
        .await?;

        let (receiver, maybe_raw_route, maybe_local_drop_tx) = match receiver {
            Some(receiver) => (receiver, None, None),
            None => {
                let (local_drop_tx, local_drop_rx) = promise::promise();
                let (envelope_sender, envelope_receiver) =
                    mpsc::channel(config.conn_pool_params.buffer_size().get());
                let raw_route = RawRoute::new(envelope_sender, local_drop_rx);

                (envelope_receiver, Some(raw_route), Some(local_drop_tx))
            }
        };

        let mut receiver = ReceiverStream::new(receiver).fuse();
        let mut registrator_rx = ReceiverStream::new(registrator_rx).fuse();
        let mut remote_drop_rx = remote_drop_rx.fuse();
        let mut stop_rx = stop_trigger.clone().fuse();
        let mut iteration_count: usize = 0;

        let mut subscribers: HashMap<RelativePath, Slab<mpsc::Sender<RouterEvent>>> =
            HashMap::new();

        loop {
            let request: Option<ConnectionRegistratorEvent<Path>> = select_biased! {
                _ = stop_rx => None,
                message = receiver.next() => message.map(ConnectionRegistratorEvent::Message),
                req = registrator_rx.next() => req.map(ConnectionRegistratorEvent::Request),
                conn_err = remote_drop_rx => {
                    match conn_err{
                        Ok(conn_err) => Some(ConnectionRegistratorEvent::ConnectionDropped(conn_err)),
                        Err(_) => None,
                    }
                }
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
                            let (tx, rx) =
                                mpsc::channel(config.conn_pool_params.buffer_size().get());
                            entry.get_mut().insert(tx);
                            rx
                        }
                        Entry::Vacant(vacancy) => {
                            let (tx, rx) =
                                mpsc::channel(config.conn_pool_params.buffer_size().get());
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
                    if tx.send(Ok((sender.clone(), None))).is_err() {
                        event!(Level::ERROR, REQUEST_ERROR);
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
                Some(ConnectionRegistratorEvent::ConnectionDropped(connection_dropped)) => {
                    broadcast(&mut subscribers, RouterEvent::ConnectionClosed).await;

                    if connection_dropped.is_recoverable() {
                        match open_connection(
                            target.clone(),
                            config.router_params.retry_strategy(),
                            &mut client_router,
                            sleep,
                            stop_trigger.clone(),
                        )
                        .await
                        {
                            Ok((new_sender, new_receiver, new_remote_drop_rx)) => {
                                sender = new_sender;
                                if let Some(new_receiver) = new_receiver {
                                    receiver = ReceiverStream::new(new_receiver).fuse()
                                }
                                remote_drop_rx = new_remote_drop_rx.fuse();
                            }
                            Err(err) => {
                                broadcast(
                                    &mut subscribers,
                                    RouterEvent::Unreachable(target.to_string()),
                                )
                                .await;

                                if let Some(local_drop_tx) = maybe_local_drop_tx {
                                    local_drop_tx.provide(ConnectionDropped::Closed).map_err(
                                        |_| ConnectionError::Closed(CloseError::closed()),
                                    )?;
                                }

                                return Err(err);
                            }
                        };
                    } else {
                        broadcast(
                            &mut subscribers,
                            RouterEvent::Unreachable(target.to_string()),
                        )
                        .await;

                        if let Some(local_drop_tx) = maybe_local_drop_tx {
                            local_drop_tx
                                .provide(ConnectionDropped::Closed)
                                .map_err(|_| ConnectionError::Closed(CloseError::closed()))?;
                        }

                        return Err(ConnectionError::Closed(CloseError::closed()));
                    }
                }
                _ => {
                    let mut futures = vec![];

                    for (_, subs) in subscribers {
                        for (_, sub) in subs {
                            futures.push(async move {
                                if sub.send(RouterEvent::Stopping).await.is_err() {
                                    event!(Level::ERROR, SUBSCRIBER_ERROR);
                                }
                            })
                        }
                    }

                    if let Some(local_drop_tx) = maybe_local_drop_tx {
                        local_drop_tx
                            .provide(ConnectionDropped::Closed)
                            .map_err(|_| ConnectionError::Closed(CloseError::closed()))?;
                    }

                    return Ok(());
                }
            }

            iteration_count += 1;
            if iteration_count % config.yield_after == 0 {
                tokio::task::yield_now().await;
            }
        }
    }
}

async fn broadcast(
    subscribers: &mut HashMap<RelativePath, Slab<Sender<RouterEvent>>>,
    event: RouterEvent,
) {
    let futures = FuturesUnordered::new();

    for (path, subs) in subscribers.iter() {
        for (idx, sub) in subs {
            let event_clone = event.clone();
            futures.push(async move {
                let result = sub.send(event_clone).await;
                (path.clone(), idx, result)
            })
        }
    }

    let results = futures.collect::<Vec<_>>().await;

    for result in results {
        if let (path, idx, Err(_)) = result {
            if let Some(subs) = subscribers.get_mut(&path) {
                subs.remove(idx);
            }
        }
    }
}

type RawConnection = (
    TaggedSender,
    Option<mpsc::Receiver<TaggedEnvelope>>,
    promise::Receiver<ConnectionDropped>,
);

async fn open_connection<Path, DelegateRouter, F, D>(
    target: RegistrationTarget,
    mut retry_strategy: RetryStrategy,
    client_router: &mut ClientRouter<Path, DelegateRouter>,
    delay_fn: F,
    stop_trigger: CloseReceiver,
) -> Result<RawConnection, ConnectionError>
where
    Path: Addressable,
    DelegateRouter: Router,
    F: Fn(Duration) -> D,
    D: Future<Output = ()>,
{
    let mut stop_rx = stop_trigger.fuse();
    loop {
        let result = match target.clone() {
            RegistrationTarget::Remote(target) => {
                try_open_remote_connection(client_router, target).await
            }
            RegistrationTarget::Local(target) => {
                try_open_local_connection(client_router, target).await
            }
        };

        match result {
            Ok(connection) => {
                break Ok(connection);
            }
            Err(err) if !err.is_fatal() => match retry_strategy.next() {
                Some(Some(dur)) => {
                    let cancelled: Option<()> = select_biased! {
                        _ = stop_rx => Some(()),
                        _ = delay_fn(dur).fuse() => None,
                    };

                    if cancelled.is_some() {
                        unimplemented!()
                    }
                }
                None => {
                    break Err(err);
                }
                _ => {}
            },
            Err(err) => {
                break Err(err);
            }
        }
    }
}

async fn try_open_remote_connection<Path, DelegateRouter>(
    client_router: &mut ClientRouter<Path, DelegateRouter>,
    target: Url,
) -> Result<RawConnection, ConnectionError>
where
    Path: Addressable,
    DelegateRouter: Router,
{
    let BidirectionalRoute {
        sender,
        receiver,
        on_drop,
    } = client_router
        .resolve_bidirectional(target)
        .await
        .map_err(ConnectionError::Resolution)?;

    Ok((sender, Some(receiver), on_drop))
}

async fn try_open_local_connection<Path, DelegateRouter>(
    client_router: &mut ClientRouter<Path, DelegateRouter>,
    target: String,
) -> Result<RawConnection, ConnectionError>
where
    Path: Addressable,
    DelegateRouter: Router,
{
    let relative_uri = RelativeUri::try_from(target)
        .map_err(|e| ConnectionError::Resolution(ResolutionError::unresolvable(e.to_string())))?;

    let routing_addr = client_router
        .lookup(None, relative_uri.clone())
        .await
        .map_err(|_| {
            ConnectionError::Resolution(ResolutionError::unresolvable(relative_uri.to_string()))
        })?;

    let Route { sender, on_drop } = client_router
        .resolve_sender(routing_addr)
        .await
        .map_err(ConnectionError::Resolution)?;

    Ok((sender, None, on_drop))
}
