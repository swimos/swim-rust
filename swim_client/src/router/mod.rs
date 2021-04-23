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

use std::collections::HashMap;
use std::ops::Deref;

use crate::configuration::router::RouterParams;
use crate::connections::{ConnectionPool, ConnectionRequest, ConnectionSender, SwimConnPool};
use crate::router::incoming::{IncomingHostTask, IncomingRequest};
use crate::router::outgoing::OutgoingHostTask;
use either::Either;
use futures::future::ready;
use futures::future::BoxFuture;
use futures::join;
use futures::select;
use futures::stream::FuturesUnordered;
use futures::{select_biased, Future, FutureExt, SinkExt, StreamExt};
use std::collections::hash_map::Entry;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use swim_common::request::request_future::RequestError;
use swim_common::request::Request;
use swim_common::routing::error::{ResolutionError, RouterError, RoutingError, Unresolvable};
use swim_common::routing::remote::config::ConnectionConfig;
use swim_common::routing::remote::net::dns::Resolver;
use swim_common::routing::remote::net::plain::TokioPlainTextNetworking;
use swim_common::routing::remote::{
    RawRoute, RemoteConnectionChannels, RemoteConnectionsTask, RoutingRequest, SchemeSocketAddr,
};
use swim_common::routing::ws::tungstenite::TungsteniteWsConnections;
use swim_common::routing::ConnectionDropped;
use swim_common::routing::{
    Route, Router, RouterFactory, RoutingAddr, TaggedEnvelope, TaggedSender,
};
use swim_common::warp::envelope::{Envelope, IncomingHeader, IncomingLinkMessage, LinkMessage};
use swim_common::warp::path::{AbsolutePath, RelativePath};
use swim_runtime::task::*;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tracing::trace_span;
use tracing::{debug, error, span, trace, warn, Level};
use tracing_futures::Instrument;
use url::Url;
use utilities::errors::Recoverable;
use utilities::future::open_ended::OpenEndedFutures;
use utilities::sync::{promise, trigger};
use utilities::uri::RelativeUri;

#[cfg(test)]
mod tests;

mod incoming;
mod outgoing;
mod retry;

#[derive(Debug, Clone)]
pub(crate) struct ClientRouterFactory {
    request_sender: mpsc::Sender<ClientRequest>,
}

impl ClientRouterFactory {
    pub(crate) fn new(request_sender: mpsc::Sender<ClientRequest>) -> Self {
        ClientRouterFactory { request_sender }
    }
}

impl RouterFactory for ClientRouterFactory {
    type Router = ClientRouter;

    fn create_for(&self, addr: RoutingAddr) -> Self::Router {
        ClientRouter {
            tag: addr,
            request_sender: self.request_sender.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ClientRouter {
    tag: RoutingAddr,
    request_sender: mpsc::Sender<ClientRequest>,
}

impl Router for ClientRouter {
    fn resolve_sender(
        &mut self,
        _addr: RoutingAddr,
        origin: Option<SchemeSocketAddr>,
    ) -> BoxFuture<'_, Result<Route, ResolutionError>> {
        async move {
            let ClientRouter {
                tag,
                request_sender,
            } = self;
            let (tx, rx) = oneshot::channel();
            if request_sender
                .send(ClientRequest::Connect {
                    request: Request::new(tx),
                    origin: origin.unwrap(),
                })
                .await
                .is_err()
            {
                Err(ResolutionError::router_dropped())
            } else {
                match rx.await {
                    Ok(Ok(RawRoute { sender, on_drop })) => {
                        Ok(Route::new(TaggedSender::new(*tag, sender), on_drop))
                    }
                    Ok(Err(err)) => Err(ResolutionError::unresolvable(err.to_string())),
                    Err(_) => Err(ResolutionError::router_dropped()),
                }
            }
        }
        .boxed()
    }

    fn lookup(
        &mut self,
        _host: Option<Url>,
        _route: RelativeUri,
    ) -> BoxFuture<'_, Result<RoutingAddr, RouterError>> {
        async move { Ok(RoutingAddr::local(0)) }.boxed()
    }
}

#[derive(Debug)]
pub enum ClientRequest {
    /// Obtain a connection.
    Connect {
        request: Request<Result<RawRoute, Unresolvable>>,
        origin: SchemeSocketAddr,
    },
    /// Subscribe to a connection.
    Subscribe {
        url: Url,
        request: Request<mpsc::Receiver<TaggedEnvelope>>,
    },
}

pub(crate) type OutgoingManagerSender = mpsc::Sender<TaggedEnvelope>;

//Todo dm rename the channels to something that makes sense
//Todo this is the client router task factory
pub(crate) async fn run_client_router_task(
    mut request_rx: mpsc::Receiver<ClientRequest>,
    mut local_rx: mpsc::Receiver<ClientRequest>,
) {
    let mut outgoing_managers: HashMap<
        String,
        (
            OutgoingManagerSender,
            mpsc::Sender<Request<mpsc::Receiver<TaggedEnvelope>>>,
        ),
    > = HashMap::new();

    let mut request_rx = ReceiverStream::new(request_rx).fuse();
    let mut local_rx = ReceiverStream::new(local_rx).fuse();
    let futures = FuturesUnordered::new();
    let mut close_txs = Vec::new();

    loop {
        let next: Option<ClientRequest> = select_biased! {
            request = request_rx.next() => request,
            sub = local_rx.next() => sub,
        };

        match next {
            Some(ClientRequest::Connect { request, origin }) => {
                eprintln!("addr = {:#?}", origin.to_string());
                let (sender, _) = outgoing_managers
                    .entry(origin.to_string())
                    .or_insert_with(|| {
                        //Todo and this is the client router task
                        let (manager, sender, sub_tx) = IncomingManager::new();

                        let handle = spawn(manager.run());
                        futures.push(handle);
                        (sender, sub_tx)
                    })
                    .clone();

                let (on_drop_tx, on_drop_rx) = promise::promise();
                close_txs.push(on_drop_tx);

                request.send(Ok(RawRoute::new(sender, on_drop_rx))).unwrap();
            }
            Some(ClientRequest::Subscribe {
                url,
                request: sub_req,
            }) => {
                eprintln!("url = {:#?}", url.to_string());
                let (_, sub_sender) =
                    outgoing_managers.entry(url.to_string()).or_insert_with(|| {
                        let (manager, sender, sub_tx) = IncomingManager::new();

                        let handle = spawn(manager.run());
                        futures.push(handle);
                        (sender, sub_tx)
                    });

                sub_sender.send(sub_req).await.unwrap();
            }
            _ => {
                close_txs.into_iter().for_each(|trigger| {
                    if let Err(err) = trigger.provide(ConnectionDropped::Closed) {
                        tracing::error!("{:?}", err);
                    }
                });

                for result in futures.collect::<Vec<_>>().await {
                    match result {
                        Ok(res) => {
                            if let Err(err) = res {
                                tracing::error!("{:?}", err);
                            }
                        }
                        Err(err) => {
                            tracing::error!("{:?}", err);
                        }
                    }
                }

                break;
            }
        }
    }
}

pub(crate) struct IncomingManager {
    envelope_rx: mpsc::Receiver<TaggedEnvelope>,
    sub_rx: mpsc::Receiver<Request<mpsc::Receiver<TaggedEnvelope>>>,
}

impl IncomingManager {
    pub(crate) fn new() -> (
        IncomingManager,
        OutgoingManagerSender,
        mpsc::Sender<Request<mpsc::Receiver<TaggedEnvelope>>>,
    ) {
        let (envelope_tx, envelope_rx) = mpsc::channel(8);
        let (sub_tx, sub_rx) = mpsc::channel(8);
        (
            IncomingManager {
                envelope_rx,
                sub_rx,
            },
            envelope_tx,
            sub_tx,
        )
    }

    pub(crate) async fn run(mut self) -> Result<(), RoutingError> {
        let IncomingManager {
            mut envelope_rx,
            mut sub_rx,
        } = self;

        let mut envelope_rx = ReceiverStream::new(envelope_rx).fuse();
        let mut sub_rx = ReceiverStream::new(sub_rx).fuse();

        let mut subs: Vec<mpsc::Sender<TaggedEnvelope>> = Vec::new();

        loop {
            let next: Either<
                Option<TaggedEnvelope>,
                Option<Request<mpsc::Receiver<TaggedEnvelope>>>,
            > = select_biased! {
                envelope = envelope_rx.next() => Either::Left(envelope),
                sub = sub_rx.next() => Either::Right(sub),
            };

            match next {
                Either::Left(Some(envelope)) => match envelope.1.clone().into_incoming() {
                    Ok(env) => {
                        broadcast(&mut subs, envelope).await?;
                    }
                    Err(env) => {
                        warn!("Unsupported message: {:?}", env);
                    }
                },
                Either::Right(Some(sub_request)) => {
                    let (sub_tx, sub_rx) = mpsc::channel(8);
                    subs.push(sub_tx);
                    sub_request.send(sub_rx).unwrap();
                }
                _ => break Ok(()),
            }
        }
    }
}

/// Broadcasts an event to all subscribers of the task that are subscribed to a given path.
/// The path is the combination of the node and lane.
///
/// # Arguments
///
/// * `subscribers`             - A map of all subscribers.
/// * `destination`             - The node and lane.
/// * `event`                   - An event to be broadcasted.
async fn broadcast(
    subscribers: &mut Vec<mpsc::Sender<TaggedEnvelope>>,
    envelope: TaggedEnvelope,
) -> Result<(), RoutingError> {
    if subscribers.len() == 1 {
        let result = subscribers
            .get_mut(0)
            .ok_or(RoutingError::ConnectionError)?
            .send(envelope)
            .await;

        if result.is_err() {
            subscribers.remove(0);
        }
    } else {
        let futures: FuturesUnordered<_> = subscribers
            .iter_mut()
            .enumerate()
            .map(|(index, sender)| index_sender(sender, envelope.clone(), index))
            .collect();

        for index in futures.filter_map(ready).collect::<Vec<_>>().await {
            subscribers.remove(index);
        }
    }

    Ok(())
}

async fn index_sender(
    sender: &mut mpsc::Sender<TaggedEnvelope>,
    envelope: TaggedEnvelope,
    index: usize,
) -> Option<usize> {
    if sender.send(envelope).await.is_err() {
        Some(index)
    } else {
        None
    }
}

//Todo dm old stuff bellow

/// The Router is responsible for routing messages between the downlinks and the connections from the
/// connection pool. It can be used to obtain a connection for a downlink or to send direct messages.
pub trait OldRouter: Send {
    type ConnectionFut: Future<Output = Result<ConnectionChannel, RequestError>> + Send;

    /// For full duplex connections
    fn connection_for(&mut self, target: &AbsolutePath) -> Self::ConnectionFut;

    /// For sending direct messages
    fn general_sink(&mut self) -> mpsc::Sender<(url::Url, Envelope)>;
}

pub(crate) type RouterConnRequest = (AbsolutePath, oneshot::Sender<ConnectionChannel>);

pub(crate) type RouterMessageRequest = (url::Url, Envelope);
pub(crate) type CloseSender = promise::Sender<mpsc::Sender<Result<(), RoutingError>>>;
type CloseResponseSender = mpsc::Sender<Result<(), RoutingError>>;
type CloseReceiver = promise::Receiver<mpsc::Sender<Result<(), RoutingError>>>;

/// The Router events are emitted by the connection streams of the router and indicate
/// messages or errors from the remote host.
#[derive(Debug, Clone, PartialEq)]
pub enum RouterEvent {
    // Incoming message from a remote host.
    Message(IncomingLinkMessage),
    // There was an error in the connection. If a retry strategy exists this will trigger it.
    ConnectionClosed,
    /// The remote host is unreachable. This will not trigger the retry system.
    Unreachable(String),
    // The router is stopping.
    Stopping,
}

pub struct SwimRouter<Pool: ConnectionPool> {
    router_connection_request_tx: mpsc::Sender<RouterConnRequest>,
    router_sink_tx: mpsc::Sender<RouterMessageRequest>,
    task_manager_handle: TaskHandle<Result<(), RoutingError>>,
    connection_pool: Pool,
    close_tx: CloseSender,
    configuration: RouterParams,
}

impl<Pool: ConnectionPool> SwimRouter<Pool> {
    /// Creates a new connection router for routing messages between the downlinks and the
    /// connection pool.
    ///
    /// # Arguments
    ///
    /// * `configuration`             - The configuration parameters of the router.
    /// * `connection_pool`           - A connection pool for obtaining connections to remote hosts.
    pub fn new(configuration: RouterParams, connection_pool: Pool) -> SwimRouter<Pool>
    where
        Pool: ConnectionPool,
    {
        let (close_tx, close_rx) = promise::promise();

        let (task_manager, router_connection_request_tx, router_sink_tx) =
            TaskManager::new(connection_pool.clone(), close_rx, configuration);

        let task_manager_handle = spawn(task_manager.run());

        SwimRouter {
            router_connection_request_tx,
            router_sink_tx,
            task_manager_handle,
            connection_pool,
            close_tx,
            configuration,
        }
    }

    /// Closes the router and all of its sub-tasks, logging any errors that have been encountered.
    /// Returns a [`RoutingError::CloseError`] if the closing fails.
    pub async fn close(self) -> Result<(), RoutingError> {
        let (result_tx, mut result_rx) = mpsc::channel(self.configuration.buffer_size().get());

        self.close_tx
            .provide(result_tx)
            .map_err(|_| RoutingError::CloseError)?;

        while let Some(result) = result_rx.recv().await {
            if let Err(e) = result {
                tracing::error!("{:?}", e);
            }
        }

        if let Err(e) = self.task_manager_handle.await {
            tracing::error!("{:?}", e);
        };

        self.connection_pool
            .close()
            .await
            .map_err(|_| RoutingError::CloseError)?
            .map_err(|_| RoutingError::CloseError)
    }
}

/// Tasks that the router can handle.
enum RouterTask {
    Connect(RouterConnRequest),
    SendMessage(Box<RouterMessageRequest>),
    Close(Option<CloseResponseSender>),
}

type HostManagerHandle = (
    mpsc::Sender<Envelope>,
    mpsc::Sender<SubscriberRequest>,
    TaskHandle<Result<(), RoutingError>>,
);

/// The task manager is the main task in the router. It is responsible for creating sub-tasks
/// for each unique remote host. It can also handle direct messages by sending them directly
/// to the appropriate sub-task.
pub(crate) struct TaskManager<Pool: ConnectionPool> {
    conn_request_rx: mpsc::Receiver<RouterConnRequest>,
    message_request_rx: mpsc::Receiver<RouterMessageRequest>,
    connection_pool: Pool,
    close_rx: CloseReceiver,
    config: RouterParams,
}

impl<Pool: ConnectionPool> TaskManager<Pool> {
    pub(crate) fn new(
        connection_pool: Pool,
        close_rx: CloseReceiver,
        config: RouterParams,
    ) -> (
        Self,
        mpsc::Sender<RouterConnRequest>,
        mpsc::Sender<RouterMessageRequest>,
    ) {
        let (conn_request_tx, conn_request_rx) = mpsc::channel(config.buffer_size().get());
        let (message_request_tx, message_request_rx) = mpsc::channel(config.buffer_size().get());
        (
            TaskManager {
                conn_request_rx,
                message_request_rx,
                connection_pool,
                close_rx,
                config,
            },
            conn_request_tx,
            message_request_tx,
        )
    }

    pub(crate) async fn run(self) -> Result<(), RoutingError> {
        let TaskManager {
            conn_request_rx,
            message_request_rx,
            connection_pool,
            close_rx,
            config,
        } = self;

        let mut message_request_rx = ReceiverStream::new(message_request_rx).fuse();
        let mut conn_request_rx = ReceiverStream::new(conn_request_rx).fuse();
        let mut close_trigger = close_rx.clone().fuse();

        let mut host_managers: HashMap<url::Url, HostManagerHandle> = HashMap::new();

        loop {
            let task = select_biased! {
                closed = &mut close_trigger => {
                    match closed {
                        Ok(tx) => Some(RouterTask::Close(Some((*tx).clone()))),
                        _ => Some(RouterTask::Close(None)),
                    }
                },
                maybe_req = conn_request_rx.next() => maybe_req.map(RouterTask::Connect),
                maybe_req = message_request_rx.next() => maybe_req.map(|payload| RouterTask::SendMessage(Box::new(payload))),
            }.ok_or(RoutingError::ConnectionError)?;

            match task {
                RouterTask::Connect((target, response_tx)) => {
                    let (sink, stream_registrator, _) = get_host_manager(
                        &mut host_managers,
                        target.host.clone(),
                        connection_pool.clone(),
                        close_rx.clone(),
                        config,
                    );

                    let (subscriber_tx, stream) = mpsc::channel(config.buffer_size().get());

                    let (_, relative_path) = target.split();

                    stream_registrator
                        .send(SubscriberRequest::new(relative_path, subscriber_tx))
                        .await
                        .map_err(|_| RoutingError::ConnectionError)?;

                    response_tx
                        .send((sink.clone(), stream))
                        .map_err(|_| RoutingError::ConnectionError)?;
                }

                RouterTask::SendMessage(payload) => {
                    let (host, message) = payload.deref();

                    let target = message
                        .header
                        .relative_path()
                        .ok_or(RoutingError::ConnectionError)?
                        .for_host(host.clone());

                    let (sink, _, _) = get_host_manager(
                        &mut host_managers,
                        target.host.clone(),
                        connection_pool.clone(),
                        close_rx.clone(),
                        config,
                    );

                    sink.send(message.clone())
                        .await
                        .map_err(|_| RoutingError::ConnectionError)?;
                }

                RouterTask::Close(close_rx) => {
                    if let Some(close_response_tx) = close_rx {
                        let futures = FuturesUnordered::new();

                        host_managers
                            .iter_mut()
                            .for_each(|(_, (_, _, handle))| futures.push(handle));

                        for result in futures.collect::<Vec<_>>().await {
                            close_response_tx
                                .send(result.unwrap_or(Err(RoutingError::CloseError)))
                                .await
                                .map_err(|_| RoutingError::CloseError)?;
                        }

                        break Ok(());
                    }
                }
            }
        }
    }
}

fn get_host_manager<Pool>(
    host_managers: &mut HashMap<url::Url, HostManagerHandle>,
    host: url::Url,
    connection_pool: Pool,
    close_rx: CloseReceiver,
    config: RouterParams,
) -> &mut HostManagerHandle
where
    Pool: ConnectionPool,
{
    host_managers.entry(host.clone()).or_insert_with(|| {
        let (host_manager, sink, stream_registrator) =
            HostManager::new(host, connection_pool, close_rx, config);
        (
            sink,
            stream_registrator,
            spawn(
                host_manager
                    .run()
                    .instrument(trace_span!(HOST_MANAGER_TASK_NAME)),
            ),
        )
    })
}

/// A connection request is used by the [`OutgoingHostTask`] to request a connection when
/// it is trying to send a message.
pub(crate) struct OldConnectionRequest {
    request_tx: oneshot::Sender<Result<ConnectionSender, RoutingError>>,
    //If the connection should be recreated or returned from cache.
    recreate: bool,
}

impl OldConnectionRequest {
    fn new(
        request_tx: oneshot::Sender<Result<ConnectionSender, RoutingError>>,
        recreate: bool,
    ) -> Self {
        OldConnectionRequest {
            request_tx,
            recreate,
        }
    }
}

/// A subscriber request is sent to the [`IncomingHostTask`] to request for a new subscriber
/// to receive all new messages for the given path.
#[derive(Debug)]
pub(crate) struct SubscriberRequest {
    path: RelativePath,
    subscriber_tx: mpsc::Sender<RouterEvent>,
}

impl SubscriberRequest {
    fn new(path: RelativePath, subscriber_tx: mpsc::Sender<RouterEvent>) -> Self {
        SubscriberRequest {
            path,
            subscriber_tx,
        }
    }
}

const INCOMING_TASK_NAME: &str = "incoming";
const OUTGOING_TASK_NAME: &str = "outgoing";
const HOST_MANAGER_TASK_NAME: &str = "host manager";

/// Tasks that the host manager can handle.
enum HostTask {
    Connect(OldConnectionRequest),
    Subscribe(SubscriberRequest),
    Close(Option<CloseResponseSender>),
}

/// The host manager is responsible for routing messages to a single host only.
/// All host managers are sub-tasks of the task manager. The host manager is responsible for
/// obtaining connections from the connection pool when needed and for registering new subscribers
/// for the given host.
///
/// Note: The host manager *DOES NOT* open connections by default when created.
/// It will only open connections when required.
struct HostManager<Pool: ConnectionPool> {
    host: url::Url,
    connection_pool: Pool,
    sink_rx: mpsc::Receiver<Envelope>,
    stream_registrator_rx: mpsc::Receiver<SubscriberRequest>,
    close_rx: CloseReceiver,
    config: RouterParams,
}

impl<Pool: ConnectionPool> HostManager<Pool> {
    fn new(
        host: url::Url,
        connection_pool: Pool,
        close_rx: CloseReceiver,
        config: RouterParams,
    ) -> (
        HostManager<Pool>,
        mpsc::Sender<Envelope>,
        mpsc::Sender<SubscriberRequest>,
    ) {
        let (sink_tx, sink_rx) = mpsc::channel(config.buffer_size().get());
        let (stream_registrator_tx, stream_registrator_rx) =
            mpsc::channel(config.buffer_size().get());

        (
            HostManager {
                host,
                connection_pool,
                sink_rx,
                stream_registrator_rx,
                close_rx,
                config,
            },
            sink_tx,
            stream_registrator_tx,
        )
    }

    async fn run(self) -> Result<(), RoutingError> {
        let HostManager {
            host,
            mut connection_pool,
            sink_rx,
            stream_registrator_rx,
            close_rx,
            config,
        } = self;

        let (connection_request_tx, connection_request_rx) =
            mpsc::channel(config.buffer_size().get());

        let (incoming_task, incoming_task_tx) =
            IncomingHostTask::new(close_rx.clone(), config.buffer_size().get());
        let outgoing_task =
            OutgoingHostTask::new(sink_rx, connection_request_tx, close_rx.clone(), config);

        let incoming_handle = spawn(
            incoming_task
                .run()
                .instrument(span!(Level::TRACE, INCOMING_TASK_NAME)),
        );
        let outgoing_handle = spawn(
            outgoing_task
                .run()
                .instrument(span!(Level::TRACE, OUTGOING_TASK_NAME)),
        );

        let mut close_trigger = close_rx.fuse();
        let mut connection_request_rx = ReceiverStream::new(connection_request_rx).fuse();
        let mut stream_registrator_rx = ReceiverStream::new(stream_registrator_rx).fuse();

        loop {
            let task = select_biased! {
                closed = &mut close_trigger => {
                    match closed {
                        Ok(tx) => Some(HostTask::Close(Some((*tx).clone()))),
                        _ => Some(HostTask::Close(None)),
                    }
                },
                maybe_req = connection_request_rx.next() => maybe_req.map(HostTask::Connect),
                maybe_reg = stream_registrator_rx.next() => maybe_reg.map(HostTask::Subscribe),
            }
            .ok_or(RoutingError::ConnectionError)?;

            match task {
                HostTask::Connect(OldConnectionRequest {
                    request_tx: connection_response_tx,
                    recreate,
                }) => {
                    let maybe_connection_channel = connection_pool
                        .request_connection(host.clone(), recreate)
                        .await
                        .map_err(|_| RoutingError::ConnectionError)?;

                    match maybe_connection_channel {
                        Ok((connection_tx, maybe_connection_rx)) => {
                            connection_response_tx
                                .send(Ok(connection_tx))
                                .map_err(|_| RoutingError::ConnectionError)?;

                            if let Some(connection_rx) = maybe_connection_rx {
                                incoming_task_tx
                                    .send(IncomingRequest::Connection(connection_rx))
                                    .await
                                    .map_err(|_| RoutingError::ConnectionError)?;
                            }
                        }
                        Err(connection_error) => match connection_error {
                            e if e.is_transient() => {
                                let _ =
                                    connection_response_tx.send(Err(RoutingError::PoolError(e)));
                            }
                            e => {
                                let _ =
                                    connection_response_tx.send(Err(RoutingError::ConnectionError));
                                let msg = format!("{}", e);
                                let _ = incoming_task_tx
                                    .send(IncomingRequest::Unreachable(msg.to_string()))
                                    .await;
                            }
                        },
                    }
                }
                HostTask::Subscribe(SubscriberRequest {
                    path: relative_path,
                    subscriber_tx: event_tx,
                }) => {
                    incoming_task_tx
                        .send(IncomingRequest::Subscribe(SubscriberRequest::new(
                            relative_path,
                            event_tx,
                        )))
                        .await
                        .map_err(|_| RoutingError::ConnectionError)?;
                }
                HostTask::Close(close_rx) => {
                    if let Some(close_response_tx) = close_rx {
                        let futures = FuturesUnordered::new();

                        futures.push(incoming_handle);
                        futures.push(outgoing_handle);

                        for result in futures.collect::<Vec<_>>().await {
                            close_response_tx
                                .send(result.unwrap_or(Err(RoutingError::CloseError)))
                                .await
                                .map_err(|_| RoutingError::CloseError)?;
                        }

                        break Ok(());
                    }
                }
            }
        }
    }
}

type ConnectionChannel = (mpsc::Sender<Envelope>, mpsc::Receiver<RouterEvent>);

impl<Pool: ConnectionPool> OldRouter for SwimRouter<Pool> {
    type ConnectionFut = BoxFuture<'static, Result<ConnectionChannel, RequestError>>;

    fn connection_for(&mut self, target: &AbsolutePath) -> Self::ConnectionFut {
        let tx = self.router_connection_request_tx.clone();
        let path = target.clone();
        async move {
            let (resp_tx, resp_rx) = oneshot::channel();
            if tx.send((path, resp_tx)).await.is_ok() {
                Ok(resp_rx.await?)
            } else {
                Err(RequestError)
            }
        }
        .boxed()
    }

    fn general_sink(&mut self) -> mpsc::Sender<(url::Url, Envelope)> {
        self.router_sink_tx.clone()
    }
}
