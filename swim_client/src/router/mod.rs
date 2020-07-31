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

use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::ops::Deref;

use futures::stream;
use futures::stream::FuturesUnordered;
use futures::{Future, Stream};
use tokio::stream::StreamExt;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot;
use tokio::sync::{mpsc, watch};
use tracing::trace_span;
use tracing::{span, Level};
use tracing_futures::Instrument;

use swim_common::request::request_future::{RequestError, RequestFuture, Sequenced};
use swim_common::sink::item::map_err::SenderErrInto;
use swim_common::sink::item::ItemSender;
use swim_common::warp::envelope::{Envelope, IncomingLinkMessage};
use swim_common::warp::path::{AbsolutePath, RelativePath};
use swim_runtime::task::*;

use crate::configuration::router::RouterParams;
use crate::connections::{ConnectionPool, ConnectionSender, SwimConnPool};
use crate::router::incoming::{IncomingHostTask, IncomingRequest};
use crate::router::outgoing::OutgoingHostTask;
use swim_common::connections::error::ConnectionError;

pub mod incoming;
pub mod outgoing;
mod retry;

#[cfg(test)]
mod tests;

/// The Router is responsible for routing messages between the downlinks and the connections from the
/// connection pool. It can be used to obtain a connection for a downlink or to send direct messages.
pub trait Router: Send {
    type ConnectionStream: Stream<Item = RouterEvent> + Send + 'static;
    type ConnectionSink: ItemSender<Envelope, RoutingError> + Send + Clone + Sync + 'static;
    type GeneralSink: ItemSender<(url::Url, Envelope), RoutingError> + Send + 'static;
    type ConnectionFut: Future<Output = Result<(Self::ConnectionSink, Self::ConnectionStream), RequestError>>
        + Send;

    /// For full duplex connections
    fn connection_for(&mut self, target: &AbsolutePath) -> Self::ConnectionFut;

    /// For sending direct messages
    fn general_sink(&mut self) -> Self::GeneralSink;
}

type RouterConnRequest = (
    AbsolutePath,
    oneshot::Sender<(
        <SwimRouter<SwimConnPool> as Router>::ConnectionSink,
        <SwimRouter<SwimConnPool> as Router>::ConnectionStream,
    )>,
);

type RouterMessageRequest = (url::Url, Envelope);
type CloseSender = watch::Sender<Option<CloseResponseSender>>;
type CloseResponseSender = mpsc::Sender<Result<(), RoutingError>>;
type CloseReceiver = watch::Receiver<Option<CloseResponseSender>>;

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
        let (close_tx, close_rx) = watch::channel(None);

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
            .broadcast(Some(result_tx))
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
            .map_err(|_| RoutingError::CloseError)?
            .await
            .map_err(|_| RoutingError::CloseError)?
            .map_err(|_| RoutingError::CloseError)?;

        Ok(())
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
struct TaskManager<Pool: ConnectionPool> {
    conn_request_rx: mpsc::Receiver<RouterConnRequest>,
    message_request_rx: mpsc::Receiver<RouterMessageRequest>,
    connection_pool: Pool,
    close_rx: CloseReceiver,
    config: RouterParams,
}

impl<Pool: ConnectionPool> TaskManager<Pool> {
    fn new(
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

    async fn run(self) -> Result<(), RoutingError> {
        let TaskManager {
            conn_request_rx,
            message_request_rx,
            connection_pool,
            close_rx,
            config,
        } = self;

        let mut host_managers: HashMap<url::Url, HostManagerHandle> = HashMap::new();

        let mut rx = combine_router_task(conn_request_rx, message_request_rx, close_rx.clone());

        loop {
            let task = rx.next().await.ok_or(RoutingError::ConnectionError)?;

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
                        .send((sink.clone().map_err_into::<RoutingError>(), stream))
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
                    if let Some(mut close_response_tx) = close_rx {
                        drop(rx);

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

fn combine_router_task(
    conn_request_rx: mpsc::Receiver<RouterConnRequest>,
    message_request_rx: mpsc::Receiver<RouterMessageRequest>,
    close_rx: CloseReceiver,
) -> impl stream::Stream<Item = RouterTask> + Send + 'static {
    let conn_requests = conn_request_rx.map(RouterTask::Connect);
    let message_requests =
        message_request_rx.map(|payload| RouterTask::SendMessage(Box::new(payload)));
    let close_requests = close_rx.map(RouterTask::Close);
    stream::select(
        stream::select(conn_requests, message_requests),
        close_requests,
    )
}

/// A connection request is used by the [`OutgoingHostTask`] to request a connection when
/// it is trying to send a message.
pub(crate) struct ConnectionRequest {
    request_tx: oneshot::Sender<Result<ConnectionSender, RoutingError>>,
    //If the connection should be recreated or returned from cache.
    recreate: bool,
}

impl ConnectionRequest {
    fn new(
        request_tx: oneshot::Sender<Result<ConnectionSender, RoutingError>>,
        recreate: bool,
    ) -> Self {
        ConnectionRequest {
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
    Connect(ConnectionRequest),
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

        let (incoming_task, mut incoming_task_tx) =
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

        let mut rx = combine_host_streams(connection_request_rx, stream_registrator_rx, close_rx);

        loop {
            let task = rx.next().await.ok_or(RoutingError::ConnectionError)?;

            match task {
                HostTask::Connect(ConnectionRequest {
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
                    if let Some(mut close_response_tx) = close_rx {
                        drop(rx);

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

fn combine_host_streams(
    connection_requests_rx: mpsc::Receiver<ConnectionRequest>,
    stream_registrator_rx: mpsc::Receiver<SubscriberRequest>,
    close_rx: CloseReceiver,
) -> impl stream::Stream<Item = HostTask> + Send + 'static {
    let connection_requests = connection_requests_rx.map(HostTask::Connect);
    let stream_reg_requests = stream_registrator_rx.map(HostTask::Subscribe);
    let close_requests = close_rx.map(HostTask::Close);
    stream::select(
        stream::select(connection_requests, stream_reg_requests),
        close_requests,
    )
}

type SwimRouterConnectionFut = Sequenced<
    RequestFuture<RouterConnRequest>,
    oneshot::Receiver<(
        <SwimRouter<SwimConnPool> as Router>::ConnectionSink,
        <SwimRouter<SwimConnPool> as Router>::ConnectionStream,
    )>,
>;

fn connect(
    target: AbsolutePath,
    router_connection_request_tx: mpsc::Sender<RouterConnRequest>,
) -> SwimRouterConnectionFut {
    let (response_tx, response_rx) = oneshot::channel();
    let request_future = RequestFuture::new(router_connection_request_tx, (target, response_tx));
    Sequenced::new(request_future, response_rx)
}

impl<Pool: ConnectionPool> Router for SwimRouter<Pool> {
    type ConnectionStream = mpsc::Receiver<RouterEvent>;
    type ConnectionSink = SenderErrInto<mpsc::Sender<Envelope>, RoutingError>;
    type GeneralSink = SenderErrInto<mpsc::Sender<(url::Url, Envelope)>, RoutingError>;
    type ConnectionFut = SwimRouterConnectionFut;

    fn connection_for(&mut self, target: &AbsolutePath) -> Self::ConnectionFut {
        connect(target.clone(), self.router_connection_request_tx.clone())
    }

    fn general_sink(&mut self) -> Self::GeneralSink {
        self.router_sink_tx.clone().map_err_into::<RoutingError>()
    }
}

// An error returned by the router
#[derive(Clone, Debug, PartialEq)]
pub enum RoutingError {
    // The connection to the remote host has been lost.
    ConnectionError,
    // The remote host is unreachable.
    HostUnreachable,
    // The connection pool has encountered an error.
    PoolError(ConnectionError),
    // The router has been stopped.
    RouterDropped,
    // The router has encountered an error while stopping.
    CloseError,
}

impl RoutingError {
    /// Returns whether or not the router can recover from the error.
    /// Inverse of [`is_fatal`].
    pub fn is_transient(&self) -> bool {
        match &self {
            RoutingError::ConnectionError => true,
            RoutingError::HostUnreachable => true,
            RoutingError::PoolError(ConnectionError::ConnectionRefused) => true,
            _ => false,
        }
    }

    /// Returns whether or not the error is unrecoverable.
    /// Inverse of [`is_transient`].
    pub fn is_fatal(&self) -> bool {
        !self.is_transient()
    }
}

impl Display for RoutingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingError::ConnectionError => write!(f, "Connection error."),
            RoutingError::HostUnreachable => write!(f, "Host unreachable."),
            RoutingError::PoolError(e) => write!(f, "Connection pool error. {}", e),
            RoutingError::RouterDropped => write!(f, "Router was dropped."),
            RoutingError::CloseError => write!(f, "Closing error."),
        }
    }
}

impl Error for RoutingError {}

impl<T> From<SendError<T>> for RoutingError {
    fn from(_: SendError<T>) -> Self {
        RoutingError::RouterDropped
    }
}

impl From<RoutingError> for RequestError {
    fn from(_: RoutingError) -> Self {
        RequestError {}
    }
}
