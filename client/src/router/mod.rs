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

use crate::configuration::router::RouterParams;
use crate::connections::{ConnectionError, ConnectionPool, ConnectionSender};
use crate::router::incoming::{IncomingHostTask, IncomingRequest};
use crate::router::outgoing::OutgoingHostTask;
use common::request::request_future::{RequestError, RequestFuture, Sequenced};
use common::sink::item::map_err::SenderErrInto;
use common::sink::item::ItemSender;
use common::warp::envelope::Envelope;
use common::warp::path::{AbsolutePath, RelativePath};
use futures::stream;
use futures::{Future, Stream};
use tokio::stream::StreamExt;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

pub mod incoming;
pub mod outgoing;
mod retry;

#[cfg(test)]
mod tests;

pub trait Router: Send {
    type ConnectionStream: Stream<Item = RouterEvent> + Send + 'static;
    type ConnectionSink: ItemSender<Envelope, RoutingError> + Send + Clone + Sync + 'static;
    type GeneralSink: ItemSender<(url::Url, Envelope), RoutingError> + Send + 'static;

    type ConnectionFut: Future<Output = Result<(Self::ConnectionSink, Self::ConnectionStream), RequestError>>
        + Send;

    fn connection_for(&mut self, target: &AbsolutePath) -> Self::ConnectionFut;

    fn general_sink(&mut self) -> Self::GeneralSink;
}

pub type RouterConnRequest = (
    AbsolutePath,
    oneshot::Sender<(
        <SwimRouter as Router>::ConnectionSink,
        <SwimRouter as Router>::ConnectionStream,
    )>,
);

pub type RouterMessageRequest = (url::Url, Envelope);

pub type CloseSender = watch::Sender<Option<CloseResponseSender>>;
pub type CloseReceiver = watch::Receiver<Option<CloseResponseSender>>;
pub type CloseResponseSender = mpsc::Sender<Result<(), RoutingError>>;

#[derive(Debug, Clone, PartialEq)]
pub enum RouterEvent {
    Envelope(Envelope),
    ConnectionClosed,
    /// The requested host is unreachable. Field contains the error message returned from the
    /// connection pool.
    Unreachable(String),
    Stopping,
}

pub struct SwimRouter {
    router_connection_request_tx: mpsc::Sender<RouterConnRequest>,
    router_sink_tx: mpsc::Sender<RouterMessageRequest>,
    task_manager_handle: JoinHandle<Result<(), RoutingError>>,
    close_tx: CloseSender,
    configuration: RouterParams,
}

impl SwimRouter {
    pub fn new<Pool>(configuration: RouterParams, connection_pool: Pool) -> SwimRouter
    where
        Pool: ConnectionPool,
    {
        let (close_tx, close_rx) = watch::channel(None);

        let (task_manager, router_connection_request_tx, router_sink_tx) =
            TaskManager::new(connection_pool, close_rx, configuration);

        let task_manager_handle = tokio::spawn(task_manager.run());

        SwimRouter {
            router_connection_request_tx,
            router_sink_tx,
            task_manager_handle,
            close_tx,
            configuration,
        }
    }

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

        Ok(())
    }
}

enum RouterTask {
    Connect(RouterConnRequest),
    SendMessage(Box<RouterMessageRequest>),
    Close(Option<CloseResponseSender>),
}

type HostManagerHandle = (
    mpsc::Sender<Envelope>,
    mpsc::Sender<SubscriberRequest>,
    JoinHandle<Result<(), RoutingError>>,
);

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
                        target.clone(),
                        connection_pool.clone(),
                        close_rx.clone(),
                        config,
                    );

                    let (subscriber_tx, stream) = mpsc::channel(config.buffer_size().get());

                    let (_, relative_path) = target.split();

                    stream_registrator
                        .send((relative_path, subscriber_tx))
                        .await
                        .map_err(|_| RoutingError::ConnectionError)?;

                    response_tx
                        .send((sink.clone().map_err_into::<RoutingError>(), stream))
                        .map_err(|_| RoutingError::ConnectionError)?;
                }

                RouterTask::SendMessage(payload) => {
                    let (host, message) = payload.deref();

                    let target = message
                        .relative_path()
                        .ok_or(RoutingError::ConnectionError)?
                        .for_host(host.clone());

                    let (sink, _, _) = get_host_manager(
                        &mut host_managers,
                        target,
                        connection_pool.clone(),
                        close_rx.clone(),
                        config,
                    );

                    sink.send(message.clone())
                        .await
                        .map_err(|_| RoutingError::ConnectionError)?;
                }

                RouterTask::Close(Some(mut close_tx)) => {
                    drop(rx);

                    for (_, (_, _, handle)) in host_managers {
                        close_tx
                            .send(handle.await.map_err(|_| RoutingError::CloseError)?)
                            .await
                            .map_err(|_| RoutingError::CloseError)?;
                    }

                    let close_result: Result<(), ConnectionError> = connection_pool
                        .close()
                        .map_err(|_| RoutingError::ConnectionError)?
                        .await
                        .map_err(|_| RoutingError::ConnectionError)?;

                    close_tx
                        .send(close_result.map_err(|_| RoutingError::ConnectionError))
                        .await
                        .map_err(|_| RoutingError::CloseError)?;

                    break Ok(());
                }

                RouterTask::Close(None) => {}
            }
        }
    }
}

fn get_host_manager<Pool>(
    host_managers: &mut HashMap<url::Url, HostManagerHandle>,
    target: AbsolutePath,
    connection_pool: Pool,
    close_rx: CloseReceiver,
    config: RouterParams,
) -> &mut HostManagerHandle
where
    Pool: ConnectionPool,
{
    let (host, _) = target.split();
    host_managers.entry(host.clone()).or_insert_with(|| {
        let (host_manager, sink, stream_registrator) =
            HostManager::new(host, connection_pool, close_rx, config);
        (sink, stream_registrator, tokio::spawn(host_manager.run()))
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

pub type ConnectionRequest = (
    oneshot::Sender<Result<ConnectionSender, RoutingError>>,
    bool, // Whether or not to recreate the connection
);

type SubscriberRequest = (RelativePath, mpsc::Sender<RouterEvent>);

enum HostTask {
    Connect(ConnectionRequest),
    Subscribe(SubscriberRequest),
    Close(Option<CloseResponseSender>),
}

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

        let incoming_handle = tokio::spawn(incoming_task.run());
        let outgoing_handle = tokio::spawn(outgoing_task.run());

        let mut rx = combine_host_streams(connection_request_rx, stream_registrator_rx, close_rx);

        loop {
            let task = rx.next().await.ok_or(RoutingError::ConnectionError)?;

            match task {
                HostTask::Connect((connection_response_tx, recreate)) => {
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
                HostTask::Subscribe((relative_path, event_tx)) => {
                    incoming_task_tx
                        .send(IncomingRequest::Subscribe((relative_path, event_tx)))
                        .await
                        .map_err(|_| RoutingError::ConnectionError)?;
                }
                HostTask::Close(Some(mut close_tx)) => {
                    drop(rx);

                    close_tx
                        .send(
                            incoming_handle
                                .await
                                .map_err(|_| RoutingError::CloseError)?,
                        )
                        .await
                        .map_err(|_| RoutingError::CloseError)?;

                    close_tx
                        .send(
                            outgoing_handle
                                .await
                                .map_err(|_| RoutingError::CloseError)?,
                        )
                        .await
                        .map_err(|_| RoutingError::CloseError)?;

                    break Ok(());
                }
                HostTask::Close(None) => {
                    break Ok(());
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
        <SwimRouter as Router>::ConnectionSink,
        <SwimRouter as Router>::ConnectionStream,
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

impl Router for SwimRouter {
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

#[derive(Clone, Debug, PartialEq)]
pub enum RoutingError {
    RouterDropped,
    ConnectionError,
    PoolError(ConnectionError),
    CloseError,
}

impl RoutingError {
    fn is_transient(&self) -> bool {
        match self {
            RoutingError::ConnectionError => true,
            _ => false,
        }
    }
}

impl Display for RoutingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingError::RouterDropped => write!(f, "Router was dropped."),
            RoutingError::ConnectionError => write!(f, "Connection error."),
            RoutingError::CloseError => write!(f, "Closing error."),
            RoutingError::PoolError(e) => write!(f, "Connection pool error. {}", e),
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
