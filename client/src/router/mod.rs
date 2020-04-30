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

use std::error::Error;
use std::fmt::{Display, Formatter};

use futures::future::Ready;
use futures::stream;
use futures::{Future, Stream};
use tokio::stream::StreamExt;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use common::request::request_future::{RequestError, RequestFuture, Sequenced};
use common::sink::item::map_err::SenderErrInto;
use common::sink::item::ItemSender;
use common::warp::envelope::Envelope;
use common::warp::path::AbsolutePath;

use crate::connections::factory::tungstenite::TungsteniteWsFactory;
use crate::connections::{ConnectionError, ConnectionPool, ConnectionSender};
use crate::configuration::router::RouterParams;
use crate::router::incoming::{IncomingHostTask, IncomingRequest};
use crate::router::outgoing::OutgoingHostTask;

use crate::router::outgoing::retry::RetryStrategy;
use std::collections::HashMap;

pub mod command;
pub mod incoming;
pub mod outgoing;

#[cfg(test)]
mod tests;

pub trait Router: Send {
    type ConnectionStream: Stream<Item = RouterEvent> + Send + 'static;
    type ConnectionSink: ItemSender<Envelope, RoutingError> + Send + Clone + Sync + 'static;
    type GeneralSink: ItemSender<(String, Envelope), RoutingError> + Send + 'static;

    type ConnectionFut: Future<Output = Result<(Self::ConnectionSink, Self::ConnectionStream), RequestError>>
        + Send;
    type GeneralFut: Future<Output = Self::GeneralSink> + Send;

    fn connection_for(&mut self, target: &AbsolutePath) -> Self::ConnectionFut;

    fn general_sink(&mut self) -> Self::GeneralFut;
}

type Host = String;

pub type ConnectionRequest = (
    AbsolutePath,
    oneshot::Sender<(
        <SwimRouter as Router>::ConnectionSink,
        <SwimRouter as Router>::ConnectionStream,
    )>,
);

pub struct SwimRouter {
    router_connection_request_tx: mpsc::Sender<ConnectionRequest>,
    _task_manager_handler: JoinHandle<Result<(), RoutingError>>,
    _configuration: RouterParams,
}

impl SwimRouter {
    pub async fn new(configuration: RouterParams) -> SwimRouter {
        let buffer_size = configuration.buffer_size().get();

        let connection_pool =
            ConnectionPool::new(buffer_size, TungsteniteWsFactory::new(buffer_size).await);

        // TODO: Accept as an argument
        let configuration = RouterConfigBuilder::default()
            .with_buffer_size(buffer_size)
            .with_idle_timeout(5)
            .with_conn_reaper_frequency(10)
            .with_retry_stategy(RetryStrategy::exponential(
                Duration::from_secs(8),
                // Indefinately try to send requests
                None,
            ))
            .build();

        let (task_manager, router_connection_tx) = TaskManager::new(connection_pool, configuration);
        let task_manager_handler = tokio::spawn(task_manager.run());

        SwimRouter {
            router_connection_request_tx: router_connection_tx,
            _task_manager_handler: task_manager_handler,
            _configuration: configuration,
        }
    }

    pub async fn close(self) {
        //Todo impl
    }

    pub fn send_command(
        &mut self,
        _target: &AbsolutePath,
        _message: String,
    ) -> Result<(), RoutingError> {
        //Todo impl

        // let AbsolutePath { host, node, lane } = target;
        //
        // self.command_tx
        //     .try_send((
        //         AbsolutePath {
        //             host: host.clone(),
        //             node: node.clone(),
        //             lane: lane.clone(),
        //         },
        //         message,
        //     ))
        //     .map_err(|_| RoutingError::ConnectionError)?;
        //
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RouterEvent {
    Envelope(Envelope),
    ConnectionClosed,
    Unreachable,
    Stopping,
}

struct TaskManager {
    //Todo maybe change this with combinators?
    request_rx: mpsc::Receiver<ConnectionRequest>,
    connection_pool: ConnectionPool,
    config: RouterConfig,
}

impl TaskManager {
    fn new(
        connection_pool: ConnectionPool,
        config: RouterParams,
    ) -> (Self, mpsc::Sender<ConnectionRequest>) {
        let (request_tx, request_rx) = mpsc::channel(config.buffer_size().get());
        (
            TaskManager {
                request_rx,
                connection_pool,
                config,
            },
            request_tx,
        )
    }

    async fn run(self) -> Result<(), RoutingError> {
        let TaskManager {
            mut request_rx,
            connection_pool,
            config,
        } = self;

        let mut host_managers: HashMap<
            String,
            (
                mpsc::Sender<Envelope>,
                mpsc::Sender<mpsc::Sender<RouterEvent>>,
            ),
        > = HashMap::new();

        loop {
            let (target, response_tx) = request_rx
                .recv()
                .await
                .ok_or(RoutingError::ConnectionError)?;

            if !host_managers.contains_key(&target.host) {
                let (host_manager, sink, stream_registrator) =
                    HostManager::new(target.clone(), connection_pool.clone(), config);
                host_managers.insert(target.host.clone(), (sink, stream_registrator));
                tokio::spawn(host_manager.run());
            }

            let (sink, mut stream_registrator) = host_managers
                .get(&target.host)
                .ok_or(RoutingError::ConnectionError)?
                .clone();

            let (subscriber_tx, stream) = mpsc::channel(config.buffer_size().get());

            stream_registrator
                .send(subscriber_tx)
                .await
                .map_err(|_| RoutingError::ConnectionError)?;

            response_tx
                .send((sink.map_err_into::<RoutingError>(), stream))
                .map_err(|_| RoutingError::ConnectionError)?;
        }
    }

    fn close() {
        //Todo impl
    }
}

enum HostTask {
    Connect((oneshot::Sender<ConnectionSender>, bool)),
    Subscribe(mpsc::Sender<RouterEvent>),
}

struct HostManager {
    target: AbsolutePath,
    connection_pool: ConnectionPool,
    sink_rx: mpsc::Receiver<Envelope>,
    stream_registrator_rx: mpsc::Receiver<mpsc::Sender<RouterEvent>>,
    config: RouterConfig,
}

impl HostManager {
    fn new(
        target: AbsolutePath,
        connection_pool: ConnectionPool,
        config: RouterConfig,
    ) -> (
        HostManager,
        mpsc::Sender<Envelope>,
        mpsc::Sender<mpsc::Sender<RouterEvent>>,
    ) {
        let (sink_tx, sink_rx) = mpsc::channel(config.buffer_size().get());
        let (stream_registrator_tx, stream_registrator_rx) =
            mpsc::channel(config.buffer_size().get());

        (
            HostManager {
                target,
                connection_pool,
                sink_rx,
                stream_registrator_rx,
                config,
            },
            sink_tx,
            stream_registrator_tx,
        )
    }

    async fn run(self) -> Result<(), RoutingError> {
        let HostManager {
            target,
            mut connection_pool,
            sink_rx,
            stream_registrator_rx,
            config,
        } = self;

        //Todo report to the downlink if this fails
        let host_url = url::Url::parse(&target.host).map_err(|_| RoutingError::ConnectionError)?;

        let (connection_request_tx, connection_request_rx) =
            mpsc::channel(config.buffer_size().get());

        let (incoming_task, mut incoming_task_tx) =
            IncomingHostTask::new(config.buffer_size().get());
        let outgoing_task = OutgoingHostTask::new(sink_rx, connection_request_tx, config);

        tokio::spawn(incoming_task.run());
        tokio::spawn(outgoing_task.run());

        let mut rx = combine_host_streams(connection_request_rx, stream_registrator_rx);
        loop {
            let task = rx.next().await.ok_or(RoutingError::ConnectionError)?;

            match task {
                HostTask::Connect((connection_response_tx, recreate)) => {
                    let maybe_connection_channel = connection_pool
                        .request_connection(host_url.clone(), recreate)
                        .map_err(|_| RoutingError::ConnectionError)?
                        .await
                        .map_err(|_| RoutingError::ConnectionError)?;

                    match maybe_connection_channel {
                        Ok((connection_tx, maybe_connection_rx)) => {
                            connection_response_tx
                                .send(connection_tx)
                                .map_err(|_| RoutingError::ConnectionError)?;

                            if let Some(connection_rx) = maybe_connection_rx {
                                incoming_task_tx
                                    .send(IncomingRequest::Connection(connection_rx))
                                    .await
                                    .map_err(|_| RoutingError::ConnectionError)?;
                            }
                        }
                        Err(connection_error) => match connection_error {
                            ConnectionError::Transient => {
                                // let _ = connection_response_tx.send(Err(RoutingError::Transient));
                            }
                            _ => {
                                // let _ =
                                //     connection_response_tx.send(Err(RoutingError::ConnectionError));

                                let _ = incoming_task_tx.send(IncomingRequest::Unreachable).await;
                            }
                        },
                    }
                }
                HostTask::Subscribe(event_tx) => {
                    incoming_task_tx
                        .send(IncomingRequest::Subscribe((target.clone(), event_tx)))
                        .await
                        .map_err(|_| RoutingError::ConnectionError)?;
                }
            }
        }
    }

    fn close() {
        //Todo impl
    }
}

fn combine_host_streams(
    connection_requests_rx: mpsc::Receiver<(oneshot::Sender<ConnectionSender>, bool)>,
    stream_registrator_rx: mpsc::Receiver<mpsc::Sender<RouterEvent>>,
) -> impl stream::Stream<Item = HostTask> + Send + 'static {
    let connection_requests = connection_requests_rx.map(HostTask::Connect);
    let stream_reg_requests = stream_registrator_rx.map(HostTask::Subscribe);
    stream::select(connection_requests, stream_reg_requests)
}

type SwimRouterConnectionFut = Sequenced<
    RequestFuture<ConnectionRequest>,
    oneshot::Receiver<(
        <SwimRouter as Router>::ConnectionSink,
        <SwimRouter as Router>::ConnectionStream,
    )>,
>;

pub fn connect(
    target: AbsolutePath,
    router_connection_request_tx: mpsc::Sender<ConnectionRequest>,
) -> SwimRouterConnectionFut {
    let (response_tx, response_rx) = oneshot::channel();
    let request_future = RequestFuture::new(router_connection_request_tx, (target, response_tx));
    Sequenced::new(request_future, response_rx)
}

impl Router for SwimRouter {
    type ConnectionStream = mpsc::Receiver<RouterEvent>;
    type ConnectionSink = SenderErrInto<mpsc::Sender<Envelope>, RoutingError>;
    type GeneralSink = SenderErrInto<mpsc::Sender<(String, Envelope)>, RoutingError>;
    type ConnectionFut = SwimRouterConnectionFut;
    type GeneralFut = Ready<Self::GeneralSink>;

    fn connection_for(&mut self, target: &AbsolutePath) -> Self::ConnectionFut {
        connect(target.clone(), self.router_connection_request_tx.clone())
    }

    fn general_sink(&mut self) -> Self::GeneralFut {
        //Todo
        unimplemented!()
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RoutingError {
    Transient,
    RouterDropped,
    ConnectionError,
}

impl Display for RoutingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingError::RouterDropped => write!(f, "Router was dropped."),
            RoutingError::ConnectionError => write!(f, "Connection error."),
            RoutingError::Transient => write!(f, "Transient error"),
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
