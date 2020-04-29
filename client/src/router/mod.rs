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
use std::pin::Pin;

use futures::future::Ready;
use futures::stream;
use futures::task::{Context, Poll};
use futures::{Future, Stream};
use tokio::stream::StreamExt;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Duration;

use common::request::request_future::{RequestError, RequestFuture, Sequenced};
use common::sink::item::map_err::SenderErrInto;
use common::sink::item::ItemSender;
use common::warp::envelope::Envelope;
use common::warp::path::AbsolutePath;

use crate::connections::factory::tungstenite::TungsteniteWsFactory;
use crate::connections::{ConnectionError, ConnectionPool, ConnectionSender};
use crate::router::configuration::{RouterConfig, RouterConfigBuilder};
use crate::router::incoming::IncomingHostTask;
use crate::router::outgoing::OutgoingHostTask;

use std::collections::HashMap;
use tokio::sync::mpsc::Receiver;
use tokio_tungstenite::tungstenite::protocol::Message;
use url::quirks::host;
use crate::router::outgoing::retry::RetryStrategy;

pub mod command;
pub mod configuration;
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
pub type CloseRequestSender = mpsc::Sender<()>;
pub type CloseRequestReceiver = mpsc::Receiver<()>;

pub type ConnectionRequest = (
    AbsolutePath,
    oneshot::Sender<(
        <SwimRouter as Router>::ConnectionSink,
        <SwimRouter as Router>::ConnectionStream,
    )>,
);

pub struct SwimRouter {
    router_connection_request_tx: mpsc::Sender<ConnectionRequest>,
    task_manager_handler: JoinHandle<Result<(), RoutingError>>,
    configuration: RouterConfig,
}

impl SwimRouter {
    pub async fn new(buffer_size: usize) -> SwimRouter {
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
            task_manager_handler,
            configuration,
        }
    }

    pub async fn close(mut self) {
        // self.outgoing_task_close_request_tx.send(()).await.unwrap();
        // let _ = self.outgoing_task_handler.await.unwrap();
        //
        // self.command_task_close_request_tx.send(()).await.unwrap();
        // let _ = self.command_task_handler.await.unwrap();
        //
        // self.connections_task_close_request_tx
        //     .send(())
        //     .await
        //     .unwrap();
        // let _ = self.connection_request_handler.await.unwrap();
        //
        // self.incoming_task_close_request_tx.send(()).await.unwrap();
        // let _ = self.incoming_task_handler.await.unwrap();
    }

    pub fn send_command(
        &mut self,
        target: &AbsolutePath,
        message: String,
    ) -> Result<(), RoutingError> {
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
        config: RouterConfig,
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

            response_tx.send((sink.map_err_into::<RoutingError>(), stream));
        }
    }

    fn close() {
        //Todo impl
    }
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

        //Todo this should be outgoing_task_request_tx
        let (connection_request_tx, mut connection_request_rx) =
            mpsc::channel(config.buffer_size().get());

        let (incoming_task, foo) = IncomingHostTask::new(config.buffer_size().get());
        let (outgoing_task, bar) = OutgoingHostTask::new(sink_rx, connection_request_tx, config);

        tokio::spawn(incoming_task.run());
        tokio::spawn(outgoing_task.run());

        loop {
            //Todo merge with stream_registrator_rx
            let (connection_response_tx, recreate) = connection_request_rx
                .recv()
                .await
                .ok_or(RoutingError::ConnectionError)?;

            let maybe_connection_channel = if recreate {
                connection_pool.recreate_connection(host_url.clone())
            } else {
                connection_pool.request_connection(host_url.clone())
            }
            .map_err(|_| RoutingError::ConnectionError)?
            .await
            .map_err(|_| RoutingError::ConnectionError)?;

            match maybe_connection_channel {
                Ok((connection_sender, maybe_connection_receiver)) => {
                    connection_response_tx
                        .send(connection_sender)
                        .map_err(|_| RoutingError::ConnectionError)?;

                    if let Some(connection_receiver) = maybe_connection_receiver {
                        //Todo push the connection_receiver to the incoming task
                    }
                }
                Err(connection_error) => {
                    //Todo handle errors
                }
            }
        }
    }

    fn close() {
        //Todo impl
    }
}

pub enum ConnectionResponse {
    Success((Host, mpsc::Receiver<Message>)),
    Failure(Host),
}

pub type ConnReqSender = mpsc::Sender<(
    Host,
    oneshot::Sender<Result<ConnectionSender, RoutingError>>,
    bool, // Whether or not to recreate the connection
)>;
type ConnReqReceiver = mpsc::Receiver<(
    Host,
    oneshot::Sender<Result<ConnectionSender, RoutingError>>,
    bool, // Whether or not to recreate the connection
)>;

// struct ConnectionRequestTask {
//     connection_pool: ConnectionPool,
//     connection_request_rx: ConnReqReceiver,
//     close_request_rx: CloseRequestReceiver,
//     incoming_task_request_tx: IncomingTaskReqSender,
// }
//
// impl ConnectionRequestTask {
//     fn new(
//         connection_pool: ConnectionPool,
//         incoming_task_request_tx: IncomingTaskReqSender,
//         config: RouterConfig,
//     ) -> (Self, ConnReqSender, CloseRequestSender) {
//         let (connection_request_tx, connection_request_rx) =
//             mpsc::channel(config.buffer_size().get());
//         let (close_request_tx, close_request_rx) = mpsc::channel(config.buffer_size().get());
//
//         (
//             ConnectionRequestTask {
//                 connection_pool,
//                 connection_request_rx,
//                 close_request_rx,
//                 incoming_task_request_tx,
//             },
//             connection_request_tx,
//             close_request_tx,
//         )
//     }
//
//     async fn run(self) -> Result<(), RoutingError> {
//         let ConnectionRequestTask {
//             mut connection_pool,
//             connection_request_rx,
//             close_request_rx,
//             mut incoming_task_request_tx,
//         } = self;
//
//         let mut rx = combine_router_streams(connection_request_rx, close_request_rx);
//
//         while let ConnectionRequestType::NewConnection {
//             host,
//             connection_tx,
//             recreate,
//         } = rx.next().await.ok_or(RoutingError::ConnectionError)?
//         {
//             let host_url = url::Url::parse(&host).map_err(|_| RoutingError::ConnectionError)?;
//
//             let connection = if recreate {
//                 connection_pool
//                     .recreate_connection(host_url.clone())
//                     .map_err(|_| RoutingError::ConnectionError)?
//                     .await
//             } else {
//                 connection_pool
//                     .request_connection(host_url.clone())
//                     .map_err(|_| RoutingError::ConnectionError)?
//                     .await
//             }
//             .map_err(|_| RoutingError::ConnectionError)?;
//
//             match connection {
//                 Ok((connection_sender, connection_receiver)) => {
//                     connection_tx
//                         .send(Ok(connection_sender))
//                         .map_err(|_| RoutingError::ConnectionError)?;
//
//                     if let Some(receiver) = connection_receiver {
//                         incoming_task_request_tx
//                             .send(ConnectionResponse::Success((host, receiver)))
//                             .await
//                             .map_err(|_| RoutingError::ConnectionError)?
//                     }
//                 }
//
//                 Err(e) => {
//                     // Need to return an error to the envelope routing task so that it can cancel
//                     // the active request and not attempt it again the request again. Some errors
//                     // are transient and they may resolve themselves after waiting
//                     match e {
//                         // Transient error that may be recoverable
//                         ConnectionError::Transient => {
//                             let _ = connection_tx.send(Err(RoutingError::Transient));
//                         }
//                         // Permanent, unrecoverable error
//                         _ => {
//                             let _ = connection_tx.send(Err(RoutingError::ConnectionError));
//                             let _ = incoming_task_request_tx
//                                 .send(ConnectionResponse::Failure(host))
//                                 .await;
//                         }
//                     }
//                 }
//             }
//         }
//         connection_pool.close().await;
//         Ok(())
//     }
// }
//
// enum ConnectionRequestType {
//     NewConnection {
//         host: Host,
//         connection_tx: oneshot::Sender<Result<ConnectionSender, RoutingError>>,
//         recreate: bool,
//     },
//     Close,
// }

// fn combine_router_streams(
//     connection_requests_rx: ConnReqReceiver,
//     close_requests_rx: CloseRequestReceiver,
// ) -> impl stream::Stream<Item = ConnectionRequestType> + Send + 'static {
//     let connection_requests =
//         connection_requests_rx.map(|r| ConnectionRequestType::NewConnection {
//             host: r.0,
//             connection_tx: r.1,
//             recreate: r.2,
//         });
//     let close_request = close_requests_rx.map(|_| ConnectionRequestType::Close);
//     stream::select(connection_requests, close_request)
// }

// type SwimRouterConnection = (
//     SenderErrInto<mpsc::Sender<Envelope>, RoutingError>,
//     mpsc::Receiver<RouterEvent>,
// );

// pub struct ConnectionFuture {
//     outgoing_rx: oneshot::Receiver<mpsc::Sender<Envelope>>,
//     incoming_rx: Option<mpsc::Receiver<RouterEvent>>,
// }

// impl ConnectionFuture {
//     fn new(
//         outgoing_rx: oneshot::Receiver<mpsc::Sender<Envelope>>,
//         incoming_rx: mpsc::Receiver<RouterEvent>,
//     ) -> ConnectionFuture {
//         ConnectionFuture {
//             outgoing_rx,
//             incoming_rx: Some(incoming_rx),
//         }
//     }
// }
//
// impl Future for ConnectionFuture {
//     type Output = Result<SwimRouterConnection, RoutingError>;
//
//     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         let ConnectionFuture {
//             outgoing_rx,
//             incoming_rx,
//         } = &mut self.get_mut();
//
//         oneshot::Receiver::poll(Pin::new(outgoing_rx), cx).map(|r| match r {
//             Ok(outgoing_rx) => Ok((
//                 outgoing_rx.map_err_into::<RoutingError>(),
//                 incoming_rx.take().ok_or(RoutingError::ConnectionError)?,
//             )),
//             _ => Err(RoutingError::ConnectionError),
//         })
//     }
// }

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
