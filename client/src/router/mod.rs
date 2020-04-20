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

use common::request::request_future::SendAndAwait;
use common::request::Request;
use common::sink::item::map_err::SenderErrInto;
use common::sink::item::ItemSender;
use common::warp::envelope::Envelope;
use common::warp::path::AbsolutePath;

use crate::connections::factory::tungstenite::TungsteniteWsFactory;
use crate::connections::{ConnectionError, ConnectionPool, ConnectionSender};
use crate::router::configuration::{RouterConfig, RouterConfigBuilder};
use crate::router::envelope_routing_task::retry::RetryStrategy;
use crate::router::envelope_routing_task::{
    HostEnvelopeTaskRequestSender, RequestEnvelopeRoutingHostTask,
};
use crate::router::message_routing_task::{
    ConnectionResponse, HostMessageNewTaskRequestSender, HostMessageRegisterTaskRequestSender,
    RequestMessageRoutingHostTask,
};

pub mod command_routing_task;
pub mod configuration;
pub mod envelope_routing_task;
pub mod message_routing_task;

#[cfg(test)]
mod tests;

pub trait Router: Send {
    type ConnectionStream: Stream<Item = RouterEvent> + Send + 'static;
    type ConnectionSink: ItemSender<Envelope, RoutingError> + Send + Clone + Sync + 'static;
    type GeneralSink: ItemSender<(String, Envelope), RoutingError> + Send + 'static;

    type ConnectionFut: Future<Output = (Self::ConnectionSink, Self::ConnectionStream)> + Send;
    type GeneralFut: Future<Output = Self::GeneralSink> + Send;

    fn connection_for(&mut self, target: &AbsolutePath) -> Self::ConnectionFut;

    fn general_sink(&mut self) -> Self::GeneralFut;
}

pub struct SwimRouter {
    configuration: RouterConfig,
    envelope_routing_host_request_tx: HostEnvelopeTaskRequestSender,
    message_routing_register_task_request_tx: HostMessageRegisterTaskRequestSender,
    request_connections_handler: JoinHandle<Result<(), RoutingError>>,
    connections_task_close_request_tx: CloseRequestSender,
    request_envelope_routing_host_handler: JoinHandle<Result<(), RoutingError>>,
    envelope_routing_task_close_request_tx: CloseRequestSender,
    request_message_routing_host_handler: JoinHandle<Result<(), RoutingError>>,
    message_routing_task_close_request_tx: CloseRequestSender,
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

        let (
            request_message_routing_host_task,
            message_routing_new_task_request_tx,
            message_routing_register_task_request_tx,
            message_routing_task_close_request_tx,
        ) = RequestMessageRoutingHostTask::new(buffer_size);

        let (request_connections_task, connection_request_tx, connections_task_close_request_tx) =
            RequestConnectionsTask::new(
                connection_pool,
                message_routing_new_task_request_tx.clone(),
                configuration,
            );

        let (
            request_envelope_routing_host_task,
            envelope_routing_host_request_tx,
            envelope_routing_task_close_request_tx,
        ) = RequestEnvelopeRoutingHostTask::new(
            connection_request_tx,
            configuration,
            message_routing_new_task_request_tx,
        );

        let request_connections_handler = tokio::spawn(request_connections_task.run());

        let request_envelope_routing_host_handler =
            tokio::spawn(request_envelope_routing_host_task.run());

        let request_message_routing_host_handler =
            tokio::spawn(request_message_routing_host_task.run());

        SwimRouter {
            configuration,
            envelope_routing_host_request_tx,
            message_routing_register_task_request_tx,
            request_connections_handler,
            connections_task_close_request_tx,
            request_envelope_routing_host_handler,
            envelope_routing_task_close_request_tx,
            request_message_routing_host_handler,
            message_routing_task_close_request_tx,
        }
    }

    pub async fn close(mut self) {
        self.envelope_routing_task_close_request_tx
            .send(())
            .await
            .unwrap();

        let _ = self.request_envelope_routing_host_handler.await.unwrap();

        self.connections_task_close_request_tx
            .send(())
            .await
            .unwrap();

        let _ = self.request_connections_handler.await.unwrap();

        self.message_routing_task_close_request_tx
            .send(())
            .await
            .unwrap();

        let _ = self.request_message_routing_host_handler.await.unwrap();
    }
}

pub type ConnReqSender = mpsc::Sender<(
    url::Url,
    oneshot::Sender<Result<ConnectionSender, RoutingError>>,
    bool, // Whether or not to recreate the connection
)>;
type ConnReqReceiver = mpsc::Receiver<(
    url::Url,
    oneshot::Sender<Result<ConnectionSender, RoutingError>>,
    bool, // Whether or not to recreate the connection
)>;

pub type CloseRequestSender = mpsc::Sender<()>;
pub type CloseRequestReceiver = mpsc::Receiver<()>;

struct RequestConnectionsTask {
    connection_pool: ConnectionPool,
    connection_request_rx: ConnReqReceiver,
    close_request_rx: CloseRequestReceiver,
    message_routing_new_task_request_tx: HostMessageNewTaskRequestSender,
}

impl RequestConnectionsTask {
    fn new(
        connection_pool: ConnectionPool,
        message_routing_new_task_request_tx: HostMessageNewTaskRequestSender,
        config: RouterConfig,
    ) -> (Self, ConnReqSender, CloseRequestSender) {
        let (connection_request_tx, connection_request_rx) =
            mpsc::channel(config.buffer_size().get());
        let (close_request_tx, close_request_rx) = mpsc::channel(config.buffer_size().get());

        (
            RequestConnectionsTask {
                connection_pool,
                connection_request_rx,
                close_request_rx,
                message_routing_new_task_request_tx,
            },
            connection_request_tx,
            close_request_tx,
        )
    }

    async fn run(self) -> Result<(), RoutingError> {
        let RequestConnectionsTask {
            mut connection_pool,
            connection_request_rx,
            close_request_rx,
            mut message_routing_new_task_request_tx,
        } = self;

        let mut rx = combine_receive_connection_requests(connection_request_rx, close_request_rx);

        while let ConnectionsRequestType::Connection {
            host,
            connection_tx,
            recreate,
        } = rx.next().await.ok_or(RoutingError::ConnectionError)?
        {
            let connection = if recreate {
                connection_pool
                    .recreate_connection(host.clone())
                    .map_err(|_| RoutingError::ConnectionError)?
                    .await
            } else {
                connection_pool
                    .request_connection(host.clone())
                    .map_err(|_| RoutingError::ConnectionError)?
                    .await
            }
            .map_err(|_| RoutingError::ConnectionError)?;

            match connection {
                Ok((connection_sender, connection_receiver)) => {
                    connection_tx
                        .send(Ok(connection_sender))
                        .map_err(|_| RoutingError::ConnectionError)?;

                    if let Some(receiver) = connection_receiver {
                        message_routing_new_task_request_tx
                            .send(ConnectionResponse::Success((host, receiver)))
                            .await
                            .map_err(|_| RoutingError::ConnectionError)?
                    }
                }

                Err(e) => {
                    // Need to return an error to the envelope routing task so that it can cancel
                    // the active request and not attempt it again the request again. Some errors
                    // are transient and they may resolve themselves after waiting
                    match e {
                        // Transient error that may be recoverable
                        ConnectionError::Transient => {
                            let _ = connection_tx.send(Err(RoutingError::Transient));
                        }
                        // Permanent, unrecoverable error
                        _ => {
                            let _ = connection_tx.send(Err(RoutingError::ConnectionError));
                            let _ = message_routing_new_task_request_tx
                                .send(ConnectionResponse::Failure(host))
                                .await;
                        }
                    }
                }
            }
        }
        connection_pool.close().await;
        Ok(())
    }
}

enum ConnectionsRequestType {
    Connection {
        host: url::Url,
        connection_tx: oneshot::Sender<Result<ConnectionSender, RoutingError>>,
        recreate: bool,
    },
    Close,
}

fn combine_receive_connection_requests(
    connection_requests_rx: ConnReqReceiver,
    close_requests_rx: CloseRequestReceiver,
) -> impl stream::Stream<Item = ConnectionsRequestType> + Send + 'static {
    let connection_requests = connection_requests_rx.map(|r| ConnectionsRequestType::Connection {
        host: r.0,
        connection_tx: r.1,
        recreate: r.2,
    });
    let close_request = close_requests_rx.map(|_| ConnectionsRequestType::Close);
    stream::select(connection_requests, close_request)
}

#[derive(Debug, Clone, PartialEq)]
pub enum RouterEvent {
    Envelope(Envelope),
    ConnectionClosed,
    Unreachable,
    Stopping,
}

pub struct SwimRouterConnection {
    envelope_task_request_tx: HostEnvelopeTaskRequestSender,
    message_register_task_request_tx: HostMessageRegisterTaskRequestSender,
    target: AbsolutePath,
    envelope_task_tx: Option<oneshot::Sender<mpsc::Sender<Envelope>>>,
    envelope_task_rx: oneshot::Receiver<mpsc::Sender<Envelope>>,
    event_tx: Option<mpsc::Sender<RouterEvent>>,
    event_rx: Option<mpsc::Receiver<RouterEvent>>,
}

impl Unpin for SwimRouterConnection {}

impl SwimRouterConnection {
    pub fn new(
        envelope_task_request_tx: HostEnvelopeTaskRequestSender,
        message_register_task_request_tx: HostMessageRegisterTaskRequestSender,
        target: AbsolutePath,
        buffer_size: usize,
    ) -> Self {
        let (envelope_task_tx, envelope_task_rx) = oneshot::channel();
        let (event_tx, event_rx) = mpsc::channel(buffer_size);

        // let host_url = url::Url::parse(&target.host).unwrap();
        // let lane = target.host;
        // let node = target.node;

        SwimRouterConnection {
            envelope_task_request_tx,
            message_register_task_request_tx,
            target,
            envelope_task_tx: Some(envelope_task_tx),
            envelope_task_rx,
            event_tx: Some(event_tx),
            event_rx: Some(event_rx),
        }
    }
}

impl Future for SwimRouterConnection {
    type Output = (
        SenderErrInto<mpsc::Sender<Envelope>, RoutingError>,
        mpsc::Receiver<RouterEvent>,
    );

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let SwimRouterConnection {
            envelope_task_request_tx,
            message_register_task_request_tx,
            target,
            envelope_task_tx,
            envelope_task_rx,
            event_tx,
            event_rx,
        } = &mut self.get_mut();

        let AbsolutePath { host, node, lane } = target;
        let host_url = url::Url::parse(host).unwrap();

        //Todo refactor this to remove duplication
        match envelope_task_request_tx.poll_ready(cx).map(|r| match r {
            Ok(_) => {
                if let Some(tx) = envelope_task_tx.take() {
                    match envelope_task_request_tx.try_send((host_url.clone(), tx)) {
                        Ok(_) => (),
                        _ => panic!("Error."),
                    }
                }
            }

            _ => panic!("Error."),
        }) {
            Poll::Ready(_) => {}
            Poll::Pending => return Poll::Pending,
        };

        match message_register_task_request_tx
            .poll_ready(cx)
            .map(|r| match r {
                Ok(_) => {
                    if let Some(tx) = event_tx.take() {
                        match message_register_task_request_tx.try_send((
                            host_url.clone(),
                            (*node).to_string(),
                            (*lane).to_string(),
                            tx,
                        )) {
                            Ok(_) => (),
                            _ => panic!("Error."),
                        }
                    }
                }

                _ => panic!("Error."),
            }) {
            Poll::Ready(_) => {}
            Poll::Pending => return Poll::Pending,
        };

        oneshot::Receiver::poll(Pin::new(envelope_task_rx), cx).map(|r| match r {
            Ok(envelope_tx) => (
                envelope_tx.map_err_into::<RoutingError>(),
                event_rx.take().unwrap(),
            ),
            _ => panic!("Error."),
        })
    }
}

pub struct ConnReq<Snk, Str>(Request<Result<(Snk, Str), ConnectionError>>, url::Url);

pub type ConnectionFuture<Str, Snk> =
    SendAndAwait<ConnReq<Snk, Str>, Result<(Snk, Str), ConnectionError>>;

impl Router for SwimRouter {
    type ConnectionStream = mpsc::Receiver<RouterEvent>;
    type ConnectionSink = SenderErrInto<mpsc::Sender<Envelope>, RoutingError>;
    type GeneralSink = SenderErrInto<mpsc::Sender<(String, Envelope)>, RoutingError>;
    type ConnectionFut = SwimRouterConnection;
    type GeneralFut = Ready<Self::GeneralSink>;

    fn connection_for(&mut self, target: &AbsolutePath) -> Self::ConnectionFut {
        SwimRouterConnection::new(
            self.envelope_routing_host_request_tx.clone(),
            self.message_routing_register_task_request_tx.clone(),
            target.clone(),
            self.configuration.buffer_size().get(),
        )
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
