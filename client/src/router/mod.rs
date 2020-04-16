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
    type ConnectionSink: ItemSender<Envelope, RoutingError> + Send + 'static;
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
    _request_connections_handler: JoinHandle<Result<(), RoutingError>>,
    connections_task_close_request_tx: CloseRequestSender,
    _request_envelope_routing_host_handler: JoinHandle<Result<(), RoutingError>>,
    _envelope_routing_task_close_request_tx: CloseRequestSender,
    _request_message_routing_host_handler: JoinHandle<Result<(), RoutingError>>,
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
                Duration::from_secs(2),
                Duration::from_secs(32),
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
                message_routing_new_task_request_tx,
                configuration,
            );

        let (
            request_envelope_routing_host_task,
            envelope_routing_host_request_tx,
            envelope_routing_task_close_request_tx,
        ) = RequestEnvelopeRoutingHostTask::new(connection_request_tx, configuration);

        let _request_connections_handler = tokio::spawn(request_connections_task.run());

        let _request_envelope_routing_host_handler =
            tokio::spawn(request_envelope_routing_host_task.run());

        let _request_message_routing_host_handler =
            tokio::spawn(request_message_routing_host_task.run());

        SwimRouter {
            configuration,
            envelope_routing_host_request_tx,
            message_routing_register_task_request_tx,
            _request_connections_handler,
            connections_task_close_request_tx,
            _request_envelope_routing_host_handler,
            _envelope_routing_task_close_request_tx: envelope_routing_task_close_request_tx,
            _request_message_routing_host_handler,
            message_routing_task_close_request_tx,
        }
    }

    pub async fn close(mut self) {
        // self.envelope_routing_task_close_request_tx
        //     .send(())
        //     .unwrap();
        // self._request_envelope_routing_host_handler.await.unwrap();

        self.connections_task_close_request_tx
            .send(())
            .await
            .unwrap();
        let _ = self._request_connections_handler.await.unwrap();

        self.message_routing_task_close_request_tx
            .send(())
            .await
            .unwrap();
        let _ = self._request_message_routing_host_handler.await.unwrap();
    }
}

pub type ConnReqSendResult = mpsc::Sender<(
    url::Url,
    oneshot::Sender<Result<ConnectionSender, RoutingError>>,
)>;
type ConnectionRequestReceiver = mpsc::Receiver<(
    url::Url,
    oneshot::Sender<Result<ConnectionSender, RoutingError>>,
)>;

pub type CloseRequestSender = mpsc::Sender<()>;
pub type CloseRequestReceiver = mpsc::Receiver<()>;

struct RequestConnectionsTask {
    connection_pool: ConnectionPool,
    connection_request_rx: ConnectionRequestReceiver,
    close_request_rx: CloseRequestReceiver,
    message_routing_new_task_request_tx: HostMessageNewTaskRequestSender,
}

impl RequestConnectionsTask {
    fn new(
        connection_pool: ConnectionPool,
        message_routing_new_task_request_tx: HostMessageNewTaskRequestSender,
        config: RouterConfig,
    ) -> (Self, ConnReqSendResult, CloseRequestSender) {
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

        while let ConnectionsRequestType::NewConnection((host, connection_tx)) =
            rx.next().await.ok_or(RoutingError::ConnectionError)?
        {
            match connection_pool
                .request_connection(host.clone())
                .map_err(|_| RoutingError::ConnectionError)?
                .await
                .map_err(|_| RoutingError::ConnectionError)?
            {
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

                Err(_e) => {
                    // Need to return an error to the request so that it can cancel and not attempt
                    // again
                    let _ = connection_tx.send(Err(RoutingError::ConnectionError));

                    message_routing_new_task_request_tx
                        .send(ConnectionResponse::Failure(host))
                        .await
                        .map_err(|_| RoutingError::ConnectionError)?;
                }
            }
        }
        Ok(())
    }
}

enum ConnectionsRequestType {
    NewConnection(
        (
            url::Url,
            oneshot::Sender<Result<ConnectionSender, RoutingError>>,
        ),
    ),
    Close,
}

fn combine_receive_connection_requests(
    connection_requests_rx: ConnectionRequestReceiver,
    close_requests_rx: CloseRequestReceiver,
) -> impl stream::Stream<Item = ConnectionsRequestType> + Send + 'static {
    let connection_requests = connection_requests_rx.map(ConnectionsRequestType::NewConnection);
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
    host_url: url::Url,
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
        host_url: url::Url,
        buffer_size: usize,
    ) -> Self {
        let (envelope_task_tx, envelope_task_rx) = oneshot::channel();
        let (event_tx, event_rx) = mpsc::channel(buffer_size);

        SwimRouterConnection {
            envelope_task_request_tx,
            message_register_task_request_tx,
            host_url,
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
            host_url,
            envelope_task_tx,
            envelope_task_rx,
            event_tx,
            event_rx,
        } = &mut self.get_mut();

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
                        match message_register_task_request_tx.try_send((host_url.clone(), tx)) {
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
        let host_url = url::Url::parse(&target.host).unwrap();

        SwimRouterConnection::new(
            self.envelope_routing_host_request_tx.clone(),
            self.message_routing_register_task_request_tx.clone(),
            host_url,
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
    RouterDropped,
    ConnectionError,
}

impl Display for RoutingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingError::RouterDropped => write!(f, "Router was dropped."),
            RoutingError::ConnectionError => write!(f, "Connection error."),
        }
    }
}

impl Error for RoutingError {}

impl<T> From<SendError<T>> for RoutingError {
    fn from(_: SendError<T>) -> Self {
        RoutingError::RouterDropped
    }
}
