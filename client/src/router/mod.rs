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

use crate::connections::{
    ConnectionError, ConnectionPool, ConnectionPoolMessage, ConnectionSender,
};

use crate::connections::factory::tungstenite::TungsteniteWsFactory;
use crate::router::envelope_routing_task::{
    HostEnvelopeTaskRequestSender, RequestEnvelopeRoutingHostTask,
};
use crate::router::message_routing_task::{
    HostMessageTaskRequestSender, RequestMessageRoutingHostTask, RouteHostMessagesTask,
};
use common::request::request_future::SendAndAwait;
use common::request::Request;
use common::sink::item::map_err::SenderErrInto;
use common::sink::item::ItemSender;
use common::warp::envelope::Envelope;
use common::warp::path::AbsolutePath;
use futures::future::Ready;
use futures::task::{Context, Poll};
use futures::{Future, Stream};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub mod command_routing_task;
pub mod envelope_routing_task;
pub mod message_routing_task;

#[cfg(test)]
mod tests;

pub trait Router: Send {
    type ConnectionStream: Stream<Item = Envelope> + Send + 'static;
    type ConnectionSink: ItemSender<Envelope, RoutingError> + Send + 'static;
    type GeneralSink: ItemSender<(String, Envelope), RoutingError> + Send + 'static;

    type ConnectionFut: Future<Output = (Self::ConnectionSink, Self::ConnectionStream)> + Send;
    type GeneralFut: Future<Output = Self::GeneralSink> + Send;

    fn connection_for(&mut self, target: &AbsolutePath) -> Self::ConnectionFut;

    fn general_sink(&mut self) -> Self::GeneralFut;
}

pub struct SwimRouter {
    buffer_size: usize,
    envelope_routing_host_request_tx: HostEnvelopeTaskRequestSender,
    message_routing_host_request_tx: HostMessageTaskRequestSender,
    _request_connections_handler: JoinHandle<Result<(), RoutingError>>,
    _request_envelope_routing_host_handler: JoinHandle<Result<(), RoutingError>>,
}

impl SwimRouter {
    pub async fn new(buffer_size: usize) -> SwimRouter {
        //Todo the router_rx is the receiving point for the connection pool messages.
        let (router_tx, _router_rx) = mpsc::channel(buffer_size);

        let connection_pool = ConnectionPool::new(
            buffer_size,
            router_tx,
            TungsteniteWsFactory::new(buffer_size).await,
        );

        let (request_connections_task, connection_request_tx) =
            RequestConnectionsTask::new(connection_pool, buffer_size);

        let (request_envelope_routing_host_task, envelope_routing_host_request_tx) =
            RequestEnvelopeRoutingHostTask::new(connection_request_tx, buffer_size);

        let (request_message_routing_host_task, message_routing_host_request_tx) =
            RequestMessageRoutingHostTask::new(buffer_size);

        // let (task_request_tx, task_request_rx) = mpsc::channel(buffer_size);
        // let request_route_tasks =
        // SwimRouter::request_envelope_route_tasks(task_request_rx, connection_request_tx);

        // let sinks = SwimRouter::request_sinks(sink_request_rx);
        // let receive = SwimRouter::receive_all_messages_from_pool(router_rx, sink_request_tx);

        let request_connections_handler = tokio::spawn(request_connections_task.run());

        let request_envelope_routing_host_handler =
            tokio::spawn(request_envelope_routing_host_task.run());

        let request_message_routing_host_handler =
            tokio::spawn(request_message_routing_host_task.run());

        // let sinks_handler = tokio::spawn(sinks);
        // let receive_handler = tokio::spawn(receive);

        SwimRouter {
            buffer_size,
            envelope_routing_host_request_tx,
            message_routing_host_request_tx,
            _request_connections_handler: request_connections_handler,
            _request_envelope_routing_host_handler: request_envelope_routing_host_handler,
        }
    }
}

type ConnectionRequestSender = mpsc::Sender<(url::Url, oneshot::Sender<ConnectionSender>)>;
type ConnectionRequestReceiver = mpsc::Receiver<(url::Url, oneshot::Sender<ConnectionSender>)>;

struct RequestConnectionsTask {
    connection_pool: ConnectionPool,
    connection_request_rx: ConnectionRequestReceiver,
}

impl RequestConnectionsTask {
    fn new(connection_pool: ConnectionPool, buffer_size: usize) -> (Self, ConnectionRequestSender) {
        let (connection_request_tx, connection_request_rx) = mpsc::channel(buffer_size);
        (
            RequestConnectionsTask {
                connection_pool,
                connection_request_rx,
            },
            connection_request_tx,
        )
    }

    async fn run(self) -> Result<(), RoutingError> {
        let RequestConnectionsTask {
            mut connection_pool,
            mut connection_request_rx,
            // mut connection_output_tx,
        } = self;

        loop {
            let (host, connection_tx) = connection_request_rx
                .recv()
                .await
                .ok_or(RoutingError::ConnectionError)?;
            let connection = connection_pool
                .request_connection(host)
                .map_err(|_| RoutingError::ConnectionError)?
                .await
                .map_err(|_| RoutingError::ConnectionError)?
                .map_err(|_| RoutingError::ConnectionError)?;

            // connection_output_tx.send(connection_rx).await;

            connection_tx
                .send(connection)
                .map_err(|_| RoutingError::ConnectionError)?;
        }
    }
}

pub struct SwimRouterConnection {
    envelope_task_request_tx: HostEnvelopeTaskRequestSender,
    message_task_request_tx: HostMessageTaskRequestSender,
    host_url: url::Url,
    envelope_task_tx: Option<oneshot::Sender<mpsc::Sender<Envelope>>>,
    envelope_task_rx: oneshot::Receiver<mpsc::Sender<Envelope>>,
    envelope_tx: Option<mpsc::Sender<Envelope>>,
    envelope_rx: Option<mpsc::Receiver<Envelope>>,
}

impl Unpin for SwimRouterConnection {}

impl SwimRouterConnection {
    pub fn new(
        envelope_task_request_tx: HostEnvelopeTaskRequestSender,
        message_task_request_tx: HostMessageTaskRequestSender,
        host_url: url::Url,
        buffer_size: usize,
    ) -> Self {
        let (envelope_task_tx, envelope_task_rx) = oneshot::channel();
        let (envelope_tx, envelope_rx) = mpsc::channel(buffer_size);

        SwimRouterConnection {
            envelope_task_request_tx,
            message_task_request_tx,
            host_url,
            envelope_task_tx: Some(envelope_task_tx),
            envelope_task_rx,
            envelope_tx: Some(envelope_tx),
            envelope_rx: Some(envelope_rx),
        }
    }
}

impl Future for SwimRouterConnection {
    type Output = (
        SenderErrInto<mpsc::Sender<Envelope>, RoutingError>,
        mpsc::Receiver<Envelope>,
    );

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let SwimRouterConnection {
            envelope_task_request_tx,
            message_task_request_tx,
            host_url,
            envelope_task_tx,
            envelope_task_rx,
            envelope_tx,
            envelope_rx,
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

        match message_task_request_tx.poll_ready(cx).map(|r| match r {
            Ok(_) => {
                if let Some(tx) = envelope_tx.take() {
                    match message_task_request_tx.try_send((host_url.clone(), tx)) {
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
                envelope_rx.take().unwrap(),
            ),
            _ => panic!("Error."),
        })
    }
}

pub struct ConnReq<Snk, Str>(Request<Result<(Snk, Str), ConnectionError>>, url::Url);

pub type ConnectionFuture<Str, Snk> =
    SendAndAwait<ConnReq<Snk, Str>, Result<(Snk, Str), ConnectionError>>;

impl Router for SwimRouter {
    type ConnectionStream = mpsc::Receiver<Envelope>;
    type ConnectionSink = SenderErrInto<mpsc::Sender<Envelope>, RoutingError>;
    type GeneralSink = SenderErrInto<mpsc::Sender<(String, Envelope)>, RoutingError>;
    type ConnectionFut = SwimRouterConnection;
    type GeneralFut = Ready<Self::GeneralSink>;

    fn connection_for(&mut self, target: &AbsolutePath) -> Self::ConnectionFut {
        let host_url = url::Url::parse(&target.host).unwrap();

        SwimRouterConnection::new(
            self.envelope_routing_host_request_tx.clone(),
            self.message_routing_host_request_tx.clone(),
            host_url,
            self.buffer_size,
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
