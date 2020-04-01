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
use common::request::request_future::SendAndAwait;
use common::request::Request;
use common::sink::item::map_err::SenderErrInto;
use common::sink::item::ItemSender;
use common::warp::envelope::{Envelope, LaneAddressed};
use common::warp::path::AbsolutePath;
use futures::future::Ready;
use futures::task::{Context, Poll};
use futures::{Future, Stream};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

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
    envelope_routing_host_request_tx: RoutingHostTaskRequestSender,
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

        // let (task_request_tx, task_request_rx) = mpsc::channel(buffer_size);
        // let request_route_tasks =
        // SwimRouter::request_envelope_route_tasks(task_request_rx, connection_request_tx);

        // let sinks = SwimRouter::request_sinks(sink_request_rx);
        // let receive = SwimRouter::receive_all_messages_from_pool(router_rx, sink_request_tx);

        let request_connections_handler = tokio::spawn(request_connections_task.run());
        let request_envelope_routing_host_handler =
            tokio::spawn(request_envelope_routing_host_task.run());

        // let sinks_handler = tokio::spawn(sinks);
        // let receive_handler = tokio::spawn(receive);

        SwimRouter {
            envelope_routing_host_request_tx,
            _request_connections_handler: request_connections_handler,
            _request_envelope_routing_host_handler: request_envelope_routing_host_handler,
        }
    }

    //-------------------------------Connection Pool to Downlink------------------------------------

    // rx receives messages directly from every open connection in the pool
    async fn _receive_all_messages_from_pool(
        mut router_rx: mpsc::Receiver<Result<ConnectionPoolMessage, ConnectionError>>,
        mut sink_request_tx: mpsc::Sender<(url::Url, oneshot::Sender<mpsc::Sender<String>>)>,
    ) {
        loop {
            let pool_message = router_rx.recv().await.unwrap().unwrap();
            let ConnectionPoolMessage {
                host,
                message: _message,
            } = pool_message;

            //TODO this needs to be implemented
            //We can have multiple sinks (downlinks) for a given host

            let host_url = url::Url::parse(&host).unwrap();
            let (sink_tx, _sink_rx) = oneshot::channel();

            sink_request_tx.send((host_url, sink_tx)).await.unwrap();

            //Todo This should be sent down to the host tasks.
            // sink.send_item(text);
        }
    }

    async fn _request_sinks(
        mut sink_request_rx: mpsc::Receiver<(url::Url, oneshot::Sender<mpsc::Sender<String>>)>,
    ) {
        let mut _sinks: HashMap<String, oneshot::Sender<mpsc::Sender<String>>> = HashMap::new();

        loop {
            let (_host, _sink_tx) = sink_request_rx.recv().await.unwrap();

            // Todo Implement this.
            // let sink = pool.request_sink();

            // sink_tx.send(sink);
        }
    }

    async fn _receive_host_messages_from_pool(
        mut message_rx: mpsc::Receiver<String>,
        downlinks_rxs: Vec<mpsc::Sender<Envelope>>,
    ) {
        loop {
            //TODO parse the message to an envelope
            let _message = message_rx.recv().await.unwrap();

            let lane_addressed = LaneAddressed {
                node_uri: String::from("node_uri"),
                lane_uri: String::from("lane_uri"),
                body: None,
            };

            let _envelope = Envelope::EventMessage(lane_addressed);

            for mut _downlink_rx in &downlinks_rxs {
                // Todo need clone for envelope
                // downlink_rx.send_item(envelope.clone());
            }
        }
    }
}

//----------------------------------Downlink to Connection Pool---------------------------------

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

            connection_tx
                .send(connection)
                .map_err(|_| RoutingError::ConnectionError)?;
        }
    }
}

type RoutingHostTaskRequestSender =
    mpsc::Sender<(url::Url, oneshot::Sender<mpsc::Sender<Envelope>>)>;

type RoutingHostTaskRequestReceiver =
    mpsc::Receiver<(url::Url, oneshot::Sender<mpsc::Sender<Envelope>>)>;

struct RequestEnvelopeRoutingHostTask {
    connection_request_tx: ConnectionRequestSender,
    task_request_rx: RoutingHostTaskRequestReceiver,
    buffer_size: usize,
}

impl RequestEnvelopeRoutingHostTask {
    fn new(
        connection_request_tx: ConnectionRequestSender,
        buffer_size: usize,
    ) -> (Self, RoutingHostTaskRequestSender) {
        let (task_request_tx, task_request_rx) = mpsc::channel(buffer_size);

        (
            RequestEnvelopeRoutingHostTask {
                connection_request_tx,
                task_request_rx,
                buffer_size,
            },
            task_request_tx,
        )
    }

    async fn run(self) -> Result<(), RoutingError> {
        let RequestEnvelopeRoutingHostTask {
            connection_request_tx,
            mut task_request_rx,
            buffer_size,
        } = self;
        let mut host_route_tasks: HashMap<String, mpsc::Sender<Envelope>> = HashMap::new();

        loop {
            let (host_url, task_tx) = task_request_rx
                .recv()
                .await
                .ok_or(RoutingError::ConnectionError)?;
            let host = host_url.to_string();

            if !host_route_tasks.contains_key(&host) {
                let (host_route_task, envelope_tx) = RouteHostEnvelopesTask::new(
                    host_url,
                    connection_request_tx.clone(),
                    buffer_size,
                );

                host_route_tasks.insert(host.clone(), envelope_tx);
                //Todo store this handler
                let _host_route_handler = tokio::spawn(host_route_task.run());
            }

            let envelope_tx = host_route_tasks
                .get(&host.to_string())
                .ok_or(RoutingError::ConnectionError)?
                .clone();
            task_tx
                .send(envelope_tx)
                .map_err(|_| RoutingError::ConnectionError)?;
        }
    }
}

struct RouteHostEnvelopesTask {
    host_url: url::Url,
    envelope_rx: mpsc::Receiver<Envelope>,
    connection_request_tx: ConnectionRequestSender,
}

impl RouteHostEnvelopesTask {
    fn new(
        host_url: url::Url,
        connection_request_tx: ConnectionRequestSender,
        buffer_size: usize,
    ) -> (Self, mpsc::Sender<Envelope>) {
        let (envelope_tx, envelope_rx) = mpsc::channel(buffer_size);

        (
            RouteHostEnvelopesTask {
                host_url,
                envelope_rx,
                connection_request_tx,
            },
            envelope_tx,
        )
    }

    async fn run(self) -> Result<(), RoutingError> {
        let RouteHostEnvelopesTask {
            host_url,
            mut envelope_rx,
            mut connection_request_tx,
        } = self;

        loop {
            let _envelope = envelope_rx
                .recv()
                .await
                .ok_or(RoutingError::ConnectionError)?;

            //Todo extract this out of the loop and request new connection only when a failure occurs.
            let (connection_tx, connection_rx) = oneshot::channel();
            connection_request_tx
                .send((host_url.clone(), connection_tx))
                .await
                .map_err(|_| RoutingError::ConnectionError)?;
            let mut connection = connection_rx
                .await
                .map_err(|_| RoutingError::ConnectionError)?;

            //TODO parse the envelope to a message
            let message = "@sync(node:\"/unit/foo\", lane:\"info\")";
            println!("{:?}", host_url.to_string());
            println!("{:?}", message);
            connection
                .send_message(message)
                .await
                .map_err(|_| RoutingError::ConnectionError)?;
        }
    }
}

pub struct SwimRouterConnection {
    task_request_tx: RoutingHostTaskRequestSender,
    host_url: url::Url,
    task_tx: Option<oneshot::Sender<mpsc::Sender<Envelope>>>,
    task_rx: oneshot::Receiver<mpsc::Sender<Envelope>>,
}

impl Unpin for SwimRouterConnection {}

impl SwimRouterConnection {
    pub fn new(task_request_tx: RoutingHostTaskRequestSender, host_url: url::Url) -> Self {
        let (task_tx, task_rx) = oneshot::channel();

        SwimRouterConnection {
            task_request_tx,
            host_url,
            task_tx: Some(task_tx),
            task_rx,
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
            task_request_tx,
            host_url,
            task_tx,
            task_rx,
        } = &mut self.get_mut();

        //Todo replace with real implementation
        let (_, envelope_rx) = mpsc::channel::<Envelope>(5);

        match task_request_tx.poll_ready(cx).map(|r| match r {
            Ok(_) => {
                if let Some(tx) = task_tx.take() {
                    match task_request_tx.try_send((host_url.clone(), tx)) {
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

        oneshot::Receiver::poll(Pin::new(task_rx), cx).map(|r| match r {
            Ok(envelope_tx) => (envelope_tx.map_err_into::<RoutingError>(), envelope_rx),
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

        SwimRouterConnection::new(self.envelope_routing_host_request_tx.clone(), host_url)
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
