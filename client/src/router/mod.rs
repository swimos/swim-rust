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
    Connection, ConnectionError, ConnectionPool, ConnectionPoolMessage, ConnectionSender,
    SwimConnection, SwimConnectionFactory,
};
use crate::sink::item::map_err::SenderErrInto;
use crate::sink::item::{ItemSender, ItemSink};
use common::warp::envelope::{Envelope, LaneAddressed};
use common::warp::path::AbsolutePath;
use futures::future::{ready, Ready};
use futures::{Future, Stream};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use tokio::sync::mpsc;
use tokio::sync::oneshot;

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
    host_send_task_tx: mpsc::Sender<(
        url::Url,
        mpsc::Sender<(oneshot::Receiver<ConnectionSender>, Envelope)>,
    )>,
    envelope_tx: mpsc::Sender<Envelope>,
}

impl SwimRouter {
    fn new(buffer_size: usize) -> SwimRouter {
        let (router_tx, router_rx) = mpsc::channel(buffer_size);

        //TODO add to struct
        // let (connection_request_tx, connection_request_rx) = mpsc::channel(buffer_size);
        let (envelope_tx, envelope_rx) = mpsc::channel(buffer_size);

        //Todo Use this to create request_sink()
        // let (sinks_tx, sinks_rx) = mpsc::channel(buffer_size);

        // Todo Use this to create request_connection()
        let connection_pool = ConnectionPool::new(buffer_size, router_tx, SwimConnectionFactory {});

        let (connection_request_tx, connection_request_rx) = mpsc::channel(buffer_size);
        let (sink_request_tx, sink_request_rx) = mpsc::channel(buffer_size);

        let connections = SwimRouter::request_connections(connection_pool, connection_request_rx);
        let sinks = SwimRouter::request_sinks(sink_request_rx);

        let receive = SwimRouter::receive_all_messages_from_pool(router_rx, sink_request_tx);

        let (host_send_task_tx, host_send_task_rx) = mpsc::channel(buffer_size);

        let send = SwimRouter::send_all_envelopes_to_pool(
            envelope_rx,
            connection_request_tx,
            host_send_task_rx,
        );

        // Todo Add the handlers to the SwimRouter
        let connections_handler = tokio::spawn(connections);
        let sinks_handler = tokio::spawn(sinks);
        let send_handler = tokio::spawn(send);
        let receive_handler = tokio::spawn(receive);

        SwimRouter {
            host_send_task_tx,
            envelope_tx,
        }
    }

    // rx receives messages directly from every open connection in the pool
    async fn receive_all_messages_from_pool(
        mut router_rx: mpsc::Receiver<Result<ConnectionPoolMessage, ConnectionError>>,
        mut sink_request_tx: mpsc::Sender<(url::Url, oneshot::Sender<mpsc::Sender<String>>)>,
    ) {
        loop {
            let pool_message = router_rx.recv().await.unwrap().unwrap();
            let ConnectionPoolMessage { host, message } = pool_message;

            //TODO this needs to be implemented
            //We can have multiple sinks (downlinks) for a given host

            let host_url = url::Url::parse(&host).unwrap();
            let (sink_tx, sink_rx) = oneshot::channel();

            sink_request_tx.send((host_url, sink_tx)).await;

            //Todo This should be sent down to the host tasks.
            // sink.send_item(text);
        }
    }

    async fn send_all_envelopes_to_pool(
        mut envelope_rx: mpsc::Receiver<Envelope>,
        mut connection_request_tx: mpsc::Sender<(url::Url, oneshot::Sender<ConnectionSender>)>,
        mut host_task_rx: mpsc::Receiver<(
            url::Url,
            mpsc::Sender<(oneshot::Receiver<ConnectionSender>, Envelope)>,
        )>,
    ) {
        let mut host_tasks: HashMap<
            String,
            mpsc::Sender<(oneshot::Receiver<ConnectionSender>, Envelope)>,
        > = HashMap::new();

        loop {
            tokio::select! {
                Some((host, host_tx)) = host_task_rx.recv() => {
                    host_tasks.insert(host.to_string(), host_tx);
                    println!("{:?}", host);
                }
                Some(envelope) = envelope_rx.recv() => {

                    //TODO Parse the envelope to obtain host
                    let host = url::Url::parse("ws://127.0.0.1:9001").unwrap();

                    let (connection_tx, connection_rx) = oneshot::channel();
                    connection_request_tx.send((host.clone(), connection_tx)).await;
                    let task_tx = host_tasks.get_mut(&host.to_string()).unwrap();

                    task_tx.send((connection_rx, envelope)).await;

                }
            }
        }
    }

    async fn request_connections(
        mut pool: ConnectionPool,
        mut connection_request_rx: mpsc::Receiver<(url::Url, oneshot::Sender<ConnectionSender>)>,
    ) {
        loop {
            let (host, connection_tx) = connection_request_rx.recv().await.unwrap();
            let connection = pool
                .request_connection(host)
                .unwrap()
                .await
                .unwrap()
                .unwrap();

            connection_tx.send(connection);
        }
    }

    async fn request_sinks(
        mut sink_request_rx: mpsc::Receiver<(url::Url, oneshot::Sender<mpsc::Sender<String>>)>,
    ) {
        let mut sinks: HashMap<String, oneshot::Sender<mpsc::Sender<String>>> = HashMap::new();

        loop {
            //Todo use select! to fork this into call from self with new sink and call for sink request.
            let (host, sink_tx) = sink_request_rx.recv().await.unwrap();

            // Todo Implement this.
            // let sink = pool.request_sink();

            // sink_tx.send(sink);
        }
    }

    async fn receive_host_messages_from_pool(
        mut message_rx: mpsc::Receiver<String>,
        downlinks_rxs: Vec<mpsc::Sender<Envelope>>,
    ) {
        loop {
            //TODO parse the message to an envelope
            let message = message_rx.recv().await.unwrap();

            let lane_addressed = LaneAddressed {
                node_uri: String::from("node_uri"),
                lane_uri: String::from("lane_uri"),
                body: None,
            };

            let envelope = Envelope::EventMessage(lane_addressed);

            for mut downlink_rx in &downlinks_rxs {
                // Todo need clone for envelope
                // downlink_rx.send_item(envelope.clone());
            }
        }
    }

    async fn send_host_envelopes_to_pool(
        mut payload_rx: mpsc::Receiver<(oneshot::Receiver<ConnectionSender>, Envelope)>,
    ) {
        loop {
            let (connection_rx, envelope) = payload_rx.recv().await.unwrap();
            let mut connection = connection_rx.await.unwrap();

            //TODO parse the envelope to a message
            let message = "@sync(node:\"/unit/foo\", lane:\"info\")";
            println!("{:?}", message);
            connection.send_message(message).await;
        }
    }
}

impl Router for SwimRouter {
    type ConnectionStream = mpsc::Receiver<Envelope>;
    type ConnectionSink = SenderErrInto<mpsc::Sender<Envelope>, RoutingError>;
    type GeneralSink = SenderErrInto<mpsc::Sender<(String, Envelope)>, RoutingError>;
    type ConnectionFut = Ready<(Self::ConnectionSink, Self::ConnectionStream)>;
    type GeneralFut = Ready<Self::GeneralSink>;

    fn connection_for(&mut self, target: &AbsolutePath) -> Self::ConnectionFut {
        let host_url = url::Url::parse(&target.host).unwrap();

        let (payload_tx, payload_rx) = mpsc::channel(5);
        let host_task = SwimRouter::send_host_envelopes_to_pool(payload_rx);
        tokio::spawn(host_task);

        self.host_send_task_tx.try_send((host_url, payload_tx));

        // Todo remove unwrap

        //Todo this should have two different channels
        let (_, envelope_rx) = mpsc::channel::<Envelope>(5);
        //
        let envelope_tx = self.envelope_tx.clone().map_err_into();
        ready((envelope_tx, envelope_rx))
    }

    fn general_sink(&mut self) -> Self::GeneralFut {
        //Todo
        unimplemented!()
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RoutingError {
    RouterDropped,
}

impl Display for RoutingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingError::RouterDropped => write!(f, "Router was dropped."),
        }
    }
}

impl Error for RoutingError {}

impl<T> From<mpsc::error::SendError<T>> for RoutingError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        //TODO add impl
        unimplemented!()
    }
}
