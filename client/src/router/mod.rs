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

pub struct SwimRouter {}

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

        let connections = SwimRouter::request_connection(connection_pool, connection_request_rx);

        let receive = SwimRouter::receive_all_messages_from_pool(router_rx);
        let send = SwimRouter::send_all_envelopes_to_pool(envelope_rx, connection_request_tx);

        // Todo Add the handlers to the SwimRouter
        let connections_handler = tokio::spawn(connections);
        let send_handler = tokio::spawn(send);
        let receive_handler = tokio::spawn(receive);

        SwimRouter {}
    }

    // rx receives messages directly from every open connection in the pool
    async fn receive_all_messages_from_pool(
        mut router_rx: mpsc::Receiver<Result<ConnectionPoolMessage, ConnectionError>>,
    ) {
        loop {
            let pool_message = router_rx.recv().await.unwrap().unwrap();
            let ConnectionPoolMessage { host, message } = pool_message;

            //TODO this needs to be implemented
            //We can have multiple sinks (downlinks) for a given host
            let mut sink = SwimRouter::request_sink(host);
            sink.send_item(text);

            //
            // sink.send_item(envelope);
        }
    }

    async fn send_all_envelopes_to_pool(
        mut envelope_rx: mpsc::Receiver<Envelope>,
        mut connection_request_tx: mpsc::Sender<(url::Url, oneshot::Sender<ConnectionSender>)>,
    ) {
        loop {
            let envelope = envelope_rx.recv().await.unwrap();
            //TODO Parse the envelope to obtain host
            let host = url::Url::parse("127.0.0.1").unwrap();

            let (connection_tx, connection_rx) = oneshot::channel();
            connection_request_tx.send((host, connection_tx)).await;

            //Todo This should be sent down to the host tasks.
            // connection.send_message(&message);
        }
    }

    async fn send_host_envelopes_to_pool(
        mut connection_rx: oneshot::Receiver<ConnectionSender>,
        mut envelope_rx: mpsc::Receiver<Envelope>,
    ) {
        let mut connection = connection_rx.await.unwrap();

        loop {
            //TODO parse the message to an envelope
            let envelope = envelope_rx.recv().await.unwrap();
            let message = "foo";

            connection.send_message(message);
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
        // Todo remove unwrap
        let host_url = url::Url::parse(&target.host).unwrap();

        //Todo this should have two different channels
        let (envelope_tx, envelope_rx) = mpsc::channel::<Envelope>(5);

        let envelope_tx = envelope_tx.map_err_into();

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
