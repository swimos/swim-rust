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

use async_trait::async_trait;
use futures::stream::ErrInto;
use futures::{Sink, StreamExt};
use futures_util::stream::Stream;
use futures_util::TryStreamExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{connect_async, tungstenite, WebSocketStream};
use url;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct ConnectionPoolMessage {
    host: String,
    message: String,
}

pub struct ConnectionRequest {
    host_url: url::Url,
    tx: oneshot::Sender<ConnectionHandler>,
}

pub struct ConnectionPool {
    pub connection_request_handler: JoinHandle<Result<(), ConnectionError>>,
    connection_request_tx: mpsc::Sender<ConnectionRequest>,
}

impl ConnectionPool {
    pub fn new<T: ConnectionProducer + Send + std::marker::Sync + 'static>(
        buffer_size: usize,
        router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
        connection_producer: T,
    ) -> ConnectionPool {
        let (connection_request_tx, connection_request_rx) = mpsc::channel(buffer_size);

        let connection_request_handler = tokio::spawn(ConnectionPool::open_connection(
            connection_request_rx,
            connection_producer,
            router_tx,
            buffer_size,
        ));

        ConnectionPool {
            connection_request_handler,
            connection_request_tx,
        }
    }

    pub fn request_connection(
        &mut self,
        host_url: url::Url,
    ) -> Result<oneshot::Receiver<ConnectionHandler>, ConnectionError> {
        let (tx, rx) = oneshot::channel();

        self.connection_request_tx
            .try_send(ConnectionRequest { host_url, tx })
            .map_err(|_| ConnectionError::ConnectError)?;

        Ok(rx)
    }

    pub async fn open_connection<T: ConnectionProducer + Send + std::marker::Sync + 'static>(
        mut rx: mpsc::Receiver<ConnectionRequest>,
        mut connection_producer: T,
        router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
        buffer_size: usize,
    ) -> Result<(), ConnectionError> {
        let mut connections = HashMap::new();

        loop {
            let connection_request = rx.recv().await.ok_or(ConnectionError::ConnectError)?;
            let ConnectionRequest {
                host_url,
                tx: request_tx,
            } = connection_request;

            let host = host_url.as_str().to_owned();

            if !&connections.contains_key(&host) {
                let connection = connection_producer
                    .create_connection(host_url, buffer_size, router_tx.clone())
                    .await?;

                connections.insert(host.clone(), connection);
            }

            let connection = connections
                .get_mut(&host)
                .ok_or(ConnectionError::ConnectError)?;

            request_tx
                .send(connection.get_handler())
                .map_err(|_| ConnectionError::ConnectError)?;
        }
    }
}

#[async_trait]
pub trait ConnectionProducer {
    type ConnectionType: Connection + Send + 'static;

    async fn create_connection(
        &mut self,
        host_url: url::Url,
        buffer_size: usize,
        router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
    ) -> Result<Self::ConnectionType, ConnectionError>;
}

struct SwimConnectionProducer {}

#[async_trait]
impl ConnectionProducer for SwimConnectionProducer {
    type ConnectionType = SwimConnection;

    async fn create_connection(
        &mut self,
        host_url: url::Url,
        buffer_size: usize,
        router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
    ) -> Result<Self::ConnectionType, ConnectionError> {
        SwimConnection::new(host_url, buffer_size, router_tx, SwimWebsocketProducer {}).await
    }
}

#[async_trait]
pub trait Connection: Sized {
    async fn send_messages<S>(
        write_sink: S,
        rx: mpsc::Receiver<Message>,
    ) -> Result<(), ConnectionError>
    where
        S: Sink<Message> + Send + 'static + Unpin,
    {
        rx.map(Ok)
            .forward(write_sink)
            .await
            .map_err(|_| ConnectionError::SendMessageError)?;
        Ok(())
    }

    async fn receive_messages<S>(
        mut read_stream: S,
        mut router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
        host: String,
    ) -> Result<(), ConnectionError>
    where
        S: TryStreamExt<Ok = Message, Error = ConnectionError> + Send + Unpin + 'static,
    {
        loop {
            let message = read_stream
                .try_next()
                .await
                .map_err(|_| ConnectionError::ReceiveMessageError)?
                .ok_or(ConnectionError::ReceiveMessageError)?;

            let message = message
                .into_text()
                .map_err(|_| ConnectionError::ReceiveMessageError)?;

            router_tx
                .send(Ok(ConnectionPoolMessage {
                    host: host.clone(),
                    message,
                }))
                .await
                .map_err(|_| ConnectionError::ReceiveMessageError)?;
        }
    }

    fn get_handler(&mut self) -> ConnectionHandler {
        ConnectionHandler {
            tx: self.get_tx().clone(),
        }
    }

    fn get_tx(&mut self) -> &mut mpsc::Sender<Message>;
}

struct SwimConnection {
    tx: mpsc::Sender<Message>,
    _send_handler: JoinHandle<Result<(), ConnectionError>>,
    _receive_handler: JoinHandle<Result<(), ConnectionError>>,
}

impl SwimConnection {
    async fn new<T: WebsocketProducer + Send + std::marker::Sync + 'static>(
        host_url: url::Url,
        buffer_size: usize,
        router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
        websocket_producer: T,
    ) -> Result<SwimConnection, ConnectionError> {
        let host = host_url.as_str().to_owned();
        let (tx, rx) = mpsc::channel(buffer_size);

        let ws_stream = websocket_producer.connect(host_url).await?;
        let (write_sink, read_stream) = ws_stream.split();

        let receive = SwimConnection::receive_messages(read_stream, router_tx, host);
        let send = SwimConnection::send_messages(write_sink, rx);

        let send_handler = tokio::spawn(send);
        let receive_handler = tokio::spawn(receive);

        Ok(SwimConnection {
            tx,
            _send_handler: send_handler,
            _receive_handler: receive_handler,
        })
    }
}

pub struct ConnectionHandler {
    tx: mpsc::Sender<Message>,
}

impl ConnectionHandler {
    pub async fn send_message(&mut self, message: &str) -> Result<(), ConnectionError> {
        self.tx
            .send(Message::text(message))
            .await
            .map_err(|_| ConnectionError::SendMessageError)?;
        Ok(())
    }
}

#[async_trait]
impl Connection for SwimConnection {
    fn get_tx(&mut self) -> &mut mpsc::Sender<Message> {
        &mut self.tx
    }
}

#[async_trait]
trait WebsocketProducer {
    type WebsocketType: Stream<Item = Result<Message, ConnectionError>>
        + Sink<Message>
        + std::marker::Send
        + 'static;

    async fn connect(self, url: url::Url) -> Result<Self::WebsocketType, ConnectionError>;
}

struct SwimWebsocketProducer {}

#[async_trait]
impl WebsocketProducer for SwimWebsocketProducer {
    type WebsocketType = ErrInto<WebSocketStream<TcpStream>, ConnectionError>;

    async fn connect(self, url: url::Url) -> Result<Self::WebsocketType, ConnectionError> {
        let (ws_stream, _) = connect_async(url)
            .await
            .map_err(|_| ConnectionError::ConnectError)?;
        let ws_stream = ws_stream.err_into::<ConnectionError>();
        Ok(ws_stream)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionError {
    ConnectError,
    SendMessageError,
    ReceiveMessageError,
}

impl From<tokio::task::JoinError> for ConnectionError {
    fn from(_: tokio::task::JoinError) -> Self {
        ConnectionError::ConnectError
    }
}

impl From<tungstenite::error::Error> for ConnectionError {
    fn from(_: tungstenite::error::Error) -> Self {
        ConnectionError::ConnectError
    }
}
