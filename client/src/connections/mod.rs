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
use futures::future::{AbortHandle, Abortable, Aborted};
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

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct ConnectionPoolMessage {
    host: String,
    message: String,
}

pub struct ConnectionRequest {
    host_url: url::Url,
    tx: oneshot::Sender<Result<ConnectionSender, ConnectionError>>,
}

pub struct ConnectionPool {
    connection_requests_abort_handle: AbortHandle,
    connection_requests_handler: JoinHandle<Result<Result<(), ConnectionError>, Aborted>>,
    connection_request_tx: mpsc::Sender<ConnectionRequest>,
}

impl ConnectionPool {
    pub fn new<T: ConnectionFactory + Send + Sync + 'static>(
        buffer_size: usize,
        router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
        connection_factory: T,
    ) -> ConnectionPool {
        let (connection_request_tx, connection_request_rx) = mpsc::channel(buffer_size);

        let (connection_requests_abort_handle, abort_registration) = AbortHandle::new_pair();

        let accept_connection_requests_future = Abortable::new(
            accept_connection_requests(
                connection_request_rx,
                connection_factory,
                router_tx,
                buffer_size,
            ),
            abort_registration,
        );

        let connection_requests_handler = tokio::spawn(accept_connection_requests_future);

        ConnectionPool {
            connection_requests_abort_handle,
            connection_requests_handler,
            connection_request_tx,
        }
    }

    pub fn request_connection(
        &mut self,
        host_url: url::Url,
    ) -> Result<oneshot::Receiver<Result<ConnectionSender, ConnectionError>>, ConnectionError> {
        let (tx, rx) = oneshot::channel();

        self.connection_request_tx
            .try_send(ConnectionRequest { host_url, tx })
            .map_err(|_| ConnectionError::ConnectError)?;

        Ok(rx)
    }

    pub async fn close(self) {
        self.connection_requests_abort_handle.abort();
        let _ = self.connection_requests_handler.await;
    }
}

async fn accept_connection_requests<T: ConnectionFactory + Send + Sync + 'static>(
    mut rx: mpsc::Receiver<ConnectionRequest>,
    mut connection_factory: T,
    router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
    buffer_size: usize,
) -> Result<(), ConnectionError> {
    let mut connections: HashMap<String, T::ConnectionType> = HashMap::new();
    loop {
        let connection_request = rx.recv().await.ok_or(ConnectionError::ConnectError)?;
        let ConnectionRequest {
            host_url,
            tx: request_tx,
        } = connection_request;

        let host = host_url.as_str().to_owned();

        let sender = if connections.contains_key(&host) {
            Ok(connections
                .get_mut(&host)
                .ok_or(ConnectionError::ConnectError)?
                .get_sender())
        } else {
            let connection_result = connection_factory
                .create_connection(host_url, buffer_size, router_tx.clone())
                .await;

            match connection_result {
                Ok(mut connection) => {
                    let sender = connection.get_sender();
                    connections.insert(host.clone(), connection);
                    Ok(sender)
                }
                Err(error) => Err(error),
            }
        };

        request_tx
            .send(sender)
            .map_err(|_| ConnectionError::ConnectError)?;
    }
}

#[async_trait]
pub trait ConnectionFactory {
    type ConnectionType: Connection + Send + 'static;

    async fn create_connection(
        &mut self,
        host_url: url::Url,
        buffer_size: usize,
        router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
    ) -> Result<Self::ConnectionType, ConnectionError>;
}

struct SwimConnectionFactory {}

#[async_trait]
impl ConnectionFactory for SwimConnectionFactory {
    type ConnectionType = SwimConnection;

    async fn create_connection(
        &mut self,
        host_url: url::Url,
        buffer_size: usize,
        router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
    ) -> Result<Self::ConnectionType, ConnectionError> {
        SwimConnection::new(host_url, buffer_size, router_tx, SwimWebsocketFactory {}).await
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

    fn get_sender(&mut self) -> ConnectionSender {
        ConnectionSender {
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
    async fn new<T: WebsocketFactory + Send + Sync + 'static>(
        host_url: url::Url,
        buffer_size: usize,
        router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
        websocket_factory: T,
    ) -> Result<SwimConnection, ConnectionError> {
        let host = host_url.as_str().to_owned();
        let (tx, rx) = mpsc::channel(buffer_size);

        let ws_stream = websocket_factory.connect(host_url).await?;
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

pub struct ConnectionSender {
    tx: mpsc::Sender<Message>,
}

impl ConnectionSender {
    pub async fn send_message(&mut self, message: &str) -> Result<(), ConnectionError> {
        self.tx
            .send(Message::text(message))
            .await
            .map_err(|_| ConnectionError::SendMessageError)
    }
}

#[async_trait]
impl Connection for SwimConnection {
    fn get_tx(&mut self) -> &mut mpsc::Sender<Message> {
        &mut self.tx
    }
}

#[async_trait]
trait WebsocketFactory {
    type WebsocketType: Stream<Item = Result<Message, ConnectionError>>
        + Sink<Message>
        + Send
        + 'static;

    async fn connect(self, url: url::Url) -> Result<Self::WebsocketType, ConnectionError>;
}

struct SwimWebsocketFactory {}

#[async_trait]
impl WebsocketFactory for SwimWebsocketFactory {
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
    ClosedError,
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

impl From<Aborted> for ConnectionError {
    fn from(_: Aborted) -> Self {
        ConnectionError::ClosedError
    }
}
