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

/// Connection pool message wraps a message from a remote host.
#[derive(Debug)]
pub struct ConnectionPoolMessage {
    /// The URL of the remote host.
    pub host: String,
    /// The message from the remote host.
    pub message: String,
}

struct ConnectionRequest {
    host_url: url::Url,
    tx: oneshot::Sender<Result<ConnectionSender, ConnectionError>>,
}

/// Connection pool is responsible for managing the opening and closing of connections
/// to remote hosts.
pub struct ConnectionPool {
    connection_requests_abort_handle: AbortHandle,
    connection_requests_handler: JoinHandle<Result<Result<(), ConnectionError>, Aborted>>,
    connection_request_tx: mpsc::Sender<ConnectionRequest>,
}

impl ConnectionPool {
    /// Creates a new connection pool for managing connections to remote hosts.
    ///
    /// # Arguments
    ///
    /// * `buffer_size`             - The buffer size of the internal channels in the connection
    ///                               pool as an integer.
    /// * `router_tx`               - Transmitting end of a channel for receiving messages
    ///                               from the connections in the pool.
    /// * `connection_factory`      - Custom factory capable of producing connections for the pool.
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

    /// Sends and asynchronous request for a connection to a specific host.
    ///
    /// # Arguments
    ///
    /// * `host_url`        - The URL of the remote host.
    ///
    /// # Returns
    ///
    /// The receiving end of a oneshot channel that can be awaited. The value from the channel is a
    /// `Result` containing either a `ConnectionSender` that can be used to send messages to the
    /// remote host or a `ConnectionError`.
    ///
    ///
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

    /// Stops the pool from accepting new connection requests and closes down all existing
    /// connections.
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

/// Trait for a factory that can produce connections.
#[async_trait]
pub trait ConnectionFactory {
    /// The type of the connections that this factory produces.
    type ConnectionType: Connection + Send + 'static;

    /// Creates a new connection and returns a `Result` with either the connection
    /// or a `ConnectionError`.
    async fn create_connection(
        &mut self,
        host_url: url::Url,
        buffer_size: usize,
        router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
    ) -> Result<Self::ConnectionType, ConnectionError>;
}

/// Factory that can produce Swim connections to remote hosts.
pub struct SwimConnectionFactory {}

#[async_trait]
impl ConnectionFactory for SwimConnectionFactory {
    type ConnectionType = SwimConnection;

    /// Creates a new Swim connection and returns a `Result` with either the connection
    /// or a `ConnectionError`.
    async fn create_connection(
        &mut self,
        host_url: url::Url,
        buffer_size: usize,
        router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
    ) -> Result<Self::ConnectionType, ConnectionError> {
        SwimConnection::new(host_url, buffer_size, router_tx, SwimWebsocketFactory {}).await
    }
}

/// Trait for a connection to a remote host.
#[async_trait]
pub trait Connection: Sized {
    /// Return a reference to the transmitting end of the channel in the connection.
    fn get_tx(&mut self) -> &mut mpsc::Sender<Message>;

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
}

/// Connection to a remote host.
pub struct SwimConnection {
    tx: mpsc::Sender<Message>,
    _send_handler: JoinHandle<Result<(), ConnectionError>>,
    _receive_handler: JoinHandle<Result<(), ConnectionError>>,
}

impl SwimConnection {
    /// Creates a new websocket connection to a remote host that can send and receive messages.
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

/// Wrapper for the transmitting end of a channel to an open connection.
pub struct ConnectionSender {
    tx: mpsc::Sender<Message>,
}

impl ConnectionSender {
    /// Sends a message asynchronously to the remote host of the connection.
    ///
    /// # Arguments
    ///
    /// * `message`         - Message to be sent to the remote host.
    ///
    /// # Returns
    ///
    /// `Ok` if the message has been sent.
    /// `ConnectionError` if it failed.
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

/// Connection error types returned by the connection pool and the connections.
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionError {
    /// Error that occurred when connecting to a remote host.
    ConnectError,
    /// Error that occurred when sending messages.
    SendMessageError,
    /// Error that occurred when receiving messages.
    ReceiveMessageError,
    /// Error that occurred when closing down connections.
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
