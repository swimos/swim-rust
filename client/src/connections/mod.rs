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

use futures::future::{AbortHandle, Abortable, Aborted};
use futures::{Sink, Stream, StreamExt};
use futures_util::TryStreamExt;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::tungstenite::protocol::Message;

pub mod factory;

//#[cfg(test)]
//mod tests;

/// Connection pool message wraps a message from a remote host.
#[derive(Debug)]
pub struct ConnectionPoolMessage {
    /// The URL of the remote host.
    host: String,
    /// The message from the remote host.
    message: String,
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
    pub fn new<WsFac>(
        buffer_size: usize,
        router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
        connection_factory: WsFac,
    ) -> ConnectionPool
    where
        WsFac: WebsocketFactory + 'static,
    {
        let (connection_request_tx, connection_request_rx) = mpsc::channel(buffer_size);

        let (connection_requests_abort_handle, abort_registration) = AbortHandle::new_pair();

        let task = PoolTask::new(
            connection_request_rx,
            connection_factory,
            router_tx,
            buffer_size,
        );

        let accept_connection_requests_future = Abortable::new(task.run(), abort_registration);

        let connection_requests_handler = tokio::task::spawn(accept_connection_requests_future);

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

struct PoolTask<WsFac>
where
    WsFac: WebsocketFactory + 'static,
{
    rx: mpsc::Receiver<ConnectionRequest>,
    connection_factory: WsFac,
    router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
    buffer_size: usize,
}

impl<WsFac> PoolTask<WsFac>
where
    WsFac: WebsocketFactory + 'static,
{
    fn new(
        rx: mpsc::Receiver<ConnectionRequest>,
        connection_factory: WsFac,
        router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
        buffer_size: usize,
    ) -> Self {
        PoolTask {
            rx,
            connection_factory,
            router_tx,
            buffer_size,
        }
    }

    async fn run(self) -> Result<(), ConnectionError> {
        let PoolTask {
            mut rx,
            mut connection_factory,
            router_tx,
            buffer_size,
        } = self;
        let mut connections: HashMap<String, SwimConnection> = HashMap::new();
        loop {
            let connection_request = rx.recv().await.ok_or(ConnectionError::ConnectError)?;
            let ConnectionRequest {
                host_url,
                tx: request_tx,
            } = connection_request;

            let host = host_url.as_str().to_owned();

            let sender = if connections.contains_key(&host) {
                let conn = connections
                    .get_mut(&host)
                    .ok_or(ConnectionError::ConnectError)?;
                Ok(ConnectionSender {
                    tx: conn.tx.clone(),
                })
            } else {
                let connection_result = SwimConnection::new(
                    host_url,
                    buffer_size,
                    router_tx.clone(),
                    &mut connection_factory,
                )
                .await;

                match connection_result {
                    Ok(connection) => {
                        let sender = ConnectionSender {
                            tx: connection.tx.clone(),
                        };
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
}

struct SendTask<S>
where
    S: Sink<Message> + Send + 'static + Unpin,
{
    write_sink: S,
    rx: mpsc::Receiver<Message>,
}

impl<S> SendTask<S>
where
    S: Sink<Message> + Send + 'static + Unpin,
{
    fn new(write_sink: S, rx: mpsc::Receiver<Message>) -> Self {
        SendTask { write_sink, rx }
    }

    async fn run(self) -> Result<(), ConnectionError> {
        let SendTask { write_sink, rx } = self;
        rx.map(Ok)
            .forward(write_sink)
            .await
            .map_err(|_| ConnectionError::SendMessageError)
    }
}

struct ReceiveTask<S>
where
    S: Stream<Item = Result<Message, ConnectionError>> + Send + Unpin + 'static,
{
    read_stream: S,
    router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
    host: String,
}

impl<S> ReceiveTask<S>
where
    S: Stream<Item = Result<Message, ConnectionError>> + Send + Unpin + 'static,
{
    fn new(
        read_stream: S,
        router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
        host: String,
    ) -> Self {
        ReceiveTask {
            read_stream,
            router_tx,
            host,
        }
    }

    async fn run(self) -> Result<(), ConnectionError> {
        let ReceiveTask {
            mut read_stream,
            mut router_tx,
            host,
        } = self;

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
        websocket_factory: &mut T,
    ) -> Result<SwimConnection, ConnectionError> {
        let host = host_url.as_str().to_owned();
        let (tx, rx) = mpsc::channel(buffer_size);

        let (write_sink, read_stream) = websocket_factory.connect(host_url).await?;

        let receive = ReceiveTask::new(read_stream, router_tx, host);
        let send = SendTask::new(write_sink, rx);

        let send_handler = tokio::spawn(send.run());
        let receive_handler = tokio::spawn(receive.run());

        Ok(SwimConnection {
            tx,
            _send_handler: send_handler,
            _receive_handler: receive_handler,
        })
    }
}

/// Wrapper for the transmitting end of a channel to an open connection.
#[derive(Debug, Clone)]
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

impl From<RequestError> for ConnectionError {
    fn from(_: RequestError) -> Self {
        ConnectionError::ClosedError
    }
}

impl From<tokio::task::JoinError> for ConnectionError {
    fn from(_: tokio::task::JoinError) -> Self {
        ConnectionError::ConnectError
    }
}

impl From<Aborted> for ConnectionError {
    fn from(_: Aborted) -> Self {
        ConnectionError::ClosedError
    }
}

use crate::connections::factory::WebsocketFactory;
use common::request::request_future::RequestError;
use tungstenite::error::Error as TError;

impl From<tungstenite::error::Error> for ConnectionError {
    fn from(e: TError) -> Self {
        match e {
            TError::ConnectionClosed | TError::AlreadyClosed => ConnectionError::ClosedError,
            TError::Http(_) | TError::HttpFormat(_) | TError::Tls(_) => {
                ConnectionError::ConnectError
            }
            _ => ConnectionError::ReceiveMessageError,
        }
    }
}
