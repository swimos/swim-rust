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

use futures::select;
use futures::stream;
use futures::{Sink, Stream, StreamExt};
use futures_util::future::TryFutureExt;
use futures_util::FutureExt;
use futures_util::TryStreamExt;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{ClosedError, SendError, TrySendError};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::tungstenite::protocol::Message;
use tungstenite::error::Error as TError;

use common::request::request_future::RequestError;

use crate::connections::factory::WebsocketFactory;
use url::Host;

pub mod factory;

#[cfg(test)]
mod tests;

/// Connection pool message wraps a message from a remote host.
#[derive(Debug)]
pub struct ConnectionPoolMessage {
    /// The URL of the remote host.
    pub host: Host,
    /// The message from the remote host.
    pub message: String,
}

struct ConnectionRequest {
    host_url: url::Url,
    tx: oneshot::Sender<Result<ConnectionChannel, ConnectionError>>,
    recreate: bool,
}

type ConnectionPoolSharedHandler = Arc<Mutex<Option<JoinHandle<Result<(), ConnectionError>>>>>;

/// Connection pool is responsible for managing the opening and closing of connections
/// to remote hosts.
#[derive(Clone)]
pub struct ConnectionPool {
    connection_request_tx: mpsc::Sender<ConnectionRequest>,
    connection_requests_handler: ConnectionPoolSharedHandler,
    stop_request_tx: mpsc::Sender<()>,
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
    pub fn new<WsFac>(buffer_size: usize, connection_factory: WsFac) -> ConnectionPool
    where
        WsFac: WebsocketFactory + 'static,
    {
        let (connection_request_tx, connection_request_rx) = mpsc::channel(buffer_size);
        let (stop_request_tx, stop_request_rx) = mpsc::channel(buffer_size);

        let task = PoolTask::new(
            connection_request_rx,
            connection_factory,
            buffer_size,
            stop_request_rx,
        );

        // TODO: Add configuration for connections
        let connection_requests_handler =
            tokio::task::spawn(task.run(Duration::from_secs(5), Duration::from_secs(10)));

        ConnectionPool {
            connection_request_tx,
            connection_requests_handler: Arc::new(Mutex::new(Some(connection_requests_handler))),
            stop_request_tx,
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
    pub fn request_connection(
        &mut self,
        host_url: url::Url,
        recreate: bool,
    ) -> Result<oneshot::Receiver<Result<ConnectionChannel, ConnectionError>>, ConnectionError>
    {
        let (tx, rx) = oneshot::channel();

        self.connection_request_tx
            .try_send(ConnectionRequest {
                host_url,
                tx,
                recreate,
            })
            .map_err(|_| ConnectionError::ConnectError(None))?;

        Ok(rx)
    }

    /// Stops the pool from accepting new connection requests and closes down all existing
    /// connections.
    pub async fn close(mut self) -> Result<(), ConnectionError> {
        self.stop_request_tx
            .send(())
            .await
            .map_err(|_| ConnectionError::ClosedError)?;

        let handle = self
            .connection_requests_handler
            .lock()
            .map_err(|_| ConnectionError::ClosedError)?
            .take()
            .ok_or(ConnectionError::ClosedError)?;

        handle.await.map_err(|_| ConnectionError::ClosedError)?
    }
}

enum RequestType {
    NewConnection(ConnectionRequest),
    Prune,
    Close,
}

struct PoolTask<WsFac>
where
    WsFac: WebsocketFactory + 'static,
{
    connection_request_rx: mpsc::Receiver<ConnectionRequest>,
    connection_factory: WsFac,
    buffer_size: usize,
    stop_request_rx: mpsc::Receiver<()>,
}

impl<WsFac> PoolTask<WsFac>
where
    WsFac: WebsocketFactory + 'static,
{
    fn new(
        connection_request_rx: mpsc::Receiver<ConnectionRequest>,
        connection_factory: WsFac,
        buffer_size: usize,
        stop_request_rx: mpsc::Receiver<()>,
    ) -> Self {
        PoolTask {
            connection_request_rx,
            connection_factory,
            buffer_size,
            stop_request_rx,
        }
    }

    async fn run(
        self,
        reaper_frequency: Duration,
        conn_timeout: Duration,
    ) -> Result<(), ConnectionError> {
        let PoolTask {
            connection_request_rx,
            mut connection_factory,
            buffer_size,
            stop_request_rx,
        } = self;
        let mut connections: HashMap<String, InnerConnection> = HashMap::new();

        let mut prune_timer = tokio::time::delay_for(reaper_frequency).fuse();
        let requests_rx = combine_connection_streams(connection_request_rx, stop_request_rx);
        let mut fused_requests = requests_rx.fuse();

        loop {
            let request: RequestType = select! {
            timer = prune_timer => Some(RequestType::Prune),
            req = fused_requests.next() => req,
            }
            .ok_or(ConnectionError::ConnectError(None))?;

            match request {
                RequestType::NewConnection(conn_req) => {
                    let ConnectionRequest {
                        host_url,
                        tx: request_tx,
                        recreate,
                    } = conn_req;

                    let host = host_url.as_str().to_owned();

                    let recreate = match (recreate, connections.get(&host)) {
                        // Connection has stopped and needs to be recreated
                        (_, Some(inner)) if inner.stopped() => true,
                        // Connection doesn't exist
                        (false, None) => true,
                        // Connection exists and is healthy
                        (false, Some(_)) => false,
                        // Connection doesn't exist
                        (true, _) => true,
                    };

                    let connection_channel = if recreate {
                        let connection_result =
                            SwimConnection::new(host_url, buffer_size, &mut connection_factory)
                                .await;

                        match connection_result {
                            Ok(connection) => {
                                let (inner, sender, receiver) = InnerConnection::from(connection)?;
                                let _ = connections.insert(host.clone(), inner);
                                Ok((sender, Some(receiver)))
                            }
                            Err(error) => Err(error),
                        }
                    } else {
                        let mut inner_connection = connections
                            .get_mut(&host)
                            .ok_or(ConnectionError::ConnectError(None))?;
                        inner_connection.last_accessed = Instant::now();

                        Ok((
                            (ConnectionSender {
                                tx: inner_connection.conn.tx.clone(),
                            }),
                            None,
                        ))
                    };

                    request_tx
                        .send(connection_channel)
                        .map_err(|_| ConnectionError::ConnectError(None))?;
                }

                RequestType::Prune => {
                    connections.retain(|_, v| v.last_accessed.elapsed() < conn_timeout);
                    prune_timer = tokio::time::delay_for(reaper_frequency).fuse();
                }

                RequestType::Close => {
                    break Ok(());
                }
            }
        }
    }
}

fn combine_connection_streams(
    connection_requests_rx: mpsc::Receiver<ConnectionRequest>,
    close_requests_rx: mpsc::Receiver<()>,
) -> impl stream::Stream<Item = RequestType> + Send + 'static {
    let connection_requests = connection_requests_rx.map(RequestType::NewConnection);
    let close_request = close_requests_rx.map(|_| RequestType::Close);

    stream::select(connection_requests, close_request)
}

struct SendTask<S>
where
    S: Sink<Message> + Send + 'static + Unpin,
{
    stopped: Arc<AtomicBool>,
    write_sink: S,
    rx: mpsc::Receiver<Message>,
}

impl<S> SendTask<S>
where
    S: Sink<Message> + Send + 'static + Unpin,
{
    fn new(write_sink: S, rx: mpsc::Receiver<Message>, stopped: Arc<AtomicBool>) -> Self {
        SendTask {
            stopped,
            write_sink,
            rx,
        }
    }

    async fn run(self) -> Result<(), ConnectionError> {
        let SendTask {
            stopped,
            write_sink,
            rx,
        } = self;

        rx.map(Ok)
            .forward(write_sink)
            .map_err(|_| {
                stopped.store(true, Ordering::Release);
                ConnectionError::SendMessageError
            })
            .await
            .map_err(|_| ConnectionError::SendMessageError)
    }
}

struct ReceiveTask<S>
where
    S: Stream<Item = Result<Message, ConnectionError>> + Send + Unpin + 'static,
{
    stopped: Arc<AtomicBool>,
    read_stream: S,
    tx: mpsc::Sender<Message>,
}

impl<S> ReceiveTask<S>
where
    S: Stream<Item = Result<Message, ConnectionError>> + Send + Unpin + 'static,
{
    fn new(read_stream: S, tx: mpsc::Sender<Message>, stopped: Arc<AtomicBool>) -> Self {
        ReceiveTask {
            stopped,
            read_stream,
            tx,
        }
    }

    async fn run(self) -> Result<(), ConnectionError> {
        let ReceiveTask {
            stopped,
            mut read_stream,
            mut tx,
        } = self;

        loop {
            let message = read_stream
                .try_next()
                .await
                .map_err(|_| {
                    stopped.store(true, Ordering::Release);
                    ConnectionError::ReceiveMessageError
                })?
                .ok_or(ConnectionError::ReceiveMessageError)?;

            tx.send(message)
                .await
                .map_err(|_| ConnectionError::ReceiveMessageError)?;
        }
    }
}

struct InnerConnection {
    conn: SwimConnection,
    _birth: Instant,
    last_accessed: Instant,
}

impl InnerConnection {
    pub fn from(
        mut conn: SwimConnection,
    ) -> Result<(InnerConnection, ConnectionSender, mpsc::Receiver<Message>), ConnectionError> {
        let sender = ConnectionSender {
            tx: conn.tx.clone(),
        };
        let receiver = conn.rx.take().ok_or(ConnectionError::ConnectError(None))?;

        let inner = InnerConnection {
            conn,
            _birth: Instant::now(),
            last_accessed: Instant::now(),
        };
        Ok((inner, sender, receiver))
    }
    pub fn stopped(&self) -> bool {
        self.conn.stopped.load(Ordering::Acquire)
    }
}

/// Connection to a remote host.
pub struct SwimConnection {
    stopped: Arc<AtomicBool>,
    tx: mpsc::Sender<Message>,
    rx: Option<mpsc::Receiver<Message>>,
    _send_handler: JoinHandle<Result<(), ConnectionError>>,
    _receive_handler: JoinHandle<Result<(), ConnectionError>>,
}

impl SwimConnection {
    async fn new<T: WebsocketFactory + Send + Sync + 'static>(
        host_url: url::Url,
        buffer_size: usize,
        websocket_factory: &mut T,
    ) -> Result<SwimConnection, ConnectionError> {
        let (sender_tx, sender_rx) = mpsc::channel(buffer_size);
        let (receiver_tx, receiver_rx) = mpsc::channel(buffer_size);

        let (write_sink, read_stream) = websocket_factory.connect(host_url).await?;

        let stopped = Arc::new(AtomicBool::new(false));

        let receive = ReceiveTask::new(read_stream, receiver_tx, stopped.clone());
        let send = SendTask::new(write_sink, sender_rx, stopped.clone());

        let send_handler = tokio::spawn(send.run());
        let receive_handler = tokio::spawn(receive.run());

        Ok(SwimConnection {
            stopped,
            tx: sender_tx,
            rx: Some(receiver_rx),
            _send_handler: send_handler,
            _receive_handler: receive_handler,
        })
    }
}

pub type ConnectionChannel = (ConnectionSender, Option<ConnectionReceiver>);

/// Wrapper for the transmitting end of a channel to an open connection.
#[derive(Debug, Clone)]
pub struct ConnectionSender {
    tx: mpsc::Sender<Message>,
}

impl ConnectionSender {
    crate_only! {
        /// Crate-only function for creating a sender. Useful for unit testing.
        fn new(tx: mpsc::Sender<Message>) -> ConnectionSender {
            ConnectionSender { tx }
        }
    }

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
    pub async fn send_message(&mut self, message: Message) -> Result<(), SendError<Message>> {
        self.tx.send(message).await
    }

    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ClosedError>> {
        self.tx.poll_ready(cx)
    }

    pub fn try_send(&mut self, message: Message) -> Result<(), TrySendError<Message>> {
        self.tx.try_send(message)
    }
}

type ConnectionReceiver = mpsc::Receiver<Message>;

/// Connection error types returned by the connection pool and the connections.
#[derive(Debug, PartialEq, Clone)]
pub enum ConnectionError {
    /// Error that occurred when connecting to a remote host. With an optional error message.
    ConnectError(Option<Cow<'static, str>>),
    /// Error that occurred when sending messages.
    SendMessageError,
    /// Error that occurred when receiving messages.
    ReceiveMessageError,
    /// Error that occurred when closing down connections.
    ClosedError,
    /// A transient, possibly recoverable error that has occured. Used to signal that reopening or
    /// attempting a request again may resolve correctly.
    Transient,
}

impl From<RequestError> for ConnectionError {
    fn from(_: RequestError) -> Self {
        ConnectionError::ClosedError
    }
}

impl From<tokio::task::JoinError> for ConnectionError {
    fn from(_: tokio::task::JoinError) -> Self {
        ConnectionError::ConnectError(None)
    }
}

impl From<tungstenite::error::Error> for ConnectionError {
    fn from(e: TError) -> Self {
        match e {
            TError::ConnectionClosed | TError::AlreadyClosed => ConnectionError::ClosedError,
            TError::Http(_) | TError::HttpFormat(_) | TError::Tls(_) => {
                ConnectionError::ConnectError(None)
            }
            _ => ConnectionError::ReceiveMessageError,
        }
    }
}
