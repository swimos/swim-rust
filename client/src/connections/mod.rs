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
use std::fmt;
use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::future::ErrInto as FutErrInto;
use futures::stream;
use futures::{select, Future};
use futures::{Sink, Stream, StreamExt};
use futures_util::future::TryFutureExt;
use futures_util::TryStreamExt;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{ClosedError, SendError, TrySendError};
use tokio::sync::oneshot;
#[cfg(not(target_arch = "wasm32"))]
use tokio_tungstenite::tungstenite;
use tracing::{instrument, trace};
#[cfg(not(target_arch = "wasm32"))]
use tungstenite::error::Error as TError;
use url::Host;

use common::request::request_future::{RequestError, RequestFuture, Sequenced};

use crate::configuration::router::ConnectionPoolParams;
use crate::connections::factory::WebsocketFactory;

pub mod factory;

#[cfg(test)]
mod tests;

/// Connection pool message wraps a message from a remote host.
#[derive(Debug)]
pub(crate) struct ConnectionPoolMessage {
    /// The URL of the remote host.
    host: Host,
    /// The message from the remote host.
    message: String,
}

pub struct ConnectionRequest {
    host_url: url::Url,
    tx: oneshot::Sender<Result<ConnectionChannel, ConnectionError>>,
    recreate_connection: bool,
}

/// Connection pool is responsible for managing the opening and closing of connections
/// to remote hosts.
pub trait ConnectionPool: Clone + Send + 'static {
    type ConnFut: Future<Output = Result<Result<Connection, ConnectionError>, RequestError>> + Send;
    type CloseFut: Future<Output = Result<Result<(), ConnectionError>, ConnectionError>> + Send;

    fn request_connection(&mut self, host_url: url::Url, recreate: bool) -> Self::ConnFut;

    fn close(self) -> Result<Self::CloseFut, ConnectionError>;
}

type ConnectionPoolSharedHandle = Arc<Mutex<Option<TaskHandle<Result<(), ConnectionError>>>>>;

/// The connection pool is responsible for opening new connections to remote hosts and managing
/// them. It is possible to request a connection to be recreated or to return a cached connection
/// for a given host if it already exists.
#[derive(Clone)]
pub struct SwimConnPool {
    connection_request_tx: mpsc::Sender<ConnectionRequest>,
    connection_requests_handle: ConnectionPoolSharedHandle,
    stop_request_tx: mpsc::Sender<()>,
}

impl SwimConnPool {
    /// Creates a new connection pool for managing connections to remote hosts.
    ///
    /// # Arguments
    ///
    /// * `buffer_size`             - The buffer size of the internal channels in the connection
    ///                               pool as an integer.
    /// * `connection_factory`      - Custom factory capable of producing connections for the pool.
    #[instrument(skip(config, connection_factory))]
    pub fn new<WsFac>(config: ConnectionPoolParams, connection_factory: WsFac) -> SwimConnPool
    where
        WsFac: WebsocketFactory + 'static,
    {
        let (connection_request_tx, connection_request_rx) =
            mpsc::channel(config.buffer_size().get());
        let (stop_request_tx, stop_request_rx) = mpsc::channel(config.buffer_size().get());

        let task = PoolTask::new(
            connection_request_rx,
            connection_factory,
            config.buffer_size(),
            stop_request_rx,
        );

        let connection_requests_handler = spawn(task.run(config));

        SwimConnPool {
            connection_request_tx,
            connection_requests_handle: Arc::new(Mutex::new(Some(connection_requests_handler))),
            stop_request_tx,
        }
    }
}

type Connection = (ConnectionSender, Option<ConnectionReceiver>);

type ConnectionFut = Sequenced<
    RequestFuture<ConnectionRequest>,
    oneshot::Receiver<Result<Connection, ConnectionError>>,
>;

type CloseFut = Sequenced<
    FutErrInto<RequestFuture<()>, ConnectionError>,
    TaskHandle<Result<(), ConnectionError>>,
>;

impl ConnectionPool for SwimConnPool {
    type ConnFut = ConnectionFut;
    type CloseFut = CloseFut;

    /// Sends and asynchronous request for a connection to a specific host.
    ///
    /// # Arguments
    ///
    /// * `host_url`                - The URL of the remote host.
    /// * `recreate_connection`     - Boolean flag indicating whether the connection should be recreated.
    ///
    /// # Returns
    ///
    /// A `Result` containing either a `Connection` that can be used to send and receive messages
    /// to the remote host or a `ConnectionError`. The `Connection` contains a `ConnectionSender`
    /// and an optional `ConnectionReceiver`. The `ConnectionReceiver` is returned either the first
    /// time a connection is opened or when it is recreated.
    fn request_connection(
        &mut self,
        host_url: url::Url,
        recreate_connection: bool,
    ) -> Self::ConnFut {
        let (tx, rx) = oneshot::channel();
        let conn_request = RequestFuture::new(
            self.connection_request_tx.clone(),
            ConnectionRequest {
                host_url,
                tx,
                recreate_connection,
            },
        );

        Sequenced::new(conn_request, rx)
    }

    /// Stops the pool from accepting new connection requests and closes down all existing
    /// connections.
    fn close(self) -> Result<Self::CloseFut, ConnectionError> {
        let close_request =
            TryFutureExt::err_into::<ConnectionError>(RequestFuture::new(self.stop_request_tx, ()));

        let handle = self
            .connection_requests_handle
            .lock()
            .map_err(|_| ConnectionErrorKind::ClosedError)?
            .take()
            .ok_or(ConnectionErrorKind::ClosedError)?;

        Ok(Sequenced::new(close_request, handle))
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
    buffer_size: NonZeroUsize,
    stop_request_rx: mpsc::Receiver<()>,
}

impl<WsFac> PoolTask<WsFac>
where
    WsFac: WebsocketFactory + 'static,
{
    fn new(
        connection_request_rx: mpsc::Receiver<ConnectionRequest>,
        connection_factory: WsFac,
        buffer_size: NonZeroUsize,
        stop_request_rx: mpsc::Receiver<()>,
    ) -> Self {
        PoolTask {
            connection_request_rx,
            connection_factory,
            buffer_size,
            stop_request_rx,
        }
    }

    #[instrument(skip(self, config))]
    async fn run(self, config: ConnectionPoolParams) -> Result<(), ConnectionError> {
        let PoolTask {
            connection_request_rx,
            mut connection_factory,
            buffer_size,
            stop_request_rx,
        } = self;
        let mut connections: HashMap<String, InnerConnection> = HashMap::new();

        let mut prune_timer = interval(config.conn_reaper_frequency()).fuse();
        let mut fused_requests =
            combine_connection_streams(connection_request_rx, stop_request_rx).fuse();
        let conn_timeout = config.idle_timeout();

        loop {
            let request: RequestType = select! {
                timer = prune_timer.next() => Some(RequestType::Prune),
                req = fused_requests.next() => req,
            }
            .ok_or(ConnectionErrorKind::ConnectError)?;

            match request {
                RequestType::NewConnection(conn_req) => {
                    let ConnectionRequest {
                        host_url,
                        tx: request_tx,
                        recreate_connection,
                    } = conn_req;

                    let host = host_url.as_str().to_owned();

                    let recreate = match (recreate_connection, connections.get(&host)) {
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
                        SwimConnection::new(
                            host_url.clone(),
                            buffer_size.get(),
                            &mut connection_factory,
                        )
                        .await
                        .and_then(|connection| {
                            let (inner, sender, receiver) = InnerConnection::from(connection)?;
                            let _ = connections.insert(host.clone(), inner);
                            Ok((sender, Some(receiver)))
                        })
                    } else {
                        let inner_connection = connections
                            .get_mut(&host)
                            .ok_or(ConnectionErrorKind::ConnectError)?;
                        inner_connection.last_accessed = Instant::now();

                        Ok(((inner_connection.as_conenction_sender()), None))
                    };

                    request_tx
                        .send(connection_channel)
                        .map_err(|_| ConnectionErrorKind::ConnectError)?;
                }

                RequestType::Prune => {
                    let before_size = connections.len();
                    connections.retain(|_, v| v.last_accessed.elapsed() < conn_timeout);
                    let after_size = connections.len();

                    trace!("Pruned {} inactive connections", (before_size - after_size));
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
    S: Sink<String> + Send + 'static + Unpin,
{
    stopped: Arc<AtomicBool>,
    write_sink: S,
    rx: mpsc::Receiver<String>,
}

impl<S> SendTask<S>
where
    S: Sink<String> + Send + 'static + Unpin,
{
    fn new(write_sink: S, rx: mpsc::Receiver<String>, stopped: Arc<AtomicBool>) -> Self {
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
                ConnectionErrorKind::SendMessageError
            })
            .await
            .map_err(|_| ConnectionError::new(ConnectionErrorKind::SendMessageError))
    }
}

struct ReceiveTask<S>
where
    S: Stream<Item = Result<String, ConnectionError>> + Send + Unpin + 'static,
{
    stopped: Arc<AtomicBool>,
    read_stream: S,
    tx: mpsc::Sender<String>,
}

impl<S> ReceiveTask<S>
where
    S: Stream<Item = Result<String, ConnectionError>> + Send + Unpin + 'static,
{
    fn new(read_stream: S, tx: mpsc::Sender<String>, stopped: Arc<AtomicBool>) -> Self {
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
                    ConnectionErrorKind::ReceiveMessageError
                })?
                .ok_or(ConnectionErrorKind::ReceiveMessageError)?;

            tx.send(message)
                .await
                .map_err(|_| ConnectionErrorKind::ReceiveMessageError)?;
        }
    }
}

struct InnerConnection {
    conn: SwimConnection,
    last_accessed: Instant,
}

impl InnerConnection {
    pub fn as_conenction_sender(&self) -> ConnectionSender {
        ConnectionSender {
            tx: self.conn.tx.clone(),
        }
    }

    pub fn from(
        mut conn: SwimConnection,
    ) -> Result<(InnerConnection, ConnectionSender, mpsc::Receiver<String>), ConnectionError> {
        let sender = ConnectionSender {
            tx: conn.tx.clone(),
        };
        let receiver = conn.rx.take().ok_or(ConnectionErrorKind::ConnectError)?;

        let inner = InnerConnection {
            conn,
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
    tx: mpsc::Sender<String>,
    rx: Option<mpsc::Receiver<String>>,
    _send_handle: TaskHandle<Result<(), ConnectionError>>,
    _receive_handle: TaskHandle<Result<(), ConnectionError>>,
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

        let send_handler = spawn(send.run());
        let receive_handler = spawn(receive.run());

        Ok(SwimConnection {
            stopped,
            tx: sender_tx,
            rx: Some(receiver_rx),
            _send_handle: send_handler,
            _receive_handle: receive_handler,
        })
    }
}

use utilities::rt::task::*;
use utilities::rt::time::instant::Instant;
use utilities::rt::time::interval::interval;

pub type ConnectionChannel = (ConnectionSender, Option<ConnectionReceiver>);

/// Wrapper for the transmitting end of a channel to an open connection.
#[derive(Debug, Clone)]
pub struct ConnectionSender {
    tx: mpsc::Sender<String>,
}

impl ConnectionSender {
    /// Crate-only function for creating a sender. Useful for unit testing.
    #[doc(hidden)]
    #[allow(dead_code)]
    pub(crate) fn new(tx: mpsc::Sender<String>) -> ConnectionSender {
        ConnectionSender { tx }
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
    /// `SendError` if it failed.
    pub async fn send_message(&mut self, message: String) -> Result<(), SendError<String>> {
        self.tx.send(message).await
    }

    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ClosedError>> {
        self.tx.poll_ready(cx)
    }

    pub fn try_send(&mut self, message: String) -> Result<(), TrySendError<String>> {
        self.tx.try_send(message)
    }
}

pub(crate) type ConnectionReceiver = mpsc::Receiver<String>;

/// Connection error types returned by the connection pool and the connections.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ConnectionErrorKind {
    /// Error that occurred when connecting to a remote host.
    ConnectError,
    /// A WebSocket error.  
    SocketError,
    /// Error that occurred when sending messages.
    SendMessageError,
    /// Error that occurred when receiving messages.
    ReceiveMessageError,
    /// Error that occurred when closing down connections.
    ClosedError,
}

impl Display for ConnectionErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self {
            ConnectionErrorKind::ConnectError => {
                write!(f, "An error was produced during a connection.")
            }
            ConnectionErrorKind::SocketError => {
                write!(f, "An error was produced by the web socket.")
            }
            ConnectionErrorKind::SendMessageError => {
                write!(f, "An error occured when sending a message.")
            }
            ConnectionErrorKind::ReceiveMessageError => {
                write!(f, "An error occured when receiving a message.")
            }
            ConnectionErrorKind::ClosedError => {
                write!(f, "An error occured when closing down connections.")
            }
        }
    }
}

#[derive(Debug)]
pub struct ConnectionError {
    kind: ConnectionErrorKind,
    #[cfg(not(target_arch = "wasm32"))]
    tungstenite_error: Option<TError>,
}

impl PartialEq for ConnectionError {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}

impl Clone for ConnectionError {
    fn clone(&self) -> Self {
        ConnectionError {
            kind: self.kind,
            #[cfg(not(target_arch = "wasm32"))]
            tungstenite_error: None,
        }
    }
}

impl Display for ConnectionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        #[cfg(not(target_arch = "wasm32"))]
        {
            match &self.tungstenite_error {
                Some(e) => match self.kind.fmt(f) {
                    Ok(_) => write!(f, " Tungstenite error: {}", e),
                    e => e,
                },
                None => self.kind.fmt(f),
            }
        }

        #[cfg(target_arch = "wasm32")]
        {
            self.kind.fmt(f)
        }
    }
}

impl ConnectionError {
    pub fn new(kind: ConnectionErrorKind) -> Self {
        ConnectionError {
            kind,
            #[cfg(not(target_arch = "wasm32"))]
            tungstenite_error: None,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_tungstenite_error(kind: ConnectionErrorKind, tungstenite_error: TError) -> Self {
        ConnectionError {
            kind,
            tungstenite_error: Some(tungstenite_error),
        }
    }

    pub fn kind(&self) -> ConnectionErrorKind {
        self.kind
    }

    pub fn into_kind(self) -> ConnectionErrorKind {
        self.kind
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn tungstenite_error(&mut self) -> Option<TError> {
        self.tungstenite_error.take()
    }

    /// Returns whether or not the error kind is deemed to be transient.
    pub fn is_transient(&self) -> bool {
        match &self.kind() {
            ConnectionErrorKind::SocketError => false,
            _ => true,
        }
    }
}

impl From<ConnectionErrorKind> for ConnectionError {
    fn from(kind: ConnectionErrorKind) -> Self {
        ConnectionError {
            kind,
            #[cfg(not(target_arch = "wasm32"))]
            tungstenite_error: None,
        }
    }
}

impl From<RequestError> for ConnectionError {
    fn from(_: RequestError) -> Self {
        ConnectionError {
            kind: ConnectionErrorKind::ConnectError,
            #[cfg(not(target_arch = "wasm32"))]
            tungstenite_error: None,
        }
    }
}

impl From<TaskError> for ConnectionError {
    fn from(_: TaskError) -> Self {
        ConnectionError {
            kind: ConnectionErrorKind::ConnectError,
            #[cfg(not(target_arch = "wasm32"))]
            tungstenite_error: None,
        }
    }
}

impl From<tokio::task::JoinError> for ConnectionError {
    fn from(_: tokio::task::JoinError) -> Self {
        ConnectionError {
            kind: ConnectionErrorKind::ConnectError,
            #[cfg(not(target_arch = "wasm32"))]
            tungstenite_error: None,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<TError> for ConnectionError {
    fn from(e: TError) -> Self {
        match e {
            TError::ConnectionClosed | TError::AlreadyClosed => {
                ConnectionError::from(ConnectionErrorKind::ClosedError)
            }
            e @ TError::Http(_)
            | e @ TError::HttpFormat(_)
            | e @ TError::Tls(_)
            | e @ TError::Protocol(_)
            | e @ TError::Io(_)
            | e @ TError::Url(_) => ConnectionError {
                kind: ConnectionErrorKind::SocketError,
                tungstenite_error: Some(e),
            },
            _ => ConnectionError {
                kind: ConnectionErrorKind::ConnectError,
                tungstenite_error: Some(e),
            },
        }
    }
}
