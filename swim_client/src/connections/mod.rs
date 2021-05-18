// Copyright 2015-2021 SWIM.AI inc.
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
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::configuration::router::ConnectionPoolParams;
use crate::router::ClientRequest;
use futures::future::BoxFuture;
use futures::select;
use futures::stream;
use futures::{FutureExt, Stream, StreamExt};
use swim_common::request::request_future::RequestError;
use swim_common::request::Request;
use swim_common::routing::error::{
    CloseError, ConnectionError, ResolutionError, ResolutionErrorKind,
};
use swim_common::routing::{RoutingAddr, TaggedEnvelope};
use swim_common::warp::envelope::Envelope;
use swim_common::warp::path::AbsolutePath;
use swim_runtime::task::*;
use swim_runtime::time::instant::Instant;
use swim_runtime::time::interval::interval;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
#[allow(unused_imports)]
#[cfg(feature = "websocket")]
use tokio_tungstenite::tungstenite;
use tracing::{instrument, trace};
use url::Host;

/// Connection pool message wraps a message from a remote host.
#[derive(Debug)]
pub(crate) struct ConnectionPoolMessage {
    /// The URL of the remote host.
    host: Host,
    /// The message from the remote host.
    message: String,
}

pub struct ConnectionRequest {
    target: AbsolutePath,
    tx: oneshot::Sender<Result<ConnectionChannel, ConnectionError>>,
    recreate_connection: bool,
}

/// Connection pool is responsible for managing the opening and closing of connections
/// to remote hosts.
pub trait ConnectionPool: Clone + Send + 'static {
    fn request_connection(
        &mut self,
        target: AbsolutePath,
        recreate: bool,
    ) -> BoxFuture<Result<Result<Connection, ConnectionError>, RequestError>>;

    fn close(self) -> BoxFuture<'static, Result<Result<(), ConnectionError>, ConnectionError>>;
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
    /// * `config`                 - The configuration for the connection pool.
    /// * `client_conn_request_tx` - A channel for requesting remote connections.
    #[instrument(skip(config))]
    pub fn new(
        config: ConnectionPoolParams,
        client_conn_request_tx: mpsc::Sender<ClientRequest>,
    ) -> SwimConnPool {
        let (connection_request_tx, connection_request_rx) =
            mpsc::channel(config.buffer_size().get());
        let (stop_request_tx, stop_request_rx) = mpsc::channel(config.buffer_size().get());

        let task = PoolTask::new(
            connection_request_rx,
            client_conn_request_tx,
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

pub type Connection = (ConnectionSender, Option<ConnectionReceiver>);

impl ConnectionPool for SwimConnPool {
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
        target: AbsolutePath,
        recreate_connection: bool,
    ) -> BoxFuture<Result<Result<Connection, ConnectionError>, RequestError>> {
        async move {
            let (tx, rx) = oneshot::channel();

            self.connection_request_tx
                .send(ConnectionRequest {
                    target,
                    tx,
                    recreate_connection,
                })
                .await?;
            Ok(rx.await?)
        }
        .boxed()
    }

    /// Stops the pool from accepting new connection requests and closes down all existing
    /// connections.
    fn close(self) -> BoxFuture<'static, Result<Result<(), ConnectionError>, ConnectionError>> {
        let handle = self
            .connection_requests_handle
            .lock()
            .map(|mut h| h.take())
            .map_err(|_| ConnectionError::Closed(CloseError::unexpected()));
        async move {
            self.stop_request_tx.send(()).await?;
            handle
                .map_err(|_| ConnectionError::Closed(CloseError::unexpected()))?
                .ok_or_else(|| ConnectionError::Closed(CloseError::unexpected()))?
                .await
                .map_err(|_| ConnectionError::Closed(CloseError::unexpected()))
        }
        .boxed()
    }
}

enum RequestType {
    NewConnection(ConnectionRequest),
    Prune,
    Close,
}

struct PoolTask {
    connection_request_rx: mpsc::Receiver<ConnectionRequest>,
    client_conn_request_tx: mpsc::Sender<ClientRequest>,
    buffer_size: NonZeroUsize,
    stop_request_rx: mpsc::Receiver<()>,
}

impl PoolTask {
    fn new(
        connection_request_rx: mpsc::Receiver<ConnectionRequest>,
        client_conn_request_tx: mpsc::Sender<ClientRequest>,
        buffer_size: NonZeroUsize,
        stop_request_rx: mpsc::Receiver<()>,
    ) -> Self {
        PoolTask {
            connection_request_rx,
            client_conn_request_tx,
            buffer_size,
            stop_request_rx,
        }
    }

    #[instrument(skip(self, config))]
    async fn run(self, config: ConnectionPoolParams) -> Result<(), ConnectionError> {
        let PoolTask {
            connection_request_rx,
            client_conn_request_tx,
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
                _ = prune_timer.next() => Some(RequestType::Prune),
                req = fused_requests.next() => req,
            }
            .ok_or_else(|| ConnectionError::Closed(CloseError::unexpected()))?;

            match request {
                RequestType::NewConnection(conn_req) => {
                    let ConnectionRequest {
                        target,
                        tx: request_tx,
                        recreate_connection,
                    } = conn_req;

                    //Todo dm this should save node for local connections and host for all other
                    let host = target.host.as_str().to_owned();

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
                        ClientConnection::new(
                            target.clone(),
                            buffer_size.get(),
                            &client_conn_request_tx,
                        )
                        .await
                        .and_then(|connection| {
                            let (inner, sender, receiver) = InnerConnection::from(connection)?;
                            let _ = connections.insert(host.clone(), inner);
                            Ok((sender, Some(receiver)))
                        })
                    } else {
                        let inner_connection = connections.get_mut(&host).ok_or_else(|| {
                            ConnectionError::Resolution(ResolutionError::new(
                                ResolutionErrorKind::Unresolvable,
                                Some(host.clone()),
                            ))
                        })?;
                        inner_connection.last_accessed = Instant::now();

                        Ok(((inner_connection.as_conenction_sender()), None))
                    };

                    request_tx
                        .send(connection_channel)
                        .map_err(|_| ConnectionError::Closed(CloseError::unexpected()))?;
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
    let connection_requests =
        ReceiverStream::new(connection_requests_rx).map(RequestType::NewConnection);
    let close_request = ReceiverStream::new(close_requests_rx).map(|_| RequestType::Close);

    stream::select(connection_requests, close_request)
}

struct SendTask {
    stopped: Arc<AtomicBool>,
    write_sink: mpsc::Sender<TaggedEnvelope>,
    rx: mpsc::Receiver<Envelope>,
}

impl SendTask {
    fn new(
        write_sink: mpsc::Sender<TaggedEnvelope>,
        rx: mpsc::Receiver<Envelope>,
        stopped: Arc<AtomicBool>,
    ) -> Self {
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

        let mut recv_stream = ReceiverStream::new(rx);

        loop {
            match recv_stream.next().await {
                Some(env) => write_sink
                    .send(TaggedEnvelope(RoutingAddr::local(0), env))
                    .await
                    .map_err(|_| ConnectionError::Closed(CloseError::unexpected()))?,
                None => {
                    stopped.store(true, Ordering::Release);
                    return Err(ConnectionError::Closed(CloseError::unexpected()));
                }
            }
        }
    }
}

struct ReceiveTask<S>
where
    S: Stream<Item = Envelope> + Send + Unpin + 'static,
{
    stopped: Arc<AtomicBool>,
    read_stream: S,
    tx: mpsc::Sender<Envelope>,
}

impl<S> ReceiveTask<S>
where
    S: Stream<Item = Envelope> + Send + Unpin + 'static,
{
    fn new(read_stream: S, tx: mpsc::Sender<Envelope>, stopped: Arc<AtomicBool>) -> Self {
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
            tx,
        } = self;

        loop {
            match read_stream.next().await {
                Some(message) => {
                    tx.send(message)
                        .await
                        .map_err(|_| ConnectionError::Closed(CloseError::unexpected()))?;
                }
                None => {
                    stopped.store(true, Ordering::Release);
                    return Err(ConnectionError::Closed(CloseError::unexpected()));
                }
            }
        }
    }
}

struct InnerConnection {
    conn: ClientConnection,
    last_accessed: Instant,
}

impl InnerConnection {
    pub fn as_conenction_sender(&self) -> ConnectionSender {
        ConnectionSender {
            tx: self.conn.tx.clone(),
        }
    }

    pub fn from(
        mut conn: ClientConnection,
    ) -> Result<(InnerConnection, ConnectionSender, mpsc::Receiver<Envelope>), ConnectionError>
    {
        let sender = ConnectionSender {
            tx: conn.tx.clone(),
        };
        let receiver = conn
            .rx
            .take()
            .ok_or_else(|| ConnectionError::Closed(CloseError::unexpected()))?;

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
pub struct ClientConnection {
    stopped: Arc<AtomicBool>,
    tx: mpsc::Sender<Envelope>,
    rx: Option<mpsc::Receiver<Envelope>>,
    _send_handle: TaskHandle<Result<(), ConnectionError>>,
    _receive_handle: TaskHandle<Result<(), ConnectionError>>,
}

impl ClientConnection {
    async fn new(
        target: AbsolutePath,
        buffer_size: usize,
        client_conn_request_tx: &mpsc::Sender<ClientRequest>,
    ) -> Result<ClientConnection, ConnectionError> {
        let (sender_tx, sender_rx) = mpsc::channel(buffer_size);
        let (receiver_tx, receiver_rx) = mpsc::channel(buffer_size);

        let (tx, rx) = oneshot::channel();
        client_conn_request_tx
            .send(ClientRequest::Subscribe {
                target,
                request: Request::new(tx),
            })
            .await
            .map_err(|_| ConnectionError::Resolution(ResolutionError::router_dropped()))?;

        let (raw_route, stream) = rx
            .await
            .map_err(|_| ConnectionError::Resolution(ResolutionError::router_dropped()))??;

        let write_sink = raw_route.sender;
        let read_stream = ReceiverStream::new(stream).fuse();

        let stopped = Arc::new(AtomicBool::new(false));

        let receive = ReceiveTask::new(read_stream, receiver_tx, stopped.clone());
        let send = SendTask::new(write_sink, sender_rx, stopped.clone());

        let send_handler = spawn(send.run());
        let receive_handler = spawn(receive.run());

        Ok(ClientConnection {
            stopped,
            tx: sender_tx,
            rx: Some(receiver_rx),
            _send_handle: send_handler,
            _receive_handle: receive_handler,
        })
    }
}

pub type ConnectionChannel = (ConnectionSender, Option<ConnectionReceiver>);

/// Wrapper for the transmitting end of a channel to an open connection.
#[derive(Debug, Clone)]
pub struct ConnectionSender {
    tx: mpsc::Sender<Envelope>,
}

impl ConnectionSender {
    /// Crate-only function for creating a sender. Useful for unit testing.
    #[doc(hidden)]
    #[allow(dead_code)]
    pub(crate) fn new(tx: mpsc::Sender<Envelope>) -> ConnectionSender {
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
    pub async fn send_message(&mut self, message: Envelope) -> Result<(), SendError<Envelope>> {
        self.tx.send(message).await
    }

    pub fn try_send(&mut self, message: Envelope) -> Result<(), TrySendError<Envelope>> {
        self.tx.try_send(message)
    }
}

pub(crate) type ConnectionReceiver = mpsc::Receiver<Envelope>;
