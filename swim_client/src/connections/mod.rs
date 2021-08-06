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

use crate::configuration::router::ConnectionPoolParams;
use crate::router::{
    ClientRouter, ClientRouterFactory, DownlinkRoutingRequest, RoutingPath, RoutingTable,
};
use either::Either;
use futures::future::BoxFuture;
use futures::select;
use futures::stream;
use futures::{FutureExt, Stream, StreamExt};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use swim_common::request::request_future::RequestError;
use swim_common::request::Request;
use swim_common::routing::error::{
    CloseError, ConnectionError, ResolutionError, ResolutionErrorKind, Unresolvable,
};
use swim_common::routing::remote::table::SchemeHostPort;
use swim_common::routing::remote::RawRoute;
use swim_common::routing::{
    BidirectionalRoute, ConnectionDropped, Route, Router, RouterFactory, RoutingAddr,
    TaggedEnvelope, TaggedSender,
};
use swim_common::warp::envelope::Envelope;
use swim_common::warp::path::Addressable;
use swim_runtime::task::*;
use swim_runtime::time::instant::Instant;
use swim_runtime::time::interval::interval;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::time;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{instrument, trace};
use url::{Host, Url};
use utilities::sync::promise;
use utilities::uri::RelativeUri;

#[cfg(test)]
mod tests;

/// Connection pool is responsible for managing the opening and closing of connections
/// to remote hosts.
pub trait ConnectionPool: Clone + Send + 'static {
    type PathType: Addressable;

    fn request_connection(
        &mut self,
        target: Self::PathType,
    ) -> BoxFuture<Result<Result<Connection, ConnectionError>, RequestError>>;

    fn close(self) -> BoxFuture<'static, Result<Result<(), ConnectionError>, ConnectionError>>;
}

pub type Connection = (ConnectionSender, Option<ConnectionReceiver>);

pub(crate) type ConnectionReceiver = mpsc::Receiver<Envelope>;
pub(crate) type ConnectionSender = TaggedSender;

/// The connection pool is responsible for opening new connections to remote hosts and managing
/// them. It is possible to request a connection to be recreated or to return a cached connection
/// for a given host if it already exists.
#[derive(Clone)]
pub struct SwimConnPool<Path: Addressable> {
    client_tx: mpsc::Sender<DownlinkRoutingRequest<Path>>,
    stop_request_tx: mpsc::Sender<()>,
}

impl<Path: Addressable> SwimConnPool<Path> {
    /// Creates a new connection pool for managing connections to remote hosts.
    ///
    /// # Arguments
    ///
    /// * `config`                 - The configuration for the connection pool.
    /// * `client_conn_request_tx` - A channel for requesting remote connections.
    #[instrument(skip(config))]
    pub fn new<DelegateFac: RouterFactory + Debug>(
        config: ConnectionPoolParams,
        client_channel: (
            mpsc::Sender<DownlinkRoutingRequest<Path>>,
            mpsc::Receiver<DownlinkRoutingRequest<Path>>,
        ),
        client_router_factory: ClientRouterFactory<Path, DelegateFac>,
    ) -> (SwimConnPool<Path>, PoolTask<Path, DelegateFac>) {
        let (client_tx, client_rx) = client_channel;
        let (stop_request_tx, stop_request_rx) = mpsc::channel(config.buffer_size().get());

        (
            SwimConnPool {
                client_tx,
                stop_request_tx,
            },
            PoolTask::new(
                client_rx,
                client_router_factory,
                config.buffer_size(),
                stop_request_rx,
            ),
        )
    }
}

impl<Path: Addressable> ConnectionPool for SwimConnPool<Path> {
    type PathType = Path;
    //Todo dm maybe add a way to request only one way connections

    /// Sends and asynchronous request for a connection to a specific path.
    ///
    /// # Arguments
    ///
    /// * `target`                  - The path to which we want to connect.
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
        target: Self::PathType,
    ) -> BoxFuture<Result<Result<Connection, ConnectionError>, RequestError>> {
        async move {
            let (tx, rx) = oneshot::channel();

            self.client_tx
                .send(DownlinkRoutingRequest::Connect {
                    target,
                    request: Request::new(tx),
                })
                .await?;
            Ok(rx.await?)
        }
        .boxed()
    }

    /// Stops the pool from accepting new connection requests and closes down all existing
    /// connections.
    fn close(self) -> BoxFuture<'static, Result<Result<(), ConnectionError>, ConnectionError>> {
        async move {
            Ok(self
                .stop_request_tx
                .send(())
                .await
                .map_err(|_| ConnectionError::Closed(CloseError::unexpected())))
        }
        .boxed()
    }
}

pub type ConnectionChannel = (ConnectionSender, Option<ConnectionReceiver>);

pub struct ConnectionRequest<Path: Addressable> {
    target: Path,
    tx: oneshot::Sender<Result<ConnectionChannel, ConnectionError>>,
}

pub struct PoolTask<Path: Addressable, DelegateFac: RouterFactory> {
    client_rx: mpsc::Receiver<DownlinkRoutingRequest<Path>>,
    client_router_factory: ClientRouterFactory<Path, DelegateFac>,
    buffer_size: NonZeroUsize,
    stop_request_rx: mpsc::Receiver<()>,
}

impl<Path: Addressable, DelegateFac: RouterFactory> PoolTask<Path, DelegateFac> {
    fn new(
        client_rx: mpsc::Receiver<DownlinkRoutingRequest<Path>>,
        client_router_factory: ClientRouterFactory<Path, DelegateFac>,
        buffer_size: NonZeroUsize,
        stop_request_rx: mpsc::Receiver<()>,
    ) -> Self {
        PoolTask {
            client_rx,
            client_router_factory,
            buffer_size,
            stop_request_rx,
        }
    }

    pub async fn run(self) -> Result<(), ConnectionError> {
        let PoolTask {
            mut client_rx,
            client_router_factory,
            buffer_size,
            //Todo dm
            stop_request_rx,
        } = self;

        let mut routing_table = RoutingTable::new();
        let mut counter: u32 = 0;

        while let Some(client_request) = client_rx.recv().await {
            match client_request {
                DownlinkRoutingRequest::Connect { target, request } => {
                    let routing_path = RoutingPath::from(target.clone());

                    let registrator = match routing_table.try_resolve_addr(&routing_path) {
                        Some(routing_addr) => {
                            match routing_table.try_resolve_endpoint(&routing_addr) {
                                Some(registrator) => registrator,
                                None => {
                                    unimplemented!()
                                }
                            }
                        }
                        None => {
                            let routing_address = RoutingAddr::client(counter);
                            counter += 1;

                            let client_router =
                                client_router_factory.create_for(routing_address.clone());
                            let (registrator, registrator_task) = ConnectionRegistrator::new(
                                buffer_size,
                                target.clone(),
                                client_router,
                            );
                            spawn(registrator_task.run());

                            routing_table.add_registrator(
                                routing_path,
                                routing_address,
                                registrator.clone(),
                            );

                            registrator
                        }
                    };

                    let connection = registrator
                        .request_connection(target, ConnectionType::Full)
                        .await;
                    request.send(connection);
                }
                DownlinkRoutingRequest::Endpoint { addr, request } => {
                    match routing_table.try_resolve_endpoint(&addr) {
                        Some(registrator) => {
                            registrator
                                .registrator_tx
                                .send(RegistratorRequest::Resolve { request })
                                .await;
                        }
                        None => {
                            request.send(Err(Unresolvable(addr)));
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

type ConnectionResult = Result<(ConnectionSender, Option<ConnectionReceiver>), ConnectionError>;

enum ConnectionType {
    Full,
    Outgoing,
}

enum RegistratorRequest<Path: Addressable> {
    Connect {
        tx: oneshot::Sender<ConnectionResult>,
        path: Path,
        conn_type: ConnectionType,
    },
    Resolve {
        request: Request<Result<RawRoute, Unresolvable>>,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct ConnectionRegistrator<Path: Addressable> {
    registrator_tx: mpsc::Sender<RegistratorRequest<Path>>,
}

// Todo dm
impl<Path: Addressable> ConnectionRegistrator<Path> {
    fn new<DelegateRouter: Router>(
        buffer_size: NonZeroUsize,
        target: Path,
        client_router: ClientRouter<Path, DelegateRouter>,
    ) -> (
        ConnectionRegistrator<Path>,
        ConnectionRegistratorTask<Path, DelegateRouter>,
    ) {
        let (registrator_tx, registrator_rx) = mpsc::channel(buffer_size.get());

        (
            ConnectionRegistrator { registrator_tx },
            ConnectionRegistratorTask::new(buffer_size, target, registrator_rx, client_router),
        )
    }

    async fn request_connection(&self, path: Path, conn_type: ConnectionType) -> ConnectionResult {
        let (tx, rx) = oneshot::channel();
        self.registrator_tx
            .send(RegistratorRequest::Connect {
                tx,
                path,
                conn_type,
            })
            .await;
        rx.await.unwrap()
    }
}

enum ConnectionRegistratorTask<Path: Addressable, DelegateRouter: Router> {
    Remote {
        buffer_size: NonZeroUsize,
        target: Url,
        registrator_rx: mpsc::Receiver<RegistratorRequest<Path>>,
        client_router: ClientRouter<Path, DelegateRouter>,
    },
    Local {
        buffer_size: NonZeroUsize,
        target: String,
        registrator_rx: mpsc::Receiver<RegistratorRequest<Path>>,
        client_router: ClientRouter<Path, DelegateRouter>,
    },
}

impl<Path: Addressable, DelegateRouter: Router> ConnectionRegistratorTask<Path, DelegateRouter> {
    fn new(
        buffer_size: NonZeroUsize,
        target: Path,
        registrator_rx: mpsc::Receiver<RegistratorRequest<Path>>,
        client_router: ClientRouter<Path, DelegateRouter>,
    ) -> Self {
        match target.host() {
            Some(url) => ConnectionRegistratorTask::Remote {
                buffer_size,
                target: url,
                registrator_rx,
                client_router,
            },
            None => ConnectionRegistratorTask::Local {
                buffer_size,
                target: target.node().to_string(),
                registrator_rx,
                client_router,
            },
        }
    }

    async fn run(self) {
        match self {
            ConnectionRegistratorTask::Remote {
                buffer_size,
                target,
                registrator_rx,
                mut client_router,
            } => {
                let BidirectionalRoute {
                    sender,
                    mut receiver,
                } = client_router.resolve_bidirectional(target).await.unwrap();

                let mut receiver = ReceiverStream::new(receiver).fuse();
                let mut registrator_rx = ReceiverStream::new(registrator_rx).fuse();

                let mut subscribers: HashMap<String, Vec<mpsc::Sender<Envelope>>> = HashMap::new();
                loop {
                    let request: Either<Option<Envelope>, Option<RegistratorRequest<Path>>> = select! {
                        message = receiver.next() => Either::Left(message),
                        req = registrator_rx.next() => Either::Right(req),
                    };

                    match request {
                        Either::Left(Some(envelope)) => {
                            //Todo dm maybe send the incoming link message directly
                            let incoming_message = envelope.clone().into_incoming().unwrap();
                            let path = incoming_message.path.node.to_string();

                            let subscribers = subscribers.get(&path).unwrap();

                            //Todo dm use futures unordered
                            for sub in subscribers {
                                sub.send(envelope.clone()).await;
                            }
                        }
                        Either::Right(Some(RegistratorRequest::Connect {
                            tx,
                            path,
                            conn_type: ConnectionType::Full,
                        })) => {
                            let receiver = match subscribers.entry(path.node().to_string()) {
                                Entry::Occupied(mut entry) => {
                                    let (tx, rx) = mpsc::channel(buffer_size.get());
                                    entry.get_mut().push(tx);
                                    rx
                                }
                                Entry::Vacant(vacancy) => {
                                    let (tx, rx) = mpsc::channel(buffer_size.get());
                                    vacancy.insert(vec![tx]);
                                    rx
                                }
                            };

                            tx.send(Ok((sender.clone(), Some(receiver))));
                        }
                        Either::Right(Some(RegistratorRequest::Connect {
                            tx,
                            path,
                            conn_type: ConnectionType::Outgoing,
                        })) => {
                            tx.send(Ok((sender.clone(), None)));
                        }
                        _ => {
                            //Todo dm closing
                            unimplemented!()
                        }
                    }
                }
            }
            ConnectionRegistratorTask::Local {
                buffer_size,
                target,
                registrator_rx,
                mut client_router,
            } => {
                //Todo dm change this
                let relative_uri = RelativeUri::try_from(format!("{}", target)).unwrap();
                let routing_addr = client_router.lookup(None, relative_uri).await.unwrap();
                let sender = client_router.resolve_sender(routing_addr).await.unwrap();

                let (on_drop_tx, on_drop_rx) = promise::promise();
                let (envelope_sender, envelope_receiver) = mpsc::channel(buffer_size.get());
                let raw_route = RawRoute::new(envelope_sender, on_drop_rx);

                let mut receiver = ReceiverStream::new(envelope_receiver).fuse();
                let mut registrator_rx = ReceiverStream::new(registrator_rx).fuse();
                let mut subscribers: HashMap<String, Vec<mpsc::Sender<Envelope>>> = HashMap::new();

                loop {
                    let request: Either<Option<TaggedEnvelope>, Option<RegistratorRequest<Path>>> = select! {
                        message = receiver.next() => Either::Left(message),
                        req = registrator_rx.next() => Either::Right(req),
                    };

                    match request {
                        Either::Left(Some(envelope)) => {
                            let incoming_message = envelope.1.clone().into_incoming().unwrap();
                            let path = incoming_message.path.node.to_string();

                            let subscribers = subscribers.get(&path).unwrap();

                            //Todo dm use futures unordered
                            for sub in subscribers {
                                sub.send(envelope.1.clone()).await;
                            }
                        }
                        Either::Right(Some(RegistratorRequest::Connect {
                            tx,
                            path,
                            conn_type: ConnectionType::Full,
                        })) => {
                            let receiver = match subscribers.entry(path.node().to_string()) {
                                Entry::Occupied(mut entry) => {
                                    let (tx, rx) = mpsc::channel(buffer_size.get());
                                    entry.get_mut().push(tx);
                                    rx
                                }
                                Entry::Vacant(vacancy) => {
                                    let (tx, rx) = mpsc::channel(buffer_size.get());
                                    vacancy.insert(vec![tx]);
                                    rx
                                }
                            };

                            tx.send(Ok((sender.sender.clone(), Some(receiver))));
                        }
                        Either::Right(Some(RegistratorRequest::Connect {
                            tx,
                            path,
                            conn_type: ConnectionType::Outgoing,
                        })) => {
                            tx.send(Ok((sender.sender.clone(), None)));
                        }
                        Either::Right(Some(RegistratorRequest::Resolve { request })) => {
                            request.send(Ok(raw_route.clone()));
                        }
                        _ => {
                            //Todo dm closing
                            on_drop_tx.provide(ConnectionDropped::Closed);
                            unimplemented!()
                        }
                    }
                }
            }
        }
    }
}

// /// Connection pool message wraps a message from a remote host.
// #[derive(Debug)]
// pub(crate) struct ConnectionPoolMessage {
//     /// The URL of the remote host.
//     host: Host,
//     /// The message from the remote host.
//     message: String,
// }
//
//
// enum RequestType<Path: Addressable> {
//     Connect(ConnectionRequest<Path>),
//     Prune,
//     Close,
// }
//
// fn combine_connection_streams<Path: Addressable>(
//     connection_requests_rx: mpsc::Receiver<ConnectionRequest<Path>>,
//     close_requests_rx: mpsc::Receiver<()>,
// ) -> impl stream::Stream<Item = RequestType<Path>> + Send + 'static {
//     let connection_requests = ReceiverStream::new(connection_requests_rx).map(RequestType::Connect);
//     let close_request = ReceiverStream::new(close_requests_rx).map(|_| RequestType::Close);
//
//     stream::select(connection_requests, close_request)
// }
//
// struct SendTask {
//     stopped: Arc<AtomicBool>,
//     write_sink: mpsc::Sender<TaggedEnvelope>,
//     rx: mpsc::Receiver<Envelope>,
// }
//
// impl SendTask {
//     fn new(
//         write_sink: mpsc::Sender<TaggedEnvelope>,
//         rx: mpsc::Receiver<Envelope>,
//         stopped: Arc<AtomicBool>,
//     ) -> Self {
//         SendTask {
//             stopped,
//             write_sink,
//             rx,
//         }
//     }
//
//     async fn run(self) -> Result<(), ConnectionError> {
//         let SendTask {
//             stopped,
//             write_sink,
//             rx,
//         } = self;
//
//         let mut recv_stream = ReceiverStream::new(rx);
//
//         loop {
//             match recv_stream.next().await {
//                 Some(env) => write_sink
//                     .send(TaggedEnvelope(RoutingAddr::client(), env))
//                     .await
//                     .map_err(|_| ConnectionError::Closed(CloseError::unexpected()))?,
//                 None => {
//                     stopped.store(true, Ordering::Release);
//                     return Err(ConnectionError::Closed(CloseError::unexpected()));
//                 }
//             }
//         }
//     }
// }
//
// struct ReceiveTask<S>
// where
//     S: Stream<Item = Envelope> + Send + Unpin + 'static,
// {
//     stopped: Arc<AtomicBool>,
//     read_stream: S,
//     tx: mpsc::Sender<Envelope>,
// }
//
// impl<S> ReceiveTask<S>
// where
//     S: Stream<Item = Envelope> + Send + Unpin + 'static,
// {
//     fn new(read_stream: S, tx: mpsc::Sender<Envelope>, stopped: Arc<AtomicBool>) -> Self {
//         ReceiveTask {
//             stopped,
//             read_stream,
//             tx,
//         }
//     }
//
//     async fn run(self) -> Result<(), ConnectionError> {
//         let ReceiveTask {
//             stopped,
//             mut read_stream,
//             tx,
//         } = self;
//
//         loop {
//             match read_stream.next().await {
//                 Some(message) => {
//                     tx.send(message)
//                         .await
//                         .map_err(|_| ConnectionError::Closed(CloseError::unexpected()))?;
//                 }
//                 None => {
//                     stopped.store(true, Ordering::Release);
//                     return Err(ConnectionError::Closed(CloseError::unexpected()));
//                 }
//             }
//         }
//     }
// }
//
// struct InnerConnection {
//     conn: ClientConnection,
//     last_accessed: Instant,
// }
//
// impl InnerConnection {
//     pub fn as_conenction_sender(&self) -> ConnectionSender {
//         ConnectionSender {
//             tx: self.conn.tx.clone(),
//         }
//     }
//
//     pub fn from(
//         mut conn: ClientConnection,
//     ) -> Result<(InnerConnection, ConnectionSender, mpsc::Receiver<Envelope>), ConnectionError>
//     {
//         let sender = ConnectionSender {
//             tx: conn.tx.clone(),
//         };
//         let receiver = conn
//             .rx
//             .take()
//             .ok_or_else(|| ConnectionError::Closed(CloseError::unexpected()))?;
//
//         let inner = InnerConnection {
//             conn,
//             last_accessed: Instant::now(),
//         };
//         Ok((inner, sender, receiver))
//     }
//     pub fn stopped(&self) -> bool {
//         self.conn.stopped.load(Ordering::Acquire)
//     }
// }
//
// /// Connection to a remote host.
// pub struct ClientConnection {
//     stopped: Arc<AtomicBool>,
//     tx: mpsc::Sender<Envelope>,
//     rx: Option<mpsc::Receiver<Envelope>>,
//     _send_handle: TaskHandle<Result<(), ConnectionError>>,
//     _receive_handle: TaskHandle<Result<(), ConnectionError>>,
// }
//
// impl ClientConnection {
//     async fn new<Path: Addressable>(
//         target: Path,
//         buffer_size: usize,
//         client_conn_request_tx: &mpsc::Sender<ClientRequest<Path>>,
//     ) -> Result<ClientConnection, ConnectionError> {
//         let (sender_tx, sender_rx) = mpsc::channel(buffer_size);
//         let (receiver_tx, receiver_rx) = mpsc::channel(buffer_size);
//
//         let (tx, rx) = oneshot::channel();
//         client_conn_request_tx
//             .send(ClientRequest::Subscribe {
//                 target,
//                 request: Request::new(tx),
//             })
//             .await
//             .map_err(|_| ConnectionError::Resolution(ResolutionError::router_dropped()))?;
//
//         let (raw_route, stream) = rx
//             .await
//             .map_err(|_| ConnectionError::Resolution(ResolutionError::router_dropped()))??;
//
//         let write_sink = raw_route.sender;
//         let read_stream = ReceiverStream::new(stream).fuse();
//
//         let stopped = Arc::new(AtomicBool::new(false));
//
//         let receive = ReceiveTask::new(read_stream, receiver_tx, stopped.clone());
//         let send = SendTask::new(write_sink, sender_rx, stopped.clone());
//
//         let send_handler = spawn(send.run());
//         let receive_handler = spawn(receive.run());
//
//         Ok(ClientConnection {
//             stopped,
//             tx: sender_tx,
//             rx: Some(receiver_rx),
//             _send_handle: send_handler,
//             _receive_handle: receive_handler,
//         })
//     }
// }
//

//
// impl ConnectionSender {
//     /// Crate-only function for creating a sender. Useful for unit testing.
//     #[doc(hidden)]
//     #[allow(dead_code)]
//     pub(crate) fn new(tx: mpsc::Sender<Envelope>) -> ConnectionSender {
//         ConnectionSender { tx }
//     }
//
//     /// Sends a message asynchronously to the remote host of the connection.
//     ///
//     /// # Arguments
//     ///
//     /// * `message`         - Message to be sent to the remote host.
//     ///
//     /// # Returns
//     ///
//     /// `Ok` if the message has been sent.
//     /// `SendError` if it failed.
//     pub async fn send_message(&mut self, message: Envelope) -> Result<(), SendError<Envelope>> {
//         self.tx.send(message).await
//     }
//
//     pub fn try_send(&mut self, message: Envelope) -> Result<(), TrySendError<Envelope>> {
//         self.tx.try_send(message)
//     }
// }
//
