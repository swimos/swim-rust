// Copyright 2015-2023 Swim Inc.
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

use fnv::FnvHashMap;
use futures::StreamExt;
use futures_util::future::{BoxFuture, Either, Fuse};
use futures_util::stream::FuturesUnordered;
use futures_util::FutureExt;
use ratchet::{ExtensionProvider, SplittableExtension, WebSocketStream};
use std::borrow::Borrow;
use std::collections::hash_map::Entry;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use swimos_remote::{
    net::{Scheme, SchemeHostPort},
    websocket::WebsocketClient,
};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::task::{JoinError, JoinHandle};
use tracing::{debug, error, info, trace};
use uuid::Uuid;

use crate::error::{DownlinkErrorKind, DownlinkRuntimeError};
use crate::models::{DownlinkRuntime, IdIssuer, Key, Peer, RemotePath};
use crate::pending::{PendingConnections, PendingDownlink, Waiting};
use crate::transport::{Transport, TransportHandle};
use swimos_api::{address::Address, agent::DownlinkKind, error::DownlinkFailureReason};
use swimos_client_api::{Downlink, DownlinkConfig};
use swimos_model::Text;
use swimos_remote::net::ClientConnections;
use swimos_remote::AttachClient;
use swimos_runtime::downlink::{AttachAction, DownlinkOptions, DownlinkRuntimeConfig};
use swimos_utilities::byte_channel::{byte_channel, ByteReader, ByteWriter};
use swimos_utilities::trigger;
use swimos_utilities::trigger::promise;

pub type BoxedDownlink = Box<dyn Downlink + Send + Sync + 'static>;
type ByteChannel = (ByteWriter, ByteReader);
type CallbackResult =
    Result<promise::Receiver<Result<(), Arc<DownlinkRuntimeError>>>, Arc<DownlinkRuntimeError>>;
pub type DownlinkCallback = oneshot::Sender<CallbackResult>;

impl Debug for DownlinkRegistrationRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let DownlinkRegistrationRequest {
            path,
            downlink,
            options,
            callback,
            runtime_config,
            downlink_config,
        } = self;
        f.debug_struct("DownlinkRegistrationRequest")
            .field("path", path)
            .field("options", options)
            .field("downlink", &downlink.kind())
            .field("callback", callback)
            .field("runtime_config", runtime_config)
            .field("downlink_config", downlink_config)
            .finish()
    }
}

/// A raw handle to the downlink runtime which can be used to register new downlinks, starting
/// connection and runtime tasks as required.
///
/// Internally, a dedicated transport task is started which opens new connection tasks, providing
/// DNS lookups and opening Websocket connections as required. When a new downlink registration
/// request is received, the runtime task first checks to see if a connection exists and if so, it
/// registers a new channel for communication to the peer. A corresponding runtime is started and
/// then a channel is opened between the runtime and the downlink's own internal runtime task; if
/// two requests for the same address and downlink kind are made but they have disjoint runtime
/// configurations then two independent runtimes are started.
#[derive(Debug)]
pub struct RawHandle {
    dispatch: mpsc::Sender<DownlinkRegistrationRequest>,
    completed: Arc<Notify>,
}

impl RawHandle {
    pub async fn completed(&self) {
        self.completed.notified().await;
    }

    pub async fn run_downlink<D>(
        &self,
        path: RemotePath,
        runtime_config: DownlinkRuntimeConfig,
        downlink_config: DownlinkConfig,
        options: DownlinkOptions,
        downlink: D,
    ) -> CallbackResult
    where
        D: Downlink + Send + Sync + 'static,
    {
        let (request_tx, request_rx) = oneshot::channel();
        let request = DownlinkRegistrationRequest {
            path,
            downlink: Box::new(downlink),
            callback: request_tx,
            options,
            runtime_config,
            downlink_config,
        };
        self.dispatch
            .send(request)
            .await
            .map_err(|_| Arc::new(DownlinkRuntimeError::new(DownlinkErrorKind::Terminated)))?;
        request_rx
            .await
            .map_err(|_| Arc::new(DownlinkRuntimeError::new(DownlinkErrorKind::Terminated)))?
    }
}

/// Downlink registration properties.
pub struct DownlinkRegistrationRequest {
    /// The path of the downlink to open.
    pub path: RemotePath,
    /// The downlink to run.
    pub downlink: BoxedDownlink,
    /// A callback for providing the result of the request. The promise will be satisfied when the
    /// 'downlink' task completed.
    pub callback: oneshot::Sender<CallbackResult>,
    /// Downlink link and sync options.
    pub options: DownlinkOptions,
    /// Properties for configuring the corresponding runtime if it has not been started.
    pub runtime_config: DownlinkRuntimeConfig,
    /// Properties for running the downlink with.
    pub downlink_config: DownlinkConfig,
}

enum RuntimeEvent {
    /// A request to start a downlink; opening a connection to the peer if required and starting the
    /// downlink type's runtime.
    StartDownlink(DownlinkRegistrationRequest),
    /// A DNS resolution task completed.
    Resolved {
        scheme: Scheme,
        host: Text,
        result: Result<Vec<SocketAddr>, DownlinkRuntimeError>,
    },
    /// A WebSocket connection result.
    ConnectionResult {
        host: Text,
        result: Result<(SocketAddr, mpsc::Sender<AttachClient>), DownlinkRuntimeError>,
    },
    /// A request to attach a downlink to its runtime has completed.
    DownlinkRuntimeStarted {
        sock: SocketAddr,
        key: Key,
        result:
            Result<(mpsc::Sender<AttachAction>, DownlinkRuntime), (DownlinkFailureReason, Text)>,
    },
    /// An attachment to a downlink runtime task has completed.
    DownlinkRuntimeAttached {
        pending: PendingDownlink,
        result: Result<ByteChannel, DownlinkRuntimeError>,
    },
    /// A downlink task has completed.
    DownlinkTaskComplete {
        kind: DownlinkKind,
        address: RemotePath,
        result: Result<(), DownlinkRuntimeError>,
        tx: promise::Sender<Result<(), Arc<DownlinkRuntimeError>>>,
    },
    /// A downlink runtime has completed.
    DownlinkRuntimeComplete {
        addr: SocketAddr,
        key: Key,
        result: Result<(), JoinError>,
    },
    /// The runtime is about to shutdown.
    Shutdown,
}

impl Debug for RuntimeEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeEvent::StartDownlink(_) => {
                write!(f, "RuntimeEvent::StartDownlink")
            }
            RuntimeEvent::Resolved { .. } => {
                write!(f, "RuntimeEvent::Resolved")
            }
            RuntimeEvent::ConnectionResult { result, .. } => {
                write!(f, "RuntimeEvent::ConnectionResult({})", result.is_ok())
            }
            RuntimeEvent::DownlinkRuntimeStarted { .. } => {
                write!(f, "RuntimeEvent::DownlinkRuntimeStarted")
            }
            RuntimeEvent::DownlinkRuntimeAttached { .. } => {
                write!(f, "RuntimeEvent::DownlinkRuntimeAttached")
            }
            RuntimeEvent::DownlinkTaskComplete { .. } => {
                write!(f, "RuntimeEvent::DownlinkTaskComplete")
            }
            RuntimeEvent::DownlinkRuntimeComplete { .. } => {
                write!(f, "RuntimeEvent::DownlinkRuntimeComplete")
            }
            RuntimeEvent::Shutdown => {
                write!(f, "RuntimeEvent::Shutdown")
            }
        }
    }
}

/// Spawns a runtime task that uses the provided transport task and returns a handle that can be
/// used to dispatch downlink registration requests.
pub fn start_runtime<Net, Ws, Provider>(
    registration_buffer_size: NonZeroUsize,
    stop_rx: trigger::Receiver,
    transport: Transport<Net, Ws, Provider>,
    transport_buffer_size: NonZeroUsize,
    interpret_frame_data: bool,
) -> (RawHandle, BoxFuture<'static, ()>)
where
    Net: ClientConnections,
    Net::ClientSocket: WebSocketStream,
    Ws: WebsocketClient + Send + Sync + 'static,
    Provider: ExtensionProvider + Send + Sync + 'static,
    Provider::Extension: SplittableExtension + Send + Sync,
{
    let (requests_tx, requests_rx) = mpsc::channel(registration_buffer_size.get());
    let notified = Arc::new(Notify::new());
    let completed = notified.clone();
    let task = async move {
        runtime_task(
            transport,
            transport_buffer_size,
            stop_rx,
            requests_rx,
            interpret_frame_data,
        )
        .await;
        completed.notify_waiters();
    };

    (
        RawHandle {
            dispatch: requests_tx,
            completed: notified,
        },
        task.boxed(),
    )
}

async fn runtime_task<Net, Ws, Provider>(
    transport: Transport<Net, Ws, Provider>,
    transport_buffer_size: NonZeroUsize,
    mut remote_stop_rx: trigger::Receiver,
    mut requests_rx: mpsc::Receiver<DownlinkRegistrationRequest>,
    interpret_frame_data: bool,
) where
    Net: ClientConnections,
    Net::ClientSocket: WebSocketStream,
    Ws: WebsocketClient + Send + Sync + 'static,
    Provider: ExtensionProvider + Send + Sync + 'static,
    Provider::Extension: SplittableExtension + Send + Sync,
{
    let (transport_tx, transport_rx) = mpsc::channel(transport_buffer_size.get());
    let transport_handle = TransportHandle::new(transport_tx);
    let mut peers: FnvHashMap<SocketAddr, Peer> = FnvHashMap::default();
    let mut pending = PendingConnections::default();
    let mut downlinks = FuturesUnordered::default();
    let mut attachment_tasks = FuturesUnordered::default();

    let mut transport_task: Fuse<JoinHandle<()>> = tokio::spawn(transport.run(transport_rx)).fuse();
    let mut runtime_id_issuer = IdIssuer::new();

    debug!("Runtime task started");

    loop {
        let event: RuntimeEvent = {
            tokio::select! {
                biased;
                _ = &mut remote_stop_rx => RuntimeEvent::Shutdown,
                req = requests_rx.recv() => {
                    match req {
                        Some(req) => RuntimeEvent::StartDownlink(req),
                        None => break,
                    }
                },
                join_result = &mut transport_task => {
                    if let Err(ref e) = join_result {
                        panic!("Transport task completed unexpectedly: {:?}", e);
                    } else {
                        break;
                    }
                }
                Some(connection) = pending.next() => {
                    match connection {
                        Either::Left(dns) => RuntimeEvent::Resolved { scheme: dns.0, host: dns.1, result: dns.2 },
                        Either::Right((host, result)) => RuntimeEvent::ConnectionResult { host, result }
                    }
                },
                Some(task) = attachment_tasks.next(), if !attachment_tasks.is_empty() => task,
                Some(res) = downlinks.next(), if !downlinks.is_empty() => res,
            }
        };

        match event {
            RuntimeEvent::StartDownlink(request) => {
                let DownlinkRegistrationRequest {
                    path,
                    downlink,
                    callback,
                    options,
                    runtime_config,
                    downlink_config,
                } = request;
                let RemotePath { host, node, lane } = path;
                let shp = match host.as_str().parse::<SchemeHostPort>() {
                    Ok(shp) => shp,
                    Err(e) => {
                        let _r = callback.send(Err(DownlinkRuntimeError::with_cause(
                            DownlinkErrorKind::Unresolvable,
                            e,
                        )
                        .shared()));
                        continue;
                    }
                };

                let task_shp = shp.clone();
                let host = Text::from(shp.host().to_string());
                let address = RemotePath::new(host.clone(), node, lane);

                pending.feed_waiter(Waiting::Connection {
                    host: host.clone(),
                    downlink: PendingDownlink {
                        callback,
                        downlink,
                        address,
                        runtime_config,
                        downlink_config,
                        options,
                    },
                });

                let handle_ref = &transport_handle;

                pending.feed_task(
                    async move {
                        Either::Left((*task_shp.scheme(), host, handle_ref.resolve(task_shp).await))
                    }
                    .boxed(),
                );
            }
            RuntimeEvent::Resolved {
                scheme,
                host,
                result: Ok(addrs),
            } => {
                match addrs
                    .iter()
                    .find_map(|sock| peers.get(sock).map(|peer| (peer, sock)))
                {
                    Some((peer, sock)) => {
                        for (key, pending_downlink) in pending.drain_connection_queue(host.clone())
                        {
                            match peer.get_view(key.borrow()) {
                                Some(view) => {
                                    attachment_tasks.push(
                                        attach_downlink(view.attach(), pending_downlink).boxed(),
                                    );
                                }
                                None => {
                                    let PendingDownlink {
                                        runtime_config: config,
                                        ..
                                    } = &pending_downlink;

                                    // Guard against starting a duplicate runtime
                                    if !pending.waiting_on(*sock, key.borrow()) {
                                        attachment_tasks.push(
                                            start_downlink_runtime(
                                                runtime_id_issuer.next_id(),
                                                *sock,
                                                key,
                                                peer.attach(),
                                                *config,
                                                host.clone(),
                                            )
                                            .boxed(),
                                        );
                                    }

                                    pending.feed_waiter(Waiting::Runtime {
                                        addr: *sock,
                                        downlink: pending_downlink,
                                    });
                                }
                            }
                        }
                    }
                    None => {
                        let handle_ref = &transport_handle;
                        let task = async move {
                            match handle_ref
                                .connection_for(scheme, host.to_string(), addrs)
                                .await
                            {
                                Ok((addr, tx)) => Either::Right((host, Ok((addr, tx)))),
                                Err(e) => Either::Right((host, Err(e))),
                            }
                        };
                        pending.feed_task(task.boxed());
                    }
                }
            }
            RuntimeEvent::Resolved {
                host,
                result: Err(e),
                ..
            } => {
                error!(error = %e, host = %host, "Failed to resolve host");

                let error = e.shared();
                for (_key, downlink) in pending.drain_connection_queue(host) {
                    let PendingDownlink {
                        callback,
                        address,
                        downlink,
                        ..
                    } = downlink;

                    if callback.send(Err(error.clone())).is_err() {
                        let kind = downlink.kind();
                        info!(address = %address, kind = ?kind, "A request for a downlink was dropped before it was completed.");
                    }
                }
            }
            RuntimeEvent::ConnectionResult {
                host,
                result: Ok((addr, attach)),
            } => {
                assert!(!peers.contains_key(&addr));
                let peer = Peer::new(attach);

                for (key, pending_downlink) in pending.drain_connection_queue(host.clone()) {
                    match peer.get_view(&key) {
                        Some(view) => {
                            attachment_tasks
                                .push(attach_downlink(view.attach(), pending_downlink).boxed());
                        }
                        None => {
                            attachment_tasks.push(
                                start_downlink_runtime(
                                    runtime_id_issuer.next_id(),
                                    addr,
                                    key,
                                    peer.attach(),
                                    pending_downlink.runtime_config,
                                    host.clone(),
                                )
                                .boxed(),
                            );
                            pending.feed_waiter(Waiting::Runtime {
                                addr,
                                downlink: pending_downlink,
                            });
                        }
                    }
                }

                peers.insert(addr, peer);
            }
            RuntimeEvent::ConnectionResult {
                host,
                result: Err(e),
            } => {
                error!(error = %e, host = %host, "Failed to start a downlink runtime to host");

                let waiters = pending
                    .drain_connection_queue(host)
                    .map(|(_key, waiters)| waiters);
                let error = e.shared();
                for pending_downlink in waiters {
                    let PendingDownlink {
                        callback,
                        address,
                        downlink,
                        ..
                    } = pending_downlink;
                    if callback.send(Err(error.clone())).is_err() {
                        let kind = downlink.kind();
                        info!(address = %address, kind = ?kind, "A request for a downlink was dropped before it was completed.");
                    }
                }
            }
            RuntimeEvent::DownlinkRuntimeStarted {
                sock,
                key,
                result: Ok((attach, runtime)),
            } => match peers.get_mut(&sock) {
                Some(peer) => {
                    let (runtime_stop_tx, runtime_stop_rx) = trigger::trigger();
                    let peer_key = key.clone();
                    downlinks.push(
                        tokio::spawn(runtime.run(runtime_stop_rx, interpret_frame_data))
                            .map(move |result| RuntimeEvent::DownlinkRuntimeComplete {
                                addr: sock,
                                key,
                                result,
                            })
                            .boxed(),
                    );

                    for (_key, pending_downlink) in pending.drain_runtime_queue(sock) {
                        attachment_tasks
                            .push(attach_downlink(attach.clone(), pending_downlink).boxed());
                    }

                    peer.insert_runtime(peer_key, runtime_stop_tx, attach);
                }
                None => {
                    let error =
                        Err(DownlinkRuntimeError::new(DownlinkErrorKind::RemoteStopped).shared());
                    for (_key, pending_downlink) in pending.drain_runtime_queue(sock) {
                        let PendingDownlink {
                            callback,
                            address,
                            downlink,
                            ..
                        } = pending_downlink;
                        if callback.send(error.clone()).is_err() {
                            let kind = downlink.kind();
                            trace!(address = %address, kind = ?kind, "A request for a downlink was dropped before it was completed.");
                        }
                    }
                }
            },
            RuntimeEvent::DownlinkRuntimeStarted {
                sock,
                result: Err((cause, host)),
                ..
            } => {
                error!(error = %cause, host = %host, "Failed to start a downlink runtime to host: ");

                let error = Err(DownlinkRuntimeError::with_cause(
                    DownlinkErrorKind::Connection,
                    cause,
                )
                .shared());
                for (_key, pending_downlink) in pending.drain_runtime_queue(sock) {
                    let PendingDownlink {
                        callback,
                        address,
                        downlink,
                        ..
                    } = pending_downlink;
                    if callback.send(error.clone()).is_err() {
                        let kind = downlink.kind();
                        trace!(address = %address, kind = ?kind, "A request for a downlink was dropped before it was completed.");
                    }
                }
            }
            RuntimeEvent::DownlinkRuntimeAttached {
                pending,
                result: Ok((io_in, io_out)),
            } => {
                let PendingDownlink {
                    callback,
                    downlink,
                    address,
                    downlink_config,
                    ..
                } = pending;
                let kind = downlink.kind();
                let (promise_tx, promise_rx) = promise::promise();
                let task = tokio::spawn(downlink.run_boxed(
                    Address::new(
                        Some(address.host.clone()),
                        address.node.clone(),
                        address.lane.clone(),
                    ),
                    downlink_config,
                    io_out,
                    io_in,
                ))
                .map(move |result| {
                    let result = match result {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(e)) => Err(e.into()),
                        Err(e) => Err(e.into()),
                    };

                    RuntimeEvent::DownlinkTaskComplete {
                        kind,
                        address,
                        result,
                        tx: promise_tx,
                    }
                });

                downlinks.push(task.boxed());
                let _r = callback.send(Ok(promise_rx));
            }
            RuntimeEvent::DownlinkRuntimeAttached {
                pending: pending_dl,
                result: Err(cause),
                ..
            } => {
                let PendingDownlink {
                    callback,
                    address,
                    downlink,
                    ..
                } = pending_dl;
                let kind = downlink.kind();

                error!(error = %cause, address = %address, kind = ?kind, "Failed to attach a downlink to runtime: ");

                if callback.send(Err(cause.shared())).is_err() {
                    trace!(address = %address, kind = ?kind, "A request for a downlink was dropped before it was completed.");
                }
            }
            RuntimeEvent::DownlinkRuntimeComplete { addr, key, result } => {
                if let Entry::Occupied(mut entry) = peers.entry(addr) {
                    let handle = entry.get_mut();
                    if handle.remove(&key) {
                        entry.remove();
                    }
                }
                if let Err(err) = result {
                    let kind = key.1;
                    error!(error = %err, address = %addr, kind = ?kind, "A downlink runtime task was either cancelled or panicked");
                }
            }
            RuntimeEvent::DownlinkTaskComplete {
                kind,
                address,
                result,
                tx,
            } => {
                match &result {
                    Ok(()) => {
                        info!(
                            kind = ?kind,
                            address = %address,
                            "Downlink completed successfully."
                        );
                    }
                    Err(e) => {
                        error!(
                          kind = ?kind,
                            address = %address,
                            error = ?e,
                            "Downlink completed with an error."
                        );
                    }
                }

                let _r = tx.provide(result.map_err(Arc::new));
            }
            RuntimeEvent::Shutdown => {
                for peer in peers.values_mut() {
                    peer.stop_all();
                }

                break;
            }
        }

        debug!("Runtime task completed");
    }
}

async fn start_downlink_runtime(
    identity: Uuid,
    remote_addr: SocketAddr,
    key: Key,
    attach: mpsc::Sender<AttachClient>,
    config: DownlinkRuntimeConfig,
    host: Text,
) -> RuntimeEvent {
    let (rel_addr, kind) = key;
    let (in_tx, in_rx) = byte_channel(config.remote_buffer_size);
    let (out_tx, out_rx) = byte_channel(config.remote_buffer_size);
    let (done_tx, done_rx) = oneshot::channel();

    let request = AttachClient::AttachDownlink {
        downlink_id: identity,
        path: rel_addr.clone(),
        sender: in_tx,
        receiver: out_rx,
        done: done_tx,
    };
    if attach.send(request).await.is_err() {
        return RuntimeEvent::DownlinkRuntimeStarted {
            sock: remote_addr,
            key: (rel_addr, kind),
            result: Err((DownlinkFailureReason::RemoteStopped, host)),
        };
    }
    let err = match done_rx.await {
        Ok(Err(e)) => Some(e.into()),
        Err(_) => Some(DownlinkFailureReason::RemoteStopped),
        _ => None,
    };
    if let Some(err) = err {
        return RuntimeEvent::DownlinkRuntimeStarted {
            sock: remote_addr,
            key: (rel_addr, kind),
            result: Err((err, host)),
        };
    }
    let io = (out_tx, in_rx);
    let (attachment_tx, attachment_rx) = mpsc::channel(config.attachment_queue_size.get());
    let runtime = DownlinkRuntime::new(identity, rel_addr.clone(), attachment_rx, kind, io, config);
    RuntimeEvent::DownlinkRuntimeStarted {
        sock: remote_addr,
        key: (rel_addr, kind),
        result: Ok((attachment_tx, runtime)),
    }
}

async fn attach_downlink(
    attach: mpsc::Sender<AttachAction>,
    pending: PendingDownlink,
) -> RuntimeEvent {
    let (in_tx, in_rx) = byte_channel(pending.runtime_config.downlink_buffer_size);
    let (out_tx, out_rx) = byte_channel(pending.runtime_config.downlink_buffer_size);
    let result = attach
        .send(AttachAction::new(out_rx, in_tx, pending.options))
        .await
        .map(move |_| (out_tx, in_rx))
        .map_err(|_| DownlinkRuntimeError::new(DownlinkErrorKind::Terminated));

    RuntimeEvent::DownlinkRuntimeAttached { pending, result }
}
