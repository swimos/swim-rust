// Copyright 2015-2021 Swim Inc.
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

mod connector;
mod pending;

pub use connector::{downlink_task_connector, DlTaskRequest, DownlinksConnector, ServerConnector};
use tracing::{error, info};

use std::{
    collections::{hash_map::Entry, HashMap},
    net::SocketAddr,
    num::NonZeroUsize,
};

use futures::{stream::FuturesUnordered, Future, FutureExt, StreamExt};
pub use pending::{DlKey, PendingDownlinks};
use swim_api::{
    downlink::DownlinkKind,
    error::{AgentRuntimeError, DownlinkFailureReason},
};
use swim_model::{
    address::{Address, RelativeAddress},
    Text,
};
use swim_remote::AttachClient;
use swim_runtime::{
    agent::DownlinkRequest,
    downlink::{
        failure::{AlwaysAbortStrategy, AlwaysIgnoreStrategy, ReportStrategy},
        AttachAction, DownlinkRuntimeConfig, Io, MapDownlinkRuntime, ValueDownlinkRuntime,
    },
    remote::{table::SchemeHostPort, BadUrl, ExternalConnections, SchemeSocketAddr},
};
use swim_utilities::{
    io::byte_channel::{byte_channel, ByteReader, ByteWriter},
    trigger,
};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinError,
};
use url::Url;
use uuid::Uuid;

use super::{
    ids::{IdIssuer, IdKind},
    ClientRegistration, EstablishedClient, NewClientError,
};

pub struct DownlinkConnectionTask<Net> {
    connector: DownlinksConnector,
    config: DownlinkRuntimeConfig,
    networking: Net,
}

struct RemoteStopped;

enum Event {
    Request(DownlinkRequest),
    Resolved {
        host: Text,
        result: Result<Vec<SchemeSocketAddr>, std::io::Error>,
    },
    Connected {
        host: Text,
        result: Result<EstablishedClient, NewClientError>,
    },
    RuntimeAttached {
        remote_address: Option<SocketAddr>,
        key: DlKey,
        result: Result<(mpsc::Sender<AttachAction>, DownlinkRuntime), RemoteStopped>,
    },
    RuntimeTerminated {
        socket_addr: Option<SocketAddr>,
        key: DlKey,
        result: Result<(), JoinError>,
    },
    Attached {
        address: Address<Text>,
        kind: DownlinkKind,
        promise: oneshot::Sender<Result<Io, AgentRuntimeError>>,
        result: Result<Io, AgentRuntimeError>,
    },
}

struct ClientHandle {
    client_tx: mpsc::Sender<AttachClient>,
    downlinks: HashMap<DlKey, mpsc::Sender<AttachAction>>,
}

impl ClientHandle {
    fn new(client_tx: mpsc::Sender<AttachClient>) -> Self {
        ClientHandle {
            client_tx,
            downlinks: Default::default(),
        }
    }

    fn insert(&mut self, key: DlKey, tx: mpsc::Sender<AttachAction>) {
        self.downlinks.insert(key, tx);
    }

    fn get(&self, key: &DlKey) -> Option<&mpsc::Sender<AttachAction>> {
        self.downlinks.get(key)
    }

    fn remove(&mut self, key: &DlKey) -> bool {
        self.downlinks.remove(key);
        self.downlinks.is_empty()
    }
}

impl<Net> DownlinkConnectionTask<Net>
where
    Net: ExternalConnections,
{
    pub fn new(
        connector: DownlinksConnector,
        config: DownlinkRuntimeConfig,
        networking: Net,
    ) -> Self {
        DownlinkConnectionTask {
            connector,
            config,
            networking,
        }
    }

    pub async fn run(self) {
        let mut tasks = FuturesUnordered::new();
        let connector = {
            let DownlinkConnectionTask {
                mut connector,
                config,
                networking,
            } = self;

            let mut downlink_channels: HashMap<SocketAddr, ClientHandle> = HashMap::new();

            let mut local_handle = ClientHandle::new(connector.local_handle());

            let mut id_issuer = IdIssuer::new(IdKind::Client);
            let mut pending = PendingDownlinks::default();

            loop {
                let event: Event = tokio::select! {
                    biased;
                    maybe_request = connector.next_request() => {
                        if let Some(req) = maybe_request {
                            Event::Request(req)
                        } else {
                            break;
                        }
                    },
                    Some(event) = tasks.next(), if !tasks.is_empty() => event,
                };

                match event {
                    Event::Request(request) => {
                        if let Some(host) = request.key.0.host.clone() {
                            if let Ok(target) = process_host(host.as_str()) {
                                pending.push_remote(host.clone(), request);
                                tasks.push(
                                    networking
                                        .lookup(target)
                                        .map(move |result| Event::Resolved { host, result })
                                        .boxed(),
                                );
                            } else {
                                let DownlinkRequest { promise, .. } = request;
                                if promise
                                    .send(Err(AgentRuntimeError::DownlinkConnectionFailed(
                                        DownlinkFailureReason::Unresolvable,
                                    )))
                                    .is_err()
                                {
                                    info!("Request for client connection dropped before it failed to resolve.");
                                }
                            }
                        } else {
                            let (addr, kind) = &request.key;
                            let rel_addr =
                                RelativeAddress::new(addr.node.clone(), addr.lane.clone());
                            let key = (rel_addr, *kind);
                            if let Some(attach_tx) = local_handle.get(&key) {
                                tasks.push(
                                    run_downlink(
                                        request,
                                        attach_tx.clone(),
                                        config.downlink_buffer_size,
                                    )
                                    .boxed(),
                                );
                            } else {
                                pending.push_local(request);
                                let identity = id_issuer.next_id();
                                tasks.push(
                                    start_downlink_runtime(
                                        identity,
                                        None,
                                        key,
                                        local_handle.client_tx.clone(),
                                        config,
                                    )
                                    .boxed(),
                                );
                            }
                        }
                    }
                    Event::Resolved {
                        host,
                        result: Ok(sock_addrs),
                    } => {
                        if let Some((addr, handle)) =
                            sock_addrs.iter().map(|s| s.addr).find_map(|addr| {
                                downlink_channels.get(&addr).map(move |inner| (addr, inner))
                            })
                        {
                            let waiting_map = pending.take_socket_ready(&host);
                            for (key, requests) in waiting_map {
                                if let Some(attach_tx) = handle.get(&key) {
                                    for request in requests {
                                        tasks.push(
                                            run_downlink(
                                                request,
                                                attach_tx.clone(),
                                                config.downlink_buffer_size,
                                            )
                                            .boxed(),
                                        );
                                    }
                                } else {
                                    pending.push_for_socket(addr, key.clone(), requests);
                                    let identity = id_issuer.next_id();
                                    tasks.push(
                                        start_downlink_runtime(
                                            identity,
                                            Some(addr),
                                            key,
                                            handle.client_tx.clone(),
                                            config,
                                        )
                                        .boxed(),
                                    );
                                }
                            }
                        } else {
                            let addr_vec = sock_addrs.into_iter().map(|s| s.addr).collect();
                            let (registration, rx) =
                                ClientRegistration::new(host.clone(), addr_vec);
                            if connector.register(registration).await.is_err() {
                                break;
                            }
                            tasks.push(
                                rx.map(move |result| {
                                    let result = match result {
                                        Ok(Ok(client)) => Ok(client),
                                        Ok(Err(e)) => Err(e),
                                        Err(_) => Err(NewClientError::ServerStopped),
                                    };
                                    Event::Connected { host, result }
                                })
                                .boxed(),
                            );
                        }
                    }
                    Event::Resolved {
                        host,
                        result: Err(_),
                    } => {
                        for request in pending.open_client_failed(&host) {
                            let DownlinkRequest { promise, .. } = request;
                            if promise
                                .send(Err(AgentRuntimeError::DownlinkConnectionFailed(
                                    DownlinkFailureReason::Unresolvable,
                                )))
                                .is_err()
                            {
                                info!("Request for client connection dropped before it failed to resolve.");
                            }
                        }
                    }
                    Event::Connected {
                        host,
                        result: Ok(EstablishedClient { tx, sock_addr }),
                    } => {
                        let handle = match downlink_channels.entry(sock_addr) {
                            Entry::Occupied(entry) => {
                                let handle = entry.into_mut();
                                handle.client_tx = tx;
                                handle.downlinks.clear();
                                handle
                            }
                            Entry::Vacant(entry) => entry.insert(ClientHandle::new(tx)),
                        };
                        for key in pending.socket_ready(host, sock_addr).into_iter().flatten() {
                            let identity = id_issuer.next_id();
                            tasks.push(
                                start_downlink_runtime(
                                    identity,
                                    Some(sock_addr),
                                    key.clone(),
                                    handle.client_tx.clone(),
                                    config,
                                )
                                .boxed(),
                            );
                        }
                    }
                    Event::Connected {
                        host,
                        result: Err(e),
                    } => {
                        let err: AgentRuntimeError = e.into();
                        for request in pending.open_client_failed(&host) {
                            let DownlinkRequest {
                                promise,
                                key: (addr, kind),
                                ..
                            } = request;
                            if promise.send(Err(err)).is_err() {
                                info!(address = %addr, kind = ?kind, "Request for a downlink dropped before it failed to complete.");
                            }
                        }
                    }
                    Event::RuntimeAttached {
                        remote_address,
                        key,
                        result: Ok((attach_tx, runtime)),
                    } => {
                        let handle = if let Some(addr) = remote_address {
                            downlink_channels.get_mut(&addr)
                        } else {
                            Some(&mut local_handle)
                        };
                        if let Some(handle) = handle {
                            handle.insert(key.clone(), attach_tx.clone());
                            let key_cpy = key.clone();
                            tasks.push(
                                tokio::spawn(runtime.run(connector.stop_handle(), config))
                                    .map(move |result| Event::RuntimeTerminated {
                                        socket_addr: remote_address,
                                        key: key_cpy,
                                        result,
                                    })
                                    .boxed(),
                            );

                            for request in pending.dl_ready(remote_address, &key) {
                                tasks.push(
                                    run_downlink(
                                        request,
                                        attach_tx.clone(),
                                        config.downlink_buffer_size,
                                    )
                                    .boxed(),
                                );
                            }
                        } else {
                            for DownlinkRequest {
                                promise,
                                key: (addr, kind),
                                ..
                            } in pending.dl_ready(remote_address, &key)
                            {
                                if promise
                                    .send(Err(AgentRuntimeError::DownlinkConnectionFailed(
                                        DownlinkFailureReason::RemoteStopped,
                                    )))
                                    .is_err()
                                {
                                    info!(address = %addr, kind = ?kind, "Request for a downlink dropped before it failed to complete.");
                                }
                            }
                        }
                    }
                    Event::RuntimeAttached {
                        remote_address,
                        key,
                        result: Err(_),
                    } => {
                        for DownlinkRequest {
                            promise,
                            key: (addr, kind),
                            ..
                        } in pending.dl_ready(remote_address, &key)
                        {
                            if promise
                                .send(Err(AgentRuntimeError::DownlinkConnectionFailed(
                                    DownlinkFailureReason::RemoteStopped,
                                )))
                                .is_err()
                            {
                                info!(address = %addr, kind = ?kind, "The remote connection stopped before a downlink was set up.");
                            }
                        }
                    }
                    Event::Attached {
                        address,
                        kind,
                        promise,
                        result,
                    } => {
                        if promise.send(result).is_err() {
                            info!(address = %address, kind = ?kind, "A request for a downlink was dropped before it was satisfied.");
                        }
                    }
                    Event::RuntimeTerminated {
                        socket_addr,
                        key,
                        result,
                    } => {
                        if let Some(addr) = socket_addr {
                            if let Entry::Occupied(mut entry) = downlink_channels.entry(addr) {
                                let handle = entry.get_mut();
                                if handle.remove(&key) {
                                    entry.remove();
                                }
                            }
                        } else {
                            local_handle.remove(&key);
                        }
                        let (address, kind) = key;
                        if let Err(e) = result {
                            error!(error = %e, socket_addr = ?socket_addr, address = %address, kind = ?kind, "A downlink runtime task panicked.");
                        }
                    }
                }
            }
            connector
        };

        while let Some(event) = tasks.next().await {
            match event {
                Event::RuntimeTerminated {
                    socket_addr,
                    key: (address, kind),
                    result: Err(e),
                    ..
                } => {
                    error!(error = %e, socket_addr = ?socket_addr, address = %address, kind = ?kind, "A downlink runtime task panicked.");
                }
                _ => continue,
            }
        }
        connector.stopped();
    }
}

#[derive(Debug, Error)]
enum BadHost {
    #[error("Specified host was not a valid URL.")]
    BadUrl(
        #[from]
        #[source]
        url::ParseError,
    ),
    #[error("Host URL is not a valid Warp URL.")]
    InvaludUrl(
        #[from]
        #[source]
        BadUrl,
    ),
}

fn process_host(host: &str) -> Result<SchemeHostPort, BadHost> {
    let url = host.parse::<Url>()?;
    Ok(SchemeHostPort::try_from(&url)?)
}

async fn run_downlink(
    request: DownlinkRequest,
    attach_tx: mpsc::Sender<AttachAction>,
    buffer_size: NonZeroUsize,
) -> Event {
    let DownlinkRequest {
        promise,
        key: (address, kind),
        options,
        ..
    } = request;
    let (in_tx, in_rx) = byte_channel(buffer_size);
    let (out_tx, out_rx) = byte_channel(buffer_size);
    let result = attach_tx
        .send(AttachAction::new(out_rx, in_tx, options))
        .await
        .map(move |_| (out_tx, in_rx))
        .map_err(|_| {
            AgentRuntimeError::DownlinkConnectionFailed(DownlinkFailureReason::ConnectionFailed)
        });
    Event::Attached {
        address,
        kind,
        promise,
        result,
    }
}

async fn start_downlink_runtime(
    identity: Uuid,
    remote_addr: Option<SocketAddr>,
    key: DlKey,
    remote_attach: mpsc::Sender<AttachClient>,
    config: DownlinkRuntimeConfig,
) -> Event {
    let (rel_addr, kind) = key;
    let (in_tx, in_rx) = byte_channel(config.remote_buffer_size);
    let (out_tx, out_rx) = byte_channel(config.remote_buffer_size);
    let (done_tx, done_rx) = trigger::trigger();

    let request = AttachClient::AttachDownlink {
        downlink_id: identity,
        path: rel_addr.clone(),
        sender: in_tx,
        receiver: out_rx,
        done: done_tx,
    };
    if remote_attach.send(request).await.is_err() {
        return Event::RuntimeAttached {
            remote_address: remote_addr,
            key: (rel_addr, kind),
            result: Err(RemoteStopped),
        };
    }
    if done_rx.await.is_err() {
        return Event::RuntimeAttached {
            remote_address: remote_addr,
            key: (rel_addr, kind),
            result: Err(RemoteStopped),
        };
    }
    let io = (out_tx, in_rx);
    let (attachment_tx, attachment_rx) = mpsc::channel(config.attachment_queue_size.get());
    let runtime = DownlinkRuntime::new(identity, rel_addr.clone(), attachment_rx, kind, io);
    Event::RuntimeAttached {
        remote_address: remote_addr,
        key: (rel_addr, kind),
        result: Ok((attachment_tx, runtime)),
    }
}

struct DownlinkRuntime {
    identity: Uuid,
    path: RelativeAddress<Text>,
    attachment_rx: mpsc::Receiver<AttachAction>,
    kind: DownlinkKind,
    io: (ByteWriter, ByteReader),
}

impl DownlinkRuntime {
    fn new(
        identity: Uuid,
        path: RelativeAddress<Text>,
        attachment_rx: mpsc::Receiver<AttachAction>,
        kind: DownlinkKind,
        io: (ByteWriter, ByteReader),
    ) -> Self {
        DownlinkRuntime {
            identity,
            path,
            attachment_rx,
            kind,
            io,
        }
    }

    fn run(
        self,
        stopping: trigger::Receiver,
        config: DownlinkRuntimeConfig,
    ) -> impl Future<Output = ()> + Send + 'static {
        let DownlinkRuntime {
            identity,
            path,
            attachment_rx,
            kind,
            io,
        } = self;
        async move {
            match kind {
                DownlinkKind::Map => {
                    let bad_frame_strat = if config.abort_on_bad_frames {
                        ReportStrategy::new(AlwaysAbortStrategy).boxed()
                    } else {
                        ReportStrategy::new(AlwaysIgnoreStrategy).boxed()
                    };
                    let runtime = MapDownlinkRuntime::new(
                        attachment_rx,
                        io,
                        stopping,
                        identity,
                        path,
                        config,
                        bad_frame_strat,
                    );
                    runtime.run().await;
                }
                _ => {
                    let runtime = ValueDownlinkRuntime::new(
                        attachment_rx,
                        io,
                        stopping,
                        identity,
                        path,
                        config,
                    );
                    runtime.run().await;
                }
            }
        }
    }
}

/* async fn resolve_remote_for_dl<Net, E>(
    host: Text,
    request: DownlinkRequest,
    networking: Arc<Net>,
) -> DownlinkEvent<Net::Socket, E>
where
    Net: ExternalConnections,
{
    let result = resolve_remote(host.as_str(), networking).await;
    DownlinkEvent::RemoteResolved {
        request,
        host,
        addrs: result,
    }
}

async fn resolve_remote<Net>(
    host: &str,
    networking: Arc<Net>,
) -> Result<Vec<SocketAddr>, NewClientError>
where
    Net: ExternalConnections,
{
    match host.parse::<Url>() {
        Err(e) => Err(NewClientError::InvalidUrl(e)),
        Ok(url) => {
            let target = SchemeHostPort::try_from(&url)?;
            let addrs = match networking.lookup(target).await {
                Ok(addrs) => addrs,
                Err(e) => {
                    return Err(NewClientError::ResolutionFailed { url, error: e });
                }
            };
            Ok(addrs.into_iter().map(|a| a.addr).collect())
        }
    }
}

async fn open_client_for_dl<Net, Ws>(
    host: Text,
    addrs: Vec<SocketAddr>,
    networking: Arc<Net>,
    websockets: Arc<Ws>,
) -> DownlinkEvent<Net::Socket, Ws::Ext>
where
    Net: ExternalConnections,
    Net::Socket: WebSocketStream,
    Ws: WsConnections<Net::Socket> + Send + Sync,
{
    let result = open_client(host.clone(), addrs, networking, websockets).await;

    DownlinkEvent::RemoteOpened { host, result }
}

async fn open_client<Net, Ws>(
    host: Text,
    addrs: Vec<SocketAddr>,
    networking: Arc<Net>,
    websockets: Arc<Ws>,
) -> Result<(SocketAddr, WebSocket<Net::Socket, Ws::Ext>), NewClientError>
where
    Net: ExternalConnections,
    Net::Socket: WebSocketStream,
    Ws: WsConnections<Net::Socket> + Send + Sync,
{
    let mut conn_failures = vec![];
    let mut sock = None;
    for addr in addrs {
        match networking.try_open(addr).await {
            Ok(socket) => {
                sock = Some((addr, socket));
                break;
            }
            Err(e) => {
                conn_failures.push((addr, e));
            }
        }
    }
    let (addr, socket) = if let Some((addr, socket)) = sock {
        (addr, socket)
    } else {
        return Err(NewClientError::OpeningSocketFailed {
            errors: conn_failures,
        });
    };
    websockets
        .open_connection(socket, host.to_string())
        .await
        .map(move |ws| (addr, ws))
        .map_err(|e| NewClientError::WsNegotationFailed { error: e.into() })
}

async fn start_downlink_runtime<S, E>(
    identity: Uuid,
    remote_addr: Option<SocketAddr>,
    rel_addr: RelativeAddress<Text>,
    kind: DownlinkKind,
    remote_attach: mpsc::Sender<AttachClient>,
    config: DownlinkRuntimeConfig,
    stopping: trigger::Receiver,
) -> DownlinkEvent<S, E> {
    let (in_tx, in_rx) = byte_channel(config.buffer_size);
    let (out_tx, out_rx) = byte_channel(config.buffer_size);
    let (done_tx, done_rx) = trigger::trigger();

    if remote_attach
        .send(AttachClient::AttachDownlink {
            path: rel_addr.clone(),
            sender: in_tx,
            receiver: out_rx,
            done: done_tx,
        })
        .await
        .is_err()
    {
        return DownlinkEvent::DownlinkRuntimeAttached {
            remote_address: remote_addr,
            key: (rel_addr, kind),
            result: Err(RemoteStopped),
        };
    }
    if done_rx.await.is_err() {
        return DownlinkEvent::DownlinkRuntimeAttached {
            remote_address: remote_addr,
            key: (rel_addr, kind),
            result: Err(RemoteStopped),
        };
    }
    let io = (out_tx, in_rx);
    let (attachment_tx, attachment_rx) = mpsc::channel(config.attachment_queue_size.get());
    let task = run_downlink_runtime(
        identity,
        rel_addr.clone(),
        attachment_rx,
        stopping,
        config,
        kind,
        io,
    );
    DownlinkEvent::DownlinkRuntimeAttached {
        remote_address: remote_addr,
        key: (rel_addr, kind),
        result: Ok(DownlinkRuntime {
            attachment_tx,
            task: task.boxed(),
        }),
    }
}

pub fn run_downlink_runtime(
    identity: Uuid,
    path: RelativeAddress<Text>,
    attachment_rx: mpsc::Receiver<AttachAction>,
    stopping: trigger::Receiver,
    config: DownlinkRuntimeConfig,
    kind: DownlinkKind,
    io: (ByteWriter, ByteReader),
) -> impl Future<Output = ()> + Send + 'static {
    async move {
        match kind {
            DownlinkKind::Map => {
                let bad_frame_strat = if config.abort_on_bad_frames {
                    ReportStrategy::new(AlwaysAbortStrategy).boxed()
                } else {
                    ReportStrategy::new(AlwaysIgnoreStrategy).boxed()
                };
                let runtime = MapDownlinkRuntime::new(
                    attachment_rx,
                    io,
                    stopping,
                    identity,
                    path,
                    config,
                    bad_frame_strat,
                );
                runtime.run().await;
            }
            _ => {
                let runtime =
                    ValueDownlinkRuntime::new(attachment_rx, io, stopping, identity, path, config);
                runtime.run().await;
            }
        }
    }
}
 */
