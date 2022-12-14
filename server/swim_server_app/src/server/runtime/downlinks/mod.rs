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
#[cfg(test)]
mod tests;

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
    remote::{net::dns::DnsResolver, table::SchemeHostPort, BadUrl, SchemeSocketAddr},
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

/// A task that manages downlink runtime tasks for the server. This taks has the following
/// responsibilities:
/// * Accept requests for new downlink connections.
/// * Perform a DNS lookup for remote downlinks.
/// * Attach new clients to existing running downlink runtime tasks where appropriate.
/// * Register a new socket with the main server task for new remotes.
/// * Spawn new downlink runtime tasks and keep track of their results.
pub struct DownlinkConnectionTask<Dns> {
    connector: DownlinksConnector,
    config: DownlinkRuntimeConfig,
    dns: Dns,
}

impl<Dns> DownlinkConnectionTask<Dns> {
    /// #Arguments
    /// * `connector` - Communication channels between this task and the main server task.
    /// * `config` - Configuration for downlink runtime tasks.
    /// * `dns` - DNS resolver implementation.
    pub fn new(connector: DownlinksConnector, config: DownlinkRuntimeConfig, dns: Dns) -> Self {
        DownlinkConnectionTask {
            connector,
            config,
            dns,
        }
    }
}

impl<Dns> DownlinkConnectionTask<Dns>
where
    Dns: DnsResolver,
{
    pub async fn run(self) {
        let mut tasks = FuturesUnordered::new();
        let connector = {
            let DownlinkConnectionTask {
                mut connector,
                config,
                dns,
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
                                    dns.resolve(target)
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
                                    attach_to_runtime(
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
                                            attach_to_runtime(
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
                                    Event::NewClientResult { host, result }
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
                    Event::NewClientResult {
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
                    Event::NewClientResult {
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
                    Event::RuntimeAttachmentResult {
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
                                    attach_to_runtime(
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
                    Event::RuntimeAttachmentResult {
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

enum Event {
    // A request has been received for a new downlink.
    Request(DownlinkRequest),
    // A DNS resolution has completed.
    Resolved {
        host: Text,
        result: Result<Vec<SchemeSocketAddr>, std::io::Error>,
    },
    // A request to the main server task to open a new socket has completed.
    NewClientResult {
        host: Text,
        result: Result<EstablishedClient, NewClientError>,
    },
    // A request to attach to a downlink runtime task has completed.
    RuntimeAttachmentResult {
        remote_address: Option<SocketAddr>,
        key: DlKey,
        result: Result<(mpsc::Sender<AttachAction>, DownlinkRuntime), DownlinkFailureReason>,
    },
    // A downlink runtime task has stopped.
    RuntimeTerminated {
        socket_addr: Option<SocketAddr>,
        key: DlKey,
        result: Result<(), JoinError>,
    },
    // An attachment to a downlink runtime task has completed.
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

async fn attach_to_runtime(
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
    let (done_tx, done_rx) = oneshot::channel();

    let request = AttachClient::AttachDownlink {
        downlink_id: identity,
        path: rel_addr.clone(),
        sender: in_tx,
        receiver: out_rx,
        done: done_tx,
    };
    if remote_attach.send(request).await.is_err() {
        return Event::RuntimeAttachmentResult {
            remote_address: remote_addr,
            key: (rel_addr, kind),
            result: Err(DownlinkFailureReason::RemoteStopped),
        };
    }
    let err = match done_rx.await {
        Ok(Err(e)) => Some(e),
        Err(_) => Some(DownlinkFailureReason::RemoteStopped),
        _ => None,
    };
    if let Some(err) = err {
        return Event::RuntimeAttachmentResult {
            remote_address: remote_addr,
            key: (rel_addr, kind),
            result: Err(err),
        };
    }
    let io = (out_tx, in_rx);
    let (attachment_tx, attachment_rx) = mpsc::channel(config.attachment_queue_size.get());
    let runtime = DownlinkRuntime::new(identity, rel_addr.clone(), attachment_rx, kind, io);
    Event::RuntimeAttachmentResult {
        remote_address: remote_addr,
        key: (rel_addr, kind),
        result: Ok((attachment_tx, runtime)),
    }
}

/// A wrapper task that can start multiple types of downlink runtime.
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
