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

mod connector;
mod pending;
#[cfg(test)]
mod tests;

pub use connector::{downlink_task_connector, DlTaskRequest, DownlinksConnector, ServerConnector};
use tracing::{debug, error, info, trace, warn};

use std::{
    collections::{hash_map::Entry, HashMap},
    net::SocketAddr,
    num::NonZeroUsize,
    sync::Arc,
};

use futures::{stream::FuturesUnordered, Future, FutureExt, StreamExt};
pub use pending::{DlKey, PendingDownlinks};
use swim_api::net::{Scheme, SchemeHostPort};
use swim_api::{
    downlink::DownlinkKind,
    error::{AgentRuntimeError, DownlinkFailureReason, DownlinkRuntimeError},
};
use swim_model::{address::RelativeAddress, Text};
use swim_remote::net::dns::DnsResolver;
use swim_remote::{AttachClient, LinkError};
use swim_runtime::downlink::IdentifiedAddress;
use swim_runtime::{
    agent::{CommanderKey, CommanderRequest, DownlinkRequest, LinkRequest},
    downlink::{
        failure::{AlwaysAbortStrategy, AlwaysIgnoreStrategy, ReportStrategy},
        AttachAction, DownlinkRuntimeConfig, Io, MapDownlinkRuntime, ValueDownlinkRuntime,
    },
};
use swim_utilities::{
    io::byte_channel::{byte_channel, BudgetedFutureExt, ByteReader, ByteWriter},
    trigger,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinError,
};
use uuid::Uuid;

use crate::server::runtime::downlinks::pending::Waiting;

use super::{
    ids::{IdIssuer, IdKind},
    ClientRegistration, EstablishedClient, NewClientError,
};

/// A task that manages downlink runtime tasks for the server. This task has the following
/// responsibilities:
/// * Accept requests for new downlink connections.
/// * Accept requests for one way links for sending ad hoc commands.
/// * Perform a DNS lookup for remote endpoints.
/// * Attach new clients to existing running downlink runtime tasks where appropriate.
/// * Register a new socket with the main server task for new remotes.
/// * Spawn new downlink runtime tasks and keep track of their results.
/// * Establish direct connections between agents and remote sockets (or other local lanes) for sending ad hoc commands.
pub struct DownlinkConnectionTask<Dns> {
    connector: DownlinksConnector,
    coop_budget: Option<NonZeroUsize>,
    config: DownlinkRuntimeConfig,
    dns: Dns,
}

impl<Dns> DownlinkConnectionTask<Dns> {
    /// #Arguments
    /// * `coop_budget` - Co-op budget for byte channel futures.
    /// * `connector` - Communication channels between this task and the main server task.
    /// * `config` - Configuration for downlink runtime tasks.
    /// * `dns` - DNS resolver implementation.
    pub fn new(
        connector: DownlinksConnector,
        coop_budget: Option<NonZeroUsize>,
        config: DownlinkRuntimeConfig,
        dns: Dns,
    ) -> Self {
        DownlinkConnectionTask {
            connector,
            coop_budget,
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
                coop_budget,
                config,
                dns,
            } = self;

            let mut clients: HashMap<SocketAddr, ClientHandle> = HashMap::new();

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
                            info!("Downlink connection task stopping.");
                            break;
                        }
                    },
                    Some(event) = tasks.next(), if !tasks.is_empty() => event,
                };

                match event {
                    Event::Request(LinkRequest::Commander(request)) => {
                        let CommanderRequest { key, .. } = &request;
                        match key {
                            CommanderKey::Remote(shp) => {
                                debug!(remote = %shp, "Handling request for remote command channel.");
                                let host_str: Text = shp.to_string().into();
                                let SchemeHostPort(scheme, host_name, port) = shp.clone();
                                if pending.push_remote_cmd(host_str.clone(), request) {
                                    tasks.push(
                                        dns.resolve(host_name, port)
                                            .map(move |result| Event::Resolved {
                                                scheme,
                                                host: host_str,
                                                result,
                                            })
                                            .boxed(),
                                    );
                                }
                            }
                            CommanderKey::Local(path) => {
                                debug!(path = %path, "Handling request for local command channel.");
                                let path = path.clone();
                                tasks.push(
                                    attach_cmd_request_local(
                                        request,
                                        path,
                                        local_handle.client_tx.clone(),
                                        config.remote_buffer_size,
                                    )
                                    .boxed(),
                                )
                            }
                        }
                    }
                    Event::Request(LinkRequest::Downlink(request)) => {
                        let DownlinkRequest {
                            remote,
                            address,
                            kind,
                            ..
                        } = &request;
                        if let Some(shp) = remote.clone() {
                            debug!(remote = %shp, node = %address.node, lane = %address.lane, kind = ?kind, "Handling request for downlink to remote lane.");
                            let host_str: Text = shp.to_string().into();
                            let SchemeHostPort(scheme, host_name, port) = shp;
                            if pending.push_remote(host_str.clone(), request) {
                                tasks.push(
                                    dns.resolve(host_name, port)
                                        .map(move |result| Event::Resolved {
                                            scheme,
                                            host: host_str,
                                            result,
                                        })
                                        .boxed(),
                                );
                            }
                        } else {
                            let key = (address.clone(), *kind);
                            if let Some((downlink_id, attach_tx)) = local_handle.get(&key) {
                                debug!(node = %address.node, lane = %address.lane, kind = ?kind, "Attempting to attach to downlink runtime for local lane.");
                                tasks.push(
                                    attach_to_runtime(
                                        *downlink_id,
                                        request,
                                        attach_tx.clone(),
                                        config.downlink_buffer_size,
                                    )
                                    .boxed(),
                                );
                            } else {
                                debug!(node = %address.node, lane = %address.lane, kind = ?kind, "Attempting to start downlink runtime for local lane.");
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
                        scheme,
                        host,
                        result: Ok(sock_addrs),
                    } => {
                        debug!(scheme = %scheme, host = %host, resolved = ?sock_addrs, "Downlink DNS resolution completed.");
                        if let Some((addr, handle)) = sock_addrs
                            .iter()
                            .find_map(|addr| clients.get(addr).map(move |inner| (addr, inner)))
                        {
                            debug!(scheme = %scheme, host = %host, socket_address = %addr, "Using already connected socket.");
                            let Waiting {
                                dl_requests: waiting_map,
                                cmd_requests,
                            } = pending.take_socket_ready(&host);
                            for (key, requests) in waiting_map {
                                let (lane_addr, kind) = &key;
                                if let Some((downlink_id, attach_tx)) = handle.get(&key) {
                                    debug!(scheme = %scheme, host = %host, socket_address = %addr, lane_addr = %lane_addr, kind = ?kind, "Connecting to existing downlink runtime.");
                                    for request in requests {
                                        tasks.push(
                                            attach_to_runtime(
                                                *downlink_id,
                                                request,
                                                attach_tx.clone(),
                                                config.downlink_buffer_size,
                                            )
                                            .boxed(),
                                        );
                                    }
                                } else {
                                    debug!(scheme = %scheme, host = %host, socket_address = %addr, lane_addr = %lane_addr, kind = ?kind, "Starting new downlink runtime.");
                                    pending.push_for_socket(*addr, key.clone(), requests);
                                    let identity = id_issuer.next_id();
                                    tasks.push(
                                        start_downlink_runtime(
                                            identity,
                                            Some(*addr),
                                            key,
                                            handle.client_tx.clone(),
                                            config,
                                        )
                                        .boxed(),
                                    );
                                }
                            }
                            for cmd_req in cmd_requests {
                                tasks.push(
                                    attach_cmd_request(
                                        *addr,
                                        scheme,
                                        host.clone(),
                                        cmd_req,
                                        handle.client_tx.clone(),
                                        config.remote_buffer_size,
                                    )
                                    .boxed(),
                                )
                            }
                        } else {
                            debug!(scheme = %scheme, host = %host, resolved = ?sock_addrs, "Requesting new client connection for downlink runtime.");
                            let addr_vec = sock_addrs.into_iter().collect();
                            if let Some(task) = new_client(&connector, addr_vec, scheme, host).await
                            {
                                tasks.push(task.boxed());
                            } else {
                                break;
                            }
                        }
                    }
                    Event::Resolved {
                        host,
                        result: Err(e),
                        ..
                    } => {
                        error!(host = %host, error = %e, "DNS resolution failed.");
                        let (dl_requests, cmd_requests) = pending.open_client_failed(&host);
                        let error = DownlinkRuntimeError::DownlinkConnectionFailed(
                            DownlinkFailureReason::UnresolvableRemote(Arc::new(e)),
                        );
                        for DownlinkRequest {
                            remote,
                            address,
                            kind,
                            promise,
                            ..
                        } in dl_requests
                        {
                            if promise.send(Err(error.clone())).is_err() {
                                debug!(error = %error, remote = ?remote, address = %address, kind = ?kind, "Request for downlink dropped before it failed to resolve.");
                            }
                        }
                        for CommanderRequest {
                            agent_id,
                            key,
                            promise,
                        } in cmd_requests
                        {
                            if promise.send(Err(error.clone())).is_err() {
                                debug!(error = %error, agent_id = %agent_id, key = ?key, "Request for client connection dropped before it failed to resolve.");
                            }
                        }
                    }
                    Event::NewClientResult {
                        scheme,
                        host,
                        result: Ok(EstablishedClient { tx, sock_addr }),
                    } => {
                        debug!(addr = %sock_addr, host = %host, "New client connection opened.");
                        let handle = match clients.entry(sock_addr) {
                            Entry::Occupied(entry) => {
                                let handle = entry.into_mut();
                                handle.client_tx = tx;
                                handle.downlinks.clear();
                                handle
                            }
                            Entry::Vacant(entry) => entry.insert(ClientHandle::new(tx)),
                        };
                        if let Some((dl_reqs, cmd_reqs)) = pending.socket_ready(&host, sock_addr) {
                            for key in dl_reqs {
                                let (addr, kind) = &key;
                                debug!(address = %addr, kind = ?kind, "Starting downlink runtime.");
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
                            for request in cmd_reqs {
                                tasks.push(
                                    attach_cmd_request(
                                        sock_addr,
                                        scheme,
                                        host.clone(),
                                        request,
                                        handle.client_tx.clone(),
                                        config.remote_buffer_size,
                                    )
                                    .boxed(),
                                );
                            }
                        }
                    }
                    Event::NewClientResult {
                        host,
                        result: Err(e),
                        ..
                    } => {
                        error!(host = %host, error = %e, "Opening a new client connection failed.");
                        let err: DownlinkRuntimeError = e.into();
                        let (dl_requests, cmd_requests) = pending.open_client_failed(&host);
                        for request in dl_requests {
                            let DownlinkRequest {
                                promise,
                                remote,
                                address,
                                kind,
                                ..
                            } = request;
                            if promise.send(Err(err.clone())).is_err() {
                                info!(remote = ?remote, address = %address, kind = ?kind, "Request for a downlink dropped before it failed to complete.");
                            }
                        }
                        for CommanderRequest {
                            agent_id,
                            key,
                            promise,
                        } in cmd_requests
                        {
                            if promise.send(Err(err.clone())).is_err() {
                                debug!(error = %err, agent_id = %agent_id, key = ?key, "Request for client connection dropped before it failed to complete.");
                            }
                        }
                    }
                    Event::RuntimeAttachmentResult {
                        downlink_id,
                        remote_address,
                        key,
                        result: Ok((attach_tx, runtime)),
                    } => {
                        let (lane_addr, kind) = &key;
                        let handle = if let Some(addr) = remote_address {
                            clients.get_mut(&addr)
                        } else {
                            Some(&mut local_handle)
                        };
                        if let Some(handle) = handle {
                            debug!(address = %lane_addr, kind = ?kind, remote = ?remote_address, "Attempting to attach to a downlink runtime.");
                            handle.insert(key.clone(), downlink_id, attach_tx.clone());
                            let key_cpy = key.clone();
                            tasks.push(
                                tokio::spawn(
                                    runtime
                                        .run(connector.stop_handle(), config)
                                        .with_budget_or_default(coop_budget),
                                )
                                .map(move |result| Event::RuntimeTerminated {
                                    socket_addr: remote_address,
                                    key: key_cpy,
                                    result,
                                })
                                .boxed(),
                            );

                            let requests = pending.dl_ready(remote_address, &key);

                            if requests.is_empty() {
                                error!(key = ?key, remote_address = ?remote_address, "No pending requests for downlink.");
                            }

                            for request in requests {
                                trace!("Attaching to a downlink runtime.");
                                tasks.push(
                                    attach_to_runtime(
                                        downlink_id,
                                        request,
                                        attach_tx.clone(),
                                        config.downlink_buffer_size,
                                    )
                                    .boxed(),
                                );
                            }
                        } else {
                            info!(address = %lane_addr, kind = ?kind, remote = ?remote_address, "Expected to attach to a downlink runtime but it had already stopped.");
                            for DownlinkRequest {
                                promise,
                                remote,
                                address,
                                kind,
                                ..
                            } in pending.dl_ready(remote_address, &key)
                            {
                                if promise
                                    .send(Err(DownlinkRuntimeError::DownlinkConnectionFailed(
                                        DownlinkFailureReason::RemoteStopped,
                                    )))
                                    .is_err()
                                {
                                    debug!(remote = ?remote, address = %address, kind = ?kind, "Request for a downlink dropped before it failed to complete.");
                                }
                            }
                        }
                    }
                    Event::RuntimeAttachmentResult {
                        downlink_id,
                        remote_address,
                        key,
                        result: Err(err),
                    } => {
                        let (lane_addr, kind) = &key;
                        error!(address = %lane_addr, kind = ?kind, error = %err, downlink_id = %downlink_id, "A request to attach to a downlink runtime failed.");
                        for DownlinkRequest {
                            promise,
                            remote,
                            address,
                            kind,
                            ..
                        } in pending.dl_ready(remote_address, &key)
                        {
                            if promise
                                .send(Err(DownlinkRuntimeError::DownlinkConnectionFailed(
                                    DownlinkFailureReason::RemoteStopped,
                                )))
                                .is_err()
                            {
                                info!(remote = ?remote, address = %address, kind = ?kind, "The remote connection stopped before a downlink was set up.");
                            }
                        }
                    }
                    Event::Attached {
                        remote,
                        address,
                        kind,
                        promise,
                        result,
                    } => {
                        debug!(remote = ?remote, address = %address, kind = ?kind, "Attaching to downlink runtime completed successfully.");
                        if promise.send(result).is_err() {
                            debug!(remote = ?remote, address = %address, kind = ?kind, "A request for a downlink was dropped before it was satisfied.");
                        }
                    }
                    Event::RuntimeTerminated {
                        socket_addr,
                        key,
                        result,
                    } => {
                        if let Some(addr) = socket_addr {
                            if let Entry::Occupied(mut entry) = clients.entry(addr) {
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
                        } else {
                            debug!(socket_addr = ?socket_addr, address = %address, kind = ?kind, "A downlink runtime terminated normally.");
                        }
                    }
                    Event::CommanderAttached {
                        request:
                            CommanderRequest {
                                agent_id,
                                key,
                                promise,
                            },
                        result,
                    } => {
                        debug!(agent_id = %agent_id, key = ?key, "Completing command channel request.");
                        if promise.send(result).is_err() {
                            debug!(agent_id = %agent_id, key = ?key, "A request for a command channel was dropped before it was satisfied.");
                        }
                    }
                    Event::ClientStopped {
                        socket_addr,
                        scheme,
                        host,
                        request,
                    } => {
                        clients.remove(&socket_addr);
                        if pending.push_remote_cmd(host.clone(), request) {
                            if let Some(task) =
                                new_client(&connector, vec![socket_addr], scheme, host).await
                            {
                                tasks.push(task.boxed());
                            } else {
                                break;
                            }
                        }
                    }
                    Event::LocalAttachFailed { request } => {
                        let CommanderRequest {
                            agent_id,
                            key,
                            promise,
                        } = request;
                        if promise
                            .send(Err(DownlinkRuntimeError::RuntimeError(
                                AgentRuntimeError::Stopping,
                            )))
                            .is_err()
                        {
                            debug!(agent_id = %agent_id, key = ?key, "Request for client connection dropped before it failed to complete.");
                        }
                        info!(agent_id = %agent_id,
                            "Local agent attachment channel stopped indicating server is stopping."
                        );
                        break;
                    }
                }
            }
            connector
        };

        debug!("Waiting for downlink runtimes to terminate.");

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

        debug!("Reporting that the downlink connector has stopped.");
        connector.stopped();
    }
}

enum Event {
    // A request has been received for a new downlink.
    Request(LinkRequest),
    // A DNS resolution has completed.
    Resolved {
        scheme: Scheme,
        host: Text,
        result: Result<Vec<SocketAddr>, std::io::Error>,
    },
    // A request to the main server task to open a new socket has completed.
    NewClientResult {
        scheme: Scheme,
        host: Text,
        result: Result<EstablishedClient, NewClientError>,
    },
    // A request to attach to a downlink runtime task has completed.
    RuntimeAttachmentResult {
        downlink_id: Uuid,
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
        remote: Option<SchemeHostPort>,
        address: RelativeAddress<Text>,
        kind: DownlinkKind,
        promise: oneshot::Sender<Result<Io, DownlinkRuntimeError>>,
        result: Result<Io, DownlinkRuntimeError>,
    },
    CommanderAttached {
        request: CommanderRequest,
        result: Result<ByteWriter, DownlinkRuntimeError>,
    },
    ClientStopped {
        socket_addr: SocketAddr,
        scheme: Scheme,
        host: Text,
        request: CommanderRequest,
    },
    LocalAttachFailed {
        request: CommanderRequest,
    },
}

struct ClientHandle {
    client_tx: mpsc::Sender<AttachClient>,
    downlinks: HashMap<DlKey, (Uuid, mpsc::Sender<AttachAction>)>,
}

impl ClientHandle {
    fn new(client_tx: mpsc::Sender<AttachClient>) -> Self {
        ClientHandle {
            client_tx,
            downlinks: Default::default(),
        }
    }

    fn insert(&mut self, key: DlKey, downlink_id: Uuid, tx: mpsc::Sender<AttachAction>) {
        let ClientHandle { downlinks, .. } = self;
        if downlinks.contains_key(&key) {
            error!(key = ?key, "Handle already contains a downlink with that key.");
        }
        downlinks.insert(key, (downlink_id, tx));
    }

    fn get(&self, key: &DlKey) -> Option<&(Uuid, mpsc::Sender<AttachAction>)> {
        self.downlinks.get(key)
    }

    fn remove(&mut self, key: &DlKey) -> bool {
        let ClientHandle { downlinks, .. } = self;
        if !downlinks.contains_key(key) {
            warn!(key = ?key, "Attempting to remove a downlink that isn't registered.");
        } else {
            debug!(key = ?key, "Removing downlink.");
        }
        downlinks.remove(key);
        downlinks.is_empty()
    }
}

async fn attach_to_runtime(
    downlink_id: Uuid,
    request: DownlinkRequest,
    attach_tx: mpsc::Sender<AttachAction>,
    buffer_size: NonZeroUsize,
) -> Event {
    let DownlinkRequest {
        promise,
        remote,
        address,
        kind,
        options,
        ..
    } = request;
    let (in_tx, in_rx) = byte_channel(buffer_size);
    let (out_tx, out_rx) = byte_channel(buffer_size);
    debug!(downlink_id = %downlink_id, "Sending attachment request to downlink runtime.");
    let result = attach_tx
        .send(AttachAction::new(out_rx, in_tx, options))
        .await
        .map(move |_| (out_tx, in_rx))
        .map_err(|_| {
            DownlinkRuntimeError::DownlinkConnectionFailed(DownlinkFailureReason::DownlinkStopped)
        });
    Event::Attached {
        remote,
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
            downlink_id: identity,
            remote_address: remote_addr,
            key: (rel_addr, kind),
            result: Err(DownlinkFailureReason::RemoteStopped),
        };
    }
    let err = match done_rx.await {
        Ok(Ok(_)) => None,
        Ok(Err(LinkError::NoEndpoint(addr))) => {
            Some(DownlinkFailureReason::UnresolvableLocal(addr))
        }
        _ => Some(DownlinkFailureReason::RemoteStopped),
    };
    if let Some(err) = err {
        return Event::RuntimeAttachmentResult {
            downlink_id: identity,
            remote_address: remote_addr,
            key: (rel_addr, kind),
            result: Err(err),
        };
    }
    let io = (out_tx, in_rx);
    let (attachment_tx, attachment_rx) = mpsc::channel(config.attachment_queue_size.get());
    let runtime = DownlinkRuntime::new(identity, rel_addr.clone(), attachment_rx, kind, io);
    Event::RuntimeAttachmentResult {
        downlink_id: identity,
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
                        IdentifiedAddress {
                            identity,
                            address: path,
                        },
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
                        IdentifiedAddress {
                            identity,
                            address: path,
                        },
                        config,
                    );
                    runtime.run().await;
                }
            }
        }
    }
}

async fn attach_cmd_request(
    addr: SocketAddr,
    scheme: Scheme,
    host: Text,
    request: CommanderRequest,
    client_tx: mpsc::Sender<AttachClient>,
    remote_buffer_size: NonZeroUsize,
) -> Event {
    let (tx, rx) = byte_channel(remote_buffer_size);
    let (done_tx, done_rx) = oneshot::channel();
    if client_tx
        .send(AttachClient::OneWay {
            agent_id: request.agent_id,
            path: None,
            receiver: rx,
            done: done_tx,
        })
        .await
        .is_err()
    {
        return Event::ClientStopped {
            socket_addr: addr,
            scheme,
            host,
            request,
        };
    };
    match done_rx.await {
        Ok(Ok(_)) => Event::CommanderAttached {
            request,
            result: Ok(tx),
        },
        Ok(Err(e)) => Event::CommanderAttached {
            request,
            result: Err(DownlinkRuntimeError::DownlinkConnectionFailed(e.into())),
        },
        Err(_) => Event::ClientStopped {
            socket_addr: addr,
            scheme,
            host,
            request,
        },
    }
}

async fn attach_cmd_request_local(
    request: CommanderRequest,
    path: RelativeAddress<Text>,
    client_tx: mpsc::Sender<AttachClient>,
    remote_buffer_size: NonZeroUsize,
) -> Event {
    let (tx, rx) = byte_channel(remote_buffer_size);
    let (done_tx, done_rx) = oneshot::channel();
    if client_tx
        .send(AttachClient::OneWay {
            agent_id: request.agent_id,
            path: Some(path),
            receiver: rx,
            done: done_tx,
        })
        .await
        .is_err()
    {
        return Event::LocalAttachFailed { request };
    };
    match done_rx.await {
        Ok(Ok(_)) => Event::CommanderAttached {
            request,
            result: Ok(tx),
        },
        Ok(Err(e)) => Event::CommanderAttached {
            request,
            result: Err(DownlinkRuntimeError::DownlinkConnectionFailed(e.into())),
        },
        Err(_) => Event::LocalAttachFailed { request },
    }
}

async fn new_client(
    connector: &DownlinksConnector,
    addr_vec: Vec<SocketAddr>,
    scheme: Scheme,
    host: Text,
) -> Option<impl Future<Output = Event> + 'static> {
    let (registration, rx) = ClientRegistration::new(scheme, host.clone(), addr_vec);
    if connector.register(registration).await.is_err() {
        None
    } else {
        Some(rx.map(move |result| {
            let result = match result {
                Ok(Ok(client)) => Ok(client),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(NewClientError::ServerStopped),
            };
            Event::NewClientResult {
                scheme,
                host,
                result,
            }
        }))
    }
}
