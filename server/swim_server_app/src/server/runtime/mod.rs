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

use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use ratchet::{WebSocket, WebSocketStream};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use swim_api::agent::BoxAgent;
use swim_api::downlink::DownlinkKind;
use swim_api::error::{AgentRuntimeError, DownlinkFailureReason, DownlinkTaskError};
use swim_model::address::RelativeAddress;
use swim_model::Text;
use swim_remote::{AgentResolutionError, AttachClient, FindNode, NoSuchAgent, RemoteTask};
use swim_runtime::agent::{
    run_agent, AgentAttachmentRequest, AgentExecError, DisconnectionReason, DownlinkRequest,
};
use swim_runtime::downlink::failure::{AlwaysAbortStrategy, AlwaysIgnoreStrategy, ReportStrategy};
use swim_runtime::downlink::{
    AttachAction, DownlinkOptions, DownlinkRuntimeConfig, MapDownlinkRuntime, ValueDownlinkRuntime,
};
use swim_runtime::error::ConnectionError;
use swim_runtime::remote::table::SchemeHostPort;
use swim_runtime::remote::{BadUrl, ExternalConnections};
use swim_runtime::remote::{Listener, SchemeSocketAddr};
use swim_runtime::ws::WsConnections;
use swim_utilities::io::byte_channel::{byte_channel, ByteReader, ByteWriter};
use swim_utilities::routing::route_pattern::RoutePattern;
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::trigger::{self, promise};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinError;
use tracing::{debug, error, info, info_span, warn};
use tracing_futures::Instrument;
use url::Url;
use uuid::Uuid;

use crate::config::SwimServerConfig;
use crate::plane::PlaneModel;
use crate::server::ServerHandle;

use self::downlinks::{DlKey, PendingDownlinks};
use self::ids::IdIssuer;

use super::Server;

pub mod downlinks;
mod ids;
#[cfg(test)]
mod tests;

/// A swim server task that listens for incoming connections on a socket and runs the
/// agents specified in a [`PlaneModel`].
pub struct SwimServer<Net, Ws> {
    plane: PlaneModel,
    addr: SocketAddr,
    networking: Net,
    websockets: Ws,
    config: SwimServerConfig,
}

enum ServerEvent<Sock, Ext> {
    NewConnection(Result<(Sock, SchemeSocketAddr), std::io::Error>),
    FindRoute(FindNode),
    FailRoute(FindNode),
    OpenDownlink(DownlinkRequest),
    FailDownlink(DownlinkRequest),
    DownlinkEvent(DownlinkEvent<Sock, Ext>),
    RemoteStopped(SocketAddr, Result<(), JoinError>),
    AgentStopped(Text, Result<Result<(), AgentExecError>, JoinError>),
    ConnectionStopped(ConnectionTerminated),
}

impl<Net, Ws> Server for SwimServer<Net, Ws>
where
    Net: ExternalConnections,
    Net::Socket: WebSocketStream,
    Ws: WsConnections<Net::Socket> + Send + Sync + 'static,
{
    fn run(
        self,
    ) -> (
        futures::future::BoxFuture<'static, Result<(), std::io::Error>>,
        ServerHandle,
    ) {
        let (fut, handle) = self.run_server();
        (fut.boxed(), handle)
    }

    fn run_box(
        self: Box<Self>,
    ) -> (
        futures::future::BoxFuture<'static, Result<(), std::io::Error>>,
        ServerHandle,
    ) {
        (*self).run()
    }
}

async fn with_sock_addr<F>(sock_addr: SocketAddr, fut: F) -> (SocketAddr, F::Output)
where
    F: Future,
{
    (sock_addr, fut.await)
}

/// Tracks the shutdown process for the server.
#[derive(Clone, Copy, Debug)]
enum TaskState {
    Running,           //The server has is running normally.
    StoppingDownlinks, //The server is stopping and waiting for downlinks to stop.
    StoppingAgents,    //The server is shutting down and waiting for the agents to stop.
    StoppingRemotes, //The server is shutting down, all agents have stopped, and the remote connections are being closed.
}

impl<Net, Ws> SwimServer<Net, Ws>
where
    Net: ExternalConnections,
    Net::Socket: WebSocketStream,
    Ws: WsConnections<Net::Socket> + Send + Sync,
{
    pub fn new(
        plane: PlaneModel,
        addr: SocketAddr,
        networking: Net,
        websockets: Ws,
        config: SwimServerConfig,
    ) -> Self {
        SwimServer {
            plane,
            addr,
            networking,
            websockets,
            config,
        }
    }

    pub fn run_server(
        self,
    ) -> (
        impl Future<Output = Result<(), std::io::Error>> + Send,
        ServerHandle,
    ) {
        let (tx, rx) = trigger::trigger();
        let (addr_tx, addr_rx) = oneshot::channel();
        let fut = self.run_inner(rx, addr_tx);
        (fut, ServerHandle::new(tx, addr_rx))
    }

    async fn run_inner(
        self,
        stop_signal: trigger::Receiver,
        addr_tx: oneshot::Sender<SocketAddr>,
    ) -> Result<(), std::io::Error> {
        let SwimServer {
            plane,
            addr,
            networking,
            websockets,
            config,
        } = self;

        let networking = Arc::new(networking);
        let websockets = Arc::new(websockets);

        let (bound_addr, listener) = networking.bind(addr).await?;
        let _ = addr_tx.send(bound_addr);
        let mut id_issuer = IdIssuer::default();

        let (find_tx, mut find_rx) = mpsc::channel(config.find_route_buffer_size.get());
        let (open_dl_tx, mut open_dl_rx) = mpsc::channel(config.open_downlink_buffer_size.get());
        let mut remote_channels = HashMap::new();
        let mut agent_channels = HashMap::new();
        let mut downlink_channels: HashMap<
            Option<SocketAddr>,
            HashMap<DlKey, mpsc::Sender<AttachAction>>,
        > = HashMap::new();
        let mut pending_downlinks = PendingDownlinks::default();

        let mut remote_tasks = FuturesUnordered::new();
        let mut agent_tasks = FuturesUnordered::new();
        let mut downlink_tasks = FuturesUnordered::new();
        let mut connection_tasks = FuturesUnordered::new();

        let mut accept_stream = listener.into_stream().take_until(stop_signal);

        let routes = Routes::new(plane.routes);

        let (dl_stop_tx, dl_stop_rx) = trigger::trigger();
        let mut dl_stop = Some(dl_stop_tx);
        let (agent_stop_tx, agent_stop_rx) = trigger::trigger();
        let mut agent_stop = Some(agent_stop_tx);
        let (remote_stop_tx, remote_stop_rx) = trigger::trigger();
        let mut remote_stop = Some(remote_stop_tx);

        let mut state = TaskState::Running;

        loop {
            let event = match state {
                TaskState::Running => {
                    tokio::select! {
                        biased;
                        Some((addr, result)) = remote_tasks.next(), if !remote_tasks.is_empty() => ServerEvent::RemoteStopped(addr, result),
                        Some((id, result)) = agent_tasks.next(), if !agent_tasks.is_empty() => ServerEvent::AgentStopped(id, result),
                        Some(reason) = connection_tasks.next(), if !connection_tasks.is_empty() => ServerEvent::ConnectionStopped(reason),
                        Some(dl_ev) = downlink_tasks.next(), if !downlink_tasks.is_empty() => ServerEvent::DownlinkEvent(dl_ev),
                        maybe_result = accept_stream.next() => {
                            if let Some(result) = maybe_result {
                                ServerEvent::NewConnection(result)
                            } else {
                                if let Some(stop) = agent_stop.take() {
                                    stop.trigger();
                                }
                                state = TaskState::StoppingDownlinks;
                                continue;
                            }
                        },
                        Some(find_route) = find_rx.recv() => ServerEvent::FindRoute(find_route),
                        Some(dl_req) = open_dl_rx.recv() => ServerEvent::OpenDownlink(dl_req),
                        else => continue,
                    }
                }
                TaskState::StoppingDownlinks => {
                    tokio::select! {
                        biased;
                        Some((id, result)) = remote_tasks.next(), if !remote_tasks.is_empty() => ServerEvent::RemoteStopped(id, result),
                        Some((id, result)) = agent_tasks.next(), if !agent_tasks.is_empty() => ServerEvent::AgentStopped(id, result),
                        Some(reason) = connection_tasks.next(), if !connection_tasks.is_empty() => ServerEvent::ConnectionStopped(reason),
                        maybe_dl_ev = downlink_tasks.next() => {
                            if let Some(dl_ev) = maybe_dl_ev {
                                ServerEvent::DownlinkEvent(dl_ev)
                            } else {
                                if let Some(stop) = dl_stop.take() {
                                    stop.trigger();
                                }
                                state = TaskState::StoppingAgents;
                                continue;
                            }
                        },
                        Some(find_route) = find_rx.recv() => ServerEvent::FindRoute(find_route),
                        Some(dl_req) = open_dl_rx.recv() => ServerEvent::FailDownlink(dl_req),
                        else => continue,
                    }
                }
                TaskState::StoppingAgents => {
                    tokio::select! {
                        biased;
                        Some((id, result)) = remote_tasks.next(), if !remote_tasks.is_empty() => ServerEvent::RemoteStopped(id, result),
                        maybe_result = agent_tasks.next() => {
                            if let Some((id, result)) = maybe_result {
                                ServerEvent::AgentStopped(id, result)
                            } else {
                                if let Some(stop) = remote_stop.take() {
                                    stop.trigger();
                                }
                                state = TaskState::StoppingRemotes;
                                continue;
                            }
                        },
                        Some(reason) = connection_tasks.next(), if !connection_tasks.is_empty() => ServerEvent::ConnectionStopped(reason),
                        Some(find_route) = find_rx.recv() => ServerEvent::FailRoute(find_route),
                        Some(dl_req) = open_dl_rx.recv() => ServerEvent::FailDownlink(dl_req),
                        else => continue,
                    }
                }
                TaskState::StoppingRemotes => {
                    tokio::select! {
                        biased;
                        maybe_result = remote_tasks.next() => {
                            if let Some((id, result)) = maybe_result {
                                ServerEvent::RemoteStopped(id, result)
                            } else {
                                break;
                            }
                        },
                        Some(reason) = connection_tasks.next(), if !connection_tasks.is_empty() => ServerEvent::ConnectionStopped(reason),
                        Some(find_route) = find_rx.recv() => ServerEvent::FailRoute(find_route),
                        Some(dl_req) = open_dl_rx.recv() => ServerEvent::FailDownlink(dl_req),
                        else => continue,
                    }
                }
            };

            match event {
                ServerEvent::NewConnection(Ok((sock, addr))) => {
                    info!(peer = %addr, "Accepting new client connection.");
                    match websockets.accept_connection(sock).await {
                        Ok(websocket) => {
                            let id = id_issuer.next_remote();
                            let (attach_tx, attach_rx) =
                                mpsc::channel(config.client_attachment_buffer_size.get());

                            let sock_addr = addr.addr;
                            remote_channels.insert(sock_addr, attach_tx);
                            let task = RemoteTask::new(
                                id,
                                remote_stop_rx.clone(),
                                websocket,
                                attach_rx,
                                find_tx.clone(),
                                config.remote.registration_buffer_size,
                            );

                            remote_tasks.push(with_sock_addr(sock_addr, tokio::spawn(task.run())));
                        }
                        Err(error) => {
                            warn!(error = %{Into::<ConnectionError>::into(error)}, "Negotiating incoming websocket connection failed.");
                        }
                    }
                }
                ServerEvent::NewConnection(Err(error)) => {
                    warn!(error = %error, "Accepting incoming connection failed.");
                }
                ServerEvent::RemoteStopped(id, result) => {
                    remote_channels.remove(&id);
                    if let Err(error) = result {
                        error!(error = %error, remote_id = %id, "Remote connection task panicked.");
                    }
                }
                ServerEvent::AgentStopped(route, result) => {
                    agent_channels.remove(&route);
                    match result {
                        Err(error) => {
                            error!(error = %error, route = %route, "Agent task panicked.");
                        }
                        Ok(Err(error)) => {
                            error!(error = %error, route = %route, "Agent task failed.")
                        }
                        _ => {}
                    }
                }
                ServerEvent::ConnectionStopped(reason) => {
                    let ConnectionTerminated {
                        remote_id,
                        agent_id,
                        reason,
                    } = reason;
                    match &reason {
                        DisconnectionReason::DuplicateRegistration(_) => {
                            error!(remote_id = %remote_id, agent_id = %agent_id, "Multiple connections attempted between a remote and an agent.");
                        }
                        _ => {
                            info!(remote_id = %remote_id, agent_id = %agent_id, reason = %reason, "A connection between and agent and a remote stopped.");
                        }
                    }
                }
                ServerEvent::FailRoute(FindNode {
                    source,
                    node,
                    lane,
                    provider,
                }) => {
                    if provider
                        .send(Err(AgentResolutionError::PlaneStopping))
                        .is_err()
                    {
                        debug!(remote_id = %source, node = %node, lane = %lane, "Remote stopped with pending agent resolution.");
                    }
                }
                ServerEvent::FindRoute(FindNode {
                    source,
                    node,
                    lane,
                    provider,
                }) => {
                    info!(source = %source, node = %node, "Attempting to connect an agent to a remote.");
                    let agent_tx = match agent_channels.entry(node) {
                        Entry::Occupied(entry) => {
                            debug!("Agent already running.");
                            Ok(entry.into_mut())
                        }
                        Entry::Vacant(entry) => {
                            debug!("Attempting to start new agent instance.");
                            if let Some((route, agent)) =
                                RelativeUri::from_str(entry.key().as_str())
                                    .ok()
                                    .and_then(|route| {
                                        routes.find_route(&route).map(move |agent| (route, agent))
                                    })
                            {
                                let id = id_issuer.next_agent();
                                let (attachment_tx, attachment_rx) = mpsc::channel(1);
                                let agent_task = run_agent(
                                    agent,
                                    id,
                                    route,
                                    attachment_rx,
                                    open_dl_tx.clone(),
                                    agent_stop_rx.clone(),
                                    config.agent,
                                    config.agent_runtime,
                                );
                                let name = entry.key().clone();
                                agent_tasks.push(tokio::spawn(agent_task).map(move |r| (name, r)));
                                Ok(entry.insert((id, attachment_tx)))
                            } else {
                                Err(entry.into_key())
                            }
                        }
                    };
                    match agent_tx {
                        Ok((agent_id, agent_tx)) => {
                            let connect_task = attach_agent(
                                source,
                                *agent_id,
                                agent_tx.clone(),
                                config.agent_runtime_buffer_size,
                                config.attachment_timeout,
                                provider,
                            ).instrument(info_span!("Remote to agent connection task.", remote_id = %source, agent_id = %agent_id));
                            connection_tasks.push(connect_task);
                        }
                        Err(node) => {
                            debug!(node = %node, "Requested agent does not exist.");
                            if let Err(Err(AgentResolutionError::NotFound(NoSuchAgent {
                                node,
                                ..
                            }))) = provider.send(Err(NoSuchAgent { node, lane }.into()))
                            {
                                debug!(source = %source, route = %node, "A remote stopped while a connection from it to an agent was pending.");
                            }
                        }
                    }
                }
                ServerEvent::FailDownlink(DownlinkRequest { promise, key, .. }) => {
                    if promise.send(Err(AgentRuntimeError::Stopping)).is_err() {
                        let (path, kind) = &key;
                        debug!(path = %path, kind = ?kind, "An agent stopped while waiting to open a downlink.");
                    }
                }
                ServerEvent::OpenDownlink(request) => {
                    let host = request.key.0.host.clone();
                    let fut = if let Some(host) = host {
                        resolve_remote_for_dl(host, request, networking.clone()).boxed()
                    } else {
                        todo!()
                    };
                    downlink_tasks.push(fut);
                }
                ServerEvent::DownlinkEvent(event) => match event {
                    DownlinkEvent::RemoteResolved {
                        request,
                        host,
                        addrs,
                    } => {
                        let DownlinkRequest { key, .. } = &request;
                        let (ref path, kind) = *key;
                        let fut = if let Ok(addrs) = addrs {
                            let rel_key = (
                                RelativeAddress::new(path.node.clone(), path.lane.clone()),
                                kind,
                            );

                            if let Some(attach_tx) = addrs.iter().find_map(|addr| {
                                downlink_channels
                                    .get(&Some(*addr))
                                    .and_then(|inner| inner.get(&rel_key))
                            }) {
                                run_downlink::<Net::Socket, Ws::Ext>(request, attach_tx.clone())
                                    .boxed()
                            } else if let Some((addr, remote_attach)) = addrs
                                .iter()
                                .find_map(|addr| remote_channels.get(addr).map(|tx| (addr, tx)))
                            {
                                let id = id_issuer.next_downlink();
                                let rel_addr =
                                    RelativeAddress::new(path.node.clone(), path.lane.clone());
                                pending_downlinks.push_relative(*addr, request);
                                start_downlink_runtime(
                                    id,
                                    Some(*addr),
                                    rel_addr,
                                    kind,
                                    remote_attach.clone(),
                                    config.downlink_runtime,
                                    dl_stop_rx.clone(),
                                )
                                .boxed()
                            } else {
                                pending_downlinks.push_remote(host.clone(), request);
                                open_client_for_dl(
                                    host,
                                    addrs,
                                    networking.clone(),
                                    websockets.clone(),
                                )
                                .boxed()
                            }
                        } else {
                            let DownlinkRequest { promise, .. } = request;
                            if promise
                                .send(Err(AgentRuntimeError::DownlinkConnectionFailed(
                                    DownlinkFailureReason::Unresolvable,
                                )))
                                .is_err()
                            {
                                todo!("Log error.")
                            }
                            continue;
                        };
                        downlink_tasks.push(fut);
                    }
                    DownlinkEvent::RemoteOpened { host, result } => {
                        if let Ok((sock_addr, ws)) = result {
                            let id = id_issuer.next_remote();
                            let (attach_tx, attach_rx) =
                                mpsc::channel(config.client_attachment_buffer_size.get());
                            let task = RemoteTask::new(
                                id,
                                remote_stop_rx.clone(),
                                ws,
                                attach_rx,
                                find_tx.clone(),
                                config.remote.registration_buffer_size,
                            );
                            remote_channels.insert(sock_addr, attach_tx.clone());
                            remote_tasks.push(with_sock_addr(sock_addr, tokio::spawn(task.run())));
                            for (rel_addr, kind) in pending_downlinks
                                .socket_ready(host, sock_addr)
                                .into_iter()
                                .flatten()
                            {
                                let id = id_issuer.next_downlink();
                                let fut = start_downlink_runtime(
                                    id,
                                    Some(sock_addr),
                                    rel_addr.clone(),
                                    *kind,
                                    attach_tx.clone(),
                                    config.downlink_runtime,
                                    dl_stop_rx.clone(),
                                )
                                .boxed();
                                downlink_tasks.push(fut);
                            }
                        } else {
                            for request in pending_downlinks.open_client_failed(&host) {
                                let DownlinkRequest { promise, .. } = request;
                                if promise
                                    .send(Err(AgentRuntimeError::DownlinkConnectionFailed(
                                        DownlinkFailureReason::ConnectionFailed,
                                    )))
                                    .is_err()
                                {
                                    todo!("Log error.")
                                }
                            }
                        }
                    }
                    DownlinkEvent::DownlinkRuntimeAttached {
                        result,
                        remote_address,
                        key,
                    } => {
                        if let Ok(DownlinkRuntime {
                            attachment_tx,
                            task,
                        }) = result
                        {
                            let key_copy = key.clone();
                            let runtime_task = tokio::spawn(task)
                                .map(move |result| DownlinkEvent::DownlinkRuntimeStopped {
                                    key: key_copy,
                                    remote_address,
                                    result,
                                })
                                .boxed();
                            downlink_tasks.push(runtime_task);
                            for request in pending_downlinks.dl_ready(remote_address, &key) {
                                downlink_channels
                                    .entry(remote_address)
                                    .or_default()
                                    .insert(key.clone(), attachment_tx.clone());
                                let dl_task = run_downlink(request, attachment_tx.clone()).boxed();
                                downlink_tasks.push(dl_task);
                            }
                        } else {
                            for request in pending_downlinks.dl_ready(remote_address, &key) {
                                let DownlinkRequest { promise, .. } = request;
                                if promise
                                    .send(Err(AgentRuntimeError::DownlinkConnectionFailed(
                                        DownlinkFailureReason::ConnectionFailed,
                                    )))
                                    .is_err()
                                {
                                    todo!("Log error.")
                                }
                            }
                        }
                    }
                    DownlinkEvent::DownlinkRuntimeStopped {
                        key,
                        remote_address,
                        result,
                    } => {
                        if let Entry::Occupied(mut entry) = downlink_channels.entry(remote_address)
                        {
                            entry.get_mut().remove(&key);
                            if entry.get_mut().is_empty() {
                                entry.remove();
                            }
                        }
                        if let Err(_e) = result {
                            todo!("Log error.");
                        }
                    }
                    DownlinkEvent::DownlinkTaskStopped { result } => {
                        if let Err(_) = result {
                            todo!("Log errors.");
                        }
                    }
                },
            }
        }

        Ok(())
    }
}

struct Routes(Vec<(RoutePattern, BoxAgent)>);

impl Routes {
    fn new(routes: Vec<(RoutePattern, BoxAgent)>) -> Self {
        Routes(routes)
    }

    fn find_route<'a>(&'a self, node: &RelativeUri) -> Option<&'a BoxAgent> {
        let Routes(routes) = self;
        routes
            .iter()
            .find(|(route, _)| route.unapply_relative_uri(node).is_ok())
            .map(|(_, agent)| agent)
    }
}

struct ConnectionTerminated {
    remote_id: Uuid,
    agent_id: Uuid,
    reason: DisconnectionReason,
}

//A task that attempts to connect a running agent instance to a remote. After the connection
//is established this task will continue to wait until the connection is terminated.
async fn attach_agent(
    remote_id: Uuid,
    agent_id: Uuid,
    tx: mpsc::Sender<AgentAttachmentRequest>,
    buffer_size: NonZeroUsize,
    connect_timeout: Duration,
    provider: oneshot::Sender<Result<(ByteWriter, ByteReader), AgentResolutionError>>,
) -> ConnectionTerminated {
    let (in_tx, in_rx) = byte_channel(buffer_size);
    let (out_tx, out_rx) = byte_channel(buffer_size);

    let (disconnect_tx, disconnect_rx) = promise::promise();
    let (connnected_tx, connected_rx) = trigger::trigger();

    let req = AgentAttachmentRequest::with_confirmation(
        remote_id,
        (out_tx, in_rx),
        disconnect_tx,
        connnected_tx,
    );

    let reason = match tokio::time::timeout(connect_timeout, async move {
        tx.send(req).await.is_ok() && connected_rx.await.is_ok()
    })
    .await
    {
        Ok(true) => {
            if provider.send(Ok((in_tx, out_rx))).is_ok() {
                if let Ok(reason) = disconnect_rx.await {
                    *reason
                } else {
                    DisconnectionReason::Failed
                }
            } else {
                DisconnectionReason::Failed
            }
        }
        _ => DisconnectionReason::Failed,
    };

    ConnectionTerminated {
        remote_id,
        agent_id,
        reason,
    }
}

#[derive(Debug, Error)]
enum NewClientError {
    #[error("Invalid host URL.")]
    InvalidUrl(#[from] url::ParseError),
    #[error("URL {0} is not valid warp address.")]
    BadWarpUrl(#[from] BadUrl),
    #[error("URL {url} could not be resolved.")]
    ResolutionFailed {
        url: Url,
        #[source]
        error: std::io::Error,
    },
    #[error("Failed to open a remote connection.")]
    OpeningSocketFailed {
        errors: Vec<(SocketAddr, std::io::Error)>,
    },
    #[error("Failed to negotiate a websocket connection.")]
    WsNegotationFailed {
        #[from]
        #[source]
        error: ConnectionError,
    },
    #[error("The server task stopped unexpectedly.")]
    ServerStopped,
}

impl From<NewClientError> for AgentRuntimeError {
    fn from(err: NewClientError) -> Self {
        match err {
            NewClientError::InvalidUrl(_)
            | NewClientError::BadWarpUrl(_)
            | NewClientError::ResolutionFailed { .. } => AgentRuntimeError::DownlinkConnectionFailed(DownlinkFailureReason::Unresolvable),
            NewClientError::OpeningSocketFailed { .. } => AgentRuntimeError::DownlinkConnectionFailed(DownlinkFailureReason::ConnectionFailed),
            NewClientError::WsNegotationFailed { .. } => AgentRuntimeError::DownlinkConnectionFailed(DownlinkFailureReason::WebsocketNegotiationFailed),
            NewClientError::ServerStopped => AgentRuntimeError::Stopping,
        }
    }
}

struct RemoteStopped;

struct DownlinkRuntime {
    attachment_tx: mpsc::Sender<AttachAction>,
    task: BoxFuture<'static, ()>,
}

enum DownlinkEvent<S, E> {
    RemoteResolved {
        request: DownlinkRequest,
        host: Text,
        addrs: Result<Vec<SocketAddr>, NewClientError>,
    },
    RemoteOpened {
        host: Text,
        result: Result<(SocketAddr, WebSocket<S, E>), NewClientError>,
    },
    DownlinkRuntimeAttached {
        remote_address: Option<SocketAddr>,
        key: DlKey,
        result: Result<DownlinkRuntime, RemoteStopped>,
    },
    DownlinkRuntimeStopped {
        remote_address: Option<SocketAddr>,
        key: DlKey,
        result: Result<(), JoinError>,
    },
    DownlinkTaskStopped {
        result: Result<Result<(), DownlinkTaskError>, JoinError>,
    },
}

async fn run_downlink<S, E>(
    request: DownlinkRequest,
    attach_tx: mpsc::Sender<AttachAction>,
) -> DownlinkEvent<S, E> {
    let DownlinkRequest {
        key: (path, _),
        config,
        downlink,
        promise,
    } = request;
    let (in_tx, in_rx) = byte_channel(config.buffer_size);
    let (out_tx, out_rx) = byte_channel(config.buffer_size);
    if attach_tx
        .send(AttachAction::new(out_rx, in_tx, DownlinkOptions::empty()))
        .await
        .is_err()
    {
        DownlinkEvent::DownlinkTaskStopped {
            result: Ok(Err(DownlinkTaskError::FailedToStart)),
        }
    } else {
        if promise.send(Ok(())).is_ok() {
            let result = tokio::spawn(downlink.run_boxed(path, config, in_rx, out_tx)).await;
            DownlinkEvent::DownlinkTaskStopped { result }
        } else {
            //TODO todo!("Log error.");
            DownlinkEvent::DownlinkTaskStopped {
                result: Ok(Err(DownlinkTaskError::FailedToStart)),
            }
        }
    }
}

async fn resolve_remote_for_dl<Net, E>(
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
