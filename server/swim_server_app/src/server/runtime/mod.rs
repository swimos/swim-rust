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

use futures::future::join;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use ratchet::{SplittableExtension, WebSocket, WebSocketStream};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use swim_api::agent::BoxAgent;
use swim_api::error::{AgentRuntimeError, DownlinkFailureReason, DownlinkRuntimeError};
use swim_api::store::PlanePersistence;
use swim_model::address::RelativeAddress;
use swim_model::Text;
use swim_remote::{AgentResolutionError, AttachClient, FindNode, NoSuchAgent, RemoteTask};
use swim_runtime::agent::{
    AgentAttachmentRequest, AgentExecError, AgentRoute, AgentRouteTask, CombinedAgentConfig,
    DisconnectionReason, DownlinkRequest,
};

use swim_runtime::error::ConnectionError;
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
use uuid::Uuid;

use crate::config::SwimServerConfig;
use crate::plane::PlaneModel;
use crate::server::runtime::downlinks::DlTaskRequest;
use crate::server::ServerHandle;

use self::downlinks::{DownlinkConnectionTask, ServerConnector};
use self::ids::{IdIssuer, IdKind};

use super::store::ServerPersistence;
use super::{Server, ServerError};

mod downlinks;
mod ids;
#[cfg(test)]
mod tests;

/// A swim server task that listens for incoming connections on a socket and runs the
/// agents specified in a [`PlaneModel`].
pub struct SwimServer<Net, Ws, Store> {
    plane: PlaneModel,
    addr: SocketAddr,
    networking: Net,
    websockets: Ws,
    config: SwimServerConfig,
    store: Store,
}

type ClientPromiseTx = oneshot::Sender<Result<EstablishedClient, NewClientError>>;
type ClientPromiseRx = oneshot::Receiver<Result<EstablishedClient, NewClientError>>;

enum ServerEvent<Sock, Ext> {
    NewConnection(Result<(Sock, SchemeSocketAddr), std::io::Error>),
    FindRoute(FindNode),
    FailRoute(FindNode),
    RemoteStopped(SocketAddr, Result<(), JoinError>),
    AgentStopped(Text, Result<Result<(), AgentExecError>, JoinError>),
    ConnectionStopped(ConnectionTerminated),
    RemoteClientRequest(ClientRegistration),
    NewClient(
        Result<(SocketAddr, WebSocket<Sock, Ext>), NewClientError>,
        ClientPromiseTx,
    ),
    LocalClient(AttachClient),
}

/// Response type, sent by the server, after receiving a [`ClientRegistration`].
pub struct EstablishedClient {
    /// Allows the client to attach the task managing the socket.
    tx: mpsc::Sender<AttachClient>,
    /// The address that was used to connect to the remote.
    sock_addr: SocketAddr,
}

impl EstablishedClient {
    pub fn new(tx: mpsc::Sender<AttachClient>, sock_addr: SocketAddr) -> EstablishedClient {
        EstablishedClient { tx, sock_addr }
    }
}

/// Request to attach a client to a remote server. A new socket should be opened if not
/// connections exists.
pub struct ClientRegistration {
    /// Original host URL that was resolved.
    host: Text,
    /// Addresses to try to connect to the remote.
    sock_addrs: Vec<SocketAddr>,
    /// Reply channel for the server task.
    responder: ClientPromiseTx,
}

impl ClientRegistration {
    fn new(host: Text, sock_addrs: Vec<SocketAddr>) -> (Self, ClientPromiseRx) {
        let (tx, rx) = oneshot::channel();
        (
            ClientRegistration {
                host,
                sock_addrs,
                responder: tx,
            },
            rx,
        )
    }
}

impl<Net, Ws, Store> Server for SwimServer<Net, Ws, Store>
where
    Net: ExternalConnections + Clone,
    Net::Socket: WebSocketStream,
    Ws: WsConnections<Net::Socket> + Send + Sync + 'static,
    Store: ServerPersistence + Send + Sync + 'static,
{
    fn run(
        self,
    ) -> (
        futures::future::BoxFuture<'static, Result<(), ServerError>>,
        ServerHandle,
    ) {
        let config = &self.config;
        let (server_conn, dl_conn) = downlinks::downlink_task_connector(
            config.client_request_channel_size,
            config.open_downlink_channel_size,
        );
        let downlinks = DownlinkConnectionTask::new(
            dl_conn,
            config.downlink_runtime,
            self.networking.dns_resolver(),
        );
        let (fut, handle) = self.run_server(server_conn);

        let downlinks_task = downlinks.run();
        let combined = join(fut, downlinks_task).map(|(r, _)| r);
        (combined.boxed(), handle)
    }

    fn run_box(
        self: Box<Self>,
    ) -> (
        futures::future::BoxFuture<'static, Result<(), ServerError>>,
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

impl<Net, Ws, Store> SwimServer<Net, Ws, Store>
where
    Net: ExternalConnections,
    Net::Socket: WebSocketStream,
    Ws: WsConnections<Net::Socket> + Send + Sync,
    Store: ServerPersistence + Send + Sync + 'static,
{
    pub fn new(
        plane: PlaneModel,
        addr: SocketAddr,
        networking: Net,
        websockets: Ws,
        config: SwimServerConfig,
        store: Store,
    ) -> Self {
        SwimServer {
            plane,
            addr,
            networking,
            websockets,
            config,
            store,
        }
    }
}

impl<Net, Ws, Store> SwimServer<Net, Ws, Store>
where
    Net: ExternalConnections,
    Net::Socket: WebSocketStream,
    Ws: WsConnections<Net::Socket> + Send + Sync,
    Store: ServerPersistence + Send + Sync + 'static,
{
    pub fn run_server(
        self,
        server_conn: ServerConnector,
    ) -> (
        impl Future<Output = Result<(), ServerError>> + Send,
        ServerHandle,
    ) {
        let (tx, rx) = trigger::trigger();
        let (addr_tx, addr_rx) = oneshot::channel();
        let fut = self.run_inner(rx, addr_tx, server_conn);
        (fut, ServerHandle::new(tx, addr_rx))
    }

    async fn run_inner(
        self,
        stop_signal: trigger::Receiver,
        addr_tx: oneshot::Sender<SocketAddr>,
        mut server_conn: ServerConnector,
    ) -> Result<(), ServerError> {
        let SwimServer {
            plane,
            addr,
            networking,
            websockets,
            config,
            store,
        } = self;

        let networking = Arc::new(networking);
        let websockets = Arc::new(websockets);

        let mut plane_store = store.open_plane(plane.name.as_str())?;

        let (bound_addr, listener) = networking.bind(addr).await?;
        let _ = addr_tx.send(bound_addr);
        let mut remote_issuer = IdIssuer::new(IdKind::Remote);

        let (find_tx, mut find_rx) = mpsc::channel(config.find_route_channel_size.get());
        let mut remote_channels = HashMap::new();

        let mut remote_tasks = FuturesUnordered::new();
        let mut agent_tasks = FuturesUnordered::new();
        let mut connection_tasks = FuturesUnordered::new();
        let mut dl_connection_tasks = FuturesUnordered::new();
        let mut client_tasks = FuturesUnordered::new();

        let mut accept_stream = listener.into_stream().take_until(stop_signal);

        let (agent_stop_tx, agent_stop_rx) = trigger::trigger();
        let mut agent_stop = Some(agent_stop_tx);
        let (remote_stop_tx, remote_stop_rx) = trigger::trigger();
        let mut remote_stop = Some(remote_stop_tx);

        let mut agents = Agents::new(
            Routes::new(plane.routes),
            CombinedAgentConfig {
                agent_config: config.agent,
                runtime_config: config.agent_runtime,
            },
            agent_stop_rx,
            server_conn.dl_requests(),
        );

        let mut state = TaskState::Running;

        loop {
            let event = match state {
                TaskState::Running => {
                    tokio::select! {
                        biased;
                        Some((addr, result)) = remote_tasks.next(), if !remote_tasks.is_empty() => ServerEvent::RemoteStopped(addr, result),
                        Some((id, result)) = agent_tasks.next(), if !agent_tasks.is_empty() => ServerEvent::AgentStopped(id, result),
                        Some(reason) = connection_tasks.next(), if !connection_tasks.is_empty() => ServerEvent::ConnectionStopped(reason),
                        Some(reason) = dl_connection_tasks.next(), if !dl_connection_tasks.is_empty() => ServerEvent::ConnectionStopped(reason),
                        Some(event) = client_tasks.next(), if !client_tasks.is_empty() => event,
                        maybe_result = accept_stream.next() => {
                            if let Some(result) = maybe_result {
                                ServerEvent::NewConnection(result)
                            } else {
                                server_conn.stop();
                                state = TaskState::StoppingDownlinks;
                                continue;
                            }
                        },
                        Some(find_route) = find_rx.recv() => ServerEvent::FindRoute(find_route),
                        maybe_dl_event = server_conn.next_message() => {
                            match maybe_dl_event {
                                Some(DlTaskRequest::Registration(reg)) => ServerEvent::RemoteClientRequest(reg),
                                Some(DlTaskRequest::Local(local)) => ServerEvent::LocalClient(local),
                                _ => {
                                    //The downlink task has failed unexpectedly so go straight to stopping agents.
                                    server_conn.stop();
                                    if let Some(stop) = agent_stop.take() {
                                        stop.trigger();
                                    }
                                    state = TaskState::StoppingAgents;
                                    continue;
                                }
                            }
                        }
                        else => continue,
                    }
                }
                TaskState::StoppingDownlinks => {
                    tokio::select! {
                        biased;
                        maybe_dl_event = server_conn.next_message() => {
                            if maybe_dl_event.is_none() {
                                if let Some(stop) = agent_stop.take() {
                                    stop.trigger();
                                }
                                state = TaskState::StoppingAgents;
                            }
                            continue;
                        },
                        Some((id, result)) = remote_tasks.next(), if !remote_tasks.is_empty() => ServerEvent::RemoteStopped(id, result),
                        Some((id, result)) = agent_tasks.next(), if !agent_tasks.is_empty() => ServerEvent::AgentStopped(id, result),
                        Some(reason) = connection_tasks.next(), if !connection_tasks.is_empty() => ServerEvent::ConnectionStopped(reason),
                        Some(reason) = dl_connection_tasks.next(), if !dl_connection_tasks.is_empty() => ServerEvent::ConnectionStopped(reason),
                        Some(find_route) = find_rx.recv() => ServerEvent::FindRoute(find_route),
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
                        Some(reason) = dl_connection_tasks.next(), if !dl_connection_tasks.is_empty() => ServerEvent::ConnectionStopped(reason),
                        Some(find_route) = find_rx.recv() => ServerEvent::FailRoute(find_route),
                        else => continue,
                    }
                }
            };

            match event {
                ServerEvent::NewConnection(Ok((sock, addr))) => {
                    info!(peer = %addr, "Accepting new client connection.");
                    match websockets.accept_connection(sock).await {
                        Ok(websocket) => {
                            let sock_addr = addr.addr;
                            let id = remote_issuer.next_id();
                            let (attach_tx, task) = register_remote(
                                id,
                                sock_addr,
                                remote_stop_rx.clone(),
                                &config,
                                websocket,
                                find_tx.clone(),
                            );
                            remote_channels.insert(sock_addr, attach_tx);
                            remote_tasks.push(task);
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
                    agents.agent_channels.remove(&route);
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
                        connected_id,
                        agent_id,
                        reason,
                    } = reason;
                    match &reason {
                        DisconnectionReason::DuplicateRegistration(_) => {
                            error!(conected_id = %connected_id, agent_id = %agent_id, "Multiple connections attempted between a remote and an agent.");
                        }
                        _ => {
                            info!(conected_id = %connected_id, agent_id = %agent_id, reason = %reason, "A connection between and agent and a remote stopped.");
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
                    let node_store = plane_store.node_store(node.as_str())?;
                    let agent_tasks_ref = &agent_tasks;
                    let result = agents.resolve_agent(node, move |name, route_task| {
                        let task = route_task.run_agent_with_store(node_store);
                        agent_tasks_ref.push(attach_node(name, task));
                    });
                    match result {
                        Ok((agent_id, agent_tx)) => {
                            let connect_task = attach_agent(
                                source,
                                agent_id,
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
                ServerEvent::RemoteClientRequest(ClientRegistration {
                    host,
                    sock_addrs,
                    responder,
                }) => {
                    if let Some((sock_addr, attach_tx)) = sock_addrs
                        .iter()
                        .find_map(|addr| remote_channels.get(addr).map(|tx| (*addr, tx.clone())))
                    {
                        if responder
                            .send(Ok(EstablishedClient::new(attach_tx, sock_addr)))
                            .is_err()
                        {
                            info!("Request for client connection dropped before it was completed.");
                        }
                    } else {
                        let net = networking.clone();
                        let ws = websockets.clone();
                        client_tasks.push(async move {
                            let result = open_client(host, sock_addrs, net, ws).await;
                            ServerEvent::NewClient(result, responder)
                        });
                    }
                }
                ServerEvent::NewClient(Ok((sock_addr, websocket)), responder) => {
                    let id = remote_issuer.next_id();
                    let (attach_tx, task) = register_remote(
                        id,
                        sock_addr,
                        remote_stop_rx.clone(),
                        &config,
                        websocket,
                        find_tx.clone(),
                    );
                    remote_channels.insert(sock_addr, attach_tx.clone());
                    remote_tasks.push(task);
                    if responder
                        .send(Ok(EstablishedClient::new(attach_tx, sock_addr)))
                        .is_err()
                    {
                        info!("Request for client connection dropped before it was completed.");
                    }
                }
                ServerEvent::NewClient(Err(e), responder) => {
                    if responder.send(Err(e)).is_err() {
                        info!(
                            "Request for client connection dropped before it failed to complete."
                        );
                    }
                }
                ServerEvent::LocalClient(AttachClient::AttachDownlink {
                    downlink_id,
                    path,
                    sender,
                    receiver,
                    done,
                }) => {
                    let RelativeAddress { node, .. } = path;
                    info!(source = %downlink_id, node = %node, "Attempting to connect a downlink to an agent.");
                    let node_store = plane_store.node_store(node.as_str())?;
                    let result = agents.resolve_agent(node, |name, route_task| {
                        let task = route_task.run_agent_with_store(node_store);
                        agent_tasks.push(attach_node(name, task));
                    });
                    match result {
                        Ok((agent_id, agent_tx)) => {
                            let task = attach_downlink(
                                downlink_id,
                                agent_id,
                                agent_tx.clone(),
                                (sender, receiver),
                                config.attachment_timeout,
                                done,
                            );
                            dl_connection_tasks.push(task);
                        }
                        Err(node) => {
                            warn!(node = %node, "Requested agent does not exist.");
                            if done.send(Err(DownlinkFailureReason::Unresolvable)).is_err() {
                                info!(node = %node, "Downlink request dropped before it was satisfied.");
                            }
                        }
                    }
                }
                ServerEvent::LocalClient(_) => {
                    todo!("Intra-plane commands not yet supported.")
                }
            }
        }

        Ok(())
    }
}

async fn attach_node<F>(
    node: Text,
    task: F,
) -> (Text, Result<Result<(), AgentExecError>, JoinError>)
where
    F: Future<Output = Result<(), AgentExecError>> + Send + 'static,
{
    let result = tokio::spawn(task).await;
    (node, result)
}

fn register_remote<S, E>(
    id: Uuid,
    sock_addr: SocketAddr,
    stop: trigger::Receiver,
    config: &SwimServerConfig,
    websocket: WebSocket<S, E>,
    find_tx: mpsc::Sender<FindNode>,
) -> (
    mpsc::Sender<AttachClient>,
    impl Future<Output = (SocketAddr, Result<(), JoinError>)>,
)
where
    S: WebSocketStream + Send,
    E: SplittableExtension + Send + 'static,
{
    let (attach_tx, attach_rx) = mpsc::channel(config.client_attachment_buffer_size.get());

    let task = RemoteTask::new(
        id,
        stop,
        websocket,
        attach_rx,
        find_tx,
        config.remote.registration_buffer_size,
    );

    (
        attach_tx,
        with_sock_addr(sock_addr, tokio::spawn(task.run())),
    )
}

struct Agents {
    plane_issuer: IdIssuer,
    agent_channels: HashMap<Text, (Uuid, mpsc::Sender<AgentAttachmentRequest>)>,
    routes: Routes,
    config: CombinedAgentConfig,
    agent_stop_rx: trigger::Receiver,
    open_dl_tx: mpsc::Sender<DownlinkRequest>,
}

impl Agents {
    fn new(
        routes: Routes,
        config: CombinedAgentConfig,
        agent_stop_rx: trigger::Receiver,
        open_dl_tx: mpsc::Sender<DownlinkRequest>,
    ) -> Self {
        Agents {
            plane_issuer: IdIssuer::new(IdKind::Plane),
            agent_channels: Default::default(),
            routes,
            config,
            agent_stop_rx,
            open_dl_tx,
        }
    }

    fn resolve_agent<'a, F>(
        &'a mut self,
        node: Text,
        spawn_task: F,
    ) -> Result<(Uuid, &'a mut mpsc::Sender<AgentAttachmentRequest>), Text>
    where
        F: for<'b> FnOnce(Text, AgentRouteTask<'b, BoxAgent>),
    {
        let Agents {
            plane_issuer,
            agent_channels,
            routes,
            config,
            agent_stop_rx,
            open_dl_tx,
        } = self;
        match agent_channels.entry(node) {
            Entry::Occupied(entry) => {
                debug!("Agent already running.");
                let (id, tx) = entry.into_mut();
                Ok((*id, tx))
            }
            Entry::Vacant(entry) => {
                debug!("Attempting to start new agent instance.");
                if let Some((route, agent)) = RelativeUri::from_str(entry.key().as_str())
                    .ok()
                    .and_then(|route| routes.find_route(&route).map(move |agent| (route, agent)))
                {
                    let id = plane_issuer.next_id();
                    let (attachment_tx, attachment_rx) =
                        mpsc::channel(config.runtime_config.attachment_queue_size.get());
                    let route_task = AgentRouteTask::new(
                        agent,
                        AgentRoute {
                            identity: id,
                            route,
                        },
                        attachment_rx,
                        open_dl_tx.clone(),
                        agent_stop_rx.clone(),
                        *config,
                    );
                    let name = entry.key().clone();
                    spawn_task(name, route_task);
                    let (id, tx) = entry.insert((id, attachment_tx));
                    Ok((*id, tx))
                } else {
                    Err(entry.into_key())
                }
            }
        }
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
    connected_id: Uuid,
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
        connected_id: remote_id,
        agent_id,
        reason,
    }
}

async fn attach_downlink(
    downlink_id: Uuid,
    agent_id: Uuid,
    tx: mpsc::Sender<AgentAttachmentRequest>,
    io: (ByteWriter, ByteReader),
    connect_timeout: Duration,
    done: oneshot::Sender<Result<(), DownlinkFailureReason>>,
) -> ConnectionTerminated {
    let (disconnect_tx, disconnect_rx) = promise::promise();
    let (connected_tx, connected_rx) = trigger::trigger();
    let req =
        AgentAttachmentRequest::with_confirmation(downlink_id, io, disconnect_tx, connected_tx);
    let reason = match tokio::time::timeout(connect_timeout, async move {
        tx.send(req).await.is_ok() && connected_rx.await.is_ok()
    })
    .await
    {
        Ok(_) => {
            if done.send(Ok(())).is_err() {
                info!("Downlink request dropped before satisfied.");
                DisconnectionReason::Failed
            } else if let Ok(reason) = disconnect_rx.await {
                *reason
            } else {
                DisconnectionReason::Failed
            }
        }
        _ => DisconnectionReason::Failed,
    };
    ConnectionTerminated {
        connected_id: downlink_id,
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

impl From<NewClientError> for DownlinkRuntimeError {
    fn from(err: NewClientError) -> Self {
        match err {
            NewClientError::InvalidUrl(_) | NewClientError::BadWarpUrl(_) => {
                DownlinkRuntimeError::DownlinkConnectionFailed(DownlinkFailureReason::Unresolvable)
            }
            NewClientError::OpeningSocketFailed { .. } => {
                DownlinkRuntimeError::DownlinkConnectionFailed(
                    DownlinkFailureReason::ConnectionFailed,
                )
            }
            NewClientError::WsNegotationFailed { .. } => {
                DownlinkRuntimeError::DownlinkConnectionFailed(
                    DownlinkFailureReason::WebsocketNegotiationFailed,
                )
            }
            NewClientError::ServerStopped => {
                DownlinkRuntimeError::RuntimeError(AgentRuntimeError::Stopping)
            }
        }
    }
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
