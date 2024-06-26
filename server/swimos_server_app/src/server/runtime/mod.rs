// Copyright 2015-2024 Swim Inc.
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

use futures::future::{join, Either};
use futures::stream::{unfold, FuturesUnordered};
use futures::{FutureExt, Stream, StreamExt};
use ratchet::{ExtensionProvider, SplittableExtension, WebSocket, WebSocketStream};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use swimos_api::agent::{Agent, BoxAgent, HttpLaneRequest};
use swimos_api::error::{
    AgentRuntimeError, DownlinkFailureReason, DownlinkRuntimeError, IntrospectionStopped,
};
use swimos_api::persistence::ServerPersistence;
use swimos_api::{address::RelativeAddress, persistence::PlanePersistence};
use swimos_introspection::IntrospectionConfig;
use swimos_introspection::{register_introspection, AgentRegistration, IntrospectionResolver};
use swimos_messages::remote_protocol::{
    AgentResolutionError, AttachClient, FindNode, LinkError, NoSuchAgent, NodeConnectionRequest,
};
use swimos_model::Text;
use swimos_remote::{BadWarpUrl, RemoteTask, Scheme};
use swimos_runtime::agent::{
    AgentAttachmentRequest, AgentExecError, AgentRouteChannels, AgentRouteDescriptor,
    AgentRouteTask, CombinedAgentConfig, DisconnectionReason, LinkRequest,
};
use swimos_utilities::routing::RouteUri;

use swimos_remote::websocket::{RatchetError, Websockets};
use swimos_remote::{ConnectionError, ExternalConnections, ListenerError};
use swimos_utilities::byte_channel::{byte_channel, BudgetedFutureExt, ByteReader, ByteWriter};
use swimos_utilities::routing::RoutePattern;
use swimos_utilities::trigger::{self, promise};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinError;
use tracing::{debug, error, info, info_span, warn, Instrument};
use uuid::Uuid;

use crate::config::SwimServerConfig;
use crate::plane::PlaneModel;
use crate::server::runtime::downlinks::DlTaskRequest;
use crate::server::ServerHandle;
use crate::Io;

use self::downlinks::{DownlinkConnectionTask, ServerConnector};
use self::ids::{IdIssuer, IdKind};

use super::error::UnresolvableRoute;
use super::{Server, ServerError};

mod downlinks;
mod ids;
#[cfg(test)]
mod tests;

/// A swimos server task that listens for incoming connections on a socket and runs the
/// agents specified in a [`PlaneModel`].
pub struct SwimServer<Net, Ws, Provider, Store> {
    plane: PlaneModel,
    addr: SocketAddr,
    networking: Net,
    websockets: Ws,
    ext_provider: Provider,
    config: SwimServerConfig,
    store: Store,
    introspection: Option<IntrospectionConfig>,
}

pub struct Transport<Net, Ws, Provider> {
    networking: Net,
    websockets: Ws,
    ext_provider: Provider,
}

impl<Net, Ws, Provider> Transport<Net, Ws, Provider> {
    pub fn new(networking: Net, websockets: Ws, ext_provider: Provider) -> Self {
        Transport {
            networking,
            websockets,
            ext_provider,
        }
    }
}

pub struct StartAgentRequest {
    route: RouteUri,
    response: oneshot::Sender<Result<(), UnresolvableRoute>>,
}

impl StartAgentRequest {
    pub fn new(route: RouteUri, response: oneshot::Sender<Result<(), UnresolvableRoute>>) -> Self {
        StartAgentRequest { route, response }
    }
}

type ClientPromiseTx = oneshot::Sender<Result<EstablishedClient, NewClientError>>;
type ClientPromiseRx = oneshot::Receiver<Result<EstablishedClient, NewClientError>>;

enum ServerEvent<Sock, Ext> {
    NewConnection(Result<(WebSocket<Sock, Ext>, SocketAddr), ListenerError>),
    FindRoute(FindNode),
    FailRoute(FindNode),
    RemoteStopped(SocketAddr, Result<(), JoinError>),
    AgentStopped(Text, Result<Result<(), AgentExecError>, JoinError>),
    ConnectionStopped(ConnectionTerminated),
    CmdChannelResult(Result<(), CmdLinkTimeout>),
    RemoteClientRequest(ClientRegistration),
    NewClient(
        Result<(SocketAddr, WebSocket<Sock, Ext>), NewClientError>,
        ClientPromiseTx,
    ),
    LocalClient(AttachClient),
    StartAgent(StartAgentRequest),
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
    /// Scheme to use for connection.
    scheme: Scheme,
    /// Addresses to try to connect to the remote.
    sock_addrs: Vec<SocketAddr>,
    /// Reply channel for the server task.
    responder: ClientPromiseTx,
}

impl ClientRegistration {
    fn new(scheme: Scheme, host: Text, sock_addrs: Vec<SocketAddr>) -> (Self, ClientPromiseRx) {
        let (tx, rx) = oneshot::channel();
        (
            ClientRegistration {
                scheme,
                host,
                sock_addrs,
                responder: tx,
            },
            rx,
        )
    }
}

impl<Net, Ws, Provider, Store> Server for SwimServer<Net, Ws, Provider, Store>
where
    Net: ExternalConnections + Clone,
    Net::Socket: WebSocketStream,
    Ws: Websockets + Send + Sync + 'static,
    Provider: ExtensionProvider + Send + Sync + Clone + Unpin + 'static,
    Provider::Extension: SplittableExtension + Send + Sync + Unpin + 'static,
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
            config.channel_coop_budget,
            config.downlink_runtime,
            self.networking.dns_resolver(),
        );
        let (fut, handle) = self.run_server(server_conn);

        let downlinks_task = downlinks
            .run()
            .instrument(info_span!("Downlink connector task."));
        let combined =
            join(fut.instrument(info_span!("Server task.")), downlinks_task).map(|(r, _)| r);
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

impl<Net, Ws, Provider, Store> SwimServer<Net, Ws, Provider, Store>
where
    Net: ExternalConnections,
    Net::Socket: WebSocketStream,
    Ws: Websockets + Send + Sync,
    Provider: ExtensionProvider + Send + Sync + Clone + Unpin + 'static,
    Provider::Extension: SplittableExtension + Send + Sync + Unpin + 'static,
    Store: ServerPersistence + Send + Sync + 'static,
{
    pub fn new(
        plane: PlaneModel,
        addr: SocketAddr,
        transport: Transport<Net, Ws, Provider>,
        config: SwimServerConfig,
        store: Store,
        introspection: Option<IntrospectionConfig>,
    ) -> Self {
        let Transport {
            networking,
            websockets,
            ext_provider,
        } = transport;
        SwimServer {
            plane,
            addr,
            networking,
            websockets,
            ext_provider,
            config,
            store,
            introspection,
        }
    }
}

fn start_req_stream(
    maybe_rx: Option<mpsc::Receiver<StartAgentRequest>>,
) -> impl Stream<Item = StartAgentRequest> + Send {
    unfold(maybe_rx, |state| async move {
        if let Some(mut rx) = state {
            rx.recv().await.map(move |req| (req, Some(rx)))
        } else {
            None
        }
    })
}

impl<Net, Ws, Provider, Store> SwimServer<Net, Ws, Provider, Store>
where
    Net: ExternalConnections,
    Net::Socket: WebSocketStream + Send,
    Ws: Websockets + Send + Sync,
    Provider: ExtensionProvider + Send + Sync + Clone + Unpin + 'static,
    Provider::Extension: SplittableExtension + Send + Sync + Unpin + 'static,
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
        let (req_tx, req_rx) = mpsc::channel(8);
        let fut = self.run_inner(rx, addr_tx, Some(req_rx), server_conn);
        (fut, ServerHandle::new(tx, addr_rx, req_tx))
    }

    async fn run_inner(
        self,
        stop_signal: trigger::Receiver,
        addr_tx: oneshot::Sender<SocketAddr>,
        start_requests_rx: Option<mpsc::Receiver<StartAgentRequest>>,
        mut server_conn: ServerConnector,
    ) -> Result<(), ServerError> {
        let SwimServer {
            plane,
            addr,
            networking,
            websockets,
            ext_provider,
            config,
            store,
            introspection,
        } = self;

        let networking = Arc::new(networking);
        let websockets = Arc::new(websockets);

        let plane_store = store.open_plane(plane.name.as_str())?;

        let (bound_addr, listener) = networking.bind(addr).await?;
        info!(bound_addr = %bound_addr, "TCP listener bound.");
        let _ = addr_tx.send(bound_addr);
        let mut remote_issuer = IdIssuer::new(IdKind::Remote);

        let (find_tx, mut find_rx) = mpsc::channel(config.find_route_channel_size.get());
        let mut remote_channels = HashMap::new();

        let mut remote_tasks = FuturesUnordered::new();
        let mut agent_tasks = FuturesUnordered::new();
        let mut connection_tasks = FuturesUnordered::new();
        let mut cmd_connection_tasks = FuturesUnordered::new();
        let mut client_tasks = FuturesUnordered::new();

        let mut web_server = websockets
            .wrap_listener(listener, ext_provider.clone(), find_tx.clone())
            .take_until(stop_signal);

        let (agent_stop_tx, agent_stop_rx) = trigger::trigger();
        let mut agent_stop = Some(agent_stop_tx);
        let (remote_stop_tx, remote_stop_rx) = trigger::trigger();
        let mut remote_stop = Some(remote_stop_tx);

        let mut routes = plane.routes.into_iter().collect();

        let mut start_reqs = pin!(start_req_stream(start_requests_rx));

        let introspection_resolver = introspection.map(|intro_config| {
            start_introspection(
                intro_config,
                config.channel_coop_budget,
                remote_stop_rx.clone(),
                &mut routes,
            )
        });

        let mut agents = Agents::new(
            routes,
            CombinedAgentConfig {
                agent_config: config.agent,
                runtime_config: config.agent_runtime,
            },
            agent_stop_rx,
            server_conn.link_requests(),
            introspection_resolver,
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
                        Some(result) = cmd_connection_tasks.next(), if !cmd_connection_tasks.is_empty() => ServerEvent::CmdChannelResult(result),
                        Some(event) = client_tasks.next(), if !client_tasks.is_empty() => event,
                        Some(req) = start_reqs.next() => ServerEvent::StartAgent(req),
                        maybe_result = web_server.next() => {
                            if let Some(result) = maybe_result {
                                ServerEvent::NewConnection(result)
                            } else {
                                info!("Server task moving to stopping downlinks.");
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
                                    info!("Server task moving to stopping agents.");
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
                                info!("Server task moving to stopping agents.");
                                state = TaskState::StoppingAgents;
                            }
                            continue;
                        },
                        Some((id, result)) = remote_tasks.next(), if !remote_tasks.is_empty() => ServerEvent::RemoteStopped(id, result),
                        Some((id, result)) = agent_tasks.next(), if !agent_tasks.is_empty() => ServerEvent::AgentStopped(id, result),
                        Some(reason) = connection_tasks.next(), if !connection_tasks.is_empty() => ServerEvent::ConnectionStopped(reason),
                        Some(result) = cmd_connection_tasks.next(), if !cmd_connection_tasks.is_empty() => ServerEvent::CmdChannelResult(result),
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
                                info!("Server task moving to stopping remotes.");
                                state = TaskState::StoppingRemotes;
                                continue;
                            }
                        },
                        Some(reason) = connection_tasks.next(), if !connection_tasks.is_empty() => ServerEvent::ConnectionStopped(reason),
                        Some(result) = cmd_connection_tasks.next(), if !cmd_connection_tasks.is_empty() => ServerEvent::CmdChannelResult(result),
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
                                info!("Server task is stopped.");
                                break;
                            }
                        },
                        Some(reason) = connection_tasks.next(), if !connection_tasks.is_empty() => ServerEvent::ConnectionStopped(reason),
                        Some(result) = cmd_connection_tasks.next(), if !cmd_connection_tasks.is_empty() => ServerEvent::CmdChannelResult(result),
                        Some(find_route) = find_rx.recv() => ServerEvent::FailRoute(find_route),
                        else => continue,
                    }
                }
            };

            match event {
                ServerEvent::NewConnection(Ok((websocket, sock_addr))) => {
                    info!(peer = %addr, "Accepting new client connection.");
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
                ServerEvent::NewConnection(Err(ListenerError::ListenerFailed(error))) => {
                    error!(error = %error, "Listening for new connections failed.");
                    return Err(ServerError::Networking(ConnectionError::ConnectionFailed(
                        error,
                    )));
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
                    if agents.remove_agent(route.as_str()).is_err() {
                        warn!("Attempted to deregister an agent from metadata reporting but the reporting system had stopped.");
                    }
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
                            error!(connected_id = %connected_id, agent_id = %agent_id, "Multiple connections attempted between a remote and an agent.");
                        }
                        _ => {
                            info!(connected_id = %connected_id, agent_id = %agent_id, reason = %reason, "A connection between an agent and a remote stopped.");
                        }
                    }
                }
                ServerEvent::CmdChannelResult(result) => {
                    if let Err(err) = result {
                        error!(error = ?err, "Connecting a local command channel failed.");
                    }
                }
                ServerEvent::FailRoute(FindNode {
                    node,
                    lane,
                    request,
                }) => {
                    let source = if let NodeConnectionRequest::Warp { source, .. } = &request {
                        Some(*source)
                    } else {
                        None
                    };
                    if request.fail(AgentResolutionError::PlaneStopping).is_err() {
                        debug!(remote_id = ?source, node = %node, lane = ?lane, "Remote stopped with pending agent resolution.");
                    }
                }
                ServerEvent::FindRoute(FindNode {
                    node,
                    lane,
                    request,
                }) => {
                    let node_store_fut = plane_store.node_store(node.as_str());
                    let agent_tasks_ref = &agent_tasks;
                    let result = agents.resolve_agent(node.clone(), move |name, route_task| {
                        let task = route_task.run_agent_with_store(node_store_fut);
                        agent_tasks_ref.push(attach_node(name, config.channel_coop_budget, task));
                    });
                    match result {
                        Ok(AgentChannel {
                            id,
                            attachment_tx,
                            http_tx,
                        }) => match request {
                            NodeConnectionRequest::Warp { promise, source } => {
                                info!(source = %source, node = %node, "Attempting to connect an agent to a remote.");
                                let connect_task = attach_agent(
                                        source,
                                        *id,
                                        attachment_tx.clone(),
                                        config.agent_runtime_buffer_size,
                                        config.attachment_timeout,
                                        promise,
                                    ).instrument(info_span!("Remote to agent connection task.", remote_id = %source, agent_id = %*id));
                                connection_tasks.push(Either::Left(connect_task));
                            }
                            NodeConnectionRequest::Http { promise } => {
                                info!(node = %node, "Attempting to route an HTTP request to an agent.");
                                if promise.send(Ok(http_tx.clone())).is_err() {
                                    debug!("A remote stopped while a HTTP request from it was pending.");
                                }
                            }
                        },
                        Err(node) => {
                            debug!(node = %node, "Requested agent does not exist.");
                            if let Err(AgentResolutionError::NotFound(NoSuchAgent {
                                node, ..
                            })) = request.fail(NoSuchAgent { node, lane }.into())
                            {
                                debug!(route = %node, "A remote stopped while a connection from it to an agent was pending.");
                            }
                        }
                    }
                }
                ServerEvent::RemoteClientRequest(ClientRegistration {
                    host,
                    scheme,
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
                        let provider = ext_provider.clone();
                        client_tasks.push(async move {
                            let result =
                                open_client(scheme, host, sock_addrs, net, ws, provider).await;
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
                    let RelativeAddress { node, .. } = &path;
                    info!(source = %downlink_id, node = %node, "Attempting to connect a downlink to an agent.");
                    let node_store_fut = plane_store.node_store(node.as_str());
                    let result = agents.resolve_agent(node.clone(), |name, route_task| {
                        let task = route_task.run_agent_with_store(node_store_fut);
                        agent_tasks.push(attach_node(name, config.channel_coop_budget, task));
                    });
                    match result {
                        Ok(AgentChannel {
                            id, attachment_tx, ..
                        }) => {
                            let task = attach_link_remote(
                                downlink_id,
                                *id,
                                attachment_tx.clone(),
                                (sender, receiver),
                                config.attachment_timeout,
                                done,
                            );
                            connection_tasks.push(Either::Right(task));
                        }
                        Err(node) => {
                            warn!(node = %node, "Requested agent does not exist.");
                            if done.send(Err(LinkError::NoEndpoint(path))).is_err() {
                                info!(node = %node, "Downlink request dropped before it was satisfied.");
                            }
                        }
                    }
                }
                ServerEvent::LocalClient(AttachClient::OneWay {
                    agent_id,
                    path,
                    receiver,
                    done,
                }) => {
                    if let Some(path) = path {
                        let RelativeAddress { node, .. } = &path;
                        info!(source = %agent_id, node = %node, "Attempting to connect a downlink to an agent.");
                        let node_store_fut = plane_store.node_store(node.as_str());
                        let result = agents.resolve_agent(node.clone(), |name, route_task| {
                            let task = route_task.run_agent_with_store(node_store_fut);
                            agent_tasks.push(attach_node(name, config.channel_coop_budget, task));
                        });
                        match result {
                            Ok(AgentChannel {
                                id, attachment_tx, ..
                            }) => {
                                let task = attach_link_local(
                                    agent_id,
                                    *id,
                                    attachment_tx.clone(),
                                    receiver,
                                    config.attachment_timeout,
                                    done,
                                );
                                cmd_connection_tasks.push(task);
                            }
                            Err(node) => {
                                warn!(node = %node, "Requested agent does not exist.");
                                if done.send(Err(LinkError::NoEndpoint(path))).is_err() {
                                    info!(node = %node, "Command channel request dropped before it was satisfied.");
                                }
                            }
                        }
                    }
                }
                ServerEvent::StartAgent(StartAgentRequest { route, response }) => {
                    info!(route = %route, "Attempting to start an agent instance.");
                    let node = Text::new(route.as_str());
                    let node_store_fut = plane_store.node_store(node.as_str());
                    let agent_tasks_ref = &agent_tasks;
                    let result = agents.resolve_agent(node, move |name, route_task| {
                        let task = route_task.run_agent_with_store(node_store_fut);
                        agent_tasks_ref.push(attach_node(name, config.channel_coop_budget, task));
                    });
                    let resp_result = if result.is_ok() {
                        Ok(())
                    } else {
                        Err(UnresolvableRoute::new(route))
                    };
                    if response.send(resp_result).is_err() {
                        info!("Agent start request dropped before it was satisfied.");
                    }
                }
            }
        }

        Ok(())
    }
}

async fn attach_node<F>(
    node: Text,
    budget: Option<NonZeroUsize>,
    task: F,
) -> (Text, Result<Result<(), AgentExecError>, JoinError>)
where
    F: Future<Output = Result<(), AgentExecError>> + Send + 'static,
{
    let result = tokio::spawn(task.with_budget_or_default(budget)).await;
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
        Some(find_tx),
        config.remote.registration_buffer_size,
        config.remote.close_timeout,
    );

    (
        attach_tx,
        with_sock_addr(
            sock_addr,
            tokio::spawn(
                task.run()
                    .with_budget_or_default(config.channel_coop_budget),
            ),
        ),
    )
}

struct AgentChannel {
    id: Uuid,
    attachment_tx: mpsc::Sender<AgentAttachmentRequest>,
    http_tx: mpsc::Sender<HttpLaneRequest>,
}

struct Agents {
    plane_issuer: IdIssuer,
    agent_channels: HashMap<Text, AgentChannel>,
    routes: Routes,
    config: CombinedAgentConfig,
    agent_stop_rx: trigger::Receiver,
    open_link_tx: mpsc::Sender<LinkRequest>,
    introspection_resolver: Option<IntrospectionResolver>,
}

impl Agents {
    fn new(
        routes: Routes,
        config: CombinedAgentConfig,
        agent_stop_rx: trigger::Receiver,
        open_link_tx: mpsc::Sender<LinkRequest>,
        introspection_resolver: Option<IntrospectionResolver>,
    ) -> Self {
        Agents {
            plane_issuer: IdIssuer::new(IdKind::Plane),
            agent_channels: Default::default(),
            routes,
            config,
            agent_stop_rx,
            open_link_tx,
            introspection_resolver,
        }
    }

    fn resolve_agent<'a, F>(
        &'a mut self,
        node: Text,
        spawn_task: F,
    ) -> Result<&'a AgentChannel, Text>
    where
        F: for<'b> FnOnce(Text, AgentRouteTask<'b, BoxAgent>),
    {
        let Agents {
            plane_issuer,
            agent_channels,
            routes,
            config,
            agent_stop_rx,
            open_link_tx,
            introspection_resolver,
        } = self;
        match agent_channels.entry(node) {
            Entry::Occupied(entry) => {
                debug!("Agent already running.");
                let channel = entry.into_mut();
                Ok(channel)
            }
            Entry::Vacant(entry) => {
                debug!("Attempting to start new agent instance.");
                if let Some((route_uri, (route, route_params))) =
                    RouteUri::from_str(entry.key().as_str())
                        .ok()
                        .and_then(|route_uri| {
                            routes
                                .find_route(&route_uri)
                                .map(move |route| (route_uri, route))
                        })
                {
                    let id = plane_issuer.next_id();
                    let (attachment_tx, attachment_rx) =
                        mpsc::channel(config.runtime_config.attachment_queue_size.get());
                    let (http_tx, http_rx) =
                        mpsc::channel(config.runtime_config.agent_http_request_channel_size.get());

                    let Route {
                        agent,
                        disable_introspection,
                        ..
                    } = route;
                    let name = entry.key().clone();

                    let node_reporting = if *disable_introspection {
                        None
                    } else if let Some(resolver) = introspection_resolver {
                        match resolver.register_agent(id, route_uri.clone(), name.clone()) {
                            Ok(reporting) => Some(reporting),
                            Err(_) => {
                                *introspection_resolver = None;
                                None
                            }
                        }
                    } else {
                        None
                    };

                    let route_task = AgentRouteTask::new(
                        agent,
                        AgentRouteDescriptor {
                            identity: id,
                            route: route_uri,
                            route_params,
                        },
                        AgentRouteChannels::new(attachment_rx, http_rx, open_link_tx.clone()),
                        agent_stop_rx.clone(),
                        *config,
                        node_reporting,
                    );
                    spawn_task(name, route_task);
                    let channel = entry.insert(AgentChannel {
                        id,
                        attachment_tx,
                        http_tx,
                    });
                    Ok(channel)
                } else {
                    Err(entry.into_key())
                }
            }
        }
    }

    fn remove_agent(&mut self, route: &str) -> Result<(), IntrospectionStopped> {
        let Agents {
            agent_channels,
            introspection_resolver,
            ..
        } = self;

        if let Some(AgentChannel { id, .. }) = agent_channels.remove(route) {
            if let Some(resolver) = introspection_resolver {
                resolver.close_agent(id)?;
            }
        }
        Ok(())
    }
}

#[derive(Default)]
struct Routes(Vec<Route>);

struct Route {
    pattern: RoutePattern,
    agent: BoxAgent,
    disable_introspection: bool,
}

impl Route {
    fn new(pattern: RoutePattern, agent: BoxAgent, disable_introspection: bool) -> Self {
        Route {
            pattern,
            agent,
            disable_introspection,
        }
    }
}

impl FromIterator<(RoutePattern, BoxAgent)> for Routes {
    fn from_iter<T: IntoIterator<Item = (RoutePattern, BoxAgent)>>(iter: T) -> Self {
        Routes(
            iter.into_iter()
                .map(|(pattern, agent)| Route::new(pattern, agent, false))
                .collect(),
        )
    }
}

impl Routes {
    fn append<A>(&mut self, route_pattern: RoutePattern, agent: A)
    where
        A: Agent + Send + 'static,
    {
        let Routes(routes) = self;
        routes.push(Route::new(route_pattern, Box::new(agent), false));
    }

    fn find_route<'a>(&'a self, node: &RouteUri) -> Option<(&'a Route, HashMap<String, String>)> {
        let Routes(routes) = self;
        routes.iter().find_map(|route| {
            route
                .pattern
                .unapply_route_uri(node)
                .ok()
                .map(move |route_params| (route, route_params))
        })
    }
}

impl AgentRegistration for Routes {
    fn register<A: Agent + Send + 'static>(&mut self, pattern: RoutePattern, agent: A) {
        self.append(pattern, agent)
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
                    reason
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

async fn attach_link_remote(
    downlink_id: Uuid,
    agent_id: Uuid,
    tx: mpsc::Sender<AgentAttachmentRequest>,
    io: Io,
    connect_timeout: Duration,
    done: oneshot::Sender<Result<(), LinkError>>,
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
                reason
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

#[derive(Debug, Clone, Copy, Error)]
#[error("Connecting command channel from {source_agent} to {target_agent} timed out.")]
struct CmdLinkTimeout {
    source_agent: Uuid,
    target_agent: Uuid,
}

async fn attach_link_local(
    source_agent_id: Uuid,
    agent_id: Uuid,
    tx: mpsc::Sender<AgentAttachmentRequest>,
    io: ByteReader,
    connect_timeout: Duration,
    done: oneshot::Sender<Result<(), LinkError>>,
) -> Result<(), CmdLinkTimeout> {
    let (connected_tx, connected_rx) = trigger::trigger();
    let req = AgentAttachmentRequest::commander(source_agent_id, io, connected_tx);
    match tokio::time::timeout(connect_timeout, async move {
        tx.send(req).await.is_ok() && connected_rx.await.is_ok()
    })
    .await
    {
        Ok(_) => {
            if done.send(Ok(())).is_err() {
                debug!("Downlink request dropped before satisfied.");
                Err(CmdLinkTimeout {
                    source_agent: source_agent_id,
                    target_agent: agent_id,
                })
            } else {
                Ok(())
            }
        }
        _ => Err(CmdLinkTimeout {
            source_agent: source_agent_id,
            target_agent: agent_id,
        }),
    }
}

#[derive(Debug, Error)]
enum NewClientError {
    #[error("Invalid host URL.")]
    InvalidUrl(#[from] url::ParseError),
    #[error("URL {0} is not valid warp address.")]
    BadWarpUrl(#[from] BadWarpUrl),
    #[error("Failed to open a remote connection.")]
    OpeningSocketFailed {
        errors: Vec<(SocketAddr, ConnectionError)>,
    },
    #[error("Failed to negotiate a websocket connection.")]
    WsNegotationFailed {
        #[from]
        #[source]
        error: RatchetError,
    },
    #[error("The server task stopped unexpectedly.")]
    ServerStopped,
}

impl From<NewClientError> for DownlinkRuntimeError {
    fn from(err: NewClientError) -> Self {
        match err {
            NewClientError::InvalidUrl(_) | NewClientError::BadWarpUrl(_) => {
                DownlinkRuntimeError::DownlinkConnectionFailed(DownlinkFailureReason::InvalidUrl)
            }
            NewClientError::OpeningSocketFailed { mut errors } => {
                let err = errors.pop().map(|(_, e)| e).unwrap_or_else(|| {
                    let e = std::io::Error::from(std::io::ErrorKind::Other);
                    ConnectionError::ConnectionFailed(e)
                });
                let reason = match err {
                    ConnectionError::ConnectionFailed(err) => {
                        DownlinkFailureReason::ConnectionFailed(Arc::new(err))
                    }
                    ConnectionError::NegotiationFailed(err) => {
                        DownlinkFailureReason::TlsConnectionFailed {
                            message: err.to_string(),
                            recoverable: true,
                        }
                    }
                    ConnectionError::BadParameter(err) => {
                        DownlinkFailureReason::TlsConnectionFailed {
                            message: err.to_string(),
                            recoverable: false,
                        }
                    }
                };
                DownlinkRuntimeError::DownlinkConnectionFailed(reason)
            }
            NewClientError::WsNegotationFailed { error } => {
                DownlinkRuntimeError::DownlinkConnectionFailed(
                    DownlinkFailureReason::WebsocketNegotiationFailed(error.to_string()),
                )
            }
            NewClientError::ServerStopped => {
                DownlinkRuntimeError::RuntimeError(AgentRuntimeError::Stopping)
            }
        }
    }
}

async fn open_client<Net, Ws, Provider>(
    scheme: Scheme,
    host: Text,
    addrs: Vec<SocketAddr>,
    networking: Arc<Net>,
    websockets: Arc<Ws>,
    provider: Provider,
) -> Result<(SocketAddr, WebSocket<Net::Socket, Provider::Extension>), NewClientError>
where
    Net: ExternalConnections,
    Net::Socket: WebSocketStream,
    Ws: Websockets + Send + Sync,
    Provider: ExtensionProvider + Send + Sync + Clone + Unpin + 'static,
    Provider::Extension: SplittableExtension + Send + Sync + Unpin + 'static,
{
    let mut conn_failures = vec![];
    let mut sock = None;
    for addr in addrs {
        match networking.try_open(scheme, Some(host.as_str()), addr).await {
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
        .open_connection(socket, &provider, host.to_string())
        .await
        .map(move |ws| (addr, ws))
        .map_err(|e| NewClientError::WsNegotationFailed { error: e })
}

fn start_introspection(
    config: IntrospectionConfig,
    coop_budget: Option<NonZeroUsize>,
    stopping: trigger::Receiver,
    routes: &mut Routes,
) -> IntrospectionResolver {
    let (resolver, task) = register_introspection(stopping, config, routes);
    tokio::spawn(task.with_budget_or_default(coop_budget));
    resolver
}
