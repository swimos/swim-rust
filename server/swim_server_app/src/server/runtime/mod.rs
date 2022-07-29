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

use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use ratchet::WebSocketStream;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::time::Duration;
use swim_api::agent::BoxAgent;
use swim_model::Text;
use swim_remote::{FindNode, LaneNotFound, RemoteTask};
use swim_runtime::agent::{run_agent, AgentAttachmentRequest, AgentExecError, DisconnectionReason};
use swim_runtime::error::ConnectionError;
use swim_runtime::remote::ExternalConnections;
use swim_runtime::remote::Listener;
use swim_runtime::routing::RoutingAddr;
use swim_runtime::ws::WsConnections;
use swim_utilities::io::byte_channel::{byte_channel, ByteReader, ByteWriter};
use swim_utilities::routing::route_pattern::RoutePattern;
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::trigger::{self, promise};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinError;
use tracing::{debug, error, info, info_span, warn};
use tracing_futures::Instrument;
use uuid::Uuid;

use crate::config::SwimServerConfig;
use crate::plane::PlaneModel;
use crate::server::ServerHandle;

use super::Server;

pub struct SwimServer<Net, Ws> {
    plane: PlaneModel,
    addr: SocketAddr,
    networking: Net,
    websockets: Ws,
    config: SwimServerConfig,
}

enum ServerEvent<Sock> {
    NewConnection(Result<Sock, std::io::Error>),
    FindRoute(FindNode),
    RemoteStopped(Uuid, Result<(), JoinError>),
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
        let fut = self.run_inner(rx);
        (fut, ServerHandle::new(tx))
    }

    async fn run_inner(self, stop_signal: trigger::Receiver) -> Result<(), std::io::Error> {
        let SwimServer {
            plane,
            addr,
            networking,
            websockets,
            config,
        } = self;
        let listener = networking.bind(addr).await?;

        let mut rem_count = 0u32;
        let mut agent_count = 0u32;
        let mut make_remote_id = move || {
            let count = rem_count;
            rem_count += 1;
            RoutingAddr::remote(count)
        };
        let mut make_agent_id = move || {
            let count = agent_count;
            agent_count += 1;
            RoutingAddr::plane(count)
        };

        let (find_tx, mut find_rx) = mpsc::channel(config.find_route_buffer_size.get());
        let mut remote_channels = HashMap::new();
        let mut agent_channels = HashMap::new();

        let mut remote_tasks = FuturesUnordered::new();
        let mut agent_tasks = FuturesUnordered::new();
        let mut connection_tasks = FuturesUnordered::new();

        let mut accept_stream = listener.into_stream().take_until(stop_signal.clone());

        let routes = Routes::new(plane.routes);

        loop {
            let event = tokio::select! {
                biased;
                Some((id, result)) = remote_tasks.next(), if !remote_tasks.is_empty() => ServerEvent::RemoteStopped(id, result),
                Some((id, result)) = agent_tasks.next(), if !agent_tasks.is_empty() => ServerEvent::AgentStopped(id, result),
                Some(reason) = connection_tasks.next(), if !connection_tasks.is_empty() => ServerEvent::ConnectionStopped(reason),
                Some(r) = accept_stream.next() => ServerEvent::NewConnection(r),
                Some(find_route) = find_rx.recv() => ServerEvent::FindRoute(find_route),

                else => break,
            };

            match event {
                ServerEvent::NewConnection(Ok((sock, addr))) => {
                    info!(peer = %addr, "Accepting new client connection.");
                    match websockets.accept_connection(sock).await {
                        Ok(websocket) => {
                            let id = *make_remote_id().uuid();
                            let (attach_tx, attach_rx) =
                                mpsc::channel(config.client_attachment_buffer_size.get());
                            remote_channels.insert(id, attach_tx);
                            let task = RemoteTask::new(
                                id,
                                stop_signal.clone(),
                                websocket,
                                attach_rx,
                                find_tx.clone(),
                                config.remote.registration_buffer_size,
                            );

                            remote_tasks.push(tokio::spawn(task.run()).map(move |r| (id, r)));
                        }
                        Err(error) => {
                            warn!(error = %{Into::<ConnectionError>::into(error)}, "Negotiating incoming websocket connection failed.");
                        }
                    }
                }
                ServerEvent::NewConnection(Err(error)) => {
                    warn!(error = %error, "Accepting incomingn connection failed.");
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
                                let id = make_agent_id();
                                let (attachment_tx, attachment_rx) = mpsc::channel(1);
                                let agent_task = run_agent(
                                    agent,
                                    id,
                                    route,
                                    attachment_rx,
                                    stop_signal.clone(),
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
                                *agent_id.uuid(),
                                agent_tx.clone(),
                                config.agent_runtime_buffer_size,
                                config.attachment_timeout,
                                provider,
                            ).instrument(info_span!("Remote to agent connection task.", remote_id = %source, agent_id = %{agent_id.uuid()}));
                            connection_tasks.push(connect_task);
                        }
                        Err(node) => {
                            debug!(node = %node, "Requested agent does not exist.");
                            if let Err(Err(LaneNotFound::NoSuchAgent { node, .. })) =
                                provider.send(Err(LaneNotFound::NoSuchAgent { node, lane }))
                            {
                                debug!(source = %source, route = %node, "A remote stopped while a connection from it to an agent was pending.");
                            }
                        }
                    }
                }
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

async fn attach_agent(
    remote_id: Uuid,
    agent_id: Uuid,
    tx: mpsc::Sender<AgentAttachmentRequest>,
    buffer_size: NonZeroUsize,
    connect_timeout: Duration,
    provider: oneshot::Sender<Result<(ByteWriter, ByteReader), LaneNotFound>>,
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
                    (*reason).clone()
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
