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

use std::{collections::HashMap, num::NonZeroUsize};

use futures::StreamExt;
use futures::{stream::select, Future};
use swim_api::error::introspection::{
    IntrospectionStopped, LaneIntrospectionError, NodeIntrospectionError,
};
use swim_api::meta::lane::LaneKind;
use swim_model::Text;
use swim_runtime::agent::{
    reporting::{UplinkReportReader, UplinkReporter},
    NodeReporting, UplinkReporterRegistration,
};
use swim_utilities::{routing::route_uri::RouteUri, trigger};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tracing::info;
use uuid::Uuid;

use crate::model::{AgentIntrospectionHandle, AgentIntrospectionUpdater, LaneView};

/// Requests that can be made by to the introspection task.
pub enum IntrospectionMessage {
    // Register a new agent instance.
    AddAgent {
        agent_id: Uuid,
        node_uri: Text,
        aggregate_reader: UplinkReportReader,
    },
    // Register a lane for an already existing agent instance.
    AddLane {
        agent_id: Uuid,
        lane_name: Text,
        kind: LaneKind,
        reader: UplinkReportReader,
    },
    // Indicate that an agent has stopped and can be removed.
    AgentClosed {
        agent_id: Uuid,
    },
    // Try get an introspection handle for a running agent instance.
    IntrospectAgent {
        node_uri: Text,
        responder: oneshot::Sender<Option<AgentIntrospectionHandle>>,
    },
    // Try to get an introspection view for a lane on a running agent instance.
    IntrospectLane {
        node_uri: Text,
        lane_name: Text,
        responder: oneshot::Sender<Result<LaneView, LaneIntrospectionError>>,
    },
}

impl From<UplinkReporterRegistration> for IntrospectionMessage {
    fn from(reg: UplinkReporterRegistration) -> Self {
        let UplinkReporterRegistration {
            agent_id,
            lane_name,
            kind,
            reader,
        } = reg;
        IntrospectionMessage::AddLane {
            agent_id,
            lane_name,
            kind,
            reader,
        }
    }
}

/// Create an additional task to run within a Swim server that maintains a registry of [`AgentIntrospectionUpdater`]s
/// for all running agents. When a new introspection meta-agent starts it will make a request to this registry
/// to obtain an introspecton handle for an agent or lane.
///
/// Returns the task and a resolver used to interact with it externally.
///
/// #Arguments
/// * `stopping` - Signal that the server is stopping.
/// * `channel_size` - Size of the channel use to register new lanes.
pub fn init_introspection(
    stopping: trigger::Receiver,
    channel_size: NonZeroUsize,
) -> (
    IntrospectionResolver,
    impl Future<Output = ()> + Send + 'static,
) {
    let (msg_tx, msg_rx) = mpsc::unbounded_channel();
    let (reg_tx, reg_rx) = mpsc::channel(channel_size.get());
    let task = introspection_task(stopping, msg_rx, reg_rx);
    let resolver = IntrospectionResolver::new(msg_tx, reg_tx);
    (resolver, task)
}

pub async fn introspection_task(
    stopping: trigger::Receiver,
    messages: mpsc::UnboundedReceiver<IntrospectionMessage>,
    registrations: mpsc::Receiver<UplinkReporterRegistration>,
) {
    let msg_stream = UnboundedReceiverStream::new(messages);
    let reg_stream = ReceiverStream::new(registrations).map(IntrospectionMessage::from);

    let mut stream = select(msg_stream, reg_stream).take_until(stopping);

    let mut name_map: HashMap<Uuid, Text> = HashMap::new();
    let mut agents: HashMap<Text, AgentIntrospectionUpdater> = HashMap::new();

    while let Some(message) = stream.next().await {
        match message {
            IntrospectionMessage::AddAgent {
                agent_id,
                node_uri,
                aggregate_reader,
            } => {
                name_map.insert(agent_id, node_uri.clone());
                let updater = AgentIntrospectionUpdater::new(aggregate_reader);
                agents.insert(node_uri, updater);
            }
            IntrospectionMessage::AddLane {
                agent_id,
                lane_name,
                kind,
                reader,
            } => {
                if let Some(updater) = name_map.get(&agent_id).and_then(|name| agents.get(name)) {
                    updater.add_lane(lane_name, kind, reader);
                }
            }
            IntrospectionMessage::AgentClosed { agent_id } => {
                name_map
                    .remove(&agent_id)
                    .and_then(|name| agents.remove(&name));
            }
            IntrospectionMessage::IntrospectAgent {
                node_uri,
                responder,
            } => {
                let handle = agents
                    .get(&node_uri)
                    .map(AgentIntrospectionUpdater::make_handle);
                if responder.send(handle).is_err() {
                    info!(node_uri = %node_uri, "A request for node introspection was dropped before it was fulfilled.");
                }
            }
            IntrospectionMessage::IntrospectLane {
                node_uri,
                lane_name,
                responder,
            } => {
                let node_uri_cpy = node_uri.clone();
                let lane_name_cpy = lane_name.clone();
                let result = if let Some(mut handle) = agents
                    .get(&node_uri)
                    .map(AgentIntrospectionUpdater::make_handle)
                {
                    if let Some(mut snapshot) = handle.new_snapshot() {
                        if let Some(lane) = snapshot.lanes.remove(&lane_name) {
                            Ok(lane)
                        } else {
                            Err(LaneIntrospectionError::NoSuchLane {
                                node_uri,
                                lane_name,
                            })
                        }
                    } else {
                        Err(LaneIntrospectionError::NoSuchAgent { node_uri })
                    }
                } else {
                    Err(LaneIntrospectionError::NoSuchAgent { node_uri })
                };
                if responder.send(result).is_err() {
                    info!(node_uri = %node_uri_cpy, lane_name = %lane_name_cpy, "A request for lane introspection was dropped before it was fulfilled.");
                }
            }
        }
    }
}

/// Provides convenience methods for interaction with the introspection task.
#[derive(Debug, Clone)]
pub struct IntrospectionResolver {
    queries: mpsc::UnboundedSender<IntrospectionMessage>,
    registrations: mpsc::Sender<UplinkReporterRegistration>,
}

impl IntrospectionResolver {
    pub(crate) fn new(
        queries: mpsc::UnboundedSender<IntrospectionMessage>,
        registrations: mpsc::Sender<UplinkReporterRegistration>,
    ) -> Self {
        IntrospectionResolver {
            queries,
            registrations,
        }
    }

    /// Register a new agent instance for introspection.
    ///
    /// #Arguments
    /// * `agent_id` - The unique ID of the agent.
    /// * `route_uri` - The node URI of the agent.
    pub fn register_agent(
        &self,
        agent_id: Uuid,
        node_uri: RouteUri,
    ) -> Result<NodeReporting, IntrospectionStopped> {
        let node_uri = Text::new(node_uri.as_str());
        let IntrospectionResolver {
            queries,
            registrations,
        } = self;
        let reporter = UplinkReporter::default();
        let message = IntrospectionMessage::AddAgent {
            agent_id,
            node_uri,
            aggregate_reader: reporter.reader(),
        };
        if queries.send(message).is_ok() {
            let reporting = NodeReporting::new(agent_id, reporter, registrations.clone());
            Ok(reporting)
        } else {
            Err(IntrospectionStopped)
        }
    }

    /// Remove a stopped agent from the intorspectio registry.
    ///
    /// #Arguments
    /// * `agent_id` - The unique ID of the agent.
    pub fn close_agent(&self, agent_id: Uuid) -> Result<(), IntrospectionStopped> {
        let IntrospectionResolver { queries, .. } = self;
        if queries
            .send(IntrospectionMessage::AgentClosed { agent_id })
            .is_err()
        {
            Err(IntrospectionStopped)
        } else {
            Ok(())
        }
    }

    /// Attempt to resolve an introspection handle for a running agent instance.
    ///
    /// #Arguments
    /// * `node_uri` - The node URI of the agent.
    pub async fn resolve_agent(
        &self,
        node_uri: Text,
    ) -> Result<AgentIntrospectionHandle, NodeIntrospectionError> {
        let (tx, rx) = oneshot::channel();
        self.queries.send(IntrospectionMessage::IntrospectAgent {
            node_uri: node_uri.clone(),
            responder: tx,
        })?;
        rx.await?
            .ok_or(NodeIntrospectionError::NoSuchAgent { node_uri })
    }

    /// Attempt to resolve an introspection view of a lane of a running agent instance.
    ///
    /// #Arguments
    ///
    /// * `node_uri` - The node URI of the host agent.
    /// * `lane_name` - The name of the lane.
    pub async fn resolve_lane(
        &self,
        node_uri: Text,
        lane_name: Text,
    ) -> Result<LaneView, LaneIntrospectionError> {
        let (tx, rx) = oneshot::channel();
        self.queries.send(IntrospectionMessage::IntrospectLane {
            node_uri: node_uri.clone(),
            lane_name: lane_name.clone(),
            responder: tx,
        })?;
        rx.await?
    }
}
