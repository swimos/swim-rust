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

use std::collections::HashMap;

use futures::StreamExt;
use swim_api::meta::lane::LaneKind;
use swim_model::Text;
use swim_runtime::agent::{reporting::UplinkReportReader, UplinkReporterRegistration};
use swim_utilities::trigger;
use tokio::sync::{oneshot, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

use crate::{model::{AgentIntrospectionHandle, LaneView, AgentIntrospectionUpdater}, error::{LaneIntrospectionError, NodeIntrospectionError}};

pub enum IntrospectionMessage {
    AddAgent {
        agent_id: Uuid,
        node_uri: Text,
        aggregate_reader: UplinkReportReader,
    },
    AddLane {
        agent_id: Uuid,
        lane_name: Text,
        kind: LaneKind,
        reader: UplinkReportReader,
    },
    AgentClosed {
        agent_id: Uuid,
    },
    IntrospectAgent {
        node_uri: Text,
        responder: oneshot::Sender<Option<AgentIntrospectionHandle>>,
    },
    IntrospectLane {
        node_uri: Text,
        lane_name: Text,
        responder: oneshot::Sender<Result<LaneView, LaneIntrospectionError>>,
    }
}

impl From<UplinkReporterRegistration> for IntrospectionMessage {
    fn from(reg: UplinkReporterRegistration) -> Self {
        let UplinkReporterRegistration { agent_id, lane_name, kind, reader } = reg;
        IntrospectionMessage::AddLane { agent_id, lane_name , kind, reader }
    }
}

pub async fn introspection_task(stopping: trigger::Receiver, messages: mpsc::Receiver<IntrospectionMessage>) {
    let mut stream = ReceiverStream::new(messages).take_until(stopping);
    
    let mut name_map: HashMap<Uuid, Text> = HashMap::new();
    let mut agents: HashMap<Text, AgentIntrospectionUpdater> = HashMap::new();
    
    while let Some(message) = stream.next().await {
        match message {
            IntrospectionMessage::AddAgent { agent_id, node_uri, aggregate_reader } => {
                name_map.insert(agent_id, node_uri.clone());
                let updater = AgentIntrospectionUpdater::new(aggregate_reader);
                agents.insert(node_uri, updater);
            },
            IntrospectionMessage::AddLane { agent_id, lane_name, kind, reader } => {
                if let Some(updater) = name_map.get(&agent_id).and_then(|name| agents.get(name)) {
                    updater.add_lane(lane_name, kind, reader);
                }
            },
            IntrospectionMessage::AgentClosed { agent_id } => {
                name_map.remove(&agent_id).and_then(|name| agents.remove(&name));
            },
            IntrospectionMessage::IntrospectAgent { node_uri, responder } => {
                let handle = agents.get(&node_uri).map(AgentIntrospectionUpdater::make_handle);
                if responder.send(handle).is_err() {
                    //TOOD Log error.
                }
            },
            IntrospectionMessage::IntrospectLane { node_uri, lane_name, responder } => {
                let result = if let Some(mut handle) = agents.get(&node_uri).map(AgentIntrospectionUpdater::make_handle) {
                    if let Some(mut snapshot) = handle.new_snapshot() {
                        if let Some(lane) = snapshot.lanes.remove(&lane_name) {
                            Ok(lane)
                        } else {
                            Err(LaneIntrospectionError::NoSuchLane { node_uri, lane_name })
                        }
                    } else {
                        Err(LaneIntrospectionError::NoSuchAgent { node_uri })
                    }
                } else {
                    Err(LaneIntrospectionError::NoSuchAgent { node_uri })
                };
                if responder.send(result).is_err() {
                    //TOOD Log error.
                }
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct IntrospectionResolver {
    queries: mpsc::Sender<IntrospectionMessage>,
}

impl IntrospectionResolver {

    pub fn new(queries: mpsc::Sender<IntrospectionMessage>) -> Self {
        IntrospectionResolver { queries }
    }

    pub async fn resolve_agent(&self, node_uri: Text) -> Result<AgentIntrospectionHandle, NodeIntrospectionError> {
        let (tx, rx) = oneshot::channel();
        self.queries.send(IntrospectionMessage::IntrospectAgent { node_uri: node_uri.clone(), responder: tx }).await?;
        rx.await?.ok_or(NodeIntrospectionError::NoSuchAgent { node_uri })
    }

    pub async fn resolve_lane(&self, node_uri: Text, lane_name: Text) -> Result<LaneView, LaneIntrospectionError> {
        let (tx, rx) = oneshot::channel();
        self.queries.send(IntrospectionMessage::IntrospectLane { node_uri: node_uri.clone(), lane_name: lane_name.clone(), responder: tx }).await?;
        rx.await?
    }
}