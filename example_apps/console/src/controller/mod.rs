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

use std::{collections::HashMap, sync::Arc, time::Duration};

use parking_lot::RwLock;
use swim_utilities::routing::route_uri::RouteUri;
use tokio::sync::mpsc;

use crate::{
    model::{
        ControllerCommand, Endpoint, EndpointOrId, Host, LinkRef, RuntimeCommand, Target, TargetRef,
    },
    oneshot::{self, ReceiveError},
    shared_state::SharedState,
};

const BAD_CHAN: &str = "Command channel dropped.";

pub struct Controller {
    shared_state: Arc<RwLock<SharedState>>,
    command_tx: mpsc::UnboundedSender<RuntimeCommand>,
    with_host: Option<Host>,
    with_node: Option<RouteUri>,
    with_lane: Option<String>,
    names: HashMap<String, usize>,
    targets: HashMap<String, Endpoint>,
    timeout: Duration,
}

impl Controller {
    pub fn new(
        shared_state: Arc<RwLock<SharedState>>,
        command_tx: mpsc::UnboundedSender<RuntimeCommand>,
        timeout: Duration,
    ) -> Self {
        Controller {
            shared_state,
            command_tx,
            with_host: None,
            with_node: None,
            with_lane: None,
            targets: HashMap::new(),
            names: HashMap::new(),
            timeout,
        }
    }

    fn links(&self) -> Vec<(usize, Endpoint)> {
        self.shared_state.read().list()
    }

    fn resolve_target(&mut self, target: Target) -> Result<EndpointOrId, String> {
        let Controller {
            shared_state,
            with_host,
            with_node,
            with_lane,
            ..
        } = self;
        let Target { remote, node, lane } = target;
        let r = remote.or_else(|| with_host.clone());
        let n = node.or_else(|| with_node.clone());
        let l = lane.or_else(|| with_lane.clone());
        if let Some(((remote, node), lane)) = r.zip(n).zip(l) {
            let endpoint = Endpoint { remote, node, lane };
            if let Some(id) = shared_state.read().get_id(&endpoint) {
                Ok(EndpointOrId::Id(id))
            } else {
                Ok(EndpointOrId::Endpoint(endpoint))
            }
        } else {
            Err("Incomplete target.".to_string())
        }
    }

    fn resolve(&mut self, target: TargetRef) -> Result<EndpointOrId, String> {
        let Controller {
            shared_state,
            names,
            targets,
            ..
        } = self;
        match target {
            TargetRef::Link(LinkRef::ById(id)) => {
                if shared_state.read().has_id(id) {
                    Ok(EndpointOrId::Id(id))
                } else {
                    Err(format!("{} is not a valid link ID.", id))
                }
            }
            TargetRef::CommandTarget(name) => {
                if let Some(endpoint) = targets.get(&name) {
                    Ok(EndpointOrId::Endpoint(endpoint.clone()))
                } else {
                    Err(format!("{} is not a defined command target.", name))
                }
            }
            TargetRef::Link(LinkRef::ByName(name)) => {
                if let Some(id) = names.get(&name) {
                    if shared_state.read().has_id(*id) {
                        Ok(EndpointOrId::Id(*id))
                    } else {
                        names.remove(&name);
                        Err(format!("{} is not a valid link name.", name))
                    }
                } else {
                    Err(format!("{} is not a valid link name.", name))
                }
            }
            TargetRef::Direct(target) => self.resolve_target(target),
        }
    }

    pub fn perform_action(&mut self, command: ControllerCommand) -> Vec<String> {
        match command {
            ControllerCommand::WithHost(h) => {
                let new = h.clone();
                vec![if let Some(old) = self.with_host.replace(h) {
                    format!("Changed active host from {} to {}.", old, new)
                } else {
                    format!("Set active host to {}.", new)
                }]
            }
            ControllerCommand::WithNode(n) => {
                let new = n.clone();
                vec![if let Some(old) = self.with_node.replace(n) {
                    format!("Changed active node URI from {} to {}.", old, new)
                } else {
                    format!("Set active node URI to {}.", new)
                }]
            }
            ControllerCommand::WithLane(l) => {
                let new = l.clone();
                vec![if let Some(old) = self.with_lane.replace(l) {
                    format!("Changed active lane from {} to {}.", old, new)
                } else {
                    format!("Set active lane to {}.", new)
                }]
            }
            ControllerCommand::ShowWith => {
                let Controller {
                    with_host,
                    with_node,
                    with_lane,
                    ..
                } = self;
                let mut response = vec![];
                if let Some(h) = with_host {
                    response.push(format!("Using host: {}", h));
                } else {
                    response.push("Using host: <not set>".to_string());
                }
                if let Some(n) = with_node {
                    response.push(format!("Using node: {}", n));
                } else {
                    response.push("Using node: <not set>".to_string());
                }
                if let Some(l) = with_lane {
                    response.push(format!("Using lane: {}", l));
                } else {
                    response.push("Using lane: <not set>".to_string());
                }
                response
            }
            ControllerCommand::ClearWith => {
                let Controller {
                    with_host,
                    with_node,
                    with_lane,
                    ..
                } = self;
                *with_host = None;
                *with_node = None;
                *with_lane = None;
                vec!["Clearing with bindings.".to_string()]
            }
            ControllerCommand::ListLinks => {
                let mut response = vec!["Active links:".to_string()];
                response.extend(self.links().into_iter().map(format_list_entry));
                response
            }
            ControllerCommand::Command { target, body } => match self.resolve(target) {
                Ok(EndpointOrId::Id(id)) => {
                    self.command_tx
                        .send(RuntimeCommand::Command(id, body))
                        .expect(BAD_CHAN);
                    vec![]
                }
                Ok(EndpointOrId::Endpoint(endpoint)) => {
                    self.command_tx
                        .send(RuntimeCommand::AdHocCommand(endpoint, body))
                        .expect(BAD_CHAN);
                    vec![]
                }
                Err(msg) => vec![msg],
            },
            ControllerCommand::Link {
                name,
                target,
                kind,
                sync,
            } => match self.resolve_target(target) {
                Ok(EndpointOrId::Id(id)) => {
                    if let Some(name) = name {
                        self.names.insert(name, id);
                    }
                    if sync {
                        self.command_tx
                            .send(RuntimeCommand::Sync(id))
                            .expect(BAD_CHAN);
                    }
                    vec!["Already linked.".to_string()]
                }
                Ok(EndpointOrId::Endpoint(endpoint)) => {
                    let (tx, rx) = oneshot::channel();
                    self.command_tx
                        .send(RuntimeCommand::Link {
                            endpoint,
                            response: tx,
                            kind,
                            immediate_sync: sync,
                        })
                        .expect(BAD_CHAN);
                    match rx.recv(self.timeout) {
                        Ok(Ok(id)) => {
                            if let Some(name) = name {
                                self.names.insert(name, id);
                            }
                            vec![format!("ID: {}", id)]
                        }
                        Ok(Err(e)) => {
                            vec![format!("Connection failed: {}", e)]
                        }
                        Err(ReceiveError::SenderDropped) => panic!("{}", BAD_CHAN),
                        Err(ReceiveError::TimedOut) => {
                            vec!["Connection request timed out.".to_string()]
                        }
                    }
                }
                Err(msg) => vec![msg],
            },
            ControllerCommand::Sync(link) => self.for_link(link, RuntimeCommand::Sync),
            ControllerCommand::Unlink(link) => self.for_link(link, RuntimeCommand::Unlink),
            ControllerCommand::UnlinkAll => {
                self.command_tx
                    .send(RuntimeCommand::UnlinkAll)
                    .expect(BAD_CHAN);
                vec![]
            }
            ControllerCommand::Query(link) => self.for_link(link, RuntimeCommand::Query),
            ControllerCommand::Target { name, target } => {
                match self.add_target(name.clone(), target) {
                    Ok(endpoint) => {
                        vec![format!("Assigned {} to target name ${}.", endpoint, name)]
                    }
                    Err(msg) => vec![msg],
                }
            }
        }
    }

    fn for_link(&self, link: LinkRef, f: impl FnOnce(usize) -> RuntimeCommand) -> Vec<String> {
        match link {
            LinkRef::ById(id) => {
                if self.shared_state.read().has_id(id) {
                    self.command_tx.send(f(id)).expect(BAD_CHAN);
                    vec![]
                } else {
                    vec![format!("{} is not a valid link ID.", id)]
                }
            }
            LinkRef::ByName(name) => {
                if let Some(id) = self.names.get(&name) {
                    if self.shared_state.read().has_id(*id) {
                        self.command_tx.send(f(*id)).expect(BAD_CHAN);
                        vec![]
                    } else {
                        vec![format!("{} is not a valid link ID.", id)]
                    }
                } else {
                    vec![format!("{} is not a valid link name.", name)]
                }
            }
        }
    }

    fn add_target(&mut self, name: String, target: Target) -> Result<Endpoint, String> {
        let Controller {
            with_host,
            with_node,
            with_lane,
            targets,
            ..
        } = self;
        let Target { remote, node, lane } = target;
        let r = remote.or_else(|| with_host.clone());
        let n = node.or_else(|| with_node.clone());
        let l = lane.or_else(|| with_lane.clone());
        if let Some(((remote, node), lane)) = r.zip(n).zip(l) {
            let endpoint = Endpoint { remote, node, lane };
            targets.insert(name, endpoint.clone());
            Ok(endpoint)
        } else {
            Err("Incomplete target.".to_string())
        }
    }
}

fn format_list_entry(entry: (usize, Endpoint)) -> String {
    let (n, Endpoint { remote, node, lane }) = entry;
    format!("{}: host = {}, node = {}, lane = {}", n, remote, node, lane)
}
