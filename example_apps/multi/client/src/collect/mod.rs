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

use std::collections::HashMap;

use swim::{
    agent::{
        agent_lifecycle::utility::{HandlerContext, JoinValueContext},
        event_handler::{EventHandler, Sequentially},
        lanes::{JoinValueLane, join_value::{lifecycle::JoinValueLaneLifecycle, LinkClosedResponse}},
        lifecycle, projections, AgentLaneModel,
    },
    route::RouteUri,
};

use crate::LANE_NAME;

#[derive(AgentLaneModel)]
#[projections]
pub struct CollectAgent {
    collected: JoinValueLane<String, i32>,
}

#[derive(Clone)]
pub struct CollectLifecycle {
    remote: String,
    nodes: Vec<RouteUri>
}

impl CollectLifecycle {
    pub fn new(remote: &str, nodes: Vec<RouteUri>) -> Self {
        CollectLifecycle { remote: remote.to_string(), nodes }
    }
}

#[lifecycle(CollectAgent)]
impl CollectLifecycle {

    #[join_value_lifecycle(collected)]
    fn register_lifecycle(
        &self,
        context: JoinValueContext<CollectAgent, String, i32>,
    ) -> impl JoinValueLaneLifecycle<String, i32, CollectAgent> + 'static {
        context.builder()
            .on_linked(|context, key, _remote| context.effect(move || println!("New link with key: {}", key)))
            .on_unlinked(|context, key, _remote| context.effect(move || {
                println!("Link with key {} closed.", key);
                LinkClosedResponse::Abandon
            }))
            .on_failed(|context, key, _remote| context.effect(move || {
                println!("Link with key {} failed.", key);
                LinkClosedResponse::Abandon
            }))
            .done()
    }
    
    #[on_start]
    pub fn open_links(&self, context: HandlerContext<CollectAgent>) -> impl EventHandler<CollectAgent> + '_ {
        let handlers = self.nodes.iter().map(move |node| context.add_downlink(
            CollectAgent::COLLECTED, node.to_string(), Some(&self.remote), node.as_str(), LANE_NAME));
        Sequentially::new(handlers)
    }

    #[on_update(collected)]
    fn received_value(
        &self,
        context: HandlerContext<CollectAgent>,
        _map: &HashMap<String, i32>,
        key: String,
        prev: Option<i32>,
        new_value: &i32,
    ) -> impl EventHandler<CollectAgent> {
        let v = *new_value;
        context.effect(move || {
            if let Some(p) = prev {
                println!("Received update for key: {}. Value changed from {} to {}.", key, p, v);
            } else {
                println!("New entry for key: {}. Value is {}.", key, v);
            }
        })
    }
}
