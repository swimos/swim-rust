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

use swim::agent::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{EventHandler, HandlerAction, HandlerActionExt},
    lanes::{CommandLane, DemandMapLane},
    lifecycle, projections, AgentLaneModel,
};

#[derive(AgentLaneModel)]
#[projections]
pub struct ExampleAgent {
    command: CommandLane<String>,
    demand_map: DemandMapLane<String, i32>,
}

#[derive(Clone)]
pub struct ExampleLifecycle(HashMap<String, i32>);

impl ExampleLifecycle {
    pub fn new(values: impl IntoIterator<Item = (impl Into<String>, i32)>) -> Self {
        let content = values
            .into_iter()
            .map(|(s, n)| (s.into(), n))
            .collect::<HashMap<_, _>>();
        ExampleLifecycle(content)
    }
}

#[lifecycle(ExampleAgent)]
impl ExampleLifecycle {
    #[on_start]
    pub fn on_start(
        &self,
        context: HandlerContext<ExampleAgent>,
    ) -> impl EventHandler<ExampleAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || {
                println!("Starting agent at: {}", uri);
            })
        })
    }

    #[on_stop]
    pub fn on_stop(
        &self,
        context: HandlerContext<ExampleAgent>,
    ) -> impl EventHandler<ExampleAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || {
                println!("Stopping agent at: {}", uri);
            })
        })
    }

    #[on_command(command)]
    pub fn trigger_cue(
        &self,
        context: HandlerContext<ExampleAgent>,
        message: &str,
    ) -> impl EventHandler<ExampleAgent> {
        context.cue_key(ExampleAgent::DEMAND_MAP, message.to_string())
    }

    #[on_cue_key(demand_map)]
    pub fn on_cue_key(
        &self,
        context: HandlerContext<ExampleAgent>,
        key: String,
    ) -> impl HandlerAction<ExampleAgent, Completion = Option<i32>> + '_ {
        context.effect(move || {
            let ExampleLifecycle(content) = self;
            content.get(&key).copied()
        })
    }
}
