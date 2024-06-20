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

use swimos::agent::{
    agent_lifecycle::HandlerContext,
    event_handler::{join3, EventHandler, HandlerActionExt},
    lanes::{CommandLane, ValueLane},
    lifecycle, projections, AgentLaneModel,
};

#[derive(AgentLaneModel)]
#[projections]
pub struct ExampleAgent {
    value: ValueLane<i32>,
    #[item(transient)]
    temporary: ValueLane<i32>,
    stop: CommandLane<()>,
}

#[derive(Clone)]
pub struct ExampleLifecycle;

#[lifecycle(ExampleAgent)]
impl ExampleLifecycle {
    #[on_start]
    pub fn on_start(
        &self,
        context: HandlerContext<ExampleAgent>,
    ) -> impl EventHandler<ExampleAgent> {
        join3(
            context.get_agent_uri(),
            context.get_value(ExampleAgent::VALUE),
            context.get_value(ExampleAgent::TEMPORARY),
        )
        .and_then(move |(uri, v1, v2)| {
            context.effect(move || {
                println!(
                    "Starting agent at: {}. value = {}, temporary = {}",
                    uri, v1, v2
                );
            })
        })
    }

    #[on_stop]
    pub fn on_stop(
        &self,
        context: HandlerContext<ExampleAgent>,
    ) -> impl EventHandler<ExampleAgent> {
        join3(
            context.get_agent_uri(),
            context.get_value(ExampleAgent::VALUE),
            context.get_value(ExampleAgent::TEMPORARY),
        )
        .and_then(move |(uri, v1, v2)| {
            context.effect(move || {
                println!(
                    "Stopping agent at: {}. value = {}, temporary = {}",
                    uri, v1, v2
                );
            })
        })
    }

    #[on_command(stop)]
    pub fn stop_agent(
        &self,
        context: HandlerContext<ExampleAgent>,
        _cmd: &(),
    ) -> impl EventHandler<ExampleAgent> {
        context.stop()
    }
}
