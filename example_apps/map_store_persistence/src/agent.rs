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

use example_util::format_map;
use swimos::agent::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{join3, EventHandler, HandlerActionExt, UnitHandler},
    lanes::CommandLane,
    lifecycle, projections,
    stores::MapStore,
    AgentLaneModel,
};

use crate::model::Instruction;

#[derive(AgentLaneModel)]
#[projections]
pub struct ExampleAgent {
    value: MapStore<String, i32>,
    #[lane(transient)]
    temporary: MapStore<String, i32>,
    instructions: CommandLane<Instruction>,
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
            context.get_map(ExampleAgent::VALUE),
            context.get_map(ExampleAgent::TEMPORARY),
        )
        .and_then(move |(uri, m1, m2)| {
            context.effect(move || {
                println!(
                    "Starting agent at: {}. value = {}, temporary = {}",
                    uri,
                    format_map(&m1),
                    format_map(&m2)
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
            context.get_map(ExampleAgent::VALUE),
            context.get_map(ExampleAgent::TEMPORARY),
        )
        .and_then(move |(uri, m1, m2)| {
            context.effect(move || {
                println!(
                    "Stopping agent at: {}. value = {}, temporary = {}",
                    uri,
                    format_map(&m1),
                    format_map(&m2)
                );
            })
        })
    }

    #[on_command(instructions)]
    pub fn instruct(
        &self,
        context: HandlerContext<ExampleAgent>,
        cmd: &Instruction,
    ) -> impl EventHandler<ExampleAgent> {
        match cmd {
            Instruction::Wake => UnitHandler::default().boxed(),
            Instruction::SetValue { key, value } => context
                .update(ExampleAgent::VALUE, key.clone(), *value)
                .boxed(),
            Instruction::SetTemp { key, value } => context
                .update(ExampleAgent::TEMPORARY, key.clone(), *value)
                .boxed(),
            Instruction::Stop => context.stop().boxed(),
        }
    }
}
