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
    event_handler::{EventHandler, HandlerActionExt},
    lanes::{CommandLane, ValueLane},
    lifecycle, projections,
    stores::MapStore,
    AgentLaneModel,
};

use crate::model::Instruction;

#[derive(AgentLaneModel)]
#[projections]
pub struct ExampleAgent {
    lane: ValueLane<i32>,
    #[item(transient)]
    saved: MapStore<String, i32>,
    command: CommandLane<Instruction>,
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

    #[on_event(lane)]
    pub fn on_event(
        &self,
        context: HandlerContext<ExampleAgent>,
        value: &i32,
    ) -> impl EventHandler<ExampleAgent> {
        let n = *value;
        context.effect(move || {
            println!("Setting value to: {}", n);
        })
    }

    #[on_command(command)]
    pub fn on_command(
        &self,
        context: HandlerContext<ExampleAgent>,
        cmd: &Instruction,
    ) -> impl EventHandler<ExampleAgent> {
        match cmd {
            Instruction::Save(name) => {
                let key = name.clone();
                context
                    .get_value(ExampleAgent::LANE)
                    .and_then(move |v| context.update(ExampleAgent::SAVED, key, v))
                    .boxed_local()
            }
            Instruction::Restore(name) => {
                let key = name.clone();
                context
                    .get_entry(ExampleAgent::SAVED, key)
                    .map(|maybe: Option<i32>| maybe.unwrap_or_default())
                    .and_then(move |v| context.set_value(ExampleAgent::LANE, v))
                    .boxed_local()
            }
        }
    }
}
