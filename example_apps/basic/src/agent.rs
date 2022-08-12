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

use swim::agent::{AgentLaneModel, lanes::{ValueLane, CommandLane}, agent_lifecycle::utility::HandlerContext, event_handler::{EventHandler, HandlerActionExt}, projections, lifecycle};

#[derive(AgentLaneModel)]
#[projections]
pub struct ExampleAgent {
    length: ValueLane<i32>,
    update_value: CommandLane<String>,
}

#[derive(Clone)]
pub struct ExampleLifecycle;

#[lifecycle(ExampleAgent)]
impl ExampleLifecycle {

    #[on_start]
    pub fn on_start(&self, context: HandlerContext<ExampleAgent>) -> impl EventHandler<ExampleAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || {
                println!("Starting agent at: {}", uri);
            })
        })
    }

    #[on_command(update_value)]
    pub fn on_update(&self, context: HandlerContext<ExampleAgent>, value: &String) -> impl EventHandler<ExampleAgent> {
        let n = value.len() as i32;
        context.set_value(ExampleAgent::LENGTH, n)
    }

}