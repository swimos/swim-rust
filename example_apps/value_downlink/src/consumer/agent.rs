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

use swim::agent::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{EventHandler, HandlerActionExt},
    lanes::ValueLane,
    lifecycle, projections, AgentLaneModel,
};

#[derive(AgentLaneModel)]
#[projections]
pub struct ConsumerAgent {
    lane: ValueLane<i32>,
}

#[derive(Clone)]
pub struct ConsumerLifecycle {
    port: u16,
}

impl ConsumerLifecycle {
    pub fn new(port: u16) -> Self {
        ConsumerLifecycle { port }
    }
}

#[lifecycle(ConsumerAgent)]
impl ConsumerLifecycle {
    #[on_start]
    pub fn on_start(
        &self,
        context: HandlerContext<ConsumerAgent>,
    ) -> impl EventHandler<ConsumerAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || {
                println!("Starting producer agent at: {}", uri);
            })
        })
    }

    #[on_stop]
    pub fn on_stop(
        &self,
        context: HandlerContext<ConsumerAgent>,
    ) -> impl EventHandler<ConsumerAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || {
                println!("Stopping producer agent at: {}", uri);
            })
        })
    }

    #[on_event(lane)]
    pub fn on_event(
        &self,
        context: HandlerContext<ConsumerAgent>,
        value: &i32,
    ) -> impl EventHandler<ConsumerAgent> {
        let n = *value;
        context.effect(move || {
            println!("Setting value on producer to: {}", n);
        })
    }
}
