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

use std::time::Duration;

use rand::Rng;
use swim::agent::{
    agent_lifecycle::utility::HandlerContext, event_handler::EventHandler, lanes::ValueLane,
    lifecycle, projections, AgentLaneModel,
};

#[derive(AgentLaneModel)]
#[projections]
pub struct ExampleAgent {
    state: ValueLane<i32>,
}

#[derive(Clone, Copy)]
pub struct EventGenerator {
    interval: Duration,
}

impl EventGenerator {
    pub fn new(interval: Duration) -> Self {
        EventGenerator { interval }
    }
}

#[lifecycle(ExampleAgent)]
impl EventGenerator {
    #[on_start]
    pub fn reg_timer(
        &self,
        context: HandlerContext<ExampleAgent>,
    ) -> impl EventHandler<ExampleAgent> {
        let EventGenerator { interval } = self;
        let effect = move || {
            let c = rand::thread_rng().gen::<i32>();
            Some(context.set_value(ExampleAgent::STATE, c))
        };
        context.schedule_repeatedly(*interval, effect)
    }
}
