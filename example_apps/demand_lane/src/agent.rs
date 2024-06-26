// Copyright 2015-2024 Swim Inc.
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

use swimos::agent::{
    agent_lifecycle::HandlerContext,
    event_handler::{EventHandler, HandlerAction, HandlerActionExt},
    lanes::{DemandLane, ValueLane},
    lifecycle, projections, AgentLaneModel,
};

#[derive(AgentLaneModel)]
#[projections]
pub struct ExampleAgent {
    lane: ValueLane<i32>,
    demand: DemandLane<i32>,
}

#[derive(Clone)]
pub struct ExampleLifecycle;

const CUE_PERIOD: Duration = Duration::from_secs(5);

#[lifecycle(ExampleAgent)]
impl ExampleLifecycle {
    #[on_start]
    pub fn on_start(
        &self,
        context: HandlerContext<ExampleAgent>,
    ) -> impl EventHandler<ExampleAgent> {
        let cue_schedule = context
            .schedule_repeatedly(CUE_PERIOD, move || Some(context.cue(ExampleAgent::DEMAND)));

        context
            .get_agent_uri()
            .and_then(move |uri| {
                context.effect(move || {
                    println!("Starting agent at: {}", uri);
                })
            })
            .followed_by(cue_schedule)
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

    #[on_cue(demand)]
    pub fn on_cue(
        &self,
        context: HandlerContext<ExampleAgent>,
    ) -> impl HandlerAction<ExampleAgent, Completion = i32> {
        context.get_value(ExampleAgent::LANE).map(|n| 2 * n)
    }
}
