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
use swimos::{
    agent::agent_lifecycle::utility::HandlerContext,
    agent::event_handler::EventHandler,
    agent::lanes::ValueLane,
    agent::lanes::{CommandLane, JoinValueLane},
    agent::AgentLaneModel,
    agent::{lifecycle, projections},
};

#[derive(AgentLaneModel)]
#[projections]
pub struct AggregateAgent {
    aggregated: JoinValueLane<String, f64>,
    average_speed: ValueLane<f64>,
    register: CommandLane<String>,
}

#[derive(Clone, Default)]
pub struct AggregateLifecycle;

#[lifecycle(AggregateAgent)]
impl AggregateLifecycle {
    #[on_update(aggregated)]
    fn aggregated(
        &self,
        context: HandlerContext<AggregateAgent>,
        averages: &HashMap<String, f64>,
        _key: String,
        _prev: Option<f64>,
        _new_value: &f64,
    ) -> impl EventHandler<AggregateAgent> {
        let average = averages.values().sum::<f64>() / averages.len() as f64;
        context.set_value(AggregateAgent::AVERAGE_SPEED, average)
    }

    #[on_command(register)]
    pub fn register(
        &self,
        context: HandlerContext<AggregateAgent>,
        area_id: &String,
    ) -> impl EventHandler<AggregateAgent> {
        context.add_downlink(
            AggregateAgent::AGGREGATED,
            area_id.clone(),
            None,
            format!("/area/{}", area_id).as_str(),
            "average_speed",
        )
    }
}
