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
    event_handler::{EventHandler, HandlerActionExt},
    lanes::{CommandLane, JoinValueLane, ValueLane},
    lifecycle, projections, AgentLaneModel,
};

use crate::model::{agency::Agency, counts::Counts};

#[derive(AgentLaneModel)]
#[projections]
#[agent(convention = "camel")]
pub struct StateAgent {
    #[lane(transient)]
    count: ValueLane<Counts>,
    #[lane(transient)]
    join_agency_count: JoinValueLane<Agency, usize>,
    #[lane(transient)]
    speed: ValueLane<f64>,
    #[lane(transient)]
    agency_speed: JoinValueLane<Agency, f64>,
    #[lane(transient)]
    join_state_speed: JoinValueLane<Agency, f64>,
    add_agency: CommandLane<Agency>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct StateLifecycle;

#[lifecycle(StateAgent)]
impl StateLifecycle {
    #[on_start]
    fn init(&self, context: HandlerContext<StateAgent>) -> impl EventHandler<StateAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || println!("Starting State agent at: {}", uri))
        })
    }

    #[on_command(add_agency)]
    fn connect_agency(
        &self,
        context: HandlerContext<StateAgent>,
        agency: &Agency,
    ) -> impl EventHandler<StateAgent> {
        let agency_uri = agency.uri();
        let country_uri = agency.country_uri();
        let link_count = context.add_downlink(
            StateAgent::JOIN_AGENCY_COUNT,
            agency.clone(),
            None,
            &agency_uri,
            "count",
        );
        let link_speed = context.add_downlink(
            StateAgent::JOIN_STATE_SPEED,
            agency.clone(),
            None,
            &agency_uri,
            "speed",
        );
        let add_to_country =
            context.send_command(None, country_uri, "addAgency".to_string(), agency.clone());
        link_count
            .followed_by(link_speed)
            .followed_by(add_to_country)
    }

    #[on_update(join_agency_count)]
    fn update_counts(
        &self,
        context: HandlerContext<StateAgent>,
        map: &HashMap<Agency, usize>,
        _key: Agency,
        _prev: Option<usize>,
        _new_value: &usize,
    ) -> impl EventHandler<StateAgent> {
        let count_sum = map.values().fold(0usize, |acc, n| acc.saturating_add(*n));
        context.get_value(StateAgent::COUNT).and_then(move |Counts { max, .. }| {
            let new_max = max.max(count_sum);
            context.set_value(StateAgent::COUNT, Counts { current: count_sum, max: new_max})
        })
    }
}
