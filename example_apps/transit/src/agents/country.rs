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
    lanes::{CommandLane, JoinValueLane, MapLane, ValueLane},
    lifecycle, projections, AgentLaneModel,
};

use crate::model::{agency::Agency, counts::Count};

#[derive(AgentLaneModel)]
#[projections]
pub struct CountryAgent {
    #[lane(transient)]
    count: ValueLane<Count>,
    #[lane(transient)]
    agencies: MapLane<String, Agency>,
    #[lane(transient)]
    states: MapLane<String, ()>,
    #[lane(transient)]
    state_count: MapLane<String, usize>,
    #[lane(transient)]
    join_state_count: JoinValueLane<String, usize>,
    #[lane(transient)]
    speed: ValueLane<f64>,
    #[lane(transient)]
    state_speed: MapLane<String, f64>,
    #[lane(transient)]
    join_state_speed: JoinValueLane<String, f64>,
    add_agency: CommandLane<Agency>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CountryLifecycle;

#[lifecycle(CountryAgent)]
impl CountryLifecycle {
    #[on_start]
    fn init(&self, context: HandlerContext<CountryAgent>) -> impl EventHandler<CountryAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || print!("Starting Country agent at: {}", uri))
        })
    }

    #[on_command(add_agency)]
    fn connect_agency(
        &self,
        context: HandlerContext<CountryAgent>,
        agency: &Agency,
    ) -> impl EventHandler<CountryAgent> {
        let insert_state = context.update(CountryAgent::STATES, agency.state.clone(), ());
        let agency_uri = agency.uri();
        let state_uri = agency.state_uri();
        let add_agency = context.update(CountryAgent::AGENCIES, agency_uri, agency.clone());
        let link_counts = context.add_downlink(
            CountryAgent::JOIN_STATE_COUNT,
            agency.state.clone(),
            None,
            &state_uri,
            "count",
        );
        let link_speeds = context.add_downlink(
            CountryAgent::JOIN_STATE_SPEED,
            agency.state.clone(),
            None,
            &state_uri,
            "speed",
        );
        insert_state
            .followed_by(add_agency)
            .followed_by(link_counts)
            .followed_by(link_speeds)
    }

    #[on_update(join_state_count)]
    fn update_counts(
        &self,
        context: HandlerContext<CountryAgent>,
        map: &HashMap<String, usize>,
        _key: String,
        _prev: Option<usize>,
        _new_value: &usize,
    ) -> impl EventHandler<CountryAgent> {
        let count_sum = map.values().fold(0usize, |acc, n| acc.saturating_add(*n));
        context
            .get_value(CountryAgent::COUNT)
            .and_then(move |Count { max, .. }| {
                let new_max = max.max(count_sum);
                context.set_value(
                    CountryAgent::COUNT,
                    Count {
                        current: count_sum,
                        max: new_max,
                    },
                )
            })
    }

    #[on_update(join_state_speed)]
    fn update_speeds(
        &self,
        context: HandlerContext<CountryAgent>,
        map: &HashMap<String, f64>,
        _key: String,
        _prev: Option<f64>,
        _new_value: &f64,
    ) -> impl EventHandler<CountryAgent> {
        let avg = map
            .values()
            .copied()
            .fold((0.0, 0usize), |(mean, count), speed| {
                let next_n = count.saturating_add(1);
                let next_mean = ((mean * count as f64) + speed) / (next_n as f64);
                (next_mean, next_n)
            })
            .0;
        context.set_value(CountryAgent::SPEED, avg)
    }
}
