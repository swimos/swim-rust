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

use std::{collections::HashMap, convert::identity};

use swim::agent::{
    agent_lifecycle::utility::{HandlerContext, JoinValueContext},
    event_handler::{EventHandler, HandlerActionExt},
    lanes::{
        join_value::lifecycle::JoinValueLaneLifecycle, CommandLane, JoinValueLane, MapLane,
        ValueLane,
    },
    lifecycle, projections, AgentLaneModel,
};
use tracing::{debug, info};

use crate::model::{agency::Agency, counts::Count};

use super::join_value_logging_lifecycle;

#[derive(AgentLaneModel)]
#[projections]
#[agent(transient, convention = "camel")]
pub struct CountryAgent {
    count: ValueLane<Count>,
    agencies: MapLane<String, Agency>,
    states: MapLane<String, ()>,
    state_count: JoinValueLane<String, Count>,
    speed: ValueLane<f64>,
    state_speed: JoinValueLane<String, f64>,
    add_agency: CommandLane<Agency>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CountryLifecycle;

#[lifecycle(CountryAgent)]
impl CountryLifecycle {
    #[on_start]
    fn init(&self, context: HandlerContext<CountryAgent>) -> impl EventHandler<CountryAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || info!(uri = %uri, "Starting country agent."))
        })
    }

    #[on_stop]
    fn stopping(&self, context: HandlerContext<CountryAgent>) -> impl EventHandler<CountryAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || info!(uri = %uri, "Stopping country agent."))
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
        let log_uri = context.value(agency_uri.clone()).and_then(move |uri| {
            context.effect(move || {
                info!(uri, "Initializing agency information for country.");
            })
        });
        let add_agency = context.update(CountryAgent::AGENCIES, agency_uri, agency.clone());
        let link_counts = context.add_downlink(
            CountryAgent::STATE_COUNT,
            agency.state.clone(),
            None,
            &state_uri,
            "count",
        );
        let link_speeds = context.add_downlink(
            CountryAgent::STATE_SPEED,
            agency.state.clone(),
            None,
            &state_uri,
            "speed",
        );
        log_uri
            .followed_by(insert_state)
            .followed_by(add_agency)
            .followed_by(link_counts)
            .followed_by(link_speeds)
    }

    #[on_update(state_count)]
    fn update_counts(
        &self,
        context: HandlerContext<CountryAgent>,
        map: &HashMap<String, Count>,
        _key: String,
        _prev: Option<Count>,
        _new_value: &Count,
    ) -> impl EventHandler<CountryAgent> {
        let count_sum = map
            .values()
            .fold(0usize, |acc, c| acc.saturating_add(c.current));
        context
            .get_value(CountryAgent::COUNT)
            .and_then(move |Count { max, .. }| {
                let new_max = max.max(count_sum);
                debug!(count_sum, new_max, "Updating country counts.");
                context.set_value(
                    CountryAgent::COUNT,
                    Count {
                        current: count_sum,
                        max: new_max,
                    },
                )
            })
    }

    #[on_update(state_speed)]
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
        debug!(avg, "Updating country average speed.");
        context.set_value(CountryAgent::SPEED, avg)
    }

    #[join_value_lifecycle(state_count)]
    fn register_count_lifecycle(
        &self,
        context: JoinValueContext<CountryAgent, String, Count>,
    ) -> impl JoinValueLaneLifecycle<String, Count, CountryAgent> + 'static {
        join_value_logging_lifecycle(context, identity, "Count")
    }

    #[join_value_lifecycle(state_speed)]
    fn register_speed_lifecycle(
        &self,
        context: JoinValueContext<CountryAgent, String, f64>,
    ) -> impl JoinValueLaneLifecycle<String, f64, CountryAgent> + 'static {
        join_value_logging_lifecycle(context, identity, "Speed")
    }
}
