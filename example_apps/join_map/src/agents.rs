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

use swimos::{
    agent::agent_lifecycle::HandlerContext,
    agent::event_handler::{EventHandler, HandlerActionExt},
    agent::lanes::{JoinMapLane, MapLane},
    agent::projections,
    agent::{lifecycle, AgentLaneModel},
};

#[derive(AgentLaneModel)]
#[projections]
pub struct StreetStatisticsAgent {
    state: MapLane<String, u64>,
}

#[derive(Clone)]
pub struct StreetStatisticsLifecycle;

#[lifecycle(StreetStatisticsAgent)]
impl StreetStatisticsLifecycle {}

#[derive(AgentLaneModel)]
#[projections]
pub struct AggregatedStatisticsAgent {
    streets: JoinMapLane<String, String, u64>,
}

#[derive(Clone)]
pub struct AggregatedLifecycle;

#[lifecycle(AggregatedStatisticsAgent)]
impl AggregatedLifecycle {
    #[on_start]
    pub fn on_start(
        &self,
        context: HandlerContext<AggregatedStatisticsAgent>,
    ) -> impl EventHandler<AggregatedStatisticsAgent> {
        let california_downlink = context.add_map_downlink(
            AggregatedStatisticsAgent::STREETS,
            "california".to_string(),
            None,
            "/state/california",
            "state",
        );
        let texas_downlink = context.add_map_downlink(
            AggregatedStatisticsAgent::STREETS,
            "texas".to_string(),
            None,
            "/state/texas",
            "state",
        );
        let florida_downlink = context.add_map_downlink(
            AggregatedStatisticsAgent::STREETS,
            "florida".to_string(),
            None,
            "/state/florida",
            "state",
        );

        context
            .get_agent_uri()
            .and_then(move |uri| context.effect(move || println!("Starting agent at: {}", uri)))
            .followed_by(california_downlink)
            .followed_by(texas_downlink)
            .followed_by(florida_downlink)
    }

    #[on_stop]
    pub fn on_stop(
        &self,
        context: HandlerContext<AggregatedStatisticsAgent>,
    ) -> impl EventHandler<AggregatedStatisticsAgent> {
        context
            .get_agent_uri()
            .and_then(move |uri| context.effect(move || println!("Stopping agent at: {}", uri)))
    }
}
