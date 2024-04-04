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

use swimos::{agent::{
    agent_lifecycle::utility::HandlerContext, event_handler::{EventHandler, HandlerActionExt},
    lanes::{CommandLane, ValueLane}, 
    lifecycle, projections, AgentLaneModel
}, route::RouteUri};
use tracing::info;

use super::model::stats::{MatchSummary, TeamTotals};

#[derive(AgentLaneModel)]
#[projections]
#[agent(transient, convention = "camel")]
pub struct TeamAgent {
    // Total stats for this team across all matches
    stats: ValueLane<TeamTotals>,
    // Add a match to the totals
    add_match: CommandLane<MatchSummary>,
}

#[derive(Clone)]
pub struct TeamLifecycle;

#[lifecycle(TeamAgent)]
impl TeamLifecycle {

    #[on_command(add_match)]
    fn add_match(
        &self,
        context: HandlerContext<TeamAgent>,
        match_summary: &MatchSummary
    ) -> impl EventHandler<TeamAgent> {
        let match_summary = match_summary.clone();
        context
            .get_value(TeamAgent::STATS)
            .and_then(move |mut current: TeamTotals| {
                current.increment(&match_summary);
                context.set_value(TeamAgent::STATS, current)
            })
    }

    #[on_start]
    fn starting(&self, context: HandlerContext<TeamAgent>) -> impl EventHandler<TeamAgent> {
        context.get_agent_uri().and_then(move |uri: RouteUri| {
            context.effect(move || info!(uri = %uri, "Starting team agent"))
        }).followed_by(
            context.get_parameter("name").and_then(move |name: Option<String>| {
                context.set_value(TeamAgent::STATS, TeamTotals::new(name.unwrap()))
            })
        )
    }

    #[on_stop]
    fn stopping(&self, context: HandlerContext<TeamAgent>) -> impl EventHandler<TeamAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || info!(uri = %uri, "Stopping team agent"))
        })
    }

}