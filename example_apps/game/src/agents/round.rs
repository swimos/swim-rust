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

use game_model::round::Round;
use swimos::agent::{
    agent_lifecycle::utility::HandlerContext, event_handler::{EventHandler, HandlerActionExt, Sequentially},
    lanes::{CommandLane, ValueLane}, 
    lifecycle, projections, AgentLaneModel
};
use tracing::info;

use super::model::stats::MatchSummary;

#[derive(AgentLaneModel)]
#[projections]
#[agent(transient, convention = "camel")]
pub struct MatchAgent {
    // Summary of the match
    stats: ValueLane<Option<MatchSummary>>,
    // Publish the match
    publish: CommandLane<Round>
}

#[derive(Clone)]
pub struct MatchLifecycle;

#[lifecycle(MatchAgent)]
impl MatchLifecycle {

    #[on_command(publish)]
    fn publish_round(
        &self,
        context: HandlerContext<MatchAgent>,
        round: &Round,
    ) -> impl EventHandler<MatchAgent> {
        let round = round.clone();

        context
            .effect(move || {
                info!(id = round.id, "New match published.");
                round
            })
            .map(move |round| MatchSummary::from_round(round))
            .and_then(move |summary: MatchSummary| {
                context.set_value(MatchAgent::STATS, Some(summary.clone()))
                .followed_by(
                    forward_match_summary(context, summary)
                )
            })
    }

    #[on_start]
    fn starting(&self, context: HandlerContext<MatchAgent>) -> impl EventHandler<MatchAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || info!(uri = %uri, "Starting match agent"))
        })
    }

    #[on_stop]
    fn stopping(&self, context: HandlerContext<MatchAgent>) -> impl EventHandler<MatchAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || info!(uri = %uri, "Stopping match agent"))
        })
    }

}

fn forward_match_summary(
    context: HandlerContext<MatchAgent>,
    match_summary: MatchSummary,
) -> impl EventHandler<MatchAgent> {
    let forward_to_players = match_summary.player_stats
        .keys()
        .map(|id| command_match_summary(context, match_summary.clone(), format!("/player/{id}")))
        .collect::<Vec<_>>();
    let forward_to_teams = 
        match_summary.team_stats
        .keys()
        .map(|name| command_match_summary(context, match_summary.clone(), format!("/team/{name}")))
        .collect::<Vec<_>>();
    let forward_to_game = command_match_summary(context, match_summary, "/match".to_string());
    forward_to_game.followed_by(Sequentially::new(forward_to_players)).followed_by(Sequentially::new(forward_to_teams))
}

fn command_match_summary(
    context: HandlerContext<MatchAgent>,
    match_summary: MatchSummary,
    node_uri: String
) -> impl EventHandler<MatchAgent> {
    context
        .send_command(None, 
            node_uri, 
            "addMatch".to_string(), 
            match_summary)
}