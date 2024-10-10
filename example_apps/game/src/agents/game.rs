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

use std::{cell::RefCell, collections::VecDeque, time::Duration};

use crate::generator::universe::Universe;
use swimos::agent::{
    agent_lifecycle::HandlerContext,
    event_handler::{EventHandler, HandlerActionExt},
    lanes::{CommandLane, MapLane, ValueLane},
    lifecycle, projections, AgentLaneModel,
};
use tracing::info;

use super::model::stats::{MatchSummary, MatchTotals};

#[derive(AgentLaneModel)]
#[projections]
#[agent(transient, convention = "camel")]
pub struct GameAgent {
    // Total stats across all matches
    stats: ValueLane<MatchTotals>,
    // History of all matches
    match_history: MapLane<u64, MatchSummary>,
    // Add a match to the totals and history
    add_match: CommandLane<MatchSummary>,
}

#[derive(Debug, Clone, Default)]
pub struct GameLifecycle {
    timestamps: RefCell<VecDeque<u64>>,
}

impl GameLifecycle {
    fn update_timestamps(&self, timestamp: u64) -> impl EventHandler<GameAgent> + '_ {
        let context: HandlerContext<GameAgent> = Default::default();
        context
            .effect(move || {
                let mut guard = self.timestamps.borrow_mut();
                guard.push_back(timestamp);
                if guard.len() > 250 {
                    guard.pop_front()
                } else {
                    None
                }
            })
            .and_then(move |to_remove: Option<u64>| to_remove.map(remove_old).discard())
    }
}

fn remove_old(to_remove: u64) -> impl EventHandler<GameAgent> {
    let context: HandlerContext<GameAgent> = Default::default();
    context.remove(GameAgent::MATCH_HISTORY, to_remove)
}

#[lifecycle(GameAgent)]
impl GameLifecycle {
    #[on_start]
    fn starting(&self, context: HandlerContext<GameAgent>) -> impl EventHandler<GameAgent> + '_ {
        let mut universe = Universe::default();

        context
            .get_agent_uri()
            .and_then(move |uri| {
                context.effect(move || info!(uri = %uri, "Starting game agent, generating matches"))
            })
            .followed_by(
                context.schedule_repeatedly(Duration::from_secs(3), move || {
                    Some(generate_match(&mut universe, context))
                }),
            )
    }

    #[on_stop]
    fn stopping(&self, context: HandlerContext<GameAgent>) -> impl EventHandler<GameAgent> {
        context
            .get_agent_uri()
            .and_then(move |uri| context.effect(move || info!(uri = %uri, "Stopping game agent")))
    }

    #[on_command(add_match)]
    fn add_match(
        &self,
        context: HandlerContext<GameAgent>,
        match_summary: &MatchSummary,
    ) -> impl EventHandler<GameAgent> + '_ {
        let summary: MatchSummary = match_summary.clone();
        let ts = summary.start_time;
        let summary_history = match_summary.clone();
        context
            .get_value(GameAgent::STATS)
            .and_then(move |mut current: MatchTotals| {
                current.increment(&summary);
                context.set_value(GameAgent::STATS, current)
            })
            .followed_by(context.update(
                GameAgent::MATCH_HISTORY,
                summary_history.start_time,
                summary_history,
            ))
            .followed_by(self.update_timestamps(ts))
    }
}

fn generate_match(
    universe: &mut Universe,
    context: HandlerContext<GameAgent>,
) -> impl EventHandler<GameAgent> {
    let game = universe.generate_game();
    context.send_command(
        None,
        format!("/match/{id}", id = game.id).as_str(),
        "publish",
        game,
    )
}
