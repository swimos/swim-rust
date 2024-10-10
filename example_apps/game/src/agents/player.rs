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

use std::{cell::RefCell, collections::VecDeque};

use swimos::agent::{
    agent_lifecycle::HandlerContext,
    event_handler::{EventHandler, HandlerActionExt},
    lanes::{CommandLane, MapLane, ValueLane},
    lifecycle, projections, AgentLaneModel,
};
use tracing::info;

use crate::generator::player::username_from_id;

use super::model::stats::{MatchSummary, PlayerTotals};

#[derive(AgentLaneModel)]
#[projections]
#[agent(transient, convention = "camel")]
pub struct PlayerAgent {
    // Total stats for this player across all matches
    stats: ValueLane<PlayerTotals>,
    // History of stats by timestamp
    stats_history: MapLane<u64, PlayerTotals>,
    // Add a match to the totals
    add_match: CommandLane<MatchSummary>,
}

#[derive(Debug, Clone, Default)]
pub struct PlayerLifecycle {
    timestamps: RefCell<VecDeque<u64>>,
}

impl PlayerLifecycle {
    pub fn new() -> Self {
        PlayerLifecycle {
            timestamps: Default::default(),
        }
    }

    fn update_timestamps(&self, timestamp: u64) -> impl EventHandler<PlayerAgent> + '_ {
        let context: HandlerContext<PlayerAgent> = Default::default();
        context
            .effect(move || {
                let mut guard = self.timestamps.borrow_mut();
                guard.push_back(timestamp);
                if guard.len() > 5 {
                    guard.pop_front()
                } else {
                    None
                }
            })
            .and_then(move |to_remove: Option<u64>| to_remove.map(remove_old).discard())
    }
}

fn remove_old(to_remove: u64) -> impl EventHandler<PlayerAgent> {
    let context: HandlerContext<PlayerAgent> = Default::default();
    context.remove(PlayerAgent::STATS_HISTORY, to_remove)
}

#[lifecycle(PlayerAgent)]
impl PlayerLifecycle {
    #[on_command(add_match)]
    fn add_match(
        &self,
        context: HandlerContext<PlayerAgent>,
        match_summary: &MatchSummary,
    ) -> impl EventHandler<PlayerAgent> + '_ {
        let match_summary = match_summary.clone();
        context
            .get_value(PlayerAgent::STATS)
            .and_then(move |mut current: PlayerTotals| {
                current.increment(&match_summary);
                context
                    .set_value(PlayerAgent::STATS, current.clone())
                    .followed_by(context.update(
                        PlayerAgent::STATS_HISTORY,
                        match_summary.end_time,
                        current,
                    ))
                    .followed_by(self.update_timestamps(match_summary.end_time))
            })
    }

    #[on_start]
    fn starting(&self, context: HandlerContext<PlayerAgent>) -> impl EventHandler<PlayerAgent> {
        context
            .get_agent_uri()
            .and_then(move |uri| context.effect(move || info!(uri = %uri, "Starting player agent")))
            .followed_by(
                context
                    .get_parameter("id")
                    .and_then(move |id: Option<String>| {
                        let id: usize = id.unwrap().parse().unwrap();
                        let username = username_from_id(id);
                        context.set_value(PlayerAgent::STATS, PlayerTotals::new(id, username))
                    }),
            )
            .followed_by(context.get_agent_uri().and_then(move |uri| {
                context.send_command(None, "/player", "addPlayer", uri)
            }))
    }

    #[on_stop]
    fn stopping(&self, context: HandlerContext<PlayerAgent>) -> impl EventHandler<PlayerAgent> {
        context
            .get_agent_uri()
            .and_then(move |uri| context.effect(move || info!(uri = %uri, "Stopping player agent")))
    }
}
