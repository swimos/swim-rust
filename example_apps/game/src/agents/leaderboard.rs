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

use std::{cell::RefCell, collections::HashMap, time::Duration};

use swimos::{
    agent::{
        agent_lifecycle::HandlerContext,
        event_handler::{EventHandler, HandlerAction, HandlerActionExt},
        lanes::{CommandLane, DemandLane, JoinValueLane, ValueLane},
        lifecycle, projections, AgentLaneModel,
    },
    route::RouteUri,
};
use tracing::info;

use crate::generator::universe::UNIVERSE_PLAYER_COUNT;

use super::model::{
    leaderboard::PlayerLeaderboard,
    stats::{LeaderboardTotals, PlayerTotals},
};

#[derive(AgentLaneModel)]
#[projections]
#[agent(transient, convention = "camel")]
pub struct LeaderboardAgent {
    // Total stats for this leaderboard
    stats: ValueLane<LeaderboardTotals>,
    // Parameterized leaderboard
    leaderboard: DemandLane<Vec<PlayerTotals>>,
    // Join lane of all players
    players: JoinValueLane<String, PlayerTotals>,
    // Add player to join lane
    add_player: CommandLane<RouteUri>,
}

#[derive(Clone, Default)]
pub struct LeaderboardLifecycle {
    leaderboards: RefCell<HashMap<String, PlayerLeaderboard>>,
}

impl LeaderboardLifecycle {
    pub fn new() -> Self {
        let mut leaderboards = HashMap::new();
        leaderboards.insert(
            "kd".to_string(),
            PlayerLeaderboard::kd_ratio(UNIVERSE_PLAYER_COUNT),
        );
        leaderboards.insert(
            "kc".to_string(),
            PlayerLeaderboard::kill_count(UNIVERSE_PLAYER_COUNT),
        );
        leaderboards.insert(
            "dc".to_string(),
            PlayerLeaderboard::death_count(UNIVERSE_PLAYER_COUNT),
        );
        leaderboards.insert(
            "xp".to_string(),
            PlayerLeaderboard::xp(UNIVERSE_PLAYER_COUNT),
        );
        Self {
            leaderboards: RefCell::new(leaderboards),
        }
    }
}

#[lifecycle(LeaderboardAgent)]
impl LeaderboardLifecycle {
    #[on_command(add_player)]
    fn add_player(
        &self,
        context: HandlerContext<LeaderboardAgent>,
        player_uri: &RouteUri,
    ) -> impl EventHandler<LeaderboardAgent> + '_ {
        context
            .add_downlink(
                LeaderboardAgent::PLAYERS,
                player_uri.clone().to_string(),
                None,
                &player_uri.to_string(),
                "stats",
            )
            .followed_by(
                context
                    .get_map(LeaderboardAgent::PLAYERS)
                    .map(|m: HashMap<String, PlayerTotals>| m.len())
                    .and_then(move |size| {
                        context.set_value(
                            LeaderboardAgent::STATS,
                            LeaderboardTotals { player_count: size },
                        )
                    }),
            )
    }

    #[on_cue(leaderboard)]
    fn leaderboard_request(
        &self,
        context: HandlerContext<LeaderboardAgent>,
    ) -> impl HandlerAction<LeaderboardAgent, Completion = Vec<PlayerTotals>> + '_ {
        context.effect(move || {
            let result = self
                .leaderboards
                .try_borrow()
                .unwrap()
                .get("kd")
                .unwrap()
                .get(0, 20);
            result
        })
    }

    #[on_update(players)]
    fn player_update<'a>(
        &'a self,
        context: HandlerContext<LeaderboardAgent>,
        _map: &HashMap<String, PlayerTotals>,
        _key: String,
        _previous_value: Option<PlayerTotals>,
        new_value: &PlayerTotals,
    ) -> impl EventHandler<LeaderboardAgent> + 'a {
        let update = new_value.clone();
        context.effect(move || {
            self.leaderboards
                .borrow_mut()
                .values_mut()
                .for_each(|leadboard| leadboard.update(update.clone()))
        })
    }

    #[on_start]
    fn starting(
        &self,
        context: HandlerContext<LeaderboardAgent>,
    ) -> impl EventHandler<LeaderboardAgent> {
        context
            .get_agent_uri()
            .and_then(move |uri| {
                context.effect(move || info!(uri = %uri, "Starting leaderboard agent"))
            })
            .followed_by(
                context.schedule_repeatedly(Duration::from_secs(3), move || {
                    Some(context.cue(LeaderboardAgent::LEADERBOARD))
                }),
            )
    }

    #[on_stop]
    fn stopping(
        &self,
        context: HandlerContext<LeaderboardAgent>,
    ) -> impl EventHandler<LeaderboardAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || info!(uri = %uri, "Stopping leaderboard agent"))
        })
    }
}
