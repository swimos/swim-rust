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

use game_model::round::{PlayerRound, Round};
use std::collections::HashMap;
use swimos_form::Form;

#[derive(Form, Default, Clone, Debug)]
#[form(fields_convention = "camel")]
pub struct LeaderboardTotals {
    pub player_count: usize,
}

#[derive(Form, Clone, Debug)]
#[form(fields_convention = "camel")]
pub struct MatchSummary {
    pub match_id: usize,
    pub duration: u64,
    pub start_time: u64,
    pub end_time: u64,
    pub first_kill_time: u64,
    pub stats: MatchStats,
    pub team_stats: HashMap<String, MatchStats>,
    pub player_stats: HashMap<usize, MatchStats>,
}

impl MatchSummary {
    pub fn from_round(round: Round) -> MatchSummary {
        MatchSummary {
            match_id: round.id,
            duration: round.duration,
            start_time: round.start_time,
            end_time: round.end_time,
            first_kill_time: round.first_kill_time,
            stats: MatchStats::match_stats_from_round(&round),
            team_stats: MatchStats::team_stats_from_round(&round),
            player_stats: MatchStats::player_stats_from_round(&round),
        }
    }
}

#[derive(Form, Default, Clone, Debug)]
#[form(fields_convention = "camel")]
pub struct MatchStats {
    pub name: String,
    pub kills: usize,
    pub deaths: usize,
    pub assists: usize,
    pub xp_gained: usize,
    pub total_xp: usize,
    pub level: usize,
    pub winner: bool,
    pub player_count: usize,
    pub team: String,
}

impl MatchStats {
    fn new(name: String, team: String) -> Self {
        Self {
            name,
            team,
            ..Default::default()
        }
    }

    fn match_stats_from_round(round: &Round) -> MatchStats {
        let mut stats = MatchStats::default();
        round
            .player_results
            .iter()
            .for_each(|player_round| stats.increment(player_round));
        stats
    }

    fn team_stats_from_round(round: &Round) -> HashMap<String, MatchStats> {
        let mut team_stats = HashMap::new();
        round.player_results.iter().for_each(|player_round| {
            team_stats
                .entry(player_round.team.clone())
                .or_insert(MatchStats::new(
                    player_round.team.clone(),
                    player_round.team.clone(),
                ))
                .increment(player_round)
        });
        team_stats
    }

    fn player_stats_from_round(round: &Round) -> HashMap<usize, MatchStats> {
        let mut player_stats = HashMap::new();
        round.player_results.iter().for_each(|player_round| {
            let mut stats = MatchStats::new(player_round.tag.clone(), player_round.team.clone());
            stats.increment(player_round);
            player_stats.insert(player_round.id, stats);
        });
        player_stats
    }

    fn increment(&mut self, player_round: &PlayerRound) {
        self.kills += player_round.kills;
        self.deaths += player_round.deaths;
        self.assists += player_round.assists;
        self.xp_gained += player_round.xp_gained;
        self.total_xp += player_round.total_xp;
        self.level += player_round.level;
        self.winner |= player_round.winner;
        self.player_count += 1;
    }
}

#[derive(Default, Form, Clone)]
#[form(fields_convention = "camel")]
pub struct MatchTotals {
    pub game_count: usize,
    pub total_duration: u64,
    pub total_first_kill_time: u64,
    pub total_kills: usize,
    pub total_deaths: usize,
    pub total_assists: usize,
    pub total_xp_gained: usize,
    pub total_player_count: usize,
}

impl MatchTotals {
    pub fn increment(&mut self, match_summary: &MatchSummary) {
        self.game_count += 1;
        self.total_duration += match_summary.duration;
        self.total_first_kill_time += match_summary.first_kill_time;
        self.total_kills += match_summary.stats.kills;
        self.total_deaths += match_summary.stats.deaths;
        self.total_assists += match_summary.stats.assists;
        self.total_xp_gained += match_summary.stats.xp_gained;
        self.total_player_count += match_summary.stats.player_count;
    }
}

#[derive(Default, Debug, Clone, Form)]
#[form(fields_convention = "camel")]
pub struct PlayerTotals {
    pub id: usize,
    pub tag: String,
    pub game_count: usize,
    // pub total_duration: u64,
    // pub last_online: u64,
    pub total_kills: usize,
    pub total_deaths: usize,
    pub total_assists: usize,
    pub kd_ratio: f64,
    pub total_xp: usize,
    pub level: usize,
    pub win_count: usize,
    pub loss_count: usize,
    pub team_count: HashMap<String, usize>,
}

impl PlayerTotals {
    pub fn new(id: usize, tag: String) -> Self {
        Self {
            id,
            tag,
            ..Default::default()
        }
    }

    pub fn increment(&mut self, match_summary: &MatchSummary) {
        self.game_count += 1;
        // self.total_duration += match_summary.duration;
        // self.last_online = match_summary.end_time;
        let player_stats = match_summary.player_stats.get(&self.id).unwrap();
        self.total_kills += player_stats.kills;
        self.total_deaths += player_stats.deaths;
        self.total_assists += player_stats.assists;
        self.kd_ratio = if self.total_deaths == 0 {
            self.total_kills as f64
        } else {
            self.total_kills as f64 / self.total_deaths as f64
        };
        self.total_xp = player_stats.total_xp;
        self.level = player_stats.level;
        if player_stats.winner {
            self.win_count += 1;
        } else {
            self.loss_count += 1;
        }
        let team_count = self
            .team_count
            .entry(player_stats.team.clone())
            .or_default();
        *team_count += 1;
    }
}

#[derive(Default, Form, Clone)]
#[form(fields_convention = "camel")]
pub struct TeamTotals {
    pub name: String,
    pub game_count: usize,
    pub win_count: usize,
    pub loss_count: usize,
    pub total_duration: u64,
    pub total_kills: usize,
    pub total_deaths: usize,
    pub total_assists: usize,
    pub kd_ratio: f64,
    pub total_xp_gained: usize,
}

impl TeamTotals {
    pub fn new(name: String) -> Self {
        Self {
            name,
            ..Default::default()
        }
    }

    pub fn increment(&mut self, match_summary: &MatchSummary) {
        self.game_count += 1;
        self.total_duration += match_summary.duration;
        let team_stats = match_summary.team_stats.get(&self.name).unwrap();
        if team_stats.winner {
            self.win_count += 1;
        } else {
            self.loss_count += 1;
        }
        self.total_kills += team_stats.kills;
        self.total_deaths += team_stats.deaths;
        self.total_assists += team_stats.assists;
        self.kd_ratio = if self.total_deaths == 0 {
            self.total_kills as f64
        } else {
            self.total_kills as f64 / self.total_deaths as f64
        };
        self.total_xp_gained += team_stats.xp_gained;
    }
}
