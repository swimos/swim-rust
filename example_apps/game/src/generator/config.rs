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

use std::collections::HashSet;
use rand::Rng;
use rand_distr::{Beta, Binomial};
use crate::generator::player::Player;

pub const MAX_MATCH_ID: usize = 250;

pub const TEAMS: [&str; 2] = ["Hubble", "Donut"];

pub const MIN_PLAYERS_PER_GAME: usize = 50;

pub const MAX_PLAYERS_PER_GAME: usize = 100;

pub const UNIVERSE_PLAYER_COUNT: usize = 2048;

pub const XP_PER_LEVEL: usize = 800;

pub const XP_PER_WIN: usize = 200;

pub const XP_PER_KILL: usize = 100;

pub const XP_PER_ASSIST: usize = 50;

pub const ASSIST_RATIO: f32 = 0.2;

pub fn generate_game_duration() -> u64 {
    rand::thread_rng().sample(Binomial::new(30, 0.5).unwrap()) * 60000 +
    rand::thread_rng().gen_range(0..60000)
}

pub fn generate_first_kill_time() -> u64 {
    rand::thread_rng().sample(Binomial::new(120, 0.3).unwrap()) * 1000 +
    rand::thread_rng().gen_range(0..1000)
}

pub fn generate_team_name_pair() -> (&'static str, &'static str) {
    let first_team_index = rand::thread_rng().gen_range(0..TEAMS.len());
    let second_team_index = rand::thread_rng().gen_range(0..(TEAMS.len() - 1));

    if first_team_index != second_team_index {
        ( TEAMS[first_team_index], TEAMS[second_team_index] )
    } else {
        ( TEAMS[first_team_index], TEAMS[TEAMS.len() - 1] )
    }
}

pub fn generate_is_assist() -> bool {
    rand::thread_rng().gen_range(0.0..=1.0) < ASSIST_RATIO
}

pub fn generate_all_players() -> Vec<Player> {
    generate_players(UNIVERSE_PLAYER_COUNT)
}

pub fn generate_players(count: usize) -> Vec<Player> {
    (0..count)
        .map(|id| Player::new(id, rand::thread_rng().sample(Beta::new(1.2, 2.2).unwrap())))
        .collect()
}

pub fn generate_round_player_ids() -> HashSet<usize> {
    generate_unique_player_ids(generate_round_player_count())
}

pub fn generate_round_player_count() -> usize {
    rand::thread_rng().gen_range(0..(MAX_PLAYERS_PER_GAME - MIN_PLAYERS_PER_GAME)) + MIN_PLAYERS_PER_GAME
}

pub fn generate_unique_player_ids(count: usize) -> HashSet<usize> {
    let mut ids = HashSet::with_capacity(count);
    while ids.len() < count {
        ids.insert(generate_player_id());
    }
    ids
}

pub fn generate_player_id() -> usize {
    rand::thread_rng().gen_range(0..UNIVERSE_PLAYER_COUNT)
}
