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

use crate::generator::{battle::Battle, player::Player};
use rand::Rng;
use rand_distr::Binomial;
use std::time::{SystemTime, UNIX_EPOCH};
use swimos_form::Form;

#[derive(Debug, Clone, Form)]
#[form(tag = "game", fields_convention = "camel")]
pub struct Game {
    pub id: usize,
    pub start_time: u64,
    pub end_time: u64,
    pub duration: u64,
    pub first_kill_time: u64,
    pub player_results: Vec<PlayerGame>,
}

#[derive(Debug, Clone, Form)]
#[form(tag = "playerGame", fields_convention = "camel")]
pub struct PlayerGame {
    pub id: usize,
    pub username: String,
    pub kills: usize,
    pub deaths: usize,
    pub assists: usize,
    pub xp_gained: usize,
    pub total_xp: usize,
    pub level: usize,
    pub winner: bool,
    pub team: String,
}

impl Game {
    pub fn generate(id: usize, players: Vec<&mut Player>) -> Game {
        // Generate the metadata
        let end_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let duration = generate_game_duration();
        let start_time = end_time - duration;
        let first_kill_time = generate_first_kill_time();

        let mut battle: Battle = players.into();
        battle.play();
        let player_results = battle.resolve();

        Game {
            id,
            start_time,
            end_time,
            duration,
            first_kill_time,
            player_results,
        }
    }
}

fn generate_game_duration() -> u64 {
    rand::thread_rng().sample(Binomial::new(30, 0.5).unwrap()) * 60000
        + rand::thread_rng().gen_range(0..60000)
}

fn generate_first_kill_time() -> u64 {
    rand::thread_rng().sample(Binomial::new(120, 0.3).unwrap()) * 1000
        + rand::thread_rng().gen_range(0..1000)
}
