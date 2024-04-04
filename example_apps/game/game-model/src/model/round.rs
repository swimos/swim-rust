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

use std::time::{SystemTime, UNIX_EPOCH};
use swim::form::Form;
use crate::{battle::Battle, config, player::Player};

#[derive(Debug, Clone, Form)]
#[form(tag = "match", fields_convention = "camel")]
pub struct Round {
    pub id: usize,
    pub start_time: u64,
    pub end_time: u64,
    pub duration: u64,
    pub first_kill_time: u64,
    pub player_results: Vec<PlayerRound>
}

#[derive(Debug, Clone, Form)]
#[form(tag = "playerMatch", fields_convention = "camel")]
pub struct PlayerRound {
    pub id: usize,
    pub tag: String,
    pub kills: usize,
    pub deaths: usize,
    pub assists: usize,
    pub xp_gained: usize,
    pub total_xp: usize,
    pub level: usize,
    pub winner: bool,
    pub team: String,
}

impl Round {

    pub fn generate(id: usize, players: Vec<&mut Player>) -> Round {

        // Generate the metadata
        let end_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let duration = config::generate_game_duration();
        let start_time = end_time - duration;
        let first_kill_time = config::generate_first_kill_time();

        let mut battle = Battle::new(players);
        battle.play();
        let player_results = battle.resolve();

        Round { id, start_time, end_time, duration, first_kill_time, player_results }
    }

}
