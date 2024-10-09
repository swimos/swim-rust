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


use crate::generator::{player::Player, round::Round};
use crate::generator::config;


#[derive(Debug, Default)]
pub struct Game {
    players: Vec<Player>,
    match_id: usize,
}

impl Game {

    pub fn new() -> Game {
        Game { players: config::generate_all_players(), match_id: 0 }
    }

    pub fn generate_round(&mut self) -> Round {

        let id = self.match_id;
        self.match_id += 1;
        if self.match_id == config::MAX_MATCH_ID {
            self.match_id = 0;
        }

        Round::generate(id, self.generate_round_players())
    }
    
    fn generate_round_players(&mut self) -> Vec<&mut Player> {
        let ids = config::generate_round_player_ids();
        // TODO: Has to be a better way of doing this? 
        // Maybe immutable references with mutable interior state?
        self.players
            .iter_mut()
            .filter(|player| ids.contains(&player.id))
            .collect()
    }

}
