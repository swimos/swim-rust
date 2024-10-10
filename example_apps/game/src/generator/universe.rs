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

use std::collections::HashMap;

use crate::generator::{game::Game, player::Player};
use rand::prelude::SliceRandom;
use rand::Rng;

pub const UNIVERSE_PLAYER_COUNT: usize = 2048;
pub const MAX_GAME_ID: usize = 250;
pub const MIN_PLAYERS_PER_GAME: usize = 50;
pub const MAX_PLAYERS_PER_GAME: usize = 100;

#[derive(Debug)]
pub struct Universe {
    players: HashMap<usize, Player>,
    next_game_id: usize,
}

impl Default for Universe {
    fn default() -> Self {
        Universe {
            players: generate_players(UNIVERSE_PLAYER_COUNT),
            next_game_id: 0,
        }
    }
}

impl Universe {
    pub fn generate_game(&mut self) -> Game {
        Game::generate(self.new_game_id(), self.pick_players_for_game())
    }

    fn new_game_id(&mut self) -> usize {
        let id = self.next_game_id;
        self.next_game_id += 1;
        if self.next_game_id == MAX_GAME_ID {
            self.next_game_id = 0;
        }
        id
    }

    fn pick_players_for_game(&mut self) -> Vec<&mut Player> {
        let count = generate_game_player_count();

        let mut players: Vec<&mut Player> = self.players.values_mut().collect();
        let mut rng = rand::thread_rng();
        players.shuffle(&mut rng);

        players.into_iter().take(count).collect()
    }
}

fn generate_players(count: usize) -> HashMap<usize, Player> {
    let mut players = HashMap::new();
    for id in 0..count {
        players.insert(id, Player::generate(id));
    }
    players
}

fn generate_game_player_count() -> usize {
    rand::thread_rng().gen_range(0..=(MAX_PLAYERS_PER_GAME - MIN_PLAYERS_PER_GAME))
        + MIN_PLAYERS_PER_GAME
}
