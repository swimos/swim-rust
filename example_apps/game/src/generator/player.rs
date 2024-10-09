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

use crate::generator::{config, gamertag};

#[derive(Debug)]
pub struct Player {
    pub id: usize,
    pub tag: String,
    pub ability: f32,
    pub xp: usize,
    pub level: usize,
}

impl Player {

    pub fn new(id: usize, ability: f32) -> Player {
        Player { id, tag: gamertag::from_id(id), ability, xp: 0, level: 0 }
    }

    pub fn add_xp(&mut self, xp: usize) {
        self.xp += xp;
        self.level = self.xp / config::XP_PER_LEVEL;
    }
    
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] 
    fn new_player() {
        let player = Player::new(0, 0.5);

        assert_eq!(0, player.id);
        assert_eq!("TheBigDog", player.tag);
        assert_eq!(0.5, player.ability);
        assert_eq!(0, player.xp);
        assert_eq!(0, player.level);
    }

    #[test] 
    fn level_up() {
        let mut player = Player::new(0, 0.5);
        player.add_xp(800);
        assert_eq!(800, player.xp);
        assert_eq!(1, player.level);
    }

}

