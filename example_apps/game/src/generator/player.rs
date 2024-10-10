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

use rand::Rng;
use rand_distr::Beta;

pub const XP_PER_LEVEL: usize = 800;

#[derive(Debug)]
pub struct Player {
    pub id: usize,
    pub username: String,
    pub ability: f32,
    pub xp: usize,
    pub level: usize,
}

impl Player {
    pub fn new(id: usize, ability: f32) -> Self {
        Player {
            id,
            username: username_from_id(id),
            ability,
            xp: 0,
            level: 0,
        }
    }

    pub fn generate(id: usize) -> Self {
        Self::new(id, rand::thread_rng().sample(Beta::new(1.2, 2.2).unwrap()))
    }

    pub fn add_xp(&mut self, xp: usize) {
        self.xp += xp;
        self.level = self.xp / XP_PER_LEVEL;
    }
}

// Username generation

const NAME_POOL_SIZE: usize = 16;
const TAG_FIRST_WORDS: [&str; NAME_POOL_SIZE] = [
    "The", "Elite", "A", "Friendly", "My", "Some", "IAm", "Just", "Simply", "Magical", "Mythical",
    "Tranquil", "OverThe", "UnderThe", "Homeward", "Outbound",
];
const TAG_SECOND_WORDS: [&str; NAME_POOL_SIZE] = [
    "Big", "Red", "Skilled", "Elite", "Huge", "Tiny", "Night", "Day", "Tall", "Short", "Flying",
    "Bright", "Evil", "Fun", "Sassy", "Fire",
];
const TAG_THIRD_WORDS: [&str; NAME_POOL_SIZE] = [
    "Dog",
    "Cat",
    "Lion",
    "Skull",
    "Poet",
    "Crocodile",
    "Bear",
    "Elite",
    "Gamer",
    "Pig",
    "Diamond",
    "Wizard",
    "Chicken",
    "Judge",
    "Artist",
    "Sky",
];

pub fn username_from_id(id: usize) -> String {
    let mut username = String::new();
    username.push_str(TAG_FIRST_WORDS[id % NAME_POOL_SIZE.pow(1)]);
    username.push_str(TAG_SECOND_WORDS[(id % NAME_POOL_SIZE.pow(2)) / NAME_POOL_SIZE]);
    username.push_str(TAG_THIRD_WORDS[(id % NAME_POOL_SIZE.pow(3)) / NAME_POOL_SIZE.pow(2)]);

    // If the ID exceeds the limit of names we can generate, append a number
    if id >= NAME_POOL_SIZE.pow(3) {
        username.push_str(&(id / NAME_POOL_SIZE.pow(3)).to_string());
    }
    username
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_player() {
        let player = Player::new(0, 0.5);

        assert_eq!(0, player.id);
        assert_eq!("TheBigDog", player.username);
        assert_eq!(0.5, player.ability);
        assert_eq!(0, player.xp);
        assert_eq!(0, player.level);
    }

    #[test]
    fn generate() {
        let player = Player::generate(0);

        assert_eq!(0, player.id);
        assert_eq!("TheBigDog", player.username);
        assert!(player.ability > 0.0);
        assert_eq!(0, player.xp);
        assert_eq!(0, player.level);
    }

    #[test]
    fn level_up() {
        let mut player = Player::new(0, 0.5);
        player.add_xp(XP_PER_LEVEL);
        assert_eq!(XP_PER_LEVEL, player.xp);
        assert_eq!(1, player.level);
    }

    #[test]
    fn username_from_id_0() {
        assert_eq!("TheBigDog", username_from_id(0));
    }

    #[test]
    fn username_from_id_1() {
        assert_eq!("EliteBigDog", username_from_id(1));
    }

    #[test]
    fn username_from_id_16() {
        assert_eq!("TheRedDog", username_from_id(16));
    }

    #[test]
    fn username_from_id_256() {
        assert_eq!("TheBigCat", username_from_id(256));
    }

    #[test]
    fn username_from_id_4095() {
        assert_eq!("OutboundFireSky", username_from_id(4095));
    }

    #[test]
    fn username_from_id_4096() {
        assert_eq!("TheBigDog1", username_from_id(4096));
    }

    #[test]
    fn username_from_id_4097() {
        assert_eq!("EliteBigDog1", username_from_id(4097));
    }
}
