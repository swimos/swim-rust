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

const TAG_FIRST_WORDS: [&str; 16] = ["The", "Elite", "A", "Friendly", "My", "Some", "IAm", "Just", "Simply", "Magical", "Mythical", "Tranquil", "OverThe", "UnderThe", "Homeward", "Outbound"];
const TAG_SECOND_WORDS: [&str; 16] = ["Big", "Red", "Skilled", "Elite", "Huge", "Tiny", "Night", "Day", "Tall", "Short", "Flying", "Bright", "Evil", "Fun", "Sassy", "Fire"];
const TAG_THIRD_WORDS: [&str; 16] = ["Dog", "Cat", "Lion", "Skull", "Poet", "Crocodile", "Bear", "Elite", "Gamer", "Pig", "Diamond", "Wizard", "Chicken", "Judge", "Artist", "Sky"];


pub fn from_id(id: usize) -> String {

    let mut gamer_tag = String::new();
    gamer_tag.push_str(TAG_FIRST_WORDS[id % 16]);
    gamer_tag.push_str(TAG_SECOND_WORDS[(id % 256) / 16]);
    gamer_tag.push_str(TAG_THIRD_WORDS[(id % 4096) / 256]);

    if id >= 4096 {
        gamer_tag.push_str(&(id / 4096).to_string());
    }

    gamer_tag
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_id_0() {
        assert_eq!("TheBigDog", from_id(0));
    }

    #[test]
    fn from_id_1() {
        assert_eq!("EliteBigDog", from_id(1));
    }

    #[test]
    fn from_id_16() {
        assert_eq!("TheRedDog", from_id(16));
    }

    #[test]
    fn from_id_256() {
        assert_eq!("TheBigCat", from_id(256));
    }

    #[test]
    fn from_id_4095() {
        assert_eq!("OutboundFireSky", from_id(4095));
    }

    #[test]
    fn from_id_4096() {
        assert_eq!("TheBigDog1", from_id(4096));
    }

    #[test]
    fn from_id_4097() {
        assert_eq!("EliteBigDog1", from_id(4097));
    }

}