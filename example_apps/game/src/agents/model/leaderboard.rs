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

use std::{cmp::Ordering, sync::Arc};

use super::stats::PlayerTotals;

#[derive(Clone)]
pub struct Leaderboard {
    comparator: Arc<dyn Fn(&PlayerTotals, &PlayerTotals) -> Ordering + Sync + Send>,
    // The players ordered by some comparator
    items: Vec<PlayerTotals>,
    // The index of each player in the items vector
    indexes: Vec<Option<usize>>
}

impl Leaderboard {
    
    pub fn kill_count(capacity: usize) -> Self {
        Leaderboard::new(capacity, Arc::new(|p1, p2| p1.total_kills.cmp(&p2.total_kills)))
    }

    pub fn death_count(capacity: usize) -> Self {
        Leaderboard::new(capacity, Arc::new(|p1, p2| p1.total_deaths.cmp(&p2.total_deaths)))
    }

    pub fn kd_ratio(capacity: usize) -> Self {
        Leaderboard::new(capacity, Arc::new(|p1, p2| p1.kd_ratio.partial_cmp(&p2.kd_ratio)
            .unwrap_or_else(|| p1.total_kills.cmp(&p2.total_kills))))
    }

    pub fn xp(capacity: usize) -> Self {
        Leaderboard::new(capacity, Arc::new(|p1, p2| p1.total_xp.cmp(&p2.total_xp)))
    }

    pub fn new(capacity: usize, f: Arc<dyn Fn(&PlayerTotals, &PlayerTotals) -> Ordering + Sync + Send>) -> Self {
        Self {
            comparator: f,
            items: Vec::with_capacity(capacity),
            indexes: vec![None; capacity]
        }
    }

    pub fn update(&mut self, player_totals: PlayerTotals) {
        let current_position = match self.indexes.get(player_totals.id).unwrap() {
            None => {
                // First time seeing this player
                let insert_index = self.items.len();
                self.indexes[player_totals.id] = Some(insert_index);
                self.items.push(player_totals);
                insert_index
            },
            Some(i) => {
                self.items[*i] = player_totals;
                *i
            },
        };
        self.resort_item(current_position);
    }

    fn resort_item(&mut self, index: usize) {
        if !self.sort_down(index) {
            self.sort_up(index);
        }
    }

    fn sort_up(&mut self, index: usize) -> bool {
        if index == self.items.len() - 1 {
            return false;
        };

        let mover_stats = self.items.get(index).unwrap();
        if (self.comparator)(mover_stats, self.items.get(index + 1).unwrap()) != Ordering::Less {
            return false;
        }

        self.swap(index, index + 1);
        self.sort_up(index + 1);
        true
    }

    fn sort_down(&mut self, index: usize) -> bool {
        if index == 0 {
            return false;
        };

        let mover_stats = self.items.get(index).unwrap();
        if (self.comparator)(mover_stats, self.items.get(index - 1).unwrap()) != Ordering::Greater {
            return false;
        };
        
        self.swap(index, index - 1);
        self.sort_down(index - 1);
        true
    }

    fn swap(&mut self, index1: usize, index2: usize) {
        self.items.swap(index1, index2);
        self.indexes[self.items[index1].id] = Some(index1);
        self.indexes[self.items[index2].id] = Some(index2);
    }

    pub fn get(&self, start: usize, count: usize) -> Vec<PlayerTotals> {
        self.items.iter().skip(start).take(count).map(|item| item.clone()).collect()
    }


}