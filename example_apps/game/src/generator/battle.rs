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

use rand::{seq::SliceRandom, Rng};

use crate::generator::{game::PlayerGame, player::Player};

pub const TEAMS: [&str; 2] = ["Hubble", "Donut"];
pub const XP_PER_WIN: usize = 200;
pub const XP_PER_KILL: usize = 100;
pub const XP_PER_ASSIST: usize = 50;
pub const ASSIST_RATIO: f32 = 0.2;

pub struct Battle<'a> {
    team1: Team<'a>,
    team2: Team<'a>,
}

impl<'a> From<Vec<&'a mut Player>> for Battle<'a> {
    fn from(mut players1: Vec<&'a mut Player>) -> Self {
        let (team1, team2) = generate_team_name_pair();
        let players2 = players1.split_off(players1.len() / 2);
        Battle {
            team1: Team::new(team1, players1),
            team2: Team::new(team2, players2),
        }
    }
}

impl<'a> Battle<'a> {
    pub fn play(&mut self) {
        while self.team1.is_alive() && self.team2.is_alive() {
            duel(&mut self.team1, &mut self.team2);
        }
    }

    pub fn resolve(self) -> Vec<PlayerGame> {
        self.team1
            .resolve()
            .into_iter()
            .chain(self.team2.resolve())
            .collect()
    }
}

fn duel(team1: &mut Team, team2: &mut Team) {
    let mut player1 = team1.take_random_alive();
    let mut player2 = team2.take_random_alive();

    let player1_ability = player1.get_player_ability();
    let player2_ability = player2.get_player_ability();

    let player1_win_probability = player1_ability / (player1_ability + player2_ability);
    let player1_is_winner = rand::thread_rng().gen_range(0.0..=1.0) < player1_win_probability;

    if player1_is_winner {
        player1.increment_kills();
        team2.add_dead(player2);
        if generate_is_assist() {
            team1.assign_random_assist()
        }
        team1.add_alive(player1);
    } else {
        player2.increment_kills();
        team1.add_dead(player1);
        if generate_is_assist() {
            team2.assign_random_assist()
        }
        team2.add_alive(player2);
    }
}

struct Team<'a> {
    name: &'static str,
    alive: Vec<PlayerBattle<'a>>,
    dead: Vec<PlayerBattle<'a>>,
}

impl<'a> Team<'a> {
    fn new(name: &'static str, players: Vec<&'a mut Player>) -> Team<'a> {
        let alive: Vec<PlayerBattle> = players.into_iter().map(PlayerBattle::from).collect();
        let dead = Vec::with_capacity(alive.len());
        Team { name, alive, dead }
    }

    fn is_alive(&self) -> bool {
        !self.alive.is_empty()
    }

    fn add_alive(&mut self, player: PlayerBattle<'a>) {
        self.alive.push(player);
    }

    fn add_dead(&mut self, player: PlayerBattle<'a>) {
        self.dead.push(player);
    }

    fn take_random_alive(&mut self) -> PlayerBattle<'a> {
        self.alive
            .swap_remove(rand::thread_rng().gen_range(0..self.alive.len()))
    }

    fn assign_random_assist(&mut self) {
        let mut rng = rand::thread_rng();
        let assister = self.alive.choose_mut(&mut rng);
        if let Some(player) = assister {
            player.increment_assists();
        }
    }

    fn resolve(self) -> Vec<PlayerGame> {
        let is_winner = self.is_alive();
        self.alive
            .into_iter()
            .map(|player| player.resolve(self.name, is_winner, false))
            .chain(
                self.dead
                    .into_iter()
                    .map(|player| player.resolve(self.name, is_winner, true)),
            )
            .collect()
    }
}

struct PlayerBattle<'a> {
    player: &'a mut Player,
    kills: usize,
    assists: usize,
}

impl<'a> From<&'a mut Player> for PlayerBattle<'a> {
    fn from(player: &'a mut Player) -> Self {
        PlayerBattle {
            player,
            kills: 0,
            assists: 0,
        }
    }
}

impl<'a> PlayerBattle<'a> {
    fn increment_kills(&mut self) {
        self.kills += 1;
    }

    fn increment_assists(&mut self) {
        self.assists += 1;
    }

    fn get_player_ability(&self) -> f32 {
        self.player.ability
    }

    fn resolve(self, team_name: &str, is_winner: bool, is_dead: bool) -> PlayerGame {
        // Add up xp and award the player
        let mut xp = 0;
        if is_winner {
            xp += XP_PER_WIN;
        }
        xp += self.kills * XP_PER_KILL;
        xp += self.assists * XP_PER_ASSIST;

        self.player.add_xp(xp);

        PlayerGame {
            id: self.player.id,
            username: self.player.username.clone(),
            kills: self.kills,
            deaths: if is_dead { 1 } else { 0 },
            assists: self.assists,
            xp_gained: xp,
            total_xp: self.player.xp,
            level: self.player.level,
            winner: is_winner,
            team: String::from(team_name),
        }
    }
}

fn generate_is_assist() -> bool {
    rand::thread_rng().gen_range(0.0..=1.0) < ASSIST_RATIO
}

fn generate_team_name_pair() -> (&'static str, &'static str) {
    let first_team_index = rand::thread_rng().gen_range(0..TEAMS.len());
    let second_team_index = rand::thread_rng().gen_range(0..(TEAMS.len() - 1));

    if first_team_index != second_team_index {
        (TEAMS[first_team_index], TEAMS[second_team_index])
    } else {
        (TEAMS[first_team_index], TEAMS[TEAMS.len() - 1])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::BorrowMut;

    #[test]
    fn player_resolve() {
        let mut player = Player::new(0, 0.5);
        player.xp = 500;

        let result = {
            let mut player_context = PlayerBattle::from(&mut player);

            player_context.increment_kills();
            player_context.increment_assists();
            player_context.increment_assists();

            player_context.resolve("TEST", true, true)
        };

        assert_eq!(player.xp, 900);

        assert_eq!(0, result.id);
        assert_eq!(1, result.kills);
        assert_eq!(1, result.deaths);
        assert_eq!(2, result.assists);
        assert_eq!(400, result.xp_gained);
        assert_eq!(900, result.total_xp);
        assert_eq!(1, result.level);
        assert_eq!(true, result.winner);
        assert_eq!("TEST", result.team);
    }

    #[test]
    fn team_duel_resolve() {
        let mut winner = Player::new(0, 1.0);
        let mut loser = Player::new(1, 0.0);

        let mut winning_team = Team::new("WIN", vec![winner.borrow_mut()]);
        let mut losing_team = Team::new("LOSE", vec![loser.borrow_mut()]);

        duel(&mut winning_team, &mut losing_team);

        assert!(winning_team.is_alive());
        assert!(!losing_team.is_alive());

        winning_team.resolve();
        losing_team.resolve();

        assert_eq!(300, winner.xp);
        assert_eq!(0, loser.xp);
    }

    #[test]
    fn battle_resolve() {
        let mut winner = Player::new(0, 1.0);
        let mut loser = Player::new(1, 0.0);

        let mut battle = Battle::from(vec![winner.borrow_mut(), loser.borrow_mut()]);
        battle.play();
        let result = battle.resolve();

        assert_eq!(300, winner.xp);
        assert_eq!(0, loser.xp);

        let winning_result = result.get(0).unwrap();
        let losing_result = result.get(1).unwrap();

        assert_eq!(0, winning_result.id);
        assert_eq!(1, winning_result.kills);
        assert_eq!(0, winning_result.deaths);
        assert_eq!(0, winning_result.assists);
        assert_eq!(300, winning_result.xp_gained);
        assert_eq!(300, winning_result.total_xp);
        assert_eq!(0, winning_result.level);
        assert_eq!(true, winning_result.winner);

        assert_eq!(1, losing_result.id);
        assert_eq!(0, losing_result.kills);
        assert_eq!(1, losing_result.deaths);
        assert_eq!(0, losing_result.assists);
        assert_eq!(0, losing_result.xp_gained);
        assert_eq!(0, losing_result.total_xp);
        assert_eq!(0, losing_result.level);
        assert_eq!(false, losing_result.winner);
    }
}
