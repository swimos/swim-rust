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

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use swimos::{
    agent::event_handler::Sequentially,
    agent::lanes::CommandLane,
    agent::lanes::MapLane,
    agent::{
        agent_lifecycle::HandlerContext,
        event_handler::{EventHandler, HandlerActionExt},
        lifecycle, projections, AgentLaneModel,
    },
};

#[derive(AgentLaneModel)]
#[projections]
pub struct TimeAgent {
    history: MapLane<u64, String>,
    add: CommandLane<String>,
}

pub struct TimeLifecycle {
    interval: u64,
    keys: RefCell<VecDeque<u64>>,
}

impl TimeLifecycle {
    pub fn new(interval: u64) -> TimeLifecycle {
        TimeLifecycle {
            interval,
            keys: Default::default(),
        }
    }
}

#[lifecycle(TimeAgent, no_clone)]
impl TimeLifecycle {
    #[on_command(add)]
    pub fn add(
        &self,
        context: HandlerContext<TimeAgent>,
        cmd: &str,
    ) -> impl EventHandler<TimeAgent> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        context.update(TimeAgent::HISTORY, now, cmd.to_string())
    }

    #[on_update(history)]
    pub fn on_update(
        &self,
        context: HandlerContext<TimeAgent>,
        _map: &HashMap<u64, String>,
        key: u64,
        _prev: Option<String>,
        _new_value: &str,
    ) -> impl EventHandler<TimeAgent> {
        let TimeLifecycle { interval, keys } = self;
        let timestamps = &mut *keys.borrow_mut();
        timestamps.push_back(key);
        let mut to_remove = Vec::new();

        let start = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        timestamps.retain(|timestamp| {
            if start - *timestamp > *interval {
                to_remove.push(context.remove(TimeAgent::HISTORY, *timestamp));
                false
            } else {
                true
            }
        });
        let handler = if to_remove.is_empty() {
            None
        } else {
            Some(Sequentially::new(to_remove))
        };

        handler.discard()
    }
}
