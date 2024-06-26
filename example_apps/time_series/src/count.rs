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
pub struct CountAgent {
    history: MapLane<u64, String>,
    add: CommandLane<String>,
}

pub struct CountLifecycle {
    max: usize,
    keys: RefCell<VecDeque<u64>>,
}

impl CountLifecycle {
    pub fn new(max: usize) -> CountLifecycle {
        CountLifecycle {
            max,
            keys: Default::default(),
        }
    }
}

#[lifecycle(CountAgent, no_clone)]
impl CountLifecycle {
    #[on_command(add)]
    pub fn add(
        &self,
        context: HandlerContext<CountAgent>,
        cmd: &str,
    ) -> impl EventHandler<CountAgent> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        context.update(CountAgent::HISTORY, now, cmd.to_string())
    }

    #[on_update(history)]
    pub fn on_update(
        &self,
        context: HandlerContext<CountAgent>,
        _map: &HashMap<u64, String>,
        key: u64,
        _prev: Option<String>,
        _new_value: &str,
    ) -> impl EventHandler<CountAgent> {
        let CountLifecycle { max, keys } = self;
        let timestamps = &mut *keys.borrow_mut();
        timestamps.push_front(key);

        let len = timestamps.len();
        let to_drop = if len > *max { len - *max } else { 0 };

        let handler = if to_drop > 0 {
            let keys = timestamps
                .split_off(to_drop)
                .into_iter()
                .take(to_drop)
                .map(move |key| context.remove(CountAgent::HISTORY, key));
            Some(Sequentially::new(keys))
        } else {
            None
        };

        handler.discard()
    }
}
