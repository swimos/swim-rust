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

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use swimos::agent::event_handler::{ConstHandler, UnitHandler};
use swimos::{
    agent::event_handler::Sequentially,
    agent::lanes::CommandLane,
    agent::lanes::MapLane,
    agent::{
        agent_lifecycle::utility::HandlerContext,
        event_handler::{EventHandler, HandlerActionExt},
        lifecycle, projections, AgentLaneModel,
    },
};

#[derive(AgentLaneModel)]
#[projections]
pub struct ExampleAgent {
    history: MapLane<u64, String>,
    add: CommandLane<String>,
}

pub struct ExampleLifecycle {
    policy: RetentionPolicy,
}

impl ExampleLifecycle {
    pub fn new(policy: RetentionPolicy) -> ExampleLifecycle {
        ExampleLifecycle { policy }
    }
}

#[lifecycle(ExampleAgent, no_clone)]
impl ExampleLifecycle {
    #[on_update(history)]
    pub fn on_update(
        &self,
        context: HandlerContext<ExampleAgent>,
        map: &HashMap<u64, String>,
        key: u64,
        _prev: Option<String>,
        _new_value: &String,
    ) -> impl EventHandler<ExampleAgent> {
        self.policy.on_event(context, key, map.clone())
    }
}

// suppress the unconstructed variants warning as the variants may be constructed when using the
// example application.
#[allow(dead_code)]
pub enum RetentionPolicy {
    Count {
        max: usize,
    },
    Time {
        start: u64,
        interval: u64,
        keys: RefCell<VecDeque<u64>>,
    },
    Recency {
        start: u64,
        max: u64,
        keys: RefCell<VecDeque<u64>>,
        scheduled: Arc<AtomicBool>,
    },
}

impl RetentionPolicy {
    fn on_event(
        &self,
        context: HandlerContext<ExampleAgent>,
        key: u64,
        map: HashMap<u64, String>,
    ) -> impl EventHandler<ExampleAgent> {
        match self {
            RetentionPolicy::Count { max } => {
                let drop = map.len().checked_sub(*max).unwrap_or_default();
                let handler = if drop > 0 {
                    let keys = map
                        .into_iter()
                        .take(drop)
                        .map(move |(key, _value)| context.remove(ExampleAgent::HISTORY, key));
                    Some(Sequentially::new(keys))
                } else {
                    None
                };
                handler.discard().boxed()
            }
            RetentionPolicy::Time {
                start,
                interval,
                keys,
            } => {
                let timestamps = &mut *keys.borrow_mut();
                timestamps.push_back(key);

                let mut to_remove = Vec::new();

                timestamps.retain(|timestamp| {
                    if *start - *timestamp > *interval {
                        to_remove.push(context.remove(ExampleAgent::HISTORY, *timestamp));
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

                handler.discard().boxed()
            }
            RetentionPolicy::Recency {
                keys,
                scheduled,
                max,
                start,
            } => {
                let timestamps = &mut *keys.borrow_mut();
                timestamps.push_back(key);

                loop {
                    let handler = if !scheduled.swap(true, Ordering::Acquire) {
                        let handler = context
                            .effect(move || {
                                scheduled.store(false, Ordering::Release);

                                let mut to_remove = Vec::new();

                                timestamps.retain(|timestamp| {
                                    if *start - *timestamp > *max {
                                        to_remove.push(*timestamp);
                                        false
                                    } else {
                                        true
                                    }
                                });

                                to_remove
                            })
                            .and_then(|to_remove: Vec<u64>| {
                                let handler = if to_remove.is_empty() {
                                    None
                                } else {
                                    Some(Sequentially::new(to_remove.into_iter().map(
                                        |timestamp| {
                                            context.remove(ExampleAgent::HISTORY, timestamp)
                                        },
                                    )))
                                };

                                handler.discard().boxed()
                            });

                        Some(
                            context
                                .run_after(Duration::from_millis(*max), handler)
                                .discard(),
                        )
                    } else {
                        None
                    };
                    handler.boxed()
                }
            }
        }
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
