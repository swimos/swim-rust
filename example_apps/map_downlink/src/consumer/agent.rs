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

use example_util::format_map;
use swimos::agent::{
    agent_lifecycle::utility::HandlerContext,
    agent_model::downlink::hosted::MapDownlinkHandle,
    event_handler::{EventHandler, HandlerAction, HandlerActionExt},
    lanes::{CommandLane, ValueLane},
    lifecycle, projections,
    state::State,
    AgentLaneModel,
};

use super::model::Instruction;

#[derive(AgentLaneModel)]
#[projections]
pub struct ConsumerAgent {
    lane: ValueLane<i32>,
    instruct: CommandLane<Instruction>,
}

pub struct ConsumerLifecycle {
    port: u16,
    key: String,
    handle: State<ConsumerAgent, Option<MapDownlinkHandle<String, i32>>>,
}

impl ConsumerLifecycle {
    pub fn new(port: u16, key: String) -> Self {
        ConsumerLifecycle {
            port,
            key,
            handle: State::default(),
        }
    }
}

#[lifecycle(ConsumerAgent, no_clone)]
impl ConsumerLifecycle {
    #[on_start]
    pub fn on_start(
        &self,
        context: HandlerContext<ConsumerAgent>,
    ) -> impl EventHandler<ConsumerAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || {
                println!("Starting consumer agent at: {}", uri);
            })
        })
    }

    #[on_stop]
    pub fn on_stop(
        &self,
        context: HandlerContext<ConsumerAgent>,
    ) -> impl EventHandler<ConsumerAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || {
                println!("Stopping consumer agent at: {}", uri);
            })
        })
    }

    #[on_event(lane)]
    pub fn on_event(
        &self,
        context: HandlerContext<ConsumerAgent>,
        value: &i32,
    ) -> impl EventHandler<ConsumerAgent> {
        let n = *value;
        context.effect(move || {
            println!("Setting value on consumer to: {}", n);
        })
    }

    #[on_command(instruct)]
    pub fn instruct<'a>(
        &'a self,
        context: HandlerContext<ConsumerAgent>,
        command: &Instruction,
    ) -> impl EventHandler<ConsumerAgent> + 'a {
        let ConsumerLifecycle { port, key, handle } = self;
        let msg = format!("Handling: {:?}", command);
        let key = key.to_string();
        let handle_instr = match *command {
            Instruction::OpenLink => handle
                .and_then_with(move |maybe| match maybe {
                    Some(dl_handle) if !dl_handle.is_stopped() => None,
                    _ => Some(
                        open_link(context, *port, key.clone())
                            .and_then(move |dl_handle| handle.set(Some(dl_handle))),
                    ),
                })
                .discard()
                .boxed_local(),
            Instruction::CloseLink => handle
                .with_mut(|h| {
                    if let Some(h) = h.as_mut() {
                        h.stop();
                    }
                })
                .boxed_local(),
            Instruction::Send(n) => handle
                .with_mut(move |h| {
                    if let Some(handle) = h.as_mut() {
                        if handle.update(key.clone(), n).is_err() {
                            *h = None;
                        }
                    }
                })
                .boxed_local(),
            Instruction::Stop => context.stop().boxed_local(),
        };
        context
            .effect(move || println!("{}", msg))
            .followed_by(handle_instr)
    }
}

fn open_link(
    context: HandlerContext<ConsumerAgent>,
    port: u16,
    target_key: String,
) -> impl HandlerAction<ConsumerAgent, Completion = MapDownlinkHandle<String, i32>> {
    let host = format!("ws://localhost:{}", port);
    let target_cpy = target_key.clone();
    context
        .map_downlink_builder::<String, i32>(Some(&host), "/producer/a", "lane", Default::default())
        .on_linked(|context| context.effect(|| println!("Link opened.")))
        .on_synced(move |context, map| {
            let message = format!(
                "{}. Using value: {:?}.",
                format_map(map),
                map.get(&target_cpy)
            );
            let set = map
                .get(&target_cpy)
                .copied()
                .map(|v| context.set_value(ConsumerAgent::LANE, v));
            let print = context.effect(move || println!("Link synchronized: {}", message));
            set.followed_by(print)
        })
        .on_update(move |context, key, _map, _prev, new_value| {
            if key == target_key {
                Some(context.value(*new_value).and_then(move |v| {
                    println!("Received new value on link: {}", v);
                    context.set_value(ConsumerAgent::LANE, v)
                }))
            } else {
                None
            }
            .discard()
        })
        .on_unlinked(|context| context.effect(|| println!("Link closed.")))
        .on_failed(|context| context.effect(|| println!("Link failed.")))
        .done()
}
