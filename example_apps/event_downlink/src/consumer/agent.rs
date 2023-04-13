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

use swim::agent::{
    agent_lifecycle::utility::HandlerContext,
    agent_model::downlink::hosted::EventDownlinkHandle,
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
    handle: State<ConsumerAgent, Option<EventDownlinkHandle>>,
}

impl ConsumerLifecycle {
    pub fn new(port: u16) -> Self {
        ConsumerLifecycle {
            port,
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
        let ConsumerLifecycle { port, handle } = self;
        let msg = format!("Handling: {:?}", command);
        let handle_instr = match *command {
            Instruction::OpenLink => handle
                .and_then_with(move |maybe| match maybe {
                    Some(dl_handle) if !dl_handle.is_stopped() => None,
                    _ => Some(
                        open_link(context, *port)
                            .and_then(move |dl_handle| handle.set(Some(dl_handle))),
                    ),
                })
                .discard()
                .boxed(),
            Instruction::CloseLink => handle
                .with_mut(|h| {
                    if let Some(h) = h.as_mut() {
                        h.stop();
                    }
                })
                .boxed(),
            Instruction::Stop => context.stop().boxed(),
        };
        context.effect(move || println!("{}", msg)).followed_by(handle_instr)
    }
}

fn open_link(
    context: HandlerContext<ConsumerAgent>,
    port: u16,
) -> impl HandlerAction<ConsumerAgent, Completion = EventDownlinkHandle> {
    let host = format!("localhost:{}", port);
    context
        .event_downlink_builder::<i32>(Some(&host), "/producer/a", "lane", Default::default())
        .on_linked(|context| context.effect(|| println!("Link opened.")))
        .on_synced(|context, _| context.effect(|| println!("Link synchronized.")))
        .on_event(|context, v| {
            context.value(v).and_then(move |v| {
                println!("Received value on link: {}", v);
                context.set_value(ConsumerAgent::LANE, v)
            })
        })
        .on_unlinked(|context| context.effect(|| println!("Link closed.")))
        .on_failed(|context| context.effect(|| println!("Link failed.")))
        .done()
}
