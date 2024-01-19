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

use std::collections::HashMap;

use static_assertions::assert_obj_safe;

pub use context::AgentContext;
pub use item::MutableValueLikeItem;
pub use router::{EitherRoute, EventRouter, ItemRoute};
use wasm_ir::agent::{CommandEvent, HostRequest, InitValueItemRequest, LaneSyncRequest};
use wasm_ir::wpc::EnvAccess;
use wasm_ir::{AgentSpec, LaneKindRepr, LaneSpec};

use crate::agent::router::RouterError;
use crate::agent::sealed::Sealed;
use crate::prelude::lanes::handlers::{HandlerContext, HandlerEffect, Modification, StepResult};

mod context;
mod item;
pub mod lanes;
mod router;

pub trait AgentItem {
    fn id(&self) -> u64;
}

pub trait LaneItem: AgentItem {
    fn kind() -> LaneKindRepr;
}

pub trait SwimAgent: 'static {
    fn new() -> (Self, AgentSpec, impl EventRouter<Self>)
    where
        Self: Sized;
}

pub struct AgentSpecBuilder {
    id: u64,
    name: String,
    lanes: HashMap<String, LaneSpec>,
}

impl AgentSpecBuilder {
    pub fn new(agent_name: &str) -> AgentSpecBuilder {
        AgentSpecBuilder {
            id: 0,
            name: agent_name.to_string(),
            lanes: HashMap::default(),
        }
    }

    fn push<L>(&mut self, uri: &str, transient: bool) -> u64
    where
        L: LaneItem,
    {
        let AgentSpecBuilder { id, lanes, .. } = self;

        if lanes.contains_key(uri) {
            panic!("Duplicate lane registration for: {uri}");
        }

        let key = *id;
        let spec = LaneSpec::new(transient, key, L::kind());

        lanes.insert(uri.to_string(), spec);
        *id += 1;

        key
    }

    pub fn build(self) -> AgentSpec {
        let AgentSpecBuilder { name, lanes, .. } = self;
        AgentSpec::new(name, lanes)
    }
}

pub unsafe fn wasm_agent<A>(host_access: impl EnvAccess) -> (AgentSpec, Box<*mut dyn Dispatcher>)
where
    A: SwimAgent,
{
    let (agent, spec, router) = A::new();
    let model = AgentModel {
        agent,
        ctx: AgentContext::new(HandlerContext::default(), host_access),
        router,
    };

    // First, box the agent into a dyn Dispatcher
    let boxed: Box<dyn Dispatcher> = Box::new(model);
    // Get the wrapped raw pointer. This is a fat pointer so it needs boxing again so we can
    // more easily pass the pointer around.
    let ptr = Box::into_raw(boxed);
    // Box it again
    (spec, Box::new(ptr))
}

pub struct AgentModel<A, R, H> {
    agent: A,
    ctx: AgentContext<A, H>,
    router: R,
}

impl<A, R, H> Dispatcher for AgentModel<A, R, H>
where
    H: EnvAccess,
    R: EventRouter<A>,
{
    fn dispatch(&mut self, data: Vec<u8>) {
        let AgentModel { agent, router, ctx } = self;

        let request = bincode::deserialize::<HostRequest>(data.as_slice())
            .expect("Failed to decode host request");

        match request {
            HostRequest::Init(InitValueItemRequest { id, with }) => {
                let _result = router.init_value(agent, id, with);
            }
            HostRequest::Sync(LaneSyncRequest { id, remote }) => {
                let _result = router.sync(ctx, agent, id, remote);
            }
            HostRequest::Command(CommandEvent { id, data, map_like }) => {
                if map_like {
                    if let Ok(handler) = router.map_command(ctx, agent, id, data) {
                        run_handler(ctx, agent, handler, router).unwrap()
                    }
                } else {
                    if let Ok(handler) = router.value_command(ctx, agent, id, data) {
                        run_handler(ctx, agent, handler, router).unwrap()
                    }
                }
            }
        }
    }
}

// This purely serves to erase the type parameters of the AgentModel so that they don't need to be
// specified in the guest's implementation.
pub trait Dispatcher: Sealed {
    fn dispatch(&mut self, data: Vec<u8>);
}

assert_obj_safe!(Dispatcher);

mod sealed {
    use crate::agent::Dispatcher;

    pub trait Sealed {}

    impl<D> Sealed for D where D: Dispatcher {}
}

fn run_handler<H, A, R, E>(
    ctx: &mut AgentContext<A, E>,
    agent: &mut A,
    mut handler: H,
    router: &mut R,
) -> Result<(), RouterError>
where
    H: HandlerEffect<A>,
    R: EventRouter<A>,
    E: EnvAccess,
{
    loop {
        match handler.step(ctx, agent) {
            StepResult::Continue { modified_item } => match modified_item {
                Some(Modification {
                    item_id,
                    trigger_handler,
                }) => {
                    if trigger_handler {
                        let handler = router.event(ctx, agent, item_id)?;
                        run_handler(ctx, agent, handler, router)?;
                    }
                }
                None => continue,
            },
            StepResult::Fail(err) => {
                panic!("Event handler failed: {:?}", err);
            }
            StepResult::Complete {
                modified_item:
                    Some(Modification {
                        item_id,
                        trigger_handler,
                    }),
            } => {
                if trigger_handler {
                    let handler = router.event(ctx, agent, item_id)?;
                    run_handler(ctx, agent, handler, router)?;
                }
                break Ok(());
            }
            StepResult::Complete {
                modified_item: None,
            } => break Ok(()),
        }
    }
}
