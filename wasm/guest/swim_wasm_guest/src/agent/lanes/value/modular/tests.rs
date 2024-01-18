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

use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::BytesMut;

use wasm_ir::wpc::EnvAccess;

use crate::agent::{AgentContext, AgentSpecBuilder, EventRouter};
use crate::prelude::host::NoHost;
use crate::prelude::lanes::handlers::{HandlerEffect, Modification, StepResult};
use crate::prelude::lanes::value::modular::ModularValueLane;

const DISPATCH_FAILURE: &str = "Dispatch failure";

#[derive(Debug)]
struct Agent {
    v1: ModularValueLane<i32, i32>,
    v2: ModularValueLane<i32, i32>,
}

#[test]
fn two_lanes() {
    let did_set = Rc::new(AtomicBool::new(false));
    let mut ctx = AgentContext::new(Default::default(), NoHost);
    let mut spec = AgentSpecBuilder::new("test");

    let (v1, r1) = ModularValueLane::<i32, i32>::builder(&mut spec, "v1", true)
        .on_update_then::<Agent, _, _>(
            |agent| &mut agent.v1,
            |ctx, _old_value, new_value| ctx.set_value(|agent| &mut agent.v2, *new_value),
        );

    let (v2, r2) = ModularValueLane::<i32, i32>::builder(&mut spec, "v2", true)
        .filter(|it| *it == 10)
        .map(|val| val * 2)
        .on_update::<Agent, _>(
            |agent| &mut agent.v2,
            |_old_value, _new_value| {
                did_set.store(true, Ordering::Relaxed);
            },
        );

    let mut dispatcher = r1.or(r2);
    let mut agent = Agent { v1, v2 };

    let effect = dispatcher
        .value_command(&mut ctx, &mut agent, 0, BytesMut::from("1"))
        .expect(DISPATCH_FAILURE);

    assert_eq!(*agent.v2.get(), 0);

    run_handler(&mut ctx, &mut agent, effect, &mut dispatcher);

    let effect = dispatcher
        .value_command(&mut ctx, &mut agent, 1, BytesMut::from("10"))
        .expect(DISPATCH_FAILURE);

    run_handler(&mut ctx, &mut agent, effect, &mut dispatcher);

    assert_eq!(*agent.v1.get(), 1);
    assert_eq!(*agent.v2.get(), 20);
    assert!(did_set.load(Ordering::Acquire));
}

fn run_handler<H, A, R, E>(
    ctx: &mut AgentContext<A, E>,
    agent: &mut A,
    mut handler: H,
    router: &mut R,
) where
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
                        let handler = router.event(ctx, agent, item_id).expect(DISPATCH_FAILURE);
                        run_handler(ctx, agent, handler, router);
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
                ..
            } => {
                if trigger_handler {
                    let handler = router.event(ctx, agent, item_id).expect(DISPATCH_FAILURE);
                    run_handler(ctx, agent, handler, router);
                }
                break;
            }
            StepResult::Complete {
                modified_item: None,
                ..
            } => break,
        }
    }
}
