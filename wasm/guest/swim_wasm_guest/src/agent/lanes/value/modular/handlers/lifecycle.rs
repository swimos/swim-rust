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

use wasm_ir::wpc::EnvAccess;

use crate::agent::AgentContext;
use crate::prelude::lanes::handlers::{Done, HandlerContext, HandlerEffect, HandlerError};
use crate::prelude::lanes::value::modular::ModularValueLane;
use crate::prelude::lanes::{ItemProjection, LaneLifecycle};

pub struct ModularValueLaneUpdate<I, A, T, H, F> {
    projection: ItemProjection<A, ModularValueLane<I, T>>,
    handler_chain: H,
    on_update: F,
}

impl<I, A, T, H, F> ModularValueLaneUpdate<I, A, T, H, F> {
    pub fn new(
        projection: ItemProjection<A, ModularValueLane<I, T>>,
        handler_chain: H,
        on_update: F,
    ) -> ModularValueLaneUpdate<I, A, T, H, F> {
        ModularValueLaneUpdate {
            projection,
            handler_chain,
            on_update,
        }
    }
}

impl<I, A, F, H> LaneLifecycle<A> for ModularValueLaneUpdate<I, A, H::Output, H, F>
where
    H: LaneLifecycle<A>,
    F: Fn(Option<&H::Output>, &H::Output),
{
    type Input = H::Input;
    type Output = (H::Output, Done);

    fn run<E>(
        &mut self,
        context: &mut AgentContext<A, E>,
        agent: &mut A,
        input: Self::Input,
    ) -> Result<Option<Self::Output>, HandlerError>
    where
        E: EnvAccess,
    {
        let ModularValueLaneUpdate {
            projection,
            handler_chain,
            on_update,
        } = self;
        match handler_chain.run(context, agent, input) {
            Ok(Some(o)) => {
                let lane = projection(agent);
                let previous = lane.previous();
                on_update(previous, &o);
                Ok(Some((o, Done)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

pub struct ValueLaneUpdateThen<I, A, T, H, F> {
    projection: ItemProjection<A, ModularValueLane<I, T>>,
    handler_chain: H,
    on_update: F,
}

impl<I, A, T, H, F> ValueLaneUpdateThen<I, A, T, H, F> {
    pub fn new(
        projection: ItemProjection<A, ModularValueLane<I, T>>,
        handler_chain: H,
        on_update: F,
    ) -> ValueLaneUpdateThen<I, A, T, H, F> {
        ValueLaneUpdateThen {
            projection,
            handler_chain,
            on_update: on_update,
        }
    }
}

impl<I, A, F, H, Next> LaneLifecycle<A> for ValueLaneUpdateThen<I, A, H::Output, H, F>
where
    H: LaneLifecycle<A>,
    F: Fn(&HandlerContext<A>, Option<&H::Output>, &H::Output) -> Next,
    Next: HandlerEffect<A>,
{
    type Input = H::Input;
    type Output = (H::Output, Next);

    fn run<E>(
        &mut self,
        context: &mut AgentContext<A, E>,
        agent: &mut A,
        input: Self::Input,
    ) -> Result<Option<Self::Output>, HandlerError>
    where
        E: EnvAccess,
    {
        let ValueLaneUpdateThen {
            projection,
            handler_chain,
            on_update,
        } = self;
        match handler_chain.run(context, agent, input) {
            Ok(Some(o)) => {
                let lane = projection(agent);
                let previous = lane.previous();
                let next = on_update(&context.handler(), previous, &o);
                Ok(Some((o, next)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}
