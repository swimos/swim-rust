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

use crate::agent::{AgentContext, AgentItem};
use crate::prelude::lanes::handlers::{EventHandlerError, HandlerEffect, Modification, StepResult};
use crate::prelude::lanes::value::modular::ModularValueLane;
use crate::prelude::lanes::ItemProjection;

pub struct TryUpdateValueHandler<I, A, T> {
    projection: ItemProjection<A, ModularValueLane<I, T>>,
    to: Option<T>,
}

impl<I, A, T> TryUpdateValueHandler<I, A, T> {
    pub fn new(
        projection: ItemProjection<A, ModularValueLane<I, T>>,
        to: T,
    ) -> TryUpdateValueHandler<I, A, T> {
        TryUpdateValueHandler {
            projection,
            to: Some(to),
        }
    }
}

impl<I, A, T> HandlerEffect<A> for TryUpdateValueHandler<I, A, T> {
    fn step<H>(&mut self, _context: &AgentContext<A, H>, agent: &mut A) -> StepResult
    where
        H: EnvAccess,
    {
        let TryUpdateValueHandler { projection, to } = self;
        match to.take() {
            Some(val) => {
                let lane = projection(agent);
                lane.try_set(val);
                StepResult::Complete {
                    modified_item: Some(Modification::of(lane.id())),
                }
            }
            None => StepResult::Fail(EventHandlerError::SteppedAfterComplete),
        }
    }
}
