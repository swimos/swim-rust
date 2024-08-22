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

use std::marker::PhantomData;

use swimos_api::address::Address;
use swimos_model::Text;

use crate::{
    event_handler::{ActionContext, EventHandler, HandlerAction, StepResult},
    AgentMetadata,
};

pub struct Commander<Context> {
    _type: PhantomData<fn(&Context)>,
    id: u16,
}

impl<Context> Commander<Context> {
    fn new(id: u16) -> Self {
        Commander {
            _type: PhantomData,
            id,
        }
    }
}

struct RegisterCommanderInner<OnDone> {
    address: Address<Text>,
    on_done: OnDone,
}

pub struct RegisterCommander<OnDone> {
    inner: Option<RegisterCommanderInner<OnDone>>,
}

impl<OnDone> RegisterCommander<OnDone> {
    pub fn new(address: Address<Text>, on_done: OnDone) -> Self {
        RegisterCommander {
            inner: Some(RegisterCommanderInner { address, on_done }),
        }
    }
}

impl<Context, OnDone, H> HandlerAction<Context> for RegisterCommander<OnDone>
where
    OnDone: FnOnce(Commander<Context>) -> H + Send + 'static,
    H: EventHandler<Context> + Send + 'static,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let RegisterCommander { inner } = self;
        if let Some(RegisterCommanderInner { address, on_done }) = inner.take() {
            action_context.register_commander(address, |id: u16| {
                let commander = Commander::new(id);
                on_done(commander)
            });
            StepResult::done(())
        } else {
            StepResult::after_done()
        }
    }
}
