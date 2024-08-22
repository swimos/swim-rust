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
    event_handler::{
        ActionContext, EventHandler, HandlerAction, HandlerActionExt, LocalBoxEventHandler,
        StepResult, UnitHandler,
    },
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

type OnDone<Context> =
    Box<dyn FnOnce(Commander<Context>) -> LocalBoxEventHandler<'static, Context> + Send + 'static>;

pub struct RegisterCommander<Context> {
    address: Address<Text>,
    on_done: Option<OnDone<Context>>,
}

impl<Context> RegisterCommander<Context> {
    pub fn new(address: Address<Text>, on_done: OnDone<Context>) -> Self {
        RegisterCommander {
            address: address,
            on_done: Some(on_done),
        }
    }
}

impl<Context> HandlerAction<Context> for RegisterCommander<Context> {
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        todo!()
    }
}

pub fn register_commander<Context, F, H>(
    address: Address<Text>,
    on_done: F,
) -> impl EventHandler<Context>
where
    F: FnOnce(Commander<Context>) -> H + Send + 'static,
    H: EventHandler<Context> + 'static,
{
    let on_done_boxed: OnDone<Context> = Box::new(move |commander: Commander<Context>| {
        let handler: LocalBoxEventHandler<'static, Context> = on_done(commander).boxed_local();
        handler
    });

    UnitHandler::default()
}
