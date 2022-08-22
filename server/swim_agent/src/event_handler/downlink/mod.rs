// Copyright 2015-2021 Swim Inc.
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

use tokio::sync::mpsc;

use crate::{downlink_lifecycle::value::ValueDownlinkLifecycle, meta::AgentMetadata};

use super::{ActionContext, HandlerAction, StepResult};

pub struct OpenValueDownlink<T, LC> {
    _type: PhantomData<fn(T) -> T>,
    lifecycle: LC,
}

pub struct ValueDownlinkHandle<T> {
    sender: mpsc::Sender<T>,
}

impl<T, LC, Context> HandlerAction<Context> for OpenValueDownlink<T, LC>
where
    LC: ValueDownlinkLifecycle<T, Context> + Send + 'static,
{
    type Completion = ValueDownlinkHandle<T>;

    fn step(
        &mut self,
        action_context: ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        todo!()
    }
}
