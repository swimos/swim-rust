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

use swim_form::structural::write::StructuralWritable;
use swim_model::address::Address;

use crate::meta::AgentMetadata;

use super::{ActionContext, HandlerAction, StepResult};

#[cfg(test)]
mod tests;

pub struct SendCommand<S, T> {
    body: Option<Body<S, T>>,
    overwrite_permitted: bool,
}

impl<S, T> SendCommand<S, T> {
    pub fn new(address: Address<S>, command: T, overwrite_permitted: bool) -> Self {
        SendCommand {
            body: Some(Body {
                address,
                value: command,
            }),
            overwrite_permitted,
        }
    }
}

struct Body<S, T> {
    address: Address<S>,
    value: T,
}

impl<Context, S, T> HandlerAction<Context> for SendCommand<S, T>
where
    S: AsRef<str>,
    T: StructuralWritable,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let SendCommand {
            body,
            overwrite_permitted,
        } = self;
        if let Some(Body { address, value }) = body.take() {
            action_context.send_command(address, value, *overwrite_permitted);
            StepResult::done(())
        } else {
            StepResult::after_done()
        }
    }
}
