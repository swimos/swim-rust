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

use swimos_api::address::Address;
use swimos_form::write::StructuralWritable;

use crate::meta::AgentMetadata;

use super::{ActionContext, HandlerAction, StepResult};

#[cfg(test)]
mod tests;

///  An [event handler](crate::event_handler::EventHandler) that will send a command to a remote lane.
pub struct SendCommand<S, T> {
    body: Option<Body<S, T>>,
    overwrite_permitted: bool,
}

impl<S, T> SendCommand<S, T> {
    /// # Arguments
    /// * `address` - The address of the remote lane.
    /// * `command` - The body of the command.
    /// * `overwrite_permitted` - Whether to enable back-pressure relief for the command. If this is set to true,
    ///    and this command has not been sent when another command is sent to the same destination, it will be
    ///    overwritten.
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
            action_context.send_ad_hoc_command(address, value, *overwrite_permitted);
            StepResult::done(())
        } else {
            StepResult::after_done()
        }
    }

    fn describe(
        &self,
        _context: &Context,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        let SendCommand {
            body,
            overwrite_permitted,
        } = self;
        if let Some(Body {
            address: Address { host, node, lane },
            ..
        }) = body
        {
            let addr: Address<&str> = Address::new(
                host.as_ref().map(|s| s.as_ref()),
                node.as_ref(),
                lane.as_ref(),
            );
            f.debug_struct("SendCommand")
                .field("address", &addr)
                .field("overwrite_permitted", overwrite_permitted)
                .field("consumed", &false)
                .finish()
        } else {
            f.debug_struct("SendCommand")
                .field("overwrite_permitted", overwrite_permitted)
                .field("consumed", &true)
                .finish()
        }
    }
}
