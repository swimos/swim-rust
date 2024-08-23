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

use swimos_agent_protocol::CommandMessageTarget;
use swimos_api::address::Address;
use swimos_form::write::StructuralWritable;
use swimos_model::Text;

use crate::{
    event_handler::{ActionContext, EventHandlerError, HandlerAction, StepResult},
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

    pub fn send<T>(&self, body: T) -> SendCommandById<T>
    where
        T: StructuralWritable,
    {
        SendCommandById::new(self.id, body, true)
    }

    pub fn send_queued<T>(&self, body: T) -> SendCommandById<T>
    where
        T: StructuralWritable,
    {
        SendCommandById::new(self.id, body, false)
    }
}

pub struct RegisterCommander {
    address: Option<Address<Text>>,
}

impl RegisterCommander {
    pub fn new(address: Address<Text>) -> Self {
        RegisterCommander {
            address: Some(address),
        }
    }
}

impl<Context> HandlerAction<Context> for RegisterCommander {
    type Completion = Commander<Context>;

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let RegisterCommander { address } = self;
        if let Some(address) = address.take() {
            match action_context.register_commander(address) {
                Ok(id) => StepResult::done(Commander::new(id)),
                Err(err) => StepResult::Fail(EventHandlerError::FailedCommanderRegistration(err)),
            }
        } else {
            StepResult::after_done()
        }
    }
}

pub struct SendCommandById<T> {
    id: u16,
    body: Option<T>,
    overwrite_permitted: bool,
}

impl<T> SendCommandById<T> {
    fn new(id: u16, body: T, overwrite_permitted: bool) -> Self {
        SendCommandById {
            id,
            body: Some(body),
            overwrite_permitted,
        }
    }
}

impl<T: StructuralWritable, Context> HandlerAction<Context> for SendCommandById<T> {
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let SendCommandById {
            id,
            body,
            overwrite_permitted,
        } = self;
        if let Some(body) = body.take() {
            action_context.send_command::<&str, T>(
                CommandMessageTarget::Registered(*id),
                body,
                *overwrite_permitted,
            );
            StepResult::done(())
        } else {
            StepResult::after_done()
        }
    }
}
