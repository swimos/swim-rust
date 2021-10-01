// Copyright 2015-2021 SWIM.AI inc.
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

#[cfg(test)]
mod tests;

use crate::downlink::error::DownlinkError;
use crate::downlink::state_machine::{DownlinkStateMachine, EventResult, Response, ResponseResult};
use crate::downlink::{Command, Message, SchemaViolations};
use swim_schema::schema::{Schema, StandardSchema};
use swim_model::Value;
use tracing::trace;

/// State machine for event downlinks.
pub struct EventStateMachine {
    schema: StandardSchema,
    violations: SchemaViolations,
}

impl EventStateMachine {
    pub fn new(schema: StandardSchema, violations: SchemaViolations) -> Self {
        EventStateMachine { schema, violations }
    }
}

impl DownlinkStateMachine<Value, ()> for EventStateMachine {
    type State = ();
    type WarpCmd = ();
    type Report = Value;

    fn initialize(&self) -> (Self::State, Option<Command<Self::WarpCmd>>) {
        ((), Some(Command::Link))
    }

    fn handle_warp_message(
        &self,
        _: &mut Self::State,
        event: Message<Value>,
    ) -> EventResult<Self::Report> {
        match event {
            Message::Linked => {
                trace!("Downlink linked");
                EventResult::default()
            }
            Message::Unlinked => {
                trace!("Downlink unlinked");
                EventResult::terminate()
            }
            Message::Action(value) => {
                if self.schema.matches(&value) {
                    EventResult::of(value)
                } else {
                    match self.violations {
                        SchemaViolations::Ignore => EventResult::default(),
                        SchemaViolations::Report => EventResult::fail(
                            DownlinkError::SchemaViolation(value, self.schema.clone()),
                        ),
                    }
                }
            }
            Message::BadEnvelope(_) => EventResult::fail(DownlinkError::MalformedMessage),
            _ => EventResult::default(),
        }
    }

    fn handle_action_request(
        &self,
        _: &mut Self::State,
        _: (),
    ) -> ResponseResult<Self::Report, Self::WarpCmd> {
        Ok(Response::default())
    }
}
