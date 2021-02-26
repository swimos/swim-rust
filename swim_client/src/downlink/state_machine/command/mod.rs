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

use crate::downlink::state_machine::{DownlinkStateMachine, EventResult, Response, ResponseResult};
use crate::downlink::{Command, Message};
use swim_common::model::schema::{Schema, StandardSchema};
use swim_common::model::Value;

struct CommandStateMachine {
    schema: StandardSchema,
}

impl CommandStateMachine {
    fn unvalidated() -> Self {
        CommandStateMachine {
            schema: StandardSchema::Anything,
        }
    }

    fn new(schema: StandardSchema) -> Self {
        CommandStateMachine { schema }
    }
}

impl DownlinkStateMachine<(), Value> for CommandStateMachine {
    type State = ();
    type Update = Value;
    type Report = ();

    fn initialize(&self) -> (Self::State, Option<Command<Self::Update>>) {
        ((), None)
    }

    fn handle_event(&self, _: &mut Self::State, event: Message<()>) -> EventResult<Self::Report> {
        if let Message::Unlinked = event {
            EventResult::terminate()
        } else {
            EventResult::default()
        }
    }

    fn handle_request(
        &self,
        _: &mut Self::State,
        value: Value,
    ) -> ResponseResult<Self::Report, Self::Update> {
        if self.schema.matches(&value) {
            Ok(Response::command(value))
        } else {
            Ok(Response::default())
        }
    }
}
