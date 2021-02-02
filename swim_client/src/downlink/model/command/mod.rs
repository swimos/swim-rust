// Copyright 2015-2020 SWIM.AI inc.
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

use crate::downlink::typed::UntypedCommandDownlink;
use crate::downlink::{
    Command, DownlinkConfig, DownlinkError, DownlinkState, Operation, Response, StateMachine,
};
use swim_common::model::schema::{Schema, StandardSchema};
use swim_common::model::Value;
use swim_common::routing::RoutingError;
use swim_common::sink::item::ItemSender;

#[cfg(test)]
mod tests;

pub fn create_downlink<Commands>(
    schema: StandardSchema,
    cmd_sender: Commands,
    config: DownlinkConfig,
) -> UntypedCommandDownlink
where
    Commands: ItemSender<Command<Value>, RoutingError> + Send + Sync + 'static,
{
    let upd_stream = futures::stream::pending();

    crate::downlink::create_downlink(
        CommandStateMachine::new(schema),
        upd_stream,
        cmd_sender,
        config,
    )
    .0
}

struct CommandStateMachine {
    schema: StandardSchema,
}

impl CommandStateMachine {
    fn new(schema: StandardSchema) -> Self {
        CommandStateMachine { schema }
    }
}

impl StateMachine<(), (), Value> for CommandStateMachine {
    type Ev = ();
    type Cmd = Value;

    fn init_state(&self) {}

    fn dl_start_state(&self) -> DownlinkState {
        DownlinkState::Unlinked
    }

    fn handle_operation(
        &self,
        _downlink_state: &mut DownlinkState,
        _state: &mut (),
        op: Operation<(), Value>,
    ) -> Result<Response<Self::Ev, Self::Cmd>, DownlinkError> {
        match op {
            Operation::Action(value) => {
                if self.schema.matches(&value) {
                    Ok(Response::for_command(Command::Action(value)))
                } else {
                    Ok(Response::none())
                }
            }

            _ => Ok(Response::none()),
        }
    }
}
