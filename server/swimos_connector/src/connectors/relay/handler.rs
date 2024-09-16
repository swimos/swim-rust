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

use swimos_agent::event_handler::{ActionContext, Discard, HandlerAction, SendCommand, StepResult};
use swimos_agent::AgentMetadata;
use swimos_agent_protocol::MapMessage;
use swimos_api::address::Address;
use swimos_model::Value;

/// An abstraction over both a Value and Map command envelope handlers.
enum EitherCommand {
    Value(Discard<SendCommand<String, Value>>),
    Map(Discard<SendCommand<String, MapMessage<Value, Value>>>),
}

pub struct CommandHandler {
    command: EitherCommand,
}

impl CommandHandler {
    /// Constructs a new value command handler.
    ///
    /// # Arguments
    /// * `node_uri` - the node URI to send the command to.
    /// * `lane_uri` - the lane URI to send the command to.
    /// * `value` - the body of the command.
    pub fn value(node_uri: String, lane_uri: String, value: Value) -> CommandHandler {
        CommandHandler {
            command: EitherCommand::Value(Discard::new(SendCommand::new(
                Address::new(None, node_uri, lane_uri),
                value,
                false,
            ))),
        }
    }

    /// Constructs a new map update command handler.
    ///
    /// # Arguments
    /// * `node_uri` - the node URI to send the command to.
    /// * `lane_uri` - the lane URI to send the command to.
    /// * `value` - the key of the map update command.
    /// * `value` - the body of the map update command.
    pub fn update(node_uri: String, lane_uri: String, key: Value, value: Value) -> CommandHandler {
        CommandHandler {
            command: EitherCommand::Map(Discard::new(SendCommand::new(
                Address::new(None, node_uri, lane_uri),
                MapMessage::Update { key, value },
                false,
            ))),
        }
    }

    /// Constructs a new map remove command handler.
    ///
    /// # Arguments
    /// * `node_uri` - the node URI to send the command to.
    /// * `lane_uri` - the lane URI to send the command to.
    /// * `key` - the key of the map remove command.
    pub fn remove(node_uri: String, lane_uri: String, key: Value) -> CommandHandler {
        CommandHandler {
            command: EitherCommand::Map(Discard::new(SendCommand::new(
                Address::new(None, node_uri, lane_uri),
                MapMessage::Remove { key },
                false,
            ))),
        }
    }
}

impl<Context> HandlerAction<Context> for CommandHandler {
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        match &mut self.command {
            EitherCommand::Value(handler) => handler.step(action_context, meta, context),
            EitherCommand::Map(handler) => handler.step(action_context, meta, context),
        }
    }
}
