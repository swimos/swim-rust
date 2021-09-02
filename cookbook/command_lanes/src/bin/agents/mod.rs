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

use std::fmt::{Debug, Display};
use swim_common::model::{Item, Value};
use swim_server::agent::command_lifecycle;
use swim_server::agent::lane::model::command::CommandLane;
use swim_server::agent::AgentContext;
use swim_server::agent::SwimAgent;
use swim_server::uri::RelativeUri;

#[derive(Debug, SwimAgent)]
pub struct UnitAgent {
    #[lifecycle(name = "PublishLifecycle")]
    pub publish: CommandLane<i32>,
    #[lifecycle(name = "PublishValueLifecycle")]
    pub publish_value: CommandLane<Value>,
}

#[command_lifecycle(agent = "UnitAgent", command_type = "i32", on_command)]
struct PublishLifecycle;

impl PublishLifecycle {
    async fn on_command<Context>(&self, command: &i32, _model: &CommandLane<i32>, context: &Context)
    where
        Context: AgentContext<UnitAgent> + Sized + Send + Sync + 'static,
    {
        let message = format!("`publish` commanded with {}", command);
        log_message(context.node_uri(), &message);

        let rec = Value::Record(
            vec![],
            vec![Item::Slot(
                Value::text("fromServer"),
                Value::text(command.to_string()),
            )],
        );

        context.agent().publish_value.commander().command(rec).await;
    }
}

#[command_lifecycle(agent = "UnitAgent", command_type = "Value", on_command)]
struct PublishValueLifecycle;

impl PublishValueLifecycle {
    async fn on_command<Context>(
        &self,
        command: &Value,
        _model: &CommandLane<Value>,
        context: &Context,
    ) where
        Context: AgentContext<UnitAgent> + Sized + Send + Sync + 'static,
    {
        let message = format!("`publish_value` commanded with {}", command);
        log_message(context.node_uri(), &message);
    }
}

fn log_message<T: Display>(node_uri: &RelativeUri, message: &T) {
    println!("{}: {}", node_uri, message);
}
