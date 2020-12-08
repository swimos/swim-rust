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

use std::fmt::{Debug, Display};
use swim_common::model::{Item, Value};
use swim_server::agent::action_lifecycle;
use swim_server::agent::command_lifecycle;
use swim_server::agent::lane::lifecycle::LaneLifecycle;
use swim_server::agent::lane::model::action::{ActionLane, CommandLane};
use swim_server::agent::AgentContext;
use swim_server::agent::SwimAgent;
use swim_server::agent_lifecycle;
use swim_server::uri::RelativeUri;

#[derive(Debug, SwimAgent)]
#[agent(config = "UnitAgentConfig")]
pub struct UnitAgent {
    #[lifecycle(public, name = "PublishLifecycle")]
    publish: CommandLane<i32>,
    #[lifecycle(public, name = "PublishValueLifecycle")]
    publish_value: ActionLane<Value, Value>,
}

#[derive(Debug, Clone)]
pub struct UnitAgentConfig;

#[agent_lifecycle(agent = "UnitAgent")]
pub struct UnitAgentLifecycle;

impl UnitAgentLifecycle {
    async fn on_start<Context>(&self, _context: &Context)
    where
        Context: AgentContext<UnitAgent> + Sized + Send + Sync,
    {
    }
}

#[command_lifecycle(agent = "UnitAgent", command_type = "i32")]
struct PublishLifecycle;

impl PublishLifecycle {
    async fn on_command<Context>(&self, command: i32, _model: &CommandLane<i32>, context: &Context)
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

impl LaneLifecycle<UnitAgentConfig> for PublishLifecycle {
    fn create(_config: &UnitAgentConfig) -> Self {
        PublishLifecycle {}
    }
}

#[action_lifecycle(agent = "UnitAgent", command_type = "Value", response_type = "Value")]
struct PublishValueLifecycle;

impl PublishValueLifecycle {
    async fn on_command<Context>(
        &self,
        command: Value,
        _model: &ActionLane<Value, Value>,
        context: &Context,
    ) -> Value
    where
        Context: AgentContext<UnitAgent> + Sized + Send + Sync + 'static,
    {
        //Todo this is a command lane in the java example
        let message = format!("`publish_value` commanded with {}", command);
        log_message(context.node_uri(), &message);
        //Todo this is currently not being returned to all linked connections
        command
    }
}

impl LaneLifecycle<UnitAgentConfig> for PublishValueLifecycle {
    fn create(_config: &UnitAgentConfig) -> Self {
        PublishValueLifecycle {}
    }
}

fn log_message<T: Display>(node_uri: &RelativeUri, message: &T) {
    println!("{}: {}", node_uri, message);
}
