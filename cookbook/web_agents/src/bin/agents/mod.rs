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
use swim_server::agent::lane::model::command::CommandLane;
use swim_server::agent::AgentContext;
use swim_server::agent::SwimAgent;
use swim_server::agent_lifecycle;
use swim_server::command_lifecycle;
use swim_server::uri::RelativeUri;

#[derive(Debug, SwimAgent)]
pub struct UnitAgent {
    #[lifecycle(name = "PublishLifecycle")]
    pub unused: CommandLane<i32>,
}

#[agent_lifecycle(agent = "UnitAgent", on_start)]
pub struct UnitAgentLifecycle;

impl UnitAgentLifecycle {
    async fn on_start<Context>(&self, context: &Context)
    where
        Context: AgentContext<UnitAgent> + Sized + Send + Sync,
    {
        log_message(context.node_uri(), "on_start");
    }
}

#[command_lifecycle(agent = "UnitAgent", command_type = "i32")]
struct PublishLifecycle;

fn log_message<T: Display>(node_uri: &RelativeUri, message: T) {
    println!("{}: {}", node_uri, message);
}
