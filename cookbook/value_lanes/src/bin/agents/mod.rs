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
use stm::transaction::atomically;
use swim_server::agent::command_lifecycle;
use swim_server::agent::lane::channels::update::StmRetryStrategy;
use swim_server::agent::lane::model::command::CommandLane;
use swim_server::agent::lane::model::value::{ValueLane, ValueLaneEvent};
use swim_server::agent::value_lifecycle;
use swim_server::agent::AgentContext;
use swim_server::agent::SwimAgent;
use swim_server::uri::RelativeUri;
use swim_server::RetryStrategy;

#[derive(Debug, SwimAgent)]
pub struct UnitAgent {
    #[lifecycle(name = "InfoLifecycle")]
    pub info: ValueLane<String>,
    #[lifecycle(name = "PublishInfoLifecycle")]
    pub publish_info: CommandLane<String>,
}

#[value_lifecycle(agent = "UnitAgent", event_type = "String", on_event)]
struct InfoLifecycle;

impl InfoLifecycle {
    async fn on_event<Context>(
        &self,
        event: &ValueLaneEvent<String>,
        _model: &ValueLane<String>,
        context: &Context,
    ) where
        Context: AgentContext<UnitAgent> + Sized + Send + Sync + 'static,
    {
        let message = if let Some(prev) = &event.previous {
            format!("`info` set to {} from {}", event.current, prev)
        } else {
            format!("`info` set to {}", event.current)
        };

        log_message(context.node_uri(), &message);
    }
}

#[command_lifecycle(agent = "UnitAgent", command_type = "String", on_command)]
struct PublishInfoLifecycle;

impl PublishInfoLifecycle {
    async fn on_command<Context>(
        &self,
        command: &str,
        _model: &CommandLane<String>,
        context: &Context,
    ) where
        Context: AgentContext<UnitAgent> + Sized + Send + Sync + 'static,
    {
        let _ = atomically(
            &context
                .agent()
                .info
                .set(format!("from publishInfo: {} ", command)),
            StmRetryStrategy::new(RetryStrategy::default()),
        )
        .await;
    }
}

fn log_message<T: Display>(node_uri: &RelativeUri, message: &T) {
    println!("{}: {}", node_uri, message);
}
