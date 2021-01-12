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
use std::sync::Arc;
use stm::transaction::atomically;
use swim_server::agent::command_lifecycle;
use swim_server::agent::lane::channels::update::StmRetryStrategy;
use swim_server::agent::lane::lifecycle::{LaneLifecycle, StatefulLaneLifecycleBase};
use swim_server::agent::lane::model::action::CommandLane;
use swim_server::agent::lane::model::value::ValueLane;
use swim_server::agent::lane::strategy::Queue;
use swim_server::agent::value_lifecycle;
use swim_server::agent::AgentContext;
use swim_server::agent::SwimAgent;
use swim_server::agent_lifecycle;
use swim_server::future::retryable::strategy::RetryStrategy;
use swim_server::uri::RelativeUri;

#[derive(Debug, SwimAgent)]
#[agent(config = "UnitAgentConfig")]
pub struct UnitAgent {
    #[lifecycle(public, name = "InfoLifecycle")]
    info: ValueLane<String>,
    #[lifecycle(public, name = "PublishInfoLifecycle")]
    publish_info: CommandLane<String>,
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

#[value_lifecycle(agent = "UnitAgent", event_type = "String")]
struct InfoLifecycle;

impl InfoLifecycle {
    async fn on_start<Context>(&self, _model: &ValueLane<String>, _context: &Context)
    where
        Context: AgentContext<UnitAgent> + Sized + Send + Sync,
    {
    }

    async fn on_event<Context>(
        &self,
        event: &Arc<String>,
        model: &ValueLane<String>,
        context: &Context,
    ) where
        Context: AgentContext<UnitAgent> + Sized + Send + Sync + 'static,
    {
        //Todo this should print the current and the previous value
        let message = format!("`info` set to {} from {}", event, model.load().await);
        log_message(context.node_uri(), &message);
    }
}

impl LaneLifecycle<UnitAgentConfig> for InfoLifecycle {
    fn create(_config: &UnitAgentConfig) -> Self {
        InfoLifecycle {}
    }
}

impl StatefulLaneLifecycleBase for InfoLifecycle {
    type WatchStrategy = Queue;

    fn create_strategy(&self) -> Self::WatchStrategy {
        Queue::default()
    }
}

#[command_lifecycle(agent = "UnitAgent", command_type = "String")]
struct PublishInfoLifecycle;

impl PublishInfoLifecycle {
    async fn on_command<Context>(
        &self,
        command: String,
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

impl LaneLifecycle<UnitAgentConfig> for PublishInfoLifecycle {
    fn create(_config: &UnitAgentConfig) -> Self {
        PublishInfoLifecycle {}
    }
}

fn log_message<T: Display>(node_uri: &RelativeUri, message: &T) {
    println!("{}: {}", node_uri, message);
}
