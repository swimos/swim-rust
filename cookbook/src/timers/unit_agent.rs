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

use async_std::task;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use stm::transaction::atomically;
use swim_common::model::Value;
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

#[derive(Debug, SwimAgent)]
#[agent(config = "UnitAgentConfig")]
pub struct UnitAgent {
    #[lifecycle(public, name = "MinutesLifecycle")]
    minutes: ValueLane<i32>,
    #[lifecycle(public, name = "PublishLifecycle")]
    publish: CommandLane<Value>,
}

#[derive(Debug, Clone)]
pub struct UnitAgentConfig;

#[agent_lifecycle(agent = "UnitAgent")]
pub struct UnitAgentLifecycle;

impl UnitAgentLifecycle {
    async fn on_start<Context>(&self, context: &Context)
    where
        Context: AgentContext<UnitAgent> + Sized + Send + Sync,
    {
        let minutes = context.agent().minutes.clone();

        tokio::spawn(async move {
            timer(1, minutes).await;
        });
    }
}

#[value_lifecycle(agent = "UnitAgent", event_type = "i32")]
struct MinutesLifecycle;

impl MinutesLifecycle {
    async fn on_start<Context>(&self, _model: &ValueLane<i32>, _context: &Context)
    where
        Context: AgentContext<UnitAgent> + Sized + Send + Sync,
    {
    }

    async fn on_event<Context>(&self, event: &Arc<i32>, _model: &ValueLane<i32>, _context: &Context)
    where
        Context: AgentContext<UnitAgent> + Sized + Send + Sync + 'static,
    {
        println!("{} seconds since last event", event);
    }
}

impl LaneLifecycle<UnitAgentConfig> for MinutesLifecycle {
    fn create(_config: &UnitAgentConfig) -> Self {
        MinutesLifecycle {}
    }
}

impl StatefulLaneLifecycleBase for MinutesLifecycle {
    type WatchStrategy = Queue;

    fn create_strategy(&self) -> Self::WatchStrategy {
        Queue::default()
    }
}

#[command_lifecycle(agent = "UnitAgent", command_type = "Value")]
struct PublishLifecycle;

impl PublishLifecycle {
    async fn on_command<Context>(
        &self,
        _command: Value,
        _model: &CommandLane<Value>,
        context: &Context,
    ) where
        Context: AgentContext<UnitAgent> + Sized + Send + Sync + 'static,
    {
        let _ = atomically(
            &context.agent().minutes.set(0),
            StmRetryStrategy::new(RetryStrategy::default()),
        )
        .await;
    }
}

impl LaneLifecycle<UnitAgentConfig> for PublishLifecycle {
    fn create(_config: &UnitAgentConfig) -> Self {
        PublishLifecycle {}
    }
}

async fn timer(delay: u64, minutes: ValueLane<i32>) {
    loop {
        update_minutes(&minutes).await;
        task::sleep(Duration::from_secs(delay)).await;
    }
}

async fn update_minutes(minutes: &ValueLane<i32>) {
    let current_value = atomically(
        &minutes.get(),
        StmRetryStrategy::new(RetryStrategy::default()),
    )
    .await;

    if let Ok(value) = current_value {
        let _ = atomically(
            &minutes.set(*value + 1),
            StmRetryStrategy::new(RetryStrategy::default()),
        )
        .await;
    }
}
