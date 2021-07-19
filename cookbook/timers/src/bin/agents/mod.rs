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

use std::fmt::Debug;
use std::time::Duration;
use stm::transaction::atomically;
use swim_common::model::Value;
use swim_server::agent::command_lifecycle;
use swim_server::agent::lane::channels::update::StmRetryStrategy;
use swim_server::agent::lane::model::command::CommandLane;
use swim_server::agent::lane::model::value::{ValueLane, ValueLaneEvent};
use swim_server::agent::value_lifecycle;
use swim_server::agent::AgentContext;
use swim_server::agent::SwimAgent;
use swim_server::{agent_lifecycle, RetryStrategy};

#[derive(Debug, SwimAgent)]
pub struct UnitAgent {
    #[lifecycle(name = "MinutesLifecycle")]
    pub minutes: ValueLane<i32>,
    #[lifecycle(name = "PublishLifecycle")]
    pub publish: CommandLane<Value>,
}

#[agent_lifecycle(agent = "UnitAgent", on_start)]
pub struct UnitAgentLifecycle;

impl UnitAgentLifecycle {
    async fn on_start<Context>(&self, context: &Context)
        where
            Context: AgentContext<UnitAgent> + Sized + Send + Sync,
    {
        let minutes = context.agent().minutes.clone();

        let effect = move || {
            let min = minutes.clone();
            async move { increment_time(&min).await }
        };

        context
            .periodically(effect, Duration::new(1, 0), None)
            .await;
    }
}

#[value_lifecycle(agent = "UnitAgent", event_type = "i32", on_event)]
struct MinutesLifecycle;

impl MinutesLifecycle {
    async fn on_event<Context>(
        &self,
        event: &ValueLaneEvent<i32>,
        _model: &ValueLane<i32>,
        _context: &Context,
    ) where
        Context: AgentContext<UnitAgent> + Sized + Send + Sync + 'static,
    {
        println!("{} seconds since last event", event.current);
    }
}

#[command_lifecycle(agent = "UnitAgent", command_type = "Value", on_command)]
struct PublishLifecycle;

impl PublishLifecycle {
    async fn on_command<Context>(
        &self,
        _command: &Value,
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

async fn increment_time(minutes: &ValueLane<i32>) {
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
