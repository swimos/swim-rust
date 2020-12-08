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
use stm::stm::Stm;
use stm::transaction::atomically;
use swim_server::agent::command_lifecycle;
use swim_server::agent::lane::channels::update::StmRetryStrategy;
use swim_server::agent::lane::lifecycle::{LaneLifecycle, StatefulLaneLifecycleBase};
use swim_server::agent::lane::model::action::CommandLane;
use swim_server::agent::lane::model::map::{MapLane, MapLaneEvent};
use swim_server::agent::lane::strategy::Queue;
use swim_server::agent::map_lifecycle;
use swim_server::agent::AgentContext;
use swim_server::agent::SwimAgent;
use swim_server::agent_lifecycle;
use swim_server::future::retryable::strategy::RetryStrategy;
use swim_server::uri::RelativeUri;

#[derive(Debug, SwimAgent)]
#[agent(config = "UnitAgentConfig")]
pub struct UnitAgent {
    #[lifecycle(public, name = "ShoppingCartLifecycle")]
    shopping_cart: MapLane<String, i32>,
    #[lifecycle(public, name = "AddItemLifecycle")]
    add_item: CommandLane<String>,
}

#[derive(Debug, Clone)]
pub struct UnitAgentConfig;

#[agent_lifecycle(agent = "UnitAgent")]
pub struct UnitAgentLifecycle;

impl UnitAgentLifecycle {
    async fn on_start<Context>(&self, _context: &Context)
        where
            Context: AgentContext<UnitAgent> + Sized + Send + Sync,
    {}
}

#[map_lifecycle(agent = "UnitAgent", key_type = "String", value_type = "i32")]
struct ShoppingCartLifecycle;

impl ShoppingCartLifecycle {
    async fn on_start<Context>(&self, _model: &MapLane<String, i32>, _context: &Context)
        where
            Context: AgentContext<UnitAgent> + Sized + Send + Sync,
    {}

    async fn on_event<Context>(
        &self,
        event: &MapLaneEvent<String, i32>,
        _model: &MapLane<String, i32>,
        context: &Context,
    ) where
        Context: AgentContext<UnitAgent> + Sized + Send + Sync + 'static,
    {
        match event {
            MapLaneEvent::Update(key, value) => {
                //Todo this should print the previous value of the key
                let message = format!("{} count changed to {} from {}", key, value, value);
                log_message(context.node_uri(), &message);
            }
            MapLaneEvent::Remove(key) => {
                //Todo this should print the previous value of the removed key
                let message = format!("removed <{}, {}>", key, "foo");
                log_message(context.node_uri(), &message);
            }
            _ => (),
        }
    }
}

impl LaneLifecycle<UnitAgentConfig> for ShoppingCartLifecycle {
    fn create(_config: &UnitAgentConfig) -> Self {
        ShoppingCartLifecycle {}
    }
}

impl StatefulLaneLifecycleBase for ShoppingCartLifecycle {
    type WatchStrategy = Queue;

    fn create_strategy(&self) -> Self::WatchStrategy {
        Queue::default()
    }
}

#[command_lifecycle(agent = "UnitAgent", command_type = "String")]
struct AddItemLifecycle;

impl AddItemLifecycle {
    async fn on_command<Context>(
        &self,
        command: String,
        _model: &CommandLane<String>,
        context: &Context,
    ) where
        Context: AgentContext<UnitAgent> + Sized + Send + Sync + 'static,
    {
        let shopping_cart = &context.agent().shopping_cart;

        let update = shopping_cart
            .get(command.clone())
            .and_then(|maybe| match maybe {
                Some(current) => shopping_cart.update(command.clone(), Arc::new(*current + 1)),
                _ => shopping_cart.update(command.clone(), Arc::new(1)),
            });

        let _ = atomically(&update, StmRetryStrategy::new(RetryStrategy::default())).await;
    }
}

impl LaneLifecycle<UnitAgentConfig> for AddItemLifecycle {
    fn create(_config: &UnitAgentConfig) -> Self {
        AddItemLifecycle {}
    }
}

fn log_message<T: Display>(node_uri: &RelativeUri, message: &T) {
    println!("{}: {}", node_uri, message);
}
