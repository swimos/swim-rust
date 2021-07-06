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
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::sync::Arc;
use stm::stm::Stm;
use stm::transaction::atomically;
use swim_server::agent::command_lifecycle;
use swim_server::agent::lane::channels::update::StmRetryStrategy;
use swim_server::agent::lane::lifecycle::LaneLifecycle;
use swim_server::agent::lane::model::command::CommandLane;
use swim_server::agent::lane::model::map::{MapLane, MapLaneEvent};
use swim_server::agent::map_lifecycle;
use swim_server::agent::AgentContext;
use swim_server::agent::SwimAgent;
use swim_server::uri::RelativeUri;
use swim_server::RetryStrategy;

#[derive(Debug, SwimAgent)]
pub struct UnitAgent {
    #[lifecycle(name = "ShoppingCartLifecycle")]
    pub shopping_cart: MapLane<String, i32>,
    #[lifecycle(name = "AddItemLifecycle")]
    pub add_item: CommandLane<String>,
}

#[map_lifecycle(agent = "UnitAgent", key_type = "String", value_type = "i32", on_event)]
struct ShoppingCartLifecycle {
    previous_state: HashMap<String, Arc<i32>>,
}

impl LaneLifecycle<()> for ShoppingCartLifecycle {
    fn create(_config: &()) -> Self {
        ShoppingCartLifecycle {
            previous_state: HashMap::new(),
        }
    }
}

impl ShoppingCartLifecycle {
    async fn on_event<Context>(
        &mut self,
        event: &MapLaneEvent<String, i32>,
        _model: &MapLane<String, i32>,
        context: &Context,
    ) where
        Context: AgentContext<UnitAgent> + Sized + Send + Sync + 'static,
    {
        match event {
            MapLaneEvent::Update(key, value) => {
                let previous_val = self.previous_state.insert(key.clone(), value.clone());

                let message = if let Some(prev_val) = previous_val {
                    format!("{} count changed to {} from {}", key, value, prev_val)
                } else {
                    format!("{} count changed to {}", key, value)
                };

                log_message(context.node_uri(), &message);
            }
            MapLaneEvent::Remove(key) => {
                let previous_val = self.previous_state.remove(key);

                if let Some(prev_val) = previous_val {
                    let message = format!("removed <{}, {}>", key, prev_val);
                    log_message(context.node_uri(), &message);
                };
            }
            _ => (),
        }
    }
}

#[command_lifecycle(agent = "UnitAgent", command_type = "String", on_command)]
struct AddItemLifecycle;

impl AddItemLifecycle {
    async fn on_command<Context>(
        &self,
        command: &str,
        _model: &CommandLane<String>,
        context: &Context,
    ) where
        Context: AgentContext<UnitAgent> + Sized + Send + Sync + 'static,
    {
        let shopping_cart = &context.agent().shopping_cart;

        let update = shopping_cart
            .get(command.to_string())
            .and_then(|maybe| match maybe {
                Some(current) => shopping_cart.update(command.to_string(), Arc::new(*current + 1)),
                _ => shopping_cart.update(command.to_string(), Arc::new(1)),
            });

        let _ = atomically(&update, StmRetryStrategy::new(RetryStrategy::default())).await;
    }
}

fn log_message<T: Display>(node_uri: &RelativeUri, message: &T) {
    println!("{}: {}", node_uri, message);
}
