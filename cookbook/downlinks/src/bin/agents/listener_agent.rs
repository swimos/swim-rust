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
use futures::StreamExt;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::sync::Arc;
use stm::stm::Stm;
use stm::transaction::atomically;
use swim_client::downlink::model::map::MapEvent;
use swim_client::downlink::typed::map::events::TypedViewWithEvent;
use swim_client::downlink::typed::map::MapDownlinkReceiver;
use swim_client::downlink::Event;
use swim_client::interface::ClientContext;
use swim_client::runtime::task;
use swim_common::form::Form;
use swim_common::model::{Item, Value};
use swim_common::warp::path::{Path, RelativePath};
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
pub struct ListenerAgent {
    #[lifecycle(name = "ShoppingCartsLifecycle")]
    pub shopping_carts: MapLane<String, Value>,
    #[lifecycle(name = "TriggerListenLifecycle")]
    pub trigger_listen: CommandLane<String>,
}

#[map_lifecycle(
    agent = "ListenerAgent",
    key_type = "String",
    value_type = "Value",
    on_event
)]
struct ShoppingCartsLifecycle;

impl ShoppingCartsLifecycle {
    async fn on_event<Context>(
        &mut self,
        event: &MapLaneEvent<String, Value>,
        _model: &MapLane<String, Value>,
        context: &Context,
    ) where
        Context: AgentContext<ListenerAgent> + Sized + Send + Sync + 'static,
    {
        if let MapLaneEvent::Update(key, value) = event {
            let message = format!("shopping_carts updated {}: {}", key, value);
            log_message(context.node_uri(), &message);
        }
    }
}

#[command_lifecycle(agent = "ListenerAgent", command_type = "String", on_command)]
struct TriggerListenLifecycle {
    shopping_cart_subscribers: HashMap<String, MapLane<String, Value>>,
}

impl TriggerListenLifecycle {
    async fn on_command<Context>(
        &mut self,
        command: &str,
        _model: &CommandLane<String>,
        context: &Context,
    ) where
        Context: AgentContext<ListenerAgent> + Sized + Send + Sync + 'static,
    {
        let message = format!("Will listen to: {}", command);
        log_message(context.node_uri(), &message);
        add_subscription(
            RelativePath::new(command, "shopping_cart"),
            context.downlinks_context(),
            context.agent().shopping_carts.clone(),
        )
        .await;

        self.shopping_cart_subscribers
            .insert(command.to_string(), context.agent().shopping_carts.clone());
    }
}

impl LaneLifecycle<()> for TriggerListenLifecycle {
    fn create(_config: &()) -> Self {
        TriggerListenLifecycle {
            shopping_cart_subscribers: HashMap::new(),
        }
    }
}

async fn add_subscription(
    path: RelativePath,
    client_context: ClientContext<Path>,
    shopping_carts: MapLane<String, Value>,
) {
    let (_, map_recv) = client_context
        .map_downlink(Path::Local(path.clone()))
        .await
        .unwrap();

    task::spawn(did_update(map_recv, path, shopping_carts));
}

async fn did_update(
    map_recv: MapDownlinkReceiver<String, i32>,
    path: RelativePath,
    shopping_carts: MapLane<String, Value>,
) {
    let shopping_carts = &shopping_carts;
    let node = &path.node;

    map_recv
        .into_stream()
        .filter_map(|event| async {
            match event {
                Event::Remote(TypedViewWithEvent {
                    view,
                    event: MapEvent::Update(key),
                }) => {
                    let value = view.get(&key);
                    Some((key, value))
                }
                _ => None,
            }
        })
        .for_each(|(cmd_key, cmd_value)| async move {
            let cmd_value = cmd_value.unwrap().into_value();
            let cmd_key = cmd_key.into_value();

            let update = shopping_carts
                .get(node.to_string())
                .and_then(|maybe_shopping_cart| {
                    let shopping_cart = if let Some(shopping_cart) = maybe_shopping_cart {
                        let mut shopping_cart = (*shopping_cart).clone();

                        if let Value::Record(_attr, items) = &mut shopping_cart {
                            let mut exists = false;

                            for item in items.iter_mut() {
                                if let Item::Slot(key, value) = item {
                                    if key == &cmd_key {
                                        *value = cmd_value.clone();
                                        exists = true;
                                        break;
                                    }
                                }
                            }

                            if !exists {
                                let slot = Item::Slot(cmd_key.clone(), cmd_value.clone());
                                items.push(slot);
                            }
                        }

                        shopping_cart
                    } else {
                        Value::record(vec![Item::Slot(cmd_key.clone(), cmd_value.clone())])
                    };

                    shopping_carts.update(node.to_string(), Arc::new(shopping_cart))
                });

            atomically(&update, StmRetryStrategy::new(RetryStrategy::default()))
                .await
                .unwrap();
        })
        .await;
}

fn log_message<T: Display>(node_uri: &RelativeUri, message: &T) {
    println!("{}: {}", node_uri, message);
}
