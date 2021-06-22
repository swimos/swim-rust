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
use swim_client::interface::SwimClient;
use swim_client::runtime::task;
use swim_common::model::Value;
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
struct ShoppingCartsLifecycle {
    shopping_carts: HashMap<String, Arc<i32>>,
}

impl ShoppingCartsLifecycle {
    async fn on_event<Context>(
        &mut self,
        event: &MapLaneEvent<String, Value>,
        _model: &MapLane<String, Value>,
        context: &Context,
    ) where
        Context: AgentContext<ListenerAgent> + Sized + Send + Sync + 'static,
    {
        eprintln!("Test");
        // match event {
        //     MapLaneEvent::Update(key, value) => {
        //         let previous_val = self.previous_state.insert(key.clone(), value.clone());
        //
        //         let message = if let Some(prev_val) = previous_val {
        //             format!("{} count changed to {} from {}", key, value, prev_val)
        //         } else {
        //             format!("{} count changed to {}", key, value)
        //         };
        //
        //         log_message(context.node_uri(), &message);
        //     }
        //     MapLaneEvent::Remove(key) => {
        //         let previous_val = self.previous_state.remove(key);
        //
        //         if let Some(prev_val) = previous_val {
        //             let message = format!("removed <{}, {}>", key, prev_val);
        //             log_message(context.node_uri(), &message);
        //         };
        //     }
        //     _ => (),
        // }
    }
}

impl LaneLifecycle<()> for ShoppingCartsLifecycle {
    fn create(_config: &()) -> Self {
        ShoppingCartsLifecycle {
            shopping_carts: HashMap::new(),
        }
    }
}

#[command_lifecycle(agent = "ListenerAgent", command_type = "String", on_command)]
struct TriggerListenLifecycle;

impl TriggerListenLifecycle {
    async fn on_command<Context>(
        &self,
        command: &String,
        _model: &CommandLane<String>,
        context: &Context,
    ) where
        Context: AgentContext<ListenerAgent> + Sized + Send + Sync + 'static,
    {
        let message = format!("Will listen to: {}", command);
        log_message(context.node_uri(), &message);
        add_subscription(
            RelativePath::new(command, "shopping_cart"),
            context.client(),
            &context.agent().shopping_carts,
        )
        .await;
    }
}

async fn add_subscription(
    path: RelativePath,
    client: SwimClient<Path>,
    shopping_carts: &MapLane<String, Value>,
) {
    let (_, map_recv) = client
        .map_downlink(Path::Local(path.clone()))
        .await
        .unwrap();

    let shopping_cart = atomically(
        &shopping_carts.get(path.to_string()),
        StmRetryStrategy::new(RetryStrategy::default()),
    )
    .await
    .unwrap()
    .unwrap();

    match &*shopping_cart {
        Value::Record(_attr, items) => {
            let item = items.get(0).unwrap();
            eprintln!("item = {:#?}", item);
        }
        _ => unimplemented!(),
    }

    task::spawn(did_update(map_recv, 0));
}

async fn did_update(map_recv: MapDownlinkReceiver<String, i32>, default: i32) {
    map_recv
        .into_stream()
        .filter_map(|event| async {
            match event {
                Event::Remote(TypedViewWithEvent {
                    view,
                    event: MapEvent::Update(key),
                }) => Some((key, view)),
                _ => None,
            }
        })
        .for_each(|(key, current)| async move {
            let shopping_cart = eprintln!("key = {:#?}", key);
            eprintln!("current = {:#?}", current);
        })
        .await;
}

fn log_message<T: Display>(node_uri: &RelativeUri, message: &T) {
    println!("{}: {}", node_uri, message);
}
