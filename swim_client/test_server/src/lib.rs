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

use server_store::plane::SwimPlaneStore;
use server_store::NoStore;
use std::fmt::Debug;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use swim_common::model::Value;
use swim_common::warp::path::{AbsolutePath, RelativePath};
use swim_server::agent::command_lifecycle;
use swim_server::agent::lane::channels::update::StmRetryStrategy;
use swim_server::agent::lane::model::command::CommandLane;
use swim_server::agent::lane::model::map::MapLane;
use swim_server::agent::lane::model::value::ValueLane;
use swim_server::agent::map_lifecycle;
use swim_server::agent::value_lifecycle;
use swim_server::agent::AgentContext;
use swim_server::agent::SwimAgent;
use swim_server::interface::{ServerHandle, SwimServer, SwimServerBuilder, SwimServerConfig};
use swim_server::stm::stm::Stm;
use swim_server::stm::transaction::atomically;
use swim_server::{RetryStrategy, RoutePattern};

pub async fn build_server() -> (SwimServer<SwimPlaneStore<NoStore>>, ServerHandle) {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
    let mut swim_server_builder =
        SwimServerBuilder::no_store(SwimServerConfig::default()).expect("Failed to build store");
    let mut plane_builder = swim_server_builder.plane_builder("test").unwrap();

    plane_builder
        .add_route::<UnitAgent, (), ()>(RoutePattern::parse_str("/unit/:id").unwrap(), (), ())
        .unwrap();

    plane_builder
        .add_route::<DownlinkAgent, (), ()>(
            RoutePattern::parse_str("/downlink/:id").unwrap(),
            (),
            (),
        )
        .unwrap();

    swim_server_builder.add_plane(plane_builder.build());
    swim_server_builder.bind_to(address).build().unwrap()
}

#[derive(Debug, SwimAgent)]
struct DownlinkAgent {
    #[lifecycle(name = "ParkLifecycle")]
    pub park: CommandLane<String>,

    #[lifecycle(name = "GarageLifecycle")]
    pub garage: ValueLane<String>,

    #[lifecycle(name = "SelfParkLifecycle")]
    pub self_park: CommandLane<String>,
}

#[command_lifecycle(agent = "DownlinkAgent", command_type = "String", on_command)]
struct ParkLifecycle;

impl ParkLifecycle {
    async fn on_command<Context>(
        &self,
        command: &str,
        _model: &CommandLane<String>,
        context: &Context,
    ) where
        Context: AgentContext<DownlinkAgent> + Sized + Send + Sync + 'static,
    {
        if command == "BMW" {
            let (dl, _recv) = context
                .downlinks_context()
                .value_downlink(
                    RelativePath::new("/downlink/1", "garage").into(),
                    "".to_string(),
                )
                .await
                .unwrap();

            let current = dl.get().await.unwrap();
            dl.set(format!("{} {}", current, "BMW")).await.unwrap();
        } else if command == "Audi" {
            let (dl, _recv) = context
                .downlinks_context()
                .value_downlink(
                    RelativePath::new("/downlink/2", "garage").into(),
                    "".to_string(),
                )
                .await
                .unwrap();

            let current = dl.get().await.unwrap();
            dl.set(format!("{} {}", current, "Audi")).await.unwrap();
        } else if command.starts_with("Toyota") {
            let splitter = command.split_whitespace();
            let port = splitter.last().unwrap();
            let host = url::Url::parse(&format!("warp://127.0.0.1:{}", port)).unwrap();

            let (dl, _recv) = context
                .downlinks_context()
                .value_downlink(
                    AbsolutePath::new(host, "/downlink/1", "garage").into(),
                    "".to_string(),
                )
                .await
                .unwrap();

            let current = dl.get().await.unwrap();
            dl.set(format!("{} {}", current, "Toyota")).await.unwrap();
        } else {
            let _ = atomically(
                &context.agent().garage.get().and_then(|current| {
                    if (*current).is_empty() {
                        context.agent().garage.set(command.to_string())
                    } else {
                        context
                            .agent()
                            .garage
                            .set(format!("{} {}", *current, command))
                    }
                }),
                StmRetryStrategy::new(RetryStrategy::default()),
            )
            .await;
        }
    }
}

#[command_lifecycle(agent = "DownlinkAgent", command_type = "String", on_command)]
struct SelfParkLifecycle;

impl SelfParkLifecycle {
    async fn on_command<Context>(
        &self,
        command: &str,
        _model: &CommandLane<String>,
        context: &Context,
    ) where
        Context: AgentContext<DownlinkAgent> + Sized + Send + Sync + 'static,
    {
        context
            .agent()
            .park
            .commander()
            .command(command.to_string())
            .await;
    }
}

#[value_lifecycle(agent = "DownlinkAgent", event_type = "String")]
struct GarageLifecycle;

#[derive(Debug, SwimAgent)]
struct UnitAgent {
    #[lifecycle(name = "IdLifecycle")]
    pub id: ValueLane<i32>,

    #[lifecycle(name = "InfoLifecycle")]
    pub info: ValueLane<String>,

    #[lifecycle(name = "PublishInfoLifecycle")]
    pub publish_info: CommandLane<String>,

    #[lifecycle(name = "ShoppingCartLifecycle")]
    pub shopping_cart: MapLane<String, i32>,

    #[lifecycle(name = "IntegerMapLifecycle")]
    pub integer_map: MapLane<i32, i32>,

    #[lifecycle(name = "DataLifecycle")]
    pub data: ValueLane<Value>,
}

#[value_lifecycle(agent = "UnitAgent", event_type = "i32")]
struct IdLifecycle;

#[value_lifecycle(agent = "UnitAgent", event_type = "String")]
struct InfoLifecycle;

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
        context
            .agent()
            .info
            .store(format!("from publish_info: {} ", command))
            .await;
    }
}

#[map_lifecycle(agent = "UnitAgent", key_type = "String", value_type = "i32")]
struct ShoppingCartLifecycle;

#[map_lifecycle(agent = "UnitAgent", key_type = "i32", value_type = "i32")]
struct IntegerMapLifecycle;

#[value_lifecycle(agent = "UnitAgent", event_type = "Value")]
struct DataLifecycle;
