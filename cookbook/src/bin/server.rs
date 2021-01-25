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
use futures::join;
use std::fmt::Debug;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use swim_server::agent::command_lifecycle;
use swim_server::agent::lane::lifecycle::StatefulLaneLifecycleBase;
use swim_server::agent::lane::model::action::CommandLane;
use swim_server::agent::lane::model::value::{ValueLane, ValueLaneEvent};
use swim_server::agent::lane::strategy::Queue;
use swim_server::agent::value_lifecycle;
use swim_server::agent::AgentContext;
use swim_server::agent::SwimAgent;
use swim_server::agent_lifecycle;
use swim_server::interface::SwimServerBuilder;
use swim_server::plane::spec::PlaneBuilder;
use swim_server::RoutePattern;

#[derive(Debug, SwimAgent)]
pub struct RustAgent {
    #[lifecycle(name = "EchoLifecycle")]
    pub echo: CommandLane<String>,
    #[lifecycle(name = "CounterLifecycle")]
    pub counter: ValueLane<i32>,
}

#[agent_lifecycle(agent = "RustAgent", on_start)]
struct RustAgentLifecycle;

impl RustAgentLifecycle {
    async fn on_start<Context>(&self, _context: &Context)
    where
        Context: AgentContext<RustAgent> + Sized + Send + Sync,
    {
        println!("Rust agent has started!");
    }
}

#[command_lifecycle(agent = "RustAgent", command_type = "String", on_command)]
struct EchoLifecycle;

impl EchoLifecycle {
    async fn on_command<Context>(
        &self,
        command: String,
        _model: &CommandLane<String>,
        _context: &Context,
    ) where
        Context: AgentContext<RustAgent> + Sized + Send + Sync + 'static,
    {
        println!("Command received: {}", command);
    }
}

#[value_lifecycle(agent = "RustAgent", event_type = "i32", on_start, on_event)]
struct CounterLifecycle;

impl CounterLifecycle {
    async fn on_start<Context>(&self, _model: &ValueLane<i32>, _context: &Context)
    where
        Context: AgentContext<RustAgent> + Sized + Send + Sync,
    {
        println!("Counter lane has started!");
    }

    async fn on_event<Context>(
        &self,
        event: &ValueLaneEvent<i32>,
        _model: &ValueLane<i32>,
        _context: &Context,
    ) where
        Context: AgentContext<RustAgent> + Sized + Send + Sync + 'static,
    {
        println!("Event received: {}", event.current);
    }
}

impl StatefulLaneLifecycleBase for CounterLifecycle {
    type WatchStrategy = Queue;

    fn create_strategy(&self) -> Self::WatchStrategy {
        Queue::default()
    }
}

#[tokio::main]
async fn main() {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

    let mut plane_builder = PlaneBuilder::new();
    plane_builder
        .add_route(
            RoutePattern::parse_str("/rust").unwrap(),
            (),
            RustAgentLifecycle {},
        )
        .unwrap();

    let mut swim_server_builder = SwimServerBuilder::default();
    swim_server_builder.add_plane(plane_builder.build());
    let (swim_server, server_handle) = swim_server_builder.bind_to(address).build().unwrap();

    let stop = async {
        task::sleep(Duration::from_secs(60)).await;
        server_handle.stop();
    };

    join!(swim_server.run(), stop).0.unwrap();
}
