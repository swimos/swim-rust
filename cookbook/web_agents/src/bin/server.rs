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

mod agents;

use crate::agents::{UnitAgent, UnitAgentLifecycle};
use futures::join;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use swim_common::model::Value;
use swim_common::warp::path::{Path, RelativePath};
use swim_server::interface::SwimServerBuilder;
use swim_server::plane::spec::PlaneBuilder;
use swim_server::RoutePattern;
use tokio::time;

#[tokio::main]
async fn main() {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9001);
    let mut plane_builder = PlaneBuilder::new();

    plane_builder
        .add_route::<UnitAgent, (), UnitAgentLifecycle>(
            RoutePattern::parse_str("/unit/:id").unwrap(),
            (),
            UnitAgentLifecycle,
        )
        .unwrap();

    let mut swim_server_builder = SwimServerBuilder::default();
    swim_server_builder.add_plane(plane_builder.build());
    let (swim_server, server_handle) = swim_server_builder.bind_to(address).build().unwrap();

    let client = swim_server.client();

    let stop = async {
        client
            .send_command(
                Path::Local(RelativePath::new("/unit/1", "unused")),
                Value::Extant,
            )
            .await
            .unwrap();
        client
            .send_command(
                Path::Local(RelativePath::new("/unit/foo", "unused")),
                Value::Extant,
            )
            .await
            .unwrap();
        client
            .send_command(
                Path::Local(RelativePath::new("/unit/foo_1", "unused")),
                Value::Extant,
            )
            .await
            .unwrap();
        time::sleep(Duration::from_secs(1)).await;
        println!("Server will shut down in 3 seconds.");
        time::sleep(Duration::from_secs(3)).await;
        println!("Sent shutdown signal to server.");
        server_handle.stop().await.unwrap();
    };

    println!("Running basic server...");
    join!(swim_server.run(), stop).0.unwrap();
}