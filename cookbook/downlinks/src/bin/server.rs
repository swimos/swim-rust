// Copyright 2015-2021 Swim Inc.
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
use crate::agents::{ListenerAgent, UnitAgent};
use futures::join;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use swim_server::interface::SwimServerBuilder;
use swim_server::RoutePattern;
use tokio::time;

mod agents;

#[tokio::main]
async fn main() {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9001);

    let mut swim_server_builder = SwimServerBuilder::no_store(Default::default()).unwrap();

    let mut plane_builder = swim_server_builder.plane_builder("example").unwrap();

    plane_builder
        .add_route::<UnitAgent, (), ()>(RoutePattern::parse_str("/unit/:id").unwrap(), (), ())
        .unwrap();

    plane_builder
        .add_route::<ListenerAgent, (), ()>(RoutePattern::parse_str("/listener").unwrap(), (), ())
        .unwrap();

    swim_server_builder.add_plane(plane_builder.build());
    let (swim_server, server_handle) = swim_server_builder.bind_to(address).build().unwrap();

    let stop = async {
        time::sleep(Duration::from_secs(300)).await;
        server_handle.stop().await.unwrap();
    };

    println!("Running basic server...");
    join!(swim_server.run(), stop).0.unwrap();
}
