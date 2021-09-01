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
use crate::agents::{ListenerAgent, UnitAgent};
use futures::future::join3;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use swim_client::downlink::SchemaViolations;
use swim_common::warp::path::{Path, RelativePath};
use swim_server::interface::SwimServerBuilder;
use swim_server::plane::spec::PlaneBuilder;
use swim_server::RoutePattern;
use tokio::time;

mod agents;

#[tokio::main]
async fn main() {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9001);
    let mut plane_builder = PlaneBuilder::new();

    plane_builder
        .add_route::<UnitAgent, (), ()>(RoutePattern::parse_str("/unit/:id").unwrap(), (), ())
        .unwrap();

    plane_builder
        .add_route::<ListenerAgent, (), ()>(RoutePattern::parse_str("/listener").unwrap(), (), ())
        .unwrap();

    let mut swim_server_builder = SwimServerBuilder::default();
    swim_server_builder.add_plane(plane_builder.build());
    let (swim_server, server_handle) = swim_server_builder.bind_to(address).build().unwrap();

    let client = swim_server.client();

    //Todo dm
    let plane_downlink = async {
        let event_dl = client
            .event_downlink::<String>(
                Path::Local(RelativePath::new("/unit/0", "add_item")),
                SchemaViolations::Ignore,
            )
            .await
            .unwrap();

        eprintln!("1");
        let mut rec = event_dl.subscribe().expect("Downlink closed unexpectedly.");

        eprintln!("2");
        while let Some(event) = rec.recv().await {
            println!("Event downlink saw: {}", event)
        }
    };

    let stop = async {
        time::sleep(Duration::from_secs(300)).await;
        server_handle.stop().await.unwrap();
    };

    println!("Running basic server...");
    join3(swim_server.run(), plane_downlink, stop)
        .await
        .0
        .unwrap();
}
