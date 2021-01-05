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

mod unit_agent;

use crate::unit_agent::{UnitAgentConfig, UnitAgentLifecycle};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use swim_server::interface::SwimServer;
use swim_server::route_pattern::RoutePattern;

#[tokio::main]
async fn main() {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9001);
    let (mut swim_server, _server_handle) = SwimServer::new_with_default(address);

    swim_server
        .add_route(
            RoutePattern::parse_str("/unit/0").unwrap(),
            UnitAgentConfig {},
            UnitAgentLifecycle {},
        )
        .unwrap();

    println!("Running basic server...");
    swim_server.run().await.unwrap();
}
