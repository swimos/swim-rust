// Copyright 2015-2024 Swim Inc.
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

use crate::agents::{
    AggregatedLifecycle, AggregatedStatisticsAgent, StreetStatisticsAgent,
    StreetStatisticsLifecycle,
};
use example_util::{example_logging, manage_handle};
use std::{error::Error, time::Duration};
use swimos::{
    agent::agent_model::AgentModel, route::RoutePattern, server::Server, server::ServerBuilder,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    example_logging()?;

    let street_route = RoutePattern::parse_str("/state/:name")?;
    let street_agent = AgentModel::new(
        StreetStatisticsAgent::default,
        StreetStatisticsLifecycle.into_lifecycle(),
    );

    let aggregated_route = RoutePattern::parse_str("/join/state/:name")?;
    let aggregated_agent = AgentModel::new(
        AggregatedStatisticsAgent::default,
        AggregatedLifecycle.into_lifecycle(),
    );

    let server = ServerBuilder::with_plane_name("Statistics Plane")
        .add_route(street_route, street_agent)
        .add_route(aggregated_route, aggregated_agent)
        .update_config(|config| {
            config.agent_runtime.inactive_timeout = Duration::from_secs(5 * 60);
        })
        .build()
        .await?;

    let (task, handle) = server.run();
    let shutdown = manage_handle(handle);
    let (_, result) = tokio::join!(shutdown, task);

    result?;
    println!("Server stopped successfully.");
    Ok(())
}
