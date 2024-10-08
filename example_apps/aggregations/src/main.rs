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

//! Reference code for the [Group and Aggregate Agents](https://www.swimos.org/server/rust/aggregations/) guide.
//!
//! Run this example using the following:
//! ```text
//! $ cargo run --bin aggregations
//! ```

use std::error::Error;
use std::str::FromStr;
use std::time::Duration;

use crate::area::Area;
use crate::{
    area::{AreaAgent, AreaLifecycle},
    car::CarAgent,
    car::CarLifecycle,
    city::{CityAgent, CityLifecycle},
};
use example_util::{example_logging, manage_handle};
use swimos::route::RouteUri;
use swimos::{
    agent::agent_model::AgentModel,
    route::RoutePattern,
    server::{Server, ServerBuilder},
};

mod area;
mod car;
mod city;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    example_logging()?;

    let car_agent = AgentModel::new(CarAgent::default, CarLifecycle.into_lifecycle());
    let area_agent = move |name| {
        AgentModel::new(
            AreaAgent::default,
            AreaLifecycle::new(name).into_lifecycle(),
        )
    };
    let aggregate_agent = AgentModel::new(CityAgent::default, CityLifecycle.into_lifecycle());

    let mut builder = ServerBuilder::with_plane_name("Example Plane")
        .set_bind_addr("127.0.0.1:8080".parse()?)
        .add_route(RoutePattern::parse_str("/cars/:car_id")?, car_agent)
        .add_route(RoutePattern::parse_str("/city")?, aggregate_agent)
        .update_config(|config| {
            config.agent_runtime.inactive_timeout = Duration::from_secs(5 * 60);
        });

    for area in Area::universe() {
        builder = builder.add_route(
            RoutePattern::parse_str(format!("/area/{}", area).as_str())?,
            area_agent(area),
        );
    }

    let server = builder.build().await?;

    let (task, handle) = server.run();
    let _task = tokio::spawn(task);

    for i in 0..1000 {
        let route = format!("/cars/{i}");
        handle
            .start_agent(RouteUri::from_str(route.as_str())?)
            .await
            .expect("Failed to start agent");
    }

    manage_handle(handle).await;
    println!("Server stopped successfully.");

    Ok(())
}
