// Copyright 2015-2023 Swim Inc.
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

use std::{error::Error, time::Duration};

use example_util::example_logging;
use swim::{agent::agent_model::AgentModel, route::RoutePattern, server::ServerBuilder};
use tokio::time::Instant;

use crate::{
    agents::{
        agency::{AgencyAgent, AgencyLifecycle},
        country::{CountryAgent, CountryLifecycle},
        state::{StateAgent, StateLifecycle},
        vehicle::{VehicleAgent, VehicleLifecycle},
    },
    buses_api::BusesApi,
};

mod agents;
mod buses_api;
mod model;

const POLL_DELAY: Duration = Duration::from_secs(10);
const WEEK: Duration = Duration::from_secs(7 * 86400);
const HISTORY_LEN: usize = 10;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    example_logging()?;

    let api = create_api();

    let agencies = model::agencies();
    let mut builder = ServerBuilder::with_plane_name("Transit Plane");

    for agency in agencies {
        let uri = agency.uri();
        let route = RoutePattern::parse_str(&uri)?;
        let lifecycle = AgencyLifecycle::new(api.clone(), agency, POLL_DELAY);
        let agent = AgentModel::new(AgencyAgent::default, lifecycle.into_lifecycle());
        builder = builder.add_route(route, agent);
    }

    let epoch = Instant::now() - WEEK;
    let vehicle_route = RoutePattern::parse_str("/vehicle/:country/:state/:id")?;
    let vehicle_lifecycle = move || VehicleLifecycle::new(epoch, HISTORY_LEN).into_lifecycle();
    let vehicle_agent = AgentModel::from_fn(VehicleAgent::default, vehicle_lifecycle);
    builder = builder.add_route(vehicle_route, vehicle_agent);

    let state_route = RoutePattern::parse_str("/state/:country/:state")?;
    let state_agent = AgentModel::new(StateAgent::default, StateLifecycle.into_lifecycle());
    builder = builder.add_route(state_route, state_agent);

    let country_route = RoutePattern::parse_str("/country/:country")?;
    let country_agent = AgentModel::new(CountryAgent::default, CountryLifecycle.into_lifecycle());
    builder = builder.add_route(country_route, country_agent);

    let server = builder
        .update_config(|config| {
            config.agent_runtime.inactive_timeout = Duration::from_secs(5 * 60);
        })
        .build()
        .await?;

    server_runner::run_server(server).await
}

#[cfg(feature = "mock-server")]
fn create_api() -> BusesApi {
    let port = std::env::args()
        .next()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(8080);
    BusesApi::new(format!("http://127.0.0.1:{}", port), false)
}

#[cfg(not(feature = "mock-server"))]
fn create_api() -> BusesApi {
    BusesApi::default()
}

#[cfg(feature = "mock-server")]
mod server_runner {
    use example_util::manage_handle;
    use std::{error::Error, sync::Arc};
    use swim::server::{BoxServer, Server};
    use tokio::sync::Notify;

    pub async fn run_server(swim_server: BoxServer) -> Result<(), Box<dyn Error + Send + Sync>> {
        let (task, handle) = swim_server.run();

        let trigger = Arc::new(Notify::new());
        let mock_server = tokio::spawn(transit_fixture::run_mock_server(trigger.clone()));

        let task_with_trigger = async move {
            let result = task.await;
            trigger.notify_one();
            result
        };

        let shutdown = manage_handle(handle);

        let (_, mock_result, result) = tokio::join!(shutdown, mock_server, task_with_trigger);

        result?;
        mock_result??;
        println!("Server stopped successfully.");
        Ok(())
    }
}
#[cfg(not(feature = "mock-server"))]
mod server_runner {
    use example_util::manage_handle;
    use std::error::Error;
    use swim::server::{BoxServer, Server};

    pub async fn run_server(swim_server: BoxServer) -> Result<(), Box<dyn Error + Send + Sync>> {
        let (task, handle) = swim_server.run();

        let shutdown = manage_handle(handle);

        let (_, result) = tokio::join!(shutdown, task);

        result?;
        println!("Server stopped successfully.");
        Ok(())
    }
}
