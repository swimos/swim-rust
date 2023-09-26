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

use std::{collections::HashSet, error::Error, time::Duration, env::args};

use example_util::{example_logging, manage_handle};
use futures::{stream::FuturesUnordered, StreamExt};
use swim::{
    agent::agent_model::AgentModel,
    route::{RoutePattern, RouteUri},
    server::{ServerBuilder, ServerHandle},
};
use tokio::time::Instant;
use transit_model::agency::Agency;

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

    let agencies = model::agencies();
    let agency_uris = agencies
        .iter()
        .map(|a| a.uri().parse::<RouteUri>())
        .collect::<Result<Vec<_>, _>>()?;

    server_runner::run_server(agency_uris, |api: BusesApi| async move {
        let mut builder = ServerBuilder::with_plane_name("Transit Plane");

        builder = create_plane(agencies, api, builder, read_params())?;

        let server = builder
            .update_config(|config| {
                config.agent_runtime.inactive_timeout = Duration::from_secs(5 * 60);
            })
            .build()
            .await?;
        Ok(server)
    })
    .await
}

fn read_params() -> HashSet<IncludeRoutes> {
    match args().next().as_deref() {
        Some("none") => HashSet::new(),
        Some("vehicles") => [IncludeRoutes::Vehicle].into_iter().collect(),
        Some("state") => [IncludeRoutes::Vehicle, IncludeRoutes::State].into_iter().collect(),
        _ => IncludeRoutes::all(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum IncludeRoutes {
    Vehicle,
    State,
    Country,
}

impl IncludeRoutes {
    fn all() -> HashSet<Self> {
        [
            IncludeRoutes::Vehicle,
            IncludeRoutes::State,
            IncludeRoutes::Country,
        ]
        .into_iter()
        .collect()
    }
}

fn create_plane(
    agencies: Vec<Agency>,
    api: BusesApi,
    mut builder: ServerBuilder,
    inc: HashSet<IncludeRoutes>,
) -> Result<ServerBuilder, Box<dyn Error + Send + Sync>> {
    for agency in agencies {
        let uri = agency.uri();
        let route = RoutePattern::parse_str(&uri)?;
        let lifecycle = AgencyLifecycle::new(api.clone(), agency, POLL_DELAY);
        let agent = AgentModel::new(AgencyAgent::default, lifecycle.into_lifecycle());
        builder = builder.add_route(route, agent);
    }

    if inc.contains(&IncludeRoutes::Vehicle) {
        let epoch = Instant::now() - WEEK;
        let vehicle_route = RoutePattern::parse_str("/vehicle/:country/:state/:id")?;
        let vehicle_lifecycle = move || VehicleLifecycle::new(epoch, HISTORY_LEN).into_lifecycle();
        let vehicle_agent = AgentModel::from_fn(VehicleAgent::default, vehicle_lifecycle);
        builder = builder.add_route(vehicle_route, vehicle_agent);
    }

    if inc.contains(&IncludeRoutes::State) {
        let state_route = RoutePattern::parse_str("/state/:country/:state")?;
        let state_agent = AgentModel::new(StateAgent::default, StateLifecycle.into_lifecycle());
        builder = builder.add_route(state_route, state_agent);
    }

    if inc.contains(&IncludeRoutes::Country) {
        let country_route = RoutePattern::parse_str("/country/:country")?;
        let country_agent =
            AgentModel::new(CountryAgent::default, CountryLifecycle.into_lifecycle());
        builder = builder.add_route(country_route, country_agent);
    }

    Ok(builder)
}

async fn start_agencies_and_wait(agency_uris: Vec<RouteUri>, handle: ServerHandle) {
    let stream = agency_uris
        .into_iter()
        .map(|route| handle.start_agent(route))
        .collect::<FuturesUnordered<_>>()
        .filter_map(|r| async move { r.err() });
    let errors = stream.collect::<Vec<_>>().await;
    for error in errors {
        println!("Failed to start agency agent: {}", error);
    }
    manage_handle(handle).await
}

#[cfg(feature = "mock-server")]
mod server_runner {
    use std::{error::Error, future::Future, sync::Arc};
    use swim::{
        route::RouteUri,
        server::{BoxServer, Server},
    };
    use tokio::{net::TcpListener, sync::Notify};

    use crate::{buses_api::BusesApi, start_agencies_and_wait};

    pub async fn run_server<F, Fut>(
        agency_uris: Vec<RouteUri>,
        f: F,
    ) -> Result<(), Box<dyn Error + Send + Sync>>
    where
        F: FnOnce(BusesApi) -> Fut,
        Fut: Future<Output = Result<BoxServer, Box<dyn Error + Send + Sync>>>,
    {
        let listener = TcpListener::bind("0.0.0.0:0").await?;
        let addr = listener.local_addr()?;
        let api = BusesApi::new(format!("http://127.0.0.1:{}", addr.port()), false);

        let swim_server = f(api).await?;
        let (task, handle) = swim_server.run();

        println!("Listening on: {}", addr);

        let trigger = Arc::new(Notify::new());
        let mock_server = tokio::spawn(transit_fixture::run_mock_server(
            listener.into_std()?,
            trigger.clone(),
        ));

        let task_with_trigger = async move {
            let result = task.await;
            trigger.notify_one();
            result
        };

        let shutdown = start_agencies_and_wait(agency_uris, handle);

        let (_, mock_result, result) = tokio::join!(shutdown, mock_server, task_with_trigger);

        result?;
        mock_result??;
        println!("Server stopped successfully.");
        Ok(())
    }
}
#[cfg(not(feature = "mock-server"))]
mod server_runner {
    use std::error::Error;
    use std::future::Future;
    use swim::{
        route::RouteUri,
        server::{BoxServer, Server},
    };

    use crate::{buses_api::BusesApi, start_agencies_and_wait};

    pub async fn run_server<F, Fut>(
        agency_uris: Vec<RouteUri>,
        f: F,
    ) -> Result<(), Box<dyn Error + Send + Sync>>
    where
        F: FnOnce(BusesApi) -> Fut,
        Fut: Future<Output = Result<BoxServer, Box<dyn Error + Send + Sync>>>,
    {
        let swim_server = f(BusesApi::default()).await?;
        let (task, handle) = swim_server.run();

        let shutdown = start_agencies_and_wait(agency_uris, handle);

        let (_, result) = tokio::join!(shutdown, task);

        result?;
        println!("Server stopped successfully.");
        Ok(())
    }
}
