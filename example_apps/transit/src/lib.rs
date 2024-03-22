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

use std::{error::Error, net::SocketAddr, time::Duration};

use example_util::manage_handle_report;
use futures::{stream::FuturesUnordered, StreamExt};
use swimos::{
    agent::agent_model::AgentModel,
    route::{RoutePattern, RouteUri},
    server::{ServerBuilder, ServerHandle},
};
use tokio::{sync::oneshot, time::Instant};
use tracing::{debug, error, info};
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

pub mod agents;
pub mod buses_api;
pub mod model;
pub mod ui;

const POLL_DELAY: Duration = Duration::from_secs(10);
const WEEK: Duration = Duration::from_secs(7 * 86400);
const HISTORY_LEN: usize = 10;

pub fn create_plane(
    agencies: Vec<Agency>,
    api: BusesApi,
    mut builder: ServerBuilder,
) -> Result<ServerBuilder, Box<dyn Error + Send + Sync>> {
    debug!("Adding agency routes.");
    for agency in agencies {
        let uri = agency.uri();
        let route = RoutePattern::parse_str(&uri)?;
        let lifecycle = AgencyLifecycle::new(api.clone(), agency, POLL_DELAY);
        let agent = AgentModel::new(AgencyAgent::default, lifecycle.into_lifecycle());
        builder = builder.add_route(route, agent);
    }

    let epoch = Instant::now() - WEEK;
    debug!(epoch = ?epoch, "Adding vehicle route.");
    let vehicle_route = RoutePattern::parse_str("/vehicle/:country/:state/:agency/:id")?;
    let vehicle_lifecycle = move || VehicleLifecycle::new(epoch, HISTORY_LEN).into_lifecycle();
    let vehicle_agent = AgentModel::from_fn(VehicleAgent::default, vehicle_lifecycle);
    builder = builder.add_route(vehicle_route, vehicle_agent);

    debug!("Adding state route.");
    let state_route = RoutePattern::parse_str("/state/:country/:state")?;
    let state_agent = AgentModel::new(StateAgent::default, StateLifecycle.into_lifecycle());
    builder = builder.add_route(state_route, state_agent);

    debug!("Adding country routes.");
    let country_route = RoutePattern::parse_str("/country/:country")?;
    let country_agent = AgentModel::new(CountryAgent::default, CountryLifecycle.into_lifecycle());
    builder = builder.add_route(country_route, country_agent);

    Ok(builder)
}

/// Starts the agents for each supported agency and then waits for the server to stop.
pub async fn start_agencies_and_wait(
    agency_uris: Vec<RouteUri>,
    handle: ServerHandle,
    bound: Option<oneshot::Sender<SocketAddr>>,
) {
    info!("Starting agency agents.");
    let stream = agency_uris
        .into_iter()
        .map(|route| handle.start_agent(route))
        .collect::<FuturesUnordered<_>>()
        .filter_map(|r| async move { r.err() });
    let errors = stream.collect::<Vec<_>>().await;
    for error in errors {
        error!(error = %error, "Failed to start agency agent.");
    }
    manage_handle_report(handle, bound).await
}
