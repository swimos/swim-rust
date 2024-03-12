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

use std::{error::Error, net::SocketAddr};
use std::str::FromStr;

use example_util::manage_handle_report;
use futures::StreamExt;
use swim::{
    agent::agent_model::AgentModel,
    route::{RoutePattern, RouteUri},
    server::{ServerBuilder, ServerHandle},
};
use tokio::sync::oneshot;
use tracing::{debug, error, info};
use crate::agents::city::{CityAgent, CityLifecycle};

pub mod agents;
pub mod ui;

const PALO_AUTO_ROUTE: &str = "/city/PaloAlto_CA_US";

pub fn create_plane(
    mut builder: ServerBuilder,
) -> Result<ServerBuilder, Box<dyn Error + Send + Sync>> {
    debug!("Adding city agent.");
    let city_route = RoutePattern::parse_str("/city/:id")?;

    let lifecycle_fn = move || CityLifecycle::new().into_lifecycle();
    let city_agent = AgentModel::from_fn(CityAgent::default, lifecycle_fn);
    builder = builder.add_route(city_route, city_agent);

    Ok(builder)
}

pub async fn start_agents_and_wait(
    handle: ServerHandle,
    bound: Option<oneshot::Sender<SocketAddr>>,
) {
    info!("Starting agent for Palo Alto.");

    if let Err(error) = handle.start_agent(RouteUri::from_str(PALO_AUTO_ROUTE).unwrap()).await {
        error!(error = %error, "Failed to start city agent.");
    }

    manage_handle_report(handle, bound).await
}
