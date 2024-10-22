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

//! An example demonstrating a Fluvio connector.
//!
//! Run the application using the following:
//! ```text
//! $ cargo run --bin fluvio_connector
//! ```

use clap::Parser;
use example_util::{example_filter, manage_handle};
use fluvio::RecordKey;
use rand::Rng;
use serde_json::json;
use std::collections::HashSet;
use std::{error::Error, str::FromStr, time::Duration};
use swimos::{
    route::{RoutePattern, RouteUri},
    server::{Server, ServerBuilder},
};
use swimos_connector::IngressConnectorModel;
use tokio::time::sleep;

mod agent;
mod params;

use crate::agent::{SensorAgent, SensorLifecycle};
use params::Params;
use swimos::agent::agent_model::AgentModel;
use swimos_connector_fluvio::{FluvioIngressConfiguration, FluvioIngressConnector};
use tracing::error;
use tracing_subscriber::filter::LevelFilter;

const FLUVIO_TOPIC: &str = "sensors";
const MAX_AGENTS: usize = 50;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let Params {
        config,
        enable_logging,
    } = Params::parse();
    if enable_logging {
        setup_logging()?;
    }

    let connector_config = load_config(config).await?;

    let route = RoutePattern::parse_str("/fluvio")?;

    let connector_agent = IngressConnectorModel::for_fn(move || {
        FluvioIngressConnector::for_config(connector_config.clone())
    });
    let sensor_agent = AgentModel::new(SensorAgent::default, SensorLifecycle.into_lifecycle());

    let server = ServerBuilder::with_plane_name("Example Plane")
        .set_bind_addr("127.0.0.1:8080".parse()?)
        .add_route(route, connector_agent)
        .add_route(RoutePattern::parse_str("/sensors/:id")?, sensor_agent)
        .update_config(|config| {
            config.agent_runtime.inactive_timeout = Duration::from_secs(5 * 60);
        })
        .build()
        .await?;

    let (task, handle) = server.run();

    let uri = RouteUri::from_str("/fluvio")?;

    let shutdown = async move {
        if let Err(error) = handle.start_agent(uri).await {
            error!(error = %error, "Failed to start connector agent.");
        }
        manage_handle(handle).await
    };

    let (_, task_result, producer_result) = tokio::join!(shutdown, task, run_fluvio());

    producer_result?;
    task_result?;
    println!("Server stopped successfully.");
    Ok(())
}

async fn run_fluvio() -> Result<(), Box<dyn Error + Send + Sync>> {
    let producer = fluvio::producer(FLUVIO_TOPIC).await?;
    let mut agent_ids = HashSet::new();

    loop {
        let (agent_id, payload) = {
            let len = agent_ids.len();
            let mut rng = rand::thread_rng();

            let agent_id = if len == MAX_AGENTS {
                rng.gen_range(0..len)
            } else {
                let id = len + 1;
                agent_ids.insert(id);
                id
            };

            let payload = json! {
                {
                    "temperature": rng.gen_range(10..100),
                    "voltage": rng.gen_range::<f64, _>(0.0..12.0)
                }
            };

            (agent_id, serde_json::to_vec(&payload)?)
        };

        producer
            .send(RecordKey::from((agent_id as u32).to_le_bytes()), payload)
            .await?;
        producer.flush().await?;

        sleep(Duration::from_millis(500)).await;
    }
}

const CONNECTOR_CONFIG: &str = include_str!("fluvio_connector.recon");

async fn load_config(
    path: Option<String>,
) -> Result<FluvioIngressConfiguration, Box<dyn Error + Send + Sync>> {
    let content: String;
    let recon = if let Some(path) = path {
        content = tokio::fs::read_to_string(path).await?;
        &content
    } else {
        CONNECTOR_CONFIG
    };
    FluvioIngressConfiguration::from_str(recon)
}

pub fn setup_logging() -> Result<(), Box<dyn Error + Send + Sync>> {
    let filter = example_filter()?
        .add_directive(LevelFilter::INFO.into())
        .add_directive("swimos_connector_fluvio=trace".parse()?)
        .add_directive("swimos_connector=info".parse()?);
    tracing_subscriber::fmt().with_env_filter(filter).init();
    Ok(())
}
