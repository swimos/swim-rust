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

use example_util::manage_handle;
use fluvio::{FluvioConfig, Offset, RecordKey};
use rand::Rng;
use serde_json::json;
use std::collections::HashSet;
use std::error::Error;
use std::str::FromStr;
use std::time::Duration;
use swimos::agent::{lifecycle, AgentLaneModel};
use swimos::route::{RoutePattern, RouteUri};
use swimos::server::{Server, ServerBuilder};
use swimos_agent::agent_lifecycle::HandlerContext;
use swimos_agent::agent_model::AgentModel;
use swimos_agent::event_handler::{EventHandler, HandlerActionExt};
use swimos_agent::lanes::ValueLane;
use swimos_connector::simple::formats::json::{
    JsonRelay, LaneSelectorPattern, NodeSelectorPattern, PayloadSelectorPattern,
};
use swimos_connector::simple::{Relay, SimpleConnectorModel};
use swimos_connector_fluvio::{FluvioConnector, FluvioConnectorConfiguration};
use tokio::time::sleep;
use tokio::{join, try_join};
use tracing::error;

const FLUVIO_TOPIC: &str = "sensors";
const MAX_AGENTS: usize = 50;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let swim_server = run_swim_server();
    let fluvio = run_fluvio();
    try_join!(swim_server, fluvio).map(|_| ())
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
                    "temperature": rng.gen_range(10..100)
                }
            };

            (
                serde_json::to_vec(&agent_id)?,
                serde_json::to_vec(&payload)?,
            )
        };

        producer.send(RecordKey::from(agent_id), payload).await?;
        producer.flush().await?;

        sleep(Duration::from_millis(500)).await;
    }
}

async fn run_swim_server() -> Result<(), Box<dyn Error + Send + Sync>> {
    let node = NodeSelectorPattern::from_str("/sensors/$key").unwrap();
    let lane = LaneSelectorPattern::from_str("temperature").unwrap();
    let payload = PayloadSelectorPattern::from_str("$value.temperature").unwrap();

    let config = FluvioConnectorConfiguration {
        topic: FLUVIO_TOPIC.to_string(),
        relay: Relay::Json(JsonRelay::new(node, lane, payload)),
        fluvio: FluvioConfig::load().unwrap(),
        partition: 0,
        offset: Offset::end(),
    };
    let connector_agent = SimpleConnectorModel::new(FluvioConnector::new(config));
    let temperature_agent = AgentModel::new(
        TemperatureAgent::default,
        TemperatureLifecycle.into_lifecycle(),
    );

    let server = ServerBuilder::with_plane_name("Example Plane")
        .set_bind_addr("127.0.0.1:8080".parse()?)
        .add_route(RoutePattern::parse_str("/fluvio").unwrap(), connector_agent)
        .add_route(
            RoutePattern::parse_str("/sensors/:id").unwrap(),
            temperature_agent,
        )
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

    let (_, result) = join!(shutdown, task);

    result?;
    println!("Server stopped successfully.");
    Ok(())
}

#[derive(AgentLaneModel)]
pub struct TemperatureAgent {
    temperature: ValueLane<i32>,
}

#[derive(Clone)]
pub struct TemperatureLifecycle;

#[lifecycle(TemperatureAgent)]
impl TemperatureLifecycle {
    #[on_start]
    pub fn on_start(
        &self,
        context: HandlerContext<TemperatureAgent>,
    ) -> impl EventHandler<TemperatureAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || {
                println!("Starting agent at: {}", uri);
            })
        })
    }

    #[on_stop]
    pub fn on_stop(
        &self,
        context: HandlerContext<TemperatureAgent>,
    ) -> impl EventHandler<TemperatureAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || {
                println!("Stopping agent at: {}", uri);
            })
        })
    }

    #[on_event(temperature)]
    pub fn on_event(
        &self,
        context: HandlerContext<TemperatureAgent>,
        value: &i32,
    ) -> impl EventHandler<TemperatureAgent> {
        let n = *value;
        context.effect(move || {
            println!("Setting temperature to: {}", n);
        })
    }
}
