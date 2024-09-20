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
use std::{collections::HashSet, error::Error, str::FromStr, time::Duration};
use swimos::{
    agent::{lifecycle, AgentLaneModel},
    route::{RoutePattern, RouteUri},
    server::{Server, ServerBuilder},
};
use swimos_agent::{
    agent_lifecycle::HandlerContext,
    agent_model::AgentModel,
    event_handler::{EventHandler, HandlerActionExt},
    lanes::ValueLane,
};
use swimos_connector::{
    deserialization::JsonDeserializer,
    relay::{AgentRelay, LaneSelector, NodeSelector, PayloadSelector, Selectors},
    ConnectorModel,
};
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
                    "temperature": rng.gen_range(10..100),
                    "voltage": rng.gen_range::<f64, _>(0.0..12.0)
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
    let node = NodeSelector::from_str("/sensors/$key")?;
    let lane = LaneSelector::from_str("temperature")?;
    let payload = PayloadSelector::value("$value.temperature", true)?;

    let config = FluvioConnectorConfiguration {
        topic: FLUVIO_TOPIC.to_string(),
        fluvio: FluvioConfig::load()?,
        partition: 0,
        offset: Offset::end(),
    };
    let connector_agent = ConnectorModel::new(FluvioConnector::relay(
        config,
        AgentRelay::new(
            Selectors::new(node, lane, payload),
            JsonDeserializer,
            JsonDeserializer,
        ),
    ));
    let temperature_agent = AgentModel::new(SensorAgent::default, SensorLifecycle.into_lifecycle());

    let server = ServerBuilder::with_plane_name("Example Plane")
        .set_bind_addr("127.0.0.1:8080".parse()?)
        .add_route(RoutePattern::parse_str("/fluvio")?, connector_agent)
        .add_route(RoutePattern::parse_str("/sensors/:id")?, temperature_agent)
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
pub struct SensorAgent {
    temperature: ValueLane<i32>,
    voltage: ValueLane<f64>,
}

#[derive(Clone)]
pub struct SensorLifecycle;

#[lifecycle(SensorAgent)]
impl SensorLifecycle {
    #[on_start]
    pub fn on_start(&self, context: HandlerContext<SensorAgent>) -> impl EventHandler<SensorAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || {
                println!("Starting agent at: {}", uri);
            })
        })
    }

    #[on_stop]
    pub fn on_stop(&self, context: HandlerContext<SensorAgent>) -> impl EventHandler<SensorAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || {
                println!("Stopping agent at: {}", uri);
            })
        })
    }

    #[on_event(temperature)]
    pub fn on_temperature(
        &self,
        context: HandlerContext<SensorAgent>,
        value: &i32,
    ) -> impl EventHandler<SensorAgent> {
        let n = *value;
        context.effect(move || {
            println!("Setting temperature to: {}", n);
        })
    }

    #[on_event(voltage)]
    pub fn on_voltage(
        &self,
        context: HandlerContext<SensorAgent>,
        value: &f64,
    ) -> impl EventHandler<SensorAgent> {
        let n = *value;
        context.effect(move || {
            println!("Setting voltage to: {}", n);
        })
    }
}
