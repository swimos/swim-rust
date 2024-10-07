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

//! An example demonstrating a Kafka ingress connector.
//!
//! Run the server using the following:
//! ```text
//! $ cargo run --bin kafka-ingress-connector
//! ```
//!
//! And run the client with the following:
//! ```text
//! $ cargo run --bin kafka_connector_client
//! ```

use std::{error::Error, str::FromStr, time::Duration};

use clap::Parser;
use example_util::{example_filter, manage_handle};
use swimos::{
    route::{RoutePattern, RouteUri},
    server::{Server, ServerBuilder},
};
use swimos_connector::IngressConnectorModel;
use swimos_connector_kafka::{
    KafkaIngressConfiguration, KafkaIngressConnector, KafkaIngressSpecification,
};
use swimos_recon::parser::parse_recognize;

mod params;

use params::Params;
use tracing::error;
use tracing_subscriber::filter::LevelFilter;

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

    let route = RoutePattern::parse_str("/kafka")?;

    let connector_agent = IngressConnectorModel::for_fn(move || {
        KafkaIngressConnector::for_config(connector_config.clone())
    });

    let server = ServerBuilder::with_plane_name("Example Plane")
        .set_bind_addr("127.0.0.1:8080".parse()?)
        .add_route(route, connector_agent)
        .update_config(|config| {
            config.agent_runtime.inactive_timeout = Duration::from_secs(5 * 60);
        })
        .build()
        .await?;

    let (task, handle) = server.run();

    let uri = RouteUri::from_str("/kafka")?;

    let shutdown = async move {
        if let Err(error) = handle.start_agent(uri).await {
            error!(error = %error, "Failed to start connector agent.");
        }
        manage_handle(handle).await
    };

    let (_, result) = tokio::join!(shutdown, task);

    result?;
    println!("Server stopped successfully.");
    Ok(())
}

const CONNECTOR_CONFIG: &str = include_str!("kafka_connector.recon");

async fn load_config(
    path: Option<String>,
) -> Result<KafkaIngressConfiguration, Box<dyn Error + Send + Sync>> {
    let content: String;
    let recon = if let Some(path) = path {
        content = tokio::fs::read_to_string(path).await?;
        &content
    } else {
        CONNECTOR_CONFIG
    };
    let config = parse_recognize::<KafkaIngressSpecification>(recon, true)?.build()?;
    Ok(config)
}

pub fn setup_logging() -> Result<(), Box<dyn Error + Send + Sync>> {
    let filter = example_filter()?.add_directive(LevelFilter::INFO.into());
    tracing_subscriber::fmt().with_env_filter(filter).init();
    Ok(())
}
