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

//! An example demonstrating Value Downlinks.
//!
//! Run the server using the following:
//! ```text
//! $ cargo run --bin local-downlinks
//! ```

use std::{error::Error, time::Duration};

use example_util::{example_logging, manage_handle};
use swimos::{
    agent::agent_model::AgentModel,
    route::RoutePattern,
    server::{Server, ServerBuilder},
};

use crate::{
    consumer::agent::{ConsumerAgent, ConsumerLifecycle},
    producer::agent::{ProducerAgent, ProducerLifecycle},
};

mod consumer;
mod producer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    example_logging()?;

    let producer_route = RoutePattern::parse_str("/producer/:name}")?;
    let consumer_route = RoutePattern::parse_str("/consumer/:name}")?;

    let producer_lifecycle = ProducerLifecycle;
    let consumer_lifecycle_fn = || ConsumerLifecycle::new().into_lifecycle();
    let producer = AgentModel::new(ProducerAgent::default, producer_lifecycle.into_lifecycle());
    let consumer = AgentModel::from_fn(ConsumerAgent::default, consumer_lifecycle_fn);

    let server = ServerBuilder::with_plane_name("Example Plane")
        .add_route(producer_route, producer)
        .add_route(consumer_route, consumer)
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
