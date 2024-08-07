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

//! An example demonstrating Value Lanes.
//!
//! Run the server using the following:
//! ```text
//! $ cargo run --bin value-lane
//! ```
//!
//! And run the client with the following:
//! ```text
//! $ cargo run --bin value_client
//! ```

use std::{error::Error, time::Duration};

use example_util::{example_logging, manage_handle};
use swimos::{
    agent::agent_model::AgentModel,
    route::RoutePattern,
    server::{Server, ServerBuilder},
};

use crate::agent::{ExampleAgent, ExampleLifecycle};

mod agent;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    example_logging()?;

    let route = RoutePattern::parse_str("/example/:name}")?;

    let lifecycle = ExampleLifecycle;
    let agent = AgentModel::new(ExampleAgent::default, lifecycle.into_lifecycle());

    let server = ServerBuilder::with_plane_name("Example Plane")
        .set_bind_addr("127.0.0.1:8080".parse()?)
        .add_route(route, agent)
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
