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

//! Reference code for the [Time Series](https://www.swimos.org/server/rust/time-series/) guide.
//!
//! Run the server using the following:
//! ```text
//! $ cargo run --bin time_series
//! ```

use std::{error::Error, time::Duration};

use example_util::{example_logging, manage_handle};
use swimos::{
    agent::agent_model::AgentModel,
    route::RoutePattern,
    server::{Server, ServerBuilder},
};

use crate::count::{CountAgent, CountLifecycle};
use crate::time::{TimeAgent, TimeLifecycle};

mod count;
mod time;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    example_logging()?;

    let count_route = RoutePattern::parse_str("/count/:name}")?;
    let count_agent = AgentModel::from_fn(CountAgent::default, move || {
        CountLifecycle::new(5).into_lifecycle()
    });

    let time_route = RoutePattern::parse_str("/time/:name}")?;
    let time_agent = AgentModel::from_fn(TimeAgent::default, move || {
        TimeLifecycle::new(13).into_lifecycle()
    });

    let server = ServerBuilder::with_plane_name("Example Plane")
        .add_route(count_route, count_agent)
        .add_route(time_route, time_agent)
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
