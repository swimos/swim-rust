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

use std::{error::Error, time::Duration};

use swim::{
    agent::agent_model::AgentModel,
    route::RoutePattern,
    server::{BoxServer, ServerBuilder},
};

use self::agent::{ConsumerAgent, ConsumerLifecycle};

mod agent;
mod model;

pub async fn make_server(
    producer_port: u16,
    key: &str,
) -> Result<BoxServer, Box<dyn Error + Send + Sync>> {
    let route = RoutePattern::parse_str("/consumer/:name}")?;
    let key = key.to_string();
    let lifecycle_fn = move || ConsumerLifecycle::new(producer_port, key.clone()).into_lifecycle();
    let agent = AgentModel::from_fn(ConsumerAgent::default, lifecycle_fn);

    let server = ServerBuilder::with_plane_name("Consumer Plane")
        .add_route(route, agent)
        .update_config(|config| {
            config.agent_runtime.inactive_timeout = Duration::from_secs(5 * 60);
        })
        .build()
        .await?;

    Ok(server)
}
