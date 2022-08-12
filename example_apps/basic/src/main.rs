// Copyright 2015-2021 Swim Inc.
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

use swim::{
    agent::agent_model::AgentModel,
    route::RoutePattern,
    server::{Server, ServerBuilder},
};

use crate::agent::{ExampleAgent, ExampleLifecycle};

mod agent;

#[tokio::main]
async fn main() {
    let route = RoutePattern::parse_str("/example").expect("Invalid route pattern.");

    let lifecycle = ExampleLifecycle;
    let agent = AgentModel::new(ExampleAgent::default, lifecycle.into_lifecycle());

    let server = ServerBuilder::default()
        .add_route(route, agent)
        .build()
        .await
        .expect("Routes are ambiguous.");

    let (task, mut handle) = server.run();

    let shutdown = async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to register iterrupt handler.");
        handle.stop();
    };

    let (_, result) = tokio::join!(shutdown, task);

    result.expect("Listener disconnected.");
}
