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

use std::{error::Error, time::Duration};

use swim::{
    agent::agent_model::AgentModel,
    route::RoutePattern,
    server::{Server, ServerBuilder, ServerHandle},
};
use tokio::select;

use crate::agent::{ExampleAgent, ExampleLifecycle};

mod agent;
mod downlink;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let route = RoutePattern::parse_str("/example")?;

    let lifecycle = ExampleLifecycle;
    let agent = AgentModel::new(ExampleAgent::default, lifecycle.into_lifecycle());

    let server = ServerBuilder::with_plane_name("Example Plane")
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

async fn manage_handle(mut handle: ServerHandle) {
    let mut shutdown_hook = Box::pin(async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to register interrupt handler.");
    });
    let print_addr = handle.bound_addr();

    let maybe_addr = select! {
        _ = &mut shutdown_hook => None,
        maybe_addr = print_addr => maybe_addr,
    };

    if let Some(addr) = maybe_addr {
        println!("Bound to: {}", addr);
        shutdown_hook.await;
    }

    println!("Stopping server.");
    handle.stop();
}
