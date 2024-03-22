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

use example_util::{example_logging, manage_handle_report};
use swimos::{
    agent::agent_model::AgentModel,
    route::RoutePattern,
    server::{Server, ServerBuilder},
};
use tokio::sync::oneshot;

use crate::{
    ui::run_web_server,
    unit_agent::{ExampleLifecycle, UnitAgent},
};

mod ui;
mod unit_agent;

const MAX_HISTORY: usize = 200;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    example_logging()?;

    let route = RoutePattern::parse_str("/unit/:name}")?;

    let lifecycle_fn = || ExampleLifecycle::new(MAX_HISTORY).into_lifecycle();
    let agent = AgentModel::from_fn(UnitAgent::default, lifecycle_fn);

    let bind_addr = "0.0.0.0:9001".parse()?;

    let server = ServerBuilder::with_plane_name("Tutorial Plane")
        .set_bind_addr(bind_addr)
        .add_route(route, agent)
        .update_config(|config| {
            config.agent_runtime.inactive_timeout = Duration::from_secs(5 * 60);
        })
        .build()
        .await?;

    let (task, handle) = server.run();

    let (bound_tx, bound_rx) = oneshot::channel();
    let (stop_tx, stop_rx) = oneshot::channel();
    let shutdown = manage_handle_report(handle, Some(bound_tx));

    let ui = run_web_server(
        async move {
            let _ = stop_rx.await;
        },
        bound_rx,
    );

    let (ui_result, _, result) = tokio::join!(ui, shutdown, async move {
        let result = task.await;
        let _ = stop_tx.send(());
        result
    });

    result?;
    ui_result?;
    println!("Server stopped successfully.");
    Ok(())
}
