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

//! Rust port of the [Simulated Stock Demo](https://github.com/nstreamio/sim-stock-demo) application.
//!
//! Run the application using the following:
//! ```shell
//! $ cargo run --bin stocks_simulated
//! ```
//!
//! Run the UI with the following:
//! ```shell
//! cd ui
//! npm install
//! npm run dev
//! ```
//! Then head to `localhost:5173` to see it in action.
//! ```

use std::str::FromStr;
use std::{error::Error, time::Duration};

use rand::{thread_rng, Rng};
use tracing_subscriber::filter::LevelFilter;

use swimos::route::RouteUri;
use swimos::server::ServerHandle;
use swimos::{
    agent::agent_model::AgentModel,
    route::RoutePattern,
    server::{Server, ServerBuilder},
};

use crate::agent::{StockAgent, StockLifecycle, SymbolsAgent, SymbolsLifecycle};

mod agent;

const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    example_logging()?;
    run_swim_server().await?;

    Ok(())
}

/// Spawns the Swim server on 0.0.0.0:9001.
async fn run_swim_server() -> Result<(), Box<dyn Error + Send + Sync>> {
    let symbols_agent = AgentModel::new(SymbolsAgent::default, SymbolsLifecycle.into_lifecycle());
    let stock_agent = AgentModel::new(StockAgent::default, StockLifecycle.into_lifecycle());

    let swim_server = ServerBuilder::with_plane_name("Ripple Plane")
        .set_bind_addr("0.0.0.0:9001".parse().unwrap())
        .add_route(RoutePattern::parse_str("/symbols")?, symbols_agent)
        .add_route(RoutePattern::parse_str("/stock/:symbol")?, stock_agent)
        .update_config(|config| {
            config.agent_runtime.inactive_timeout = Duration::from_secs(5 * 60);
        })
        .build()
        .await?;

    let (task, handle) = swim_server.run();
    let task = tokio::spawn(task);

    start_agents(&handle).await?;
    task.await??;

    println!("Server stopped");

    Ok(())
}

async fn start_agents(handle: &ServerHandle) -> Result<(), Box<dyn Error + Send + Sync>> {
    for _ in 0..20000 {
        let stock_id = {
            let mut rng = thread_rng();
            (0..4)
                .map(|_| {
                    let idx = rng.gen_range(0..CHARSET.len());
                    CHARSET[idx] as char
                })
                .collect::<String>()
        };
        handle
            .start_agent(RouteUri::from_str(format!("/stock/{stock_id}").as_str())?)
            .await?;
    }
    Ok(())
}

/// Enables logging if `--enable-logging` was provided.
fn example_logging() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).map(String::as_str) == Some("--enable-logging") {
        let filter = example_util::example_filter()?
            .add_directive("stocks_simulated=trace".parse()?)
            .add_directive(LevelFilter::WARN.into());
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
    Ok(())
}
