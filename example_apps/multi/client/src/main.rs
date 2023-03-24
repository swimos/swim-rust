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

use std::{collections::HashMap, error::Error, time::Duration};

use clap::Parser;
use swim::{
    agent::agent_model::AgentModel,
    route::RoutePattern,
    server::{Server, ServerBuilder, ServerHandle},
};
use tokio::select;

use crate::{
    args::AppArgs,
    collect::{CollectAgent, CollectLifecycle},
    link::{LinkAgent, LinkLifecycle},
};

mod args;
mod collect;
mod link;

const LANE_NAME: &str = "state";

const EXAMPLE_LINK: &str = "/example";
const EXAMPLE_LINKS: &[&str] = &["/link0", "/link2", "/link3", "/link4"];

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let AppArgs {
        host,
        port,
        link,
        collect,
    } = AppArgs::parse();
    let remote = format!("{}:{}", host, port);

    let link_pat = RoutePattern::parse_str("/link")?;
    let link_route = link_pat.apply(&HashMap::new())?.parse()?;
    let collect_pat = RoutePattern::parse_str("/collect")?;
    let collect_route = collect_pat.apply(&HashMap::new())?.parse()?;

    let link_lifecycle = LinkLifecycle::new(&remote, EXAMPLE_LINK.parse()?);
    let link_agent = AgentModel::new(LinkAgent::default, link_lifecycle.into_lifecycle());

    let mut links = vec![];
    for link in EXAMPLE_LINKS {
        links.push(link.parse()?);
    }
    let collect_lifecycle = CollectLifecycle::new(&remote, links);
    let collect_agent = AgentModel::new(CollectAgent::default, collect_lifecycle.into_lifecycle());

    let server = ServerBuilder::with_plane_name("Server Plane")
        .add_route(link_pat, link_agent)
        .add_route(collect_pat, collect_agent)
        .update_config(|config| {
            config.agent_runtime.inactive_timeout = Duration::from_secs(5 * 60);
        })
        .build()
        .await?;

    let (task, handle) = server.run();

    if link {
        handle.start_agent(link_route).await?;
    }
    if collect {
        handle.start_agent(collect_route).await?;
    }

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
