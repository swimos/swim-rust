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

use std::{
    collections::HashSet, error::Error, net::SocketAddr, pin::pin, sync::Arc, time::Duration,
};

use clap::Parser;

use futures::future::{select, Either};
use swim::{
    route::RouteUri,
    server::{Server, ServerBuilder},
};
use tokio::sync::{oneshot, Notify};
use transit::start_agencies_and_wait;
use transit::{
    buses_api::BusesApi, configure_logging, create_plane, ui::ui_server_router, IncludeRoutes,
};
use transit_model::agency::Agency;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let Params {
        routes,
        enable_logging,
        include_ui,
        port,
    } = Params::parse();
    if enable_logging {
        configure_logging()?;
    }
    let agencies = transit::model::agencies();

    let routes = match routes {
        Some(IncludeRoutes::Vehicle) => [IncludeRoutes::Vehicle].into_iter().collect(),
        Some(IncludeRoutes::State) => [IncludeRoutes::Vehicle, IncludeRoutes::State]
            .into_iter()
            .collect(),
        Some(IncludeRoutes::Country) => [
            IncludeRoutes::Vehicle,
            IncludeRoutes::State,
            IncludeRoutes::Country,
        ]
        .into_iter()
        .collect(),
        None => HashSet::new(),
    };

    let (addr_tx, addr_rx) = oneshot::channel::<SocketAddr>();
    let shutdown_tx = Arc::new(Notify::new());
    let shutdown_rx = shutdown_tx.clone();

    let bind_to = if let Some(p) = port {
        Some(format!("0.0.0.0:{}", p).parse()?)
    } else {
        None
    };

    let server_task = tokio::spawn(run_swim_server(agencies, addr_tx, routes, bind_to));
    if include_ui {
        let ui_task = tokio::spawn(ui_server(addr_rx, shutdown_rx, SHUTDOWN_TIMEOUT));
        ui_task.await??;
    }
    server_task.await??;

    Ok(())
}

async fn ui_server(
    swim_addr_rx: oneshot::Receiver<SocketAddr>,
    shutdown_signal: Arc<Notify>,
    shutdown_timeout: Duration,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if let Ok(addr) = swim_addr_rx.await {
        let app = ui_server_router(addr.port());
        let server = axum::Server::try_bind(&"0.0.0.0:0".parse()?)?.serve(app.into_make_service());
        let ui_addr = server.local_addr();
        println!("UI bound to: {}", ui_addr);
        let stop_tx = Arc::new(Notify::new());
        let stop_rx = stop_tx.clone();
        let server_task = pin!(server.with_graceful_shutdown(stop_rx.notified()));
        let shutdown_notified = pin!(shutdown_signal.notified());
        match select(server_task, shutdown_notified).await {
            Either::Left((result, _)) => result?,
            Either::Right((_, server)) => {
                stop_tx.notify_one();
                tokio::time::timeout(shutdown_timeout, server).await??;
            }
        }
    }
    Ok(())
}

async fn run_swim_server(
    agencies: Vec<Agency>,
    bound: oneshot::Sender<SocketAddr>,
    routes: HashSet<IncludeRoutes>,
    bind_to: Option<SocketAddr>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let api = BusesApi::default();
    let agency_uris = agencies
        .iter()
        .map(|a| a.uri().parse::<RouteUri>())
        .collect::<Result<Vec<_>, _>>()?;
    let mut builder = ServerBuilder::with_plane_name("Transit Plane");

    builder = create_plane(agencies, api, builder, routes)?;

    if let Some(addr) = bind_to {
        builder = builder.set_bind_addr(addr);
    }

    let swim_server = builder
        .update_config(|config| {
            config.agent_runtime.inactive_timeout = Duration::from_secs(5 * 60);
        })
        .build()
        .await?;
    let (task, handle) = swim_server.run();

    let shutdown = start_agencies_and_wait(agency_uris, handle, Some(bound));

    let (_, result) = futures::future::join(shutdown, task).await;

    result?;
    println!("Server stopped successfully.");
    Ok(())
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Params {
    /// Most general routes to include.
    #[arg(short, long)]
    routes: Option<IncludeRoutes>,
    /// Switch on logging to the console.
    #[arg(long)]
    enable_logging: bool,
    /// Run the web UI.
    #[arg(short, long)]
    include_ui: bool,
    /// Bind to a specific port.
    #[arg(short, long)]
    port: Option<u16>,
}
