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

//! Rust port of the [Transit](https://github.com/swimos/transit) application.
//!
//! Run the server using the following:
//! ```text
//! $ cargo run --bin transit -- --port 8001 --include-ui --ui-port 8002
//! ```
//!
//! The web UI can then be found at:
//!
//! ```
//! http://127.0.0.1:8002/index.html
//! ```

use std::future::IntoFuture;
use std::{error::Error, net::SocketAddr, pin::pin, sync::Arc, time::Duration};

use clap::Parser;

use example_util::example_filter;
use futures::future::{select, Either};
use swimos::{
    route::RouteUri,
    server::{Server, ServerBuilder},
};
use swimos_utilities::trigger::trigger;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Notify};
use tracing_subscriber::filter::LevelFilter;
use transit::start_agencies_and_wait;
use transit::{buses_api::BusesApi, create_plane, ui::ui_server_router};
use transit_model::agency::Agency;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let Params {
        enable_logging,
        include_ui,
        port,
        ui_port,
    } = Params::parse();

    if enable_logging {
        configure_logging()?;
    }

    let agencies = transit::model::agencies();

    let (addr_tx, addr_rx) = oneshot::channel::<SocketAddr>();
    let shutdown_tx = Arc::new(Notify::new());
    let shutdown_rx = shutdown_tx.clone();

    let bind_to = if let Some(p) = port {
        Some(format!("0.0.0.0:{}", p).parse()?)
    } else {
        None
    };

    let server_task = tokio::spawn(run_swim_server(agencies, addr_tx, bind_to));

    if include_ui {
        let ui_task = tokio::spawn(ui_server(addr_rx, shutdown_rx, SHUTDOWN_TIMEOUT, ui_port));
        ui_task.await??;
    }
    server_task.await??;

    Ok(())
}

async fn ui_server(
    swim_addr_rx: oneshot::Receiver<SocketAddr>,
    shutdown_signal: Arc<Notify>,
    shutdown_timeout: Duration,
    port: Option<u16>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if let Ok(addr) = swim_addr_rx.await {
        let app = ui_server_router(addr.port());
        let bind_to: SocketAddr = format!("0.0.0.0:{}", port.unwrap_or_default()).parse()?;
        let (stop_tx, stop_rx) = trigger();

        let listener = TcpListener::bind(bind_to).await?;
        let ui_addr = listener.local_addr()?;
        println!("UI bound to: {}", ui_addr);

        let server =
            axum::serve(listener, app.into_make_service()).with_graceful_shutdown(async move {
                let _ = stop_rx.await;
            });

        let server_task = pin!(server.into_future());
        let shutdown_notified = pin!(shutdown_signal.notified());
        match select(server_task, shutdown_notified).await {
            Either::Left((result, _)) => result?,
            Either::Right((_, server)) => {
                assert!(stop_tx.trigger());
                tokio::time::timeout(shutdown_timeout, server).await??;
            }
        }
    }
    Ok(())
}

async fn run_swim_server(
    agencies: Vec<Agency>,
    bound: oneshot::Sender<SocketAddr>,
    bind_to: Option<SocketAddr>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let api = BusesApi::default();
    let agency_uris = agencies
        .iter()
        .map(|a| a.uri().parse::<RouteUri>())
        .collect::<Result<Vec<_>, _>>()?;
    let mut builder = ServerBuilder::with_plane_name("Transit Plane");

    builder = create_plane(agencies, api, builder)?;

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
    /// Switch on logging to the console.
    #[arg(long)]
    enable_logging: bool,
    /// Run the web UI.
    #[arg(short, long)]
    include_ui: bool,
    /// Bind to a specific port.
    #[arg(short, long)]
    port: Option<u16>,
    /// Bind the UI a specific port.
    #[arg(short, long)]
    ui_port: Option<u16>,
}

fn configure_logging() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let filter = example_filter()?
        .add_directive("swimos_server_app=info".parse()?)
        .add_directive("swimos_runtime=warn".parse()?)
        .add_directive("swimos_agent=info".parse()?)
        .add_directive("swimos_messages=warn".parse()?)
        .add_directive("swimos_remote=warn".parse()?)
        .add_directive("transit=info".parse()?)
        .add_directive(LevelFilter::WARN.into());

    tracing_subscriber::fmt().with_env_filter(filter).init();
    Ok(())
}
