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

use std::{error::Error, net::SocketAddr, pin::pin, str::FromStr, sync::Arc, time::Duration};

use clap::Parser;
use example_util::{example_filter, manage_handle_report};
use futures::future::{select, Either};
use game::{
    agents::{
        game::{GameAgent, GameLifecycle},
        leaderboard::{LeaderboardAgent, LeaderboardLifecycle},
        player::{PlayerAgent, PlayerLifecycle},
        round::{MatchAgent, MatchLifecycle},
        team::{TeamAgent, TeamLifecycle},
    },
    ui::ui_server_router,
};
use swimos::{
    agent::agent_model::AgentModel,
    route::{RoutePattern, RouteUri},
    server::{Server, ServerBuilder, ServerHandle},
};
use tokio::sync::{oneshot, Notify};
use tracing::info;
use tracing_subscriber::filter::LevelFilter;

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

    let (addr_tx, addr_rx) = oneshot::channel::<SocketAddr>();
    let shutdown_tx = Arc::new(Notify::new());
    let shutdown_rx = shutdown_tx.clone();

    let bind_to = if let Some(p) = port {
        Some(format!("0.0.0.0:{}", p).parse()?)
    } else {
        None
    };

    let server_task = tokio::spawn(run_swim_server(addr_tx, bind_to));

    if include_ui {
        let ui_task = tokio::spawn(ui_server(addr_rx, shutdown_rx, SHUTDOWN_TIMEOUT, ui_port));
        ui_task.await??;
    }

    server_task.await??;
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
        .add_directive("swimos_server_app=warn".parse()?)
        .add_directive("swimos_runtime=warn".parse()?)
        .add_directive("swimos_agent=warn".parse()?)
        .add_directive("swimos_messages=warn".parse()?)
        .add_directive("swimos_remote=warn".parse()?)
        .add_directive("game=info".parse()?)
        .add_directive(LevelFilter::WARN.into());

    tracing_subscriber::fmt().with_env_filter(filter).init();
    Ok(())
}

async fn run_swim_server(
    bound: oneshot::Sender<SocketAddr>,
    bind_to: Option<SocketAddr>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut builder = ServerBuilder::with_plane_name("Game Plane");
    builder = add_routes(builder)?;

    if let Some(addr) = bind_to {
        builder = builder.set_bind_addr(addr);
    }

    let server = builder
        .update_config(|config| {
            config.agent_runtime.inactive_timeout = Duration::from_secs(5 * 60);
        })
        .build()
        .await?;

    let (task, handle) = server.run();

    let shutdown = start_agents_and_wait(handle, Some(bound));

    let (_, result) = futures::future::join(shutdown, task).await;

    result?;
    println!("Server stopped successfully.");
    Ok(())
}

pub fn add_routes(
    mut builder: ServerBuilder,
) -> Result<ServerBuilder, Box<dyn Error + Send + Sync>> {
    info!("Adding game route");
    let game_route = RoutePattern::parse_str("/match")?;
    let game_agent = AgentModel::from_fn(GameAgent::default, move || {
        GameLifecycle::new().into_lifecycle()
    });
    builder = builder.add_route(game_route, game_agent);

    info!("Adding leaderboard route");
    let leaderboard_route = RoutePattern::parse_str("/player")?;
    let leaderboard_agent = AgentModel::from_fn(LeaderboardAgent::default, move || {
        LeaderboardLifecycle::new().into_lifecycle()
    });
    builder = builder.add_route(leaderboard_route, leaderboard_agent);

    info!("Adding team routes");
    let team_route = RoutePattern::parse_str("/team/:name")?;
    let team_agent = AgentModel::new(TeamAgent::default, TeamLifecycle.into_lifecycle());
    builder = builder.add_route(team_route, team_agent);

    info!("Adding player routes");
    let player_route = RoutePattern::parse_str("/player/:id")?;
    let player_agent = AgentModel::from_fn(PlayerAgent::default, move || {
        PlayerLifecycle::new().into_lifecycle()
    });
    builder = builder.add_route(player_route, player_agent);

    info!("Adding match routes");
    let match_route = RoutePattern::parse_str("/match/:id")?;
    let match_agent = AgentModel::new(MatchAgent::default, MatchLifecycle.into_lifecycle());
    builder = builder.add_route(match_route, match_agent);

    Ok(builder)
}

pub async fn start_agents_and_wait(
    handle: ServerHandle,
    bound: Option<oneshot::Sender<SocketAddr>>,
) {
    info!("Starting game agent");
    handle
        .start_agent(RouteUri::from_str("/match").unwrap())
        .await
        .unwrap();

    manage_handle_report(handle, bound).await
}

async fn ui_server(
    swim_addr_rx: oneshot::Receiver<SocketAddr>,
    shutdown_signal: Arc<Notify>,
    shutdown_timeout: Duration,
    port: Option<u16>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if let Ok(addr) = swim_addr_rx.await {
        let app = ui_server_router(addr.port());

        let bind_to = format!("0.0.0.0:{}", port.unwrap_or_default()).parse()?;

        let server = axum::Server::try_bind(&bind_to)?.serve(app.into_make_service());
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
