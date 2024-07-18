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

//! Rust port of the [Ripple](https://github.com/swimos/ripple) application.
//!
//! Run the application using the following:
//! ```text
//! $ cargo run --bin ripple
//! ```
//!
//! The web UI can then be found at:
//!
//! ```
//! http://127.0.0.1:9002/index.html
//! ```

use std::convert::Infallible;
use std::{error::Error, net::SocketAddr, time::Duration};

use axum::body::Body;
use axum::http::{header, HeaderValue};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tokio::net::TcpListener;
use tracing_subscriber::filter::LevelFilter;

use swimos::agent::agent_model::AgentModel;
use swimos::server::{Server, ServerBuilder};
use swimos_utilities::routing::RoutePattern;

use crate::agent::{MirrorAgent, MirrorLifecycle};

mod agent;

static HTML: HeaderValue = HeaderValue::from_static("text/html; charset=utf-8");
const INDEX: &[u8] = include_bytes!("../ui/index.html");
const RIPPLE: &[u8] = include_bytes!("../ui/swim-ripple.js");
const RIPPLE_MAP: &[u8] = include_bytes!("../ui/swim-ripple.js.map");

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    example_logging()?;

    let server_task = tokio::spawn(run_swim_server());
    let ui_task = tokio::spawn(ui_server());
    ui_task.await??;
    server_task.await??;

    Ok(())
}

/// Spawns the UI server on 0.0.0.0:9002.
async fn ui_server() -> Result<(), Box<dyn Error + Send + Sync>> {
    let app = ui_server_router();
    let bind_to: SocketAddr = "0.0.0.0:9002".parse()?;
    let listener = TcpListener::bind(bind_to).await?;

    axum::serve(listener, app.into_make_service()).await?;

    Ok(())
}

/// Spawns the Swim server on 0.0.0.0:9001.
async fn run_swim_server() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mirror_agent = AgentModel::from_fn(MirrorAgent::default, || {
        MirrorLifecycle::new(StdRng::from_entropy()).into_lifecycle()
    });
    let swim_server = ServerBuilder::with_plane_name("Ripple Plane")
        .set_bind_addr("0.0.0.0:9001".parse().unwrap())
        .add_route(RoutePattern::parse_str("mirror/:id")?, mirror_agent)
        .update_config(|config| {
            config.agent_runtime.inactive_timeout = Duration::from_secs(5 * 60);
        })
        .build()
        .await?;

    let (task, _handle) = swim_server.run();
    task.await?;

    println!("Server stopped");

    Ok(())
}

/// Enables logging if `--enable-logging` was provided.
fn example_logging() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).map(String::as_str) == Some("--enable-logging") {
        let filter = example_util::example_filter()?
            .add_directive("ripple=trace".parse()?)
            .add_directive(LevelFilter::WARN.into());
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
    Ok(())
}

/// Builds the Axum router for serving the UI.
pub fn ui_server_router() -> Router {
    Router::new()
        .route("/index.html", get(|| response(INDEX)))
        .route("/dist/main/swim-ripple.js", get(|| response(RIPPLE)))
        .route(
            "/dist/main/swim-ripple.js.map",
            get(|| response(RIPPLE_MAP)),
        )
}

/// Axum method router handler which returns `bytes` as the response body.
async fn response(bytes: &'static [u8]) -> impl IntoResponse {
    let headers = [
        (header::CONTENT_TYPE, HTML.clone()),
        (header::CONTENT_LENGTH, HeaderValue::from(bytes.len())),
    ];
    Ok::<_, Infallible>((headers, Body::from(bytes)))
}
