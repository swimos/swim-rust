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

use std::future::IntoFuture;
use std::{error::Error, pin::pin, time::Duration};

use clap::Parser;
use futures::future::{select, Either};
use tokio::net::TcpListener;

use swimos_utilities::trigger::trigger;
use transit::ui::ui_server_router;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let Params { port } = Params::parse();
    ui_server(port, SHUTDOWN_TIMEOUT).await
}

async fn ui_server(
    port: u16,
    shutdown_timeout: Duration,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let app = ui_server_router(port);
    let (stop_tx, stop_rx) = trigger();

    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let ui_addr = listener.local_addr()?;
    println!("UI bound to: {}", ui_addr);

    let server =
        axum::serve(listener, app.into_make_service()).with_graceful_shutdown(async move {
            let _r = stop_rx.await;
        });
    let server_task = pin!(server.into_future());
    let shutdown_notified = pin!(async move {
        let _ = tokio::signal::ctrl_c().await;
    });
    match select(server_task, shutdown_notified).await {
        Either::Left((result, _)) => result?,
        Either::Right((_, server)) => {
            assert!(stop_tx.trigger());
            tokio::time::timeout(shutdown_timeout, server).await??;
        }
    }
    Ok(())
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Params {
    #[arg(short, long)]
    port: u16,
}
