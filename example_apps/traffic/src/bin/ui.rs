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

use std::{error::Error, pin::pin, sync::Arc, time::Duration};

use clap::Parser;
use futures::future::{select, Either};
use tokio::sync::Notify;
use traffic::ui::ui_server_router;

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
    let server = axum::Server::try_bind(&"0.0.0.0:0".parse()?)?.serve(app.into_make_service());
    let ui_addr = server.local_addr();
    println!("UI bound to: {}", ui_addr);
    let stop_tx = Arc::new(Notify::new());
    let stop_rx = stop_tx.clone();
    let server_task = pin!(server.with_graceful_shutdown(stop_rx.notified()));
    let shutdown_notified = pin!(async move {
        let _ = tokio::signal::ctrl_c().await;
    });
    match select(server_task, shutdown_notified).await {
        Either::Left((result, _)) => result?,
        Either::Right((_, server)) => {
            stop_tx.notify_one();
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
