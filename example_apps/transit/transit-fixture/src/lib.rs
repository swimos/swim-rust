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

use std::{error::Error, ops::Add, pin::pin, sync::Arc, time::Duration};

use agency::AgencyWithRoutes;
use futures::future::join3;
use state::AgenciesState;
use tokio::{
    sync::Notify,
    time::{sleep, Instant},
};
use tracing::{debug, info};

pub mod agency;
pub mod server;
pub mod state;
pub mod vehicles;

async fn update_task(state: Arc<AgenciesState>, interval: Duration, stop: Arc<Notify>) {
    let mut sleep = pin!(sleep(interval));
    let mut notified = pin!(stop.notified());
    info!("Starting update task.");
    state.update();
    loop {
        tokio::select! {
            _ = &mut notified => break,
            _ = &mut sleep => {
                sleep.as_mut().reset(Instant::now().add(interval));
            },
        }
        state.update();
    }
}

const UPDATE_INTERVAL_SEC: u64 = 1;
const UPDATE_STATE_INTERVAL: Duration = Duration::from_secs(3);

pub async fn run_mock_server(
    agencies: Vec<AgencyWithRoutes>,
    listener: std::net::TcpListener,
    shutdown_rx: Arc<Notify>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let state = Arc::new(AgenciesState::generate(agencies, UPDATE_INTERVAL_SEC));
    let app = server::make_server_router(state.clone());

    let update_trigger = Arc::new(Notify::new());
    let server_trigger = Arc::new(Notify::new());
    let update_trigger_rx = update_trigger.clone();
    let server_trigger_rx = server_trigger.clone();

    let shutdown = async move {
        shutdown_rx.notified().await;
        debug!("Mock server stop signal received.");
        update_trigger.notify_one();
        server_trigger.notify_one();
    };

    let update = update_task(state, UPDATE_STATE_INTERVAL, update_trigger_rx);

    info!("Starting mock web service.");
    let server_task = axum::Server::from_tcp(listener)?
        .serve(app.into_make_service())
        .with_graceful_shutdown(server_trigger_rx.notified());

    join3(shutdown, update, server_task).await.2?;
    info!("Mock sever has stopped.");
    Ok(())
}
