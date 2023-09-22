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

use futures::future::join3;
use state::AgenciesState;
use swim_utilities::trigger;
use tokio::{
    net::TcpListener,
    signal::ctrl_c,
    time::{sleep, Instant},
};

pub mod agency;
pub mod server;
pub mod state;
pub mod vehicles;

async fn update_task(state: Arc<AgenciesState>, interval: Duration, mut stop: trigger::Receiver) {
    let mut sleep = pin!(sleep(interval));
    state.update();
    loop {
        tokio::select! {
            _ = &mut stop => break,
            _ = &mut sleep => {
                sleep.as_mut().reset(Instant::now().add(interval));
            },
        }
        state.update();
    }
}

const UPDATE_INTERVAL_SEC: u64 = 1;
const UPDATE_STATE_INTERVAL: Duration = Duration::from_secs(3);

pub async fn run_mock_server() -> Result<(), Box<dyn Error>> {
    let (shutdown_tx, shutdown_rx) = trigger::trigger();

    let shutdown = Box::pin(async move {
        ctrl_c()
            .await
            .expect("Failed to register interrupt handler.");
        println!("Stopping server.");
        shutdown_tx.trigger();
    });

    let state = Arc::new(AgenciesState::generate(UPDATE_INTERVAL_SEC));
    let app = server::make_server_router(state.clone());
    let listener = TcpListener::bind("0.0.0.0:3000").await?;
    println!("Listening on: {}", listener.local_addr()?);

    let update = update_task(state, UPDATE_STATE_INTERVAL, shutdown_rx.clone());

    let server_task = axum::Server::from_tcp(listener.into_std()?)?
        .serve(app.into_make_service())
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.await;
        });

    join3(shutdown, update, server_task).await.2?;
    Ok(())
}
