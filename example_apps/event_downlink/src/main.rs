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

use std::error::Error;

use example_util::{manage_producer_and_consumer, StartDependent};
use swim::server::Server;
use tokio::sync::oneshot;

mod consumer;
mod producer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let server = producer::make_server().await?;

    let (producer_task, handle) = server.run();

    let (dep_tx, dep_rx) = oneshot::channel();
    let consumer_task = start_consumer(dep_rx);
    let shutdown = manage_producer_and_consumer(handle, dep_tx);

    let (_, producer_result, consumer_result) =
        tokio::join!(shutdown, producer_task, consumer_task);

    producer_result?;
    consumer_result?;

    println!("Servers stopped successfully.");
    Ok(())
}

async fn start_consumer(rx: oneshot::Receiver<StartDependent>) -> Result<(), Box<dyn Error>> {
    let StartDependent { bound, request } = rx.await?;
    let server = consumer::make_server(bound.port()).await?;
    let (task, handle) = server.run();
    if let Err(mut handle) = request.send(handle) {
        handle.stop();
    }
    task.await?;
    Ok(())
}
