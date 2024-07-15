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

use std::error::Error;
use swimos_client::{BasicValueDownlinkLifecycle, RemotePath, SwimClientBuilder};
use swimos_form::Form;

#[derive(Debug, Form, Copy, Clone)]
pub enum Operation {
    Add(i32),
    Sub(i32),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Build a Swim Client using the default configuration.
    // The `build` method returns a `SwimClient` instance and its internal
    // runtime future that is spawned below.
    let (client, task) = SwimClientBuilder::default().build().await;
    let client_task = tokio::spawn(task);
    let handle = client.handle();

    // Build a path the downlink.
    let state_path = RemotePath::new(
        // The host address
        "ws://0.0.0.0:8080",
        // You can provide any agent URI that matches the pattern
        // "/example/:id"
        "/example/1",
        // This is the URI of the ValueLane<i32> in our ExampleAgent
        "state",
    );

    let lifecycle = BasicValueDownlinkLifecycle::<usize>::default()
        // Register an event handler that is invoked when the downlink receives an event.
        .on_event_blocking(|value| println!("Downlink event: {value:?}"));

    handle
        .value_downlink::<usize>(state_path)
        .lifecycle(lifecycle)
        .open()
        .await?;

    let exec_path = RemotePath::new(
        // The host address
        "ws://0.0.0.0:8080",
        // You can provide any agent URI that matches the pattern
        // "/example/:id"
        "/example/1",
        // This is the URI of the ValueLane<i32> in our ExampleAgent
        "exec",
    );

    let exec_downlink = handle.value_downlink::<Operation>(exec_path).open().await?;

    exec_downlink.set(Operation::Add(1000)).await?;
    exec_downlink.set(Operation::Sub(13)).await?;

    client_task.await?;
    Ok(())
}
