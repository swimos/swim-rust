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
use swimos_client::{BasicMapDownlinkLifecycle, RemotePath, SwimClientBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let host = "ws://127.0.0.1:8080";

    let (client, task) = SwimClientBuilder::default().build().await;
    let task_handle = tokio::spawn(task);

    let client_handle = client.handle();

    let command_view = client_handle
        .value_downlink::<String>(RemotePath::new(host, "/example/1", "command"))
        .open()
        .await?;

    let demand_lifecycle = BasicMapDownlinkLifecycle::default()
        .on_update_blocking(|key, _map, _old_state, new_state| println!("{key} -> {new_state}"));
    client_handle
        .map_downlink::<String, i32>(RemotePath::new(host, "/example/1", "demand_map"))
        .lifecycle(demand_lifecycle)
        .open()
        .await?;

    command_view.set("Red".to_string()).await?;
    command_view.set("Green".to_string()).await?;
    command_view.set("Blue".to_string()).await?;

    task_handle.await?;

    Ok(())
}
