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

    let aggregated_lifecycle = BasicMapDownlinkLifecycle::default()
        .on_update_blocking(|key, _map, _old_state, new_state| println!("{key} -> {new_state}"));
    client_handle
        .map_downlink::<String, u64>(RemotePath::new(host, "/join/state/example", "streets"))
        .lifecycle(aggregated_lifecycle)
        .open()
        .await?;

    let california_view = client_handle
        .map_downlink::<String, u64>(RemotePath::new(host, "/state/california", "state"))
        .open()
        .await?;
    california_view.update("street_a".to_string(), 1).await?;

    let texas_view = client_handle
        .map_downlink::<String, u64>(RemotePath::new(host, "/state/texas", "state"))
        .open()
        .await?;
    texas_view.update("street_b".to_string(), 2).await?;

    let florida_view = client_handle
        .map_downlink::<String, u64>(RemotePath::new(host, "/state/florida", "state"))
        .open()
        .await?;
    florida_view.update("street_c".to_string(), 3).await?;

    task_handle.await?;

    Ok(())
}
