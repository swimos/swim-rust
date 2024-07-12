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

use rand::Rng;
use std::error::Error;
use std::time::Duration;
use swimos_client::{
    BasicMapDownlinkLifecycle, BasicValueDownlinkLifecycle, RemotePath, SwimClientBuilder,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let host = "ws://127.0.0.1:8080";

    let (client, task) = SwimClientBuilder::default().build().await;
    let task_handle = tokio::spawn(task);

    let client_handle = client.handle();

    let rooms = ["living_room", "kitchen", "office"];
    let buildings = ["a", "b", "c"];

    for building in buildings {
        let aggregated_lifecycle = BasicMapDownlinkLifecycle::default().on_update_blocking(
            |key, _map, _old_state, new_state| println!("Building {key} -> {new_state}"),
        );
        client_handle
            .map_downlink::<String, bool>(RemotePath::new(
                host,
                format!("/buildings/{building}"),
                "lights",
            ))
            .lifecycle(aggregated_lifecycle)
            .open()
            .await?;
    }

    for building in buildings {
        for room in rooms {
            let handle = client_handle.clone();
            let task = async move {
                let node_address = format!("/rooms/{building}/{building}_{room}");
                let building_lifecycle = BasicValueDownlinkLifecycle::default()
                    .on_synced_blocking(move |state| println!("Building '{building}' -> {state}"));
                let building_view = handle
                    .value_downlink::<bool>(RemotePath::new(host, node_address.as_str(), "lights"))
                    .lifecycle(building_lifecycle)
                    .open()
                    .await
                    .unwrap_or_else(|_| panic!("Failed to open downlink to {node_address}"));

                let mut rng = rand::rngs::OsRng;
                loop {
                    let state = rng.gen_bool(0.5);
                    building_view
                        .set(state)
                        .await
                        .expect("Failed to set downlink");

                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            };
            tokio::spawn(task);
        }
    }

    task_handle.await?;

    Ok(())
}
