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
use swimos_client::{BasicEventDownlinkLifecycle, RemotePath, SwimClientBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let host = "ws://127.0.0.1:8080";
    let (client, task) = SwimClientBuilder::default().build().await;
    let task_handle = tokio::spawn(task);

    let client_handle = client.handle();

    let state_view = client_handle
        .value_downlink(RemotePath::new(host, "/example/1", "lane"))
        .open()
        .await?;

    let doubled_lifecycle = BasicEventDownlinkLifecycle::default().on_event_blocking(|event| {
        println!("{event:?}");
    });
    client_handle
        .event_downlink::<i32>(RemotePath::new(host, "/example/1", "supply"))
        .lifecycle(doubled_lifecycle)
        .open()
        .await?;

    let values = [1, 2, 3, 4, 5];

    for value in values {
        state_view.set(value).await?;
    }

    task_handle.await?;
    Ok(())
}
