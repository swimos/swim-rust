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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let host = "ws://127.0.0.1:8080";

    let (client, task) = SwimClientBuilder::default().build().await;
    let task_handle = tokio::spawn(task);

    let client_handle = client.handle();

    let car_lifecycle = BasicValueDownlinkLifecycle::default()
        .on_event_blocking(|speed| println!("/cars/1 speed: {speed}"));
    client_handle
        .value_downlink::<u64>(RemotePath::new(host, "/cars/1", "speed"))
        .lifecycle(car_lifecycle)
        .open()
        .await?;

    let area_lifecycle = BasicValueDownlinkLifecycle::default()
        .on_event_blocking(|speed| println!("/area/arbury average speed: {speed}"));
    client_handle
        .value_downlink::<f64>(RemotePath::new(host, "/area/arbury", "average_speed"))
        .lifecycle(area_lifecycle)
        .open()
        .await?;

    let city_lifecycle = BasicValueDownlinkLifecycle::default()
        .on_event_blocking(|speed| println!("/city average speed: {speed}"));
    client_handle
        .value_downlink::<f64>(RemotePath::new(host, "/city", "average_speed"))
        .lifecycle(city_lifecycle)
        .open()
        .await?;

    task_handle.await?;

    Ok(())
}
