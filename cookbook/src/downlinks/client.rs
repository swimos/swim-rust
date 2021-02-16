// Copyright 2015-2020 SWIM.AI inc.
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

use async_std::task;
use rand::seq::SliceRandom;
use std::time::Duration;
use swim_client::interface::SwimClient;
use swim_common::warp::path::AbsolutePath;

#[tokio::main]
async fn main() {
    let mut client = SwimClient::new_with_default().await;
    let host_uri = url::Url::parse(&"ws://127.0.0.1:9001".to_string()).unwrap();
    let node_uri_prefix = "/unit/";

    let path = AbsolutePath::new(
        host_uri.clone(),
        &*format!("{}{}", node_uri_prefix, "0"),
        "shopping_cart",
    );

    let (map_downlink, receiver) = client
        .map_downlink::<String, i32>(path)
        .await
        .expect("Failed to create downlink!");

    map_downlink
        .update(String::from("FromClientLink"), 25)
        .await
        .expect("Failed to send message!");

    task::sleep(Duration::from_secs(2)).await;

    drop(receiver);
    drop(map_downlink);

    let items = vec!["bat", "cat", "rat"];

    for i in 0..50 {
        let path = AbsolutePath::new(
            host_uri.clone(),
            &*format!("{}{}", node_uri_prefix, (i % 3).to_string()),
            "add_item",
        );

        client
            .send_command(
                path,
                items
                    .choose(&mut rand::thread_rng())
                    .expect("No items to send!")
                    .to_string(),
            )
            .await
            .expect("Failed to send command!");
    }

    println!("Stopping client in 2 seconds");
    task::sleep(Duration::from_secs(2)).await;
}
