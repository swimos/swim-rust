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
use std::time::Duration;
use swim_client::downlink::Downlink;
use swim_client::interface::SwimClient;
use swim_common::warp::path::AbsolutePath;

#[tokio::main]
async fn main() {
    let mut client = SwimClient::new_with_default().await;
    let host_uri = url::Url::parse(&"ws://127.0.0.1:9001".to_string()).unwrap();
    let node_uri = "/unit/foo";

    let path = AbsolutePath::new(host_uri.clone(), node_uri, "info");

    let (value_downlink, _) = client
        .value_downlink(path, String::new())
        .await
        .expect("Failed to create value downlink!");

    let (_dl_topic, mut dl_sink) = value_downlink.split();


    for i in (0..100000).step_by(2) {
        println!("Setting {}", i);
        dl_sink
            .set(i.to_string())
            .await
            .expect("Failed to send message!");

    }

    println!("Stopping client in 2 seconds");
    task::sleep(Duration::from_secs(2)).await;
}