// Copyright 2015-2021 SWIM.AI inc.
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

use std::time::Duration;
use swim_client::downlink::model::SchemaViolations;
use swim_client::interface::SwimClientBuilder;
use swim_common::warp::path::AbsolutePath;
use swim_client::runtime::time::delay::delay_for;
use tokio::task;

#[tokio::main]
async fn main() {
    let mut client = SwimClientBuilder::build_with_default().await;
    let host_uri = url::Url::parse(&"ws://127.0.0.1:9001".to_string()).unwrap();
    let node_uri = "unit/foo";
    let lane_uri = "publish";

    let path = AbsolutePath::new(host_uri, node_uri, lane_uri);

    let event_dl = client
        .event_downlink::<i64>(path.clone(), SchemaViolations::Ignore)
        .await
        .unwrap();

    let mut rec = event_dl.subscribe().expect("Downlink closed unexpectedly.");

    task::spawn(async move {
        while let Some(event) = rec.recv().await {
            println!("Link received event: {}", event)
        }
    });

    // command() `msg` TO
    // the "publish" lane OF
    // the agent addressable by `/unit/foo` RUNNING ON
    // the plane with hostUri "warp://localhost:9001"
    let msg = 9035768;
    client
        .send_command(path, msg)
        .await
        .expect("Failed to send a command!");
    delay_for(Duration::from_secs(2)).await;

    println!("Stopping client in 2 seconds");
    delay_for(Duration::from_secs(2)).await;
}
