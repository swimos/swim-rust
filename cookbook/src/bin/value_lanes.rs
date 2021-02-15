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

use futures::StreamExt;
use std::time::Duration;
use swim_client::downlink::typed::value::ValueDownlinkReceiver;
use swim_client::downlink::Event::Remote;
use swim_client::interface::SwimClientBuilder;
use swim_client::runtime::time::delay::delay_for;
use swim_common::warp::path::AbsolutePath;
use tokio::task;

async fn did_set(value_recv: ValueDownlinkReceiver<String>, initial_value: String) {
    value_recv
        .into_stream()
        .filter_map(|event| async {
            match event {
                Remote(event) => Some(event),
                _ => None,
            }
        })
        .scan(initial_value, |state, current| {
            let previous = std::mem::replace(state, current.clone());
            async { Some((previous, current)) }
        })
        .for_each(|(previous, current)| async move {
            println!(
                "Link watched info change TO {:?} FROM {:?}",
                current, previous
            )
        })
        .await;
}

#[tokio::main]
async fn main() {
    let mut client = SwimClientBuilder::build_with_default().await;
    let host_uri = url::Url::parse(&"ws://127.0.0.1:9001".to_string()).unwrap();
    let node_uri = "unit/foo";
    let lane_uri = "info";

    let path = AbsolutePath::new(host_uri, node_uri, lane_uri);
    let (value_downlink, value_recv) = client
        .value_downlink(path.clone(), String::new())
        .await
        .expect("Failed to create value downlink!");

    let initial_value = value_downlink
        .get()
        .await
        .expect("Failed to retrieve initial downlink value!");

    task::spawn(did_set(value_recv, initial_value));

    // Send using either the proxy command lane...
    client
        .send_command(path, "Hello from command, world!".to_string())
        .await
        .expect("Failed to send command!");
    delay_for(Duration::from_secs(2)).await;

    // ...or a downlink set()
    value_downlink
        .set("Hello from link, world!".to_string())
        .await
        .expect("Failed to send message!");
    delay_for(Duration::from_secs(2)).await;

    println!("Stopping client in 2 seconds");
    delay_for(Duration::from_secs(2)).await;
}
