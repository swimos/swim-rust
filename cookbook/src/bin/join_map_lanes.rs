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
use swim_client::downlink::model::map::MapEvent;
use swim_client::downlink::subscription::TypedMapReceiver;
use swim_client::downlink::typed::event::TypedViewWithEvent;
use swim_client::downlink::Event::Remote;
use swim_client::interface::SwimClient;
use swim_common::warp::path::AbsolutePath;
use swim_runtime::time::delay::delay_for;
use tokio::task;

const THRESHOLD: i32 = 1000;

async fn did_update(map_recv: TypedMapReceiver<String, i32>, default: i32) {
    map_recv
        .filter_map(|event| async {
            match event {
                Remote(TypedViewWithEvent {
                    view,
                    event: MapEvent::Update(key),
                }) => {
                    let value = view.get(&key).unwrap_or(default);

                    if value > THRESHOLD {
                        Some((key, value))
                    } else {
                        None
                    }
                }
                _ => None,
            }
        })
        .for_each(|(street_name, population)| async move {
            println!("{:?} has {:?} residents", street_name, population,)
        })
        .await;
}

#[tokio::main]
async fn main() {
    let mut client = SwimClient::new_with_default().await;
    let host_uri = url::Url::parse(&"ws://127.0.0.1:53556".to_string()).unwrap();
    let node_uri = "/join/state/all";
    let lane_uri = "join";

    let path = AbsolutePath::new(host_uri.clone(), node_uri, lane_uri);

    let (_downlink, map_recv) = client
        .map_downlink::<String, i32>(path)
        .await
        .expect("Failed to create downlink!");

    task::spawn(did_update(map_recv, 0));

    delay_for(Duration::from_secs(2)).await;

    println!("Stopping client in 2 seconds");
    delay_for(Duration::from_secs(2)).await;
}
