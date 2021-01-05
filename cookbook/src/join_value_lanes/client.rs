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
use futures::StreamExt;
use std::time::Duration;
use swim_client::downlink::model::map::MapEvent;
use swim_client::downlink::subscription::TypedMapReceiver;
use swim_client::downlink::typed::event::TypedViewWithEvent;
use swim_client::downlink::Event::Remote;
use swim_client::interface::SwimClient;
use swim_common::model::Value;
use swim_common::warp::path::AbsolutePath;

async fn did_update(map_recv: TypedMapReceiver<i32, bool>, default: bool) {
    map_recv
        .filter_map(|event| async {
            match event {
                Remote(TypedViewWithEvent {
                    view,
                    event: MapEvent::Update(key),
                }) => Some((key, view)),
                _ => None,
            }
        })
        .for_each(|(key, current)| async move {
            if current.get(&key).unwrap_or(default) {
                println!("The lights in room {:?} are on", key)
            } else {
                println!("The lights in room {:?} are off", key)
            }
        })
        .await;
}

#[tokio::main]
async fn main() {
    let mut client = SwimClient::new_with_default().await;
    let host_uri = url::Url::parse(&"ws://127.0.0.1:9001".to_string()).unwrap();

    let building_node = "/building/swim";
    let first_room_node = "/swim/1";
    let second_room_node = "/swim/2";
    let third_room_node = "/swim/3";

    let status_lane = "lights";
    let switch_lane = "toggleLights";

    let path = AbsolutePath::new(host_uri.clone(), building_node, status_lane);

    let (_downlink, map_recv) = client
        .map_downlink::<i32, bool>(path)
        .await
        .expect("Failed to create downlink!");

    task::spawn(did_update(map_recv, false));

    task::sleep(Duration::from_secs(2)).await;

    let first_room_uri = AbsolutePath::new(host_uri.clone(), first_room_node, switch_lane);
    let second_room_uri = AbsolutePath::new(host_uri.clone(), second_room_node, switch_lane);
    let third_room_uri = AbsolutePath::new(host_uri.clone(), third_room_node, switch_lane);

    client
        .send_command(first_room_uri, Value::Extant)
        .await
        .expect("Failed to send command!");

    task::sleep(Duration::from_secs(1)).await;

    client
        .send_command(second_room_uri.clone(), Value::Extant)
        .await
        .expect("Failed to send command!");

    task::sleep(Duration::from_secs(1)).await;

    client
        .send_command(third_room_uri.clone(), Value::Extant)
        .await
        .expect("Failed to send command!");

    task::sleep(Duration::from_secs(1)).await;

    client
        .send_command(second_room_uri.clone(), Value::Extant)
        .await
        .expect("Failed to send command!");

    task::sleep(Duration::from_secs(1)).await;

    client
        .send_command(second_room_uri, Value::Extant)
        .await
        .expect("Failed to send command!");

    task::sleep(Duration::from_secs(1)).await;

    client
        .send_command(third_room_uri, Value::Extant)
        .await
        .expect("Failed to send command!");

    task::sleep(Duration::from_secs(1)).await;

    println!("Stopping client in 2 seconds");
    task::sleep(Duration::from_secs(2)).await;
}
