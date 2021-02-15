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
use swim_client::downlink::typed::map::events::{TypedMapView, TypedViewWithEvent};
use swim_client::downlink::typed::map::MapDownlinkReceiver;
use swim_client::downlink::Event::Remote;
use swim_client::interface::SwimClientBuilder;
use swim_client::runtime::time::delay::delay_for;
use swim_common::warp::path::AbsolutePath;
use tokio::task;

async fn did_update(
    map_recv: MapDownlinkReceiver<String, i32>,
    initial_value: TypedMapView<String, i32>,
    default: i32,
) {
    map_recv
        .into_stream()
        .filter_map(|event| async {
            match event {
                Remote(TypedViewWithEvent {
                    view,
                    event: MapEvent::Update(key),
                }) => Some((key, view)),
                Remote(TypedViewWithEvent {
                    view,
                    event: MapEvent::Remove(key),
                }) => Some((key, view)),
                _ => None,
            }
        })
        .scan(initial_value, |state, (key, current_view)| {
            let previous_view = state.clone();
            *state = current_view.clone();
            async { Some((key, previous_view, current_view)) }
        })
        .for_each(|(key, previous, current)| async move {
            println!(
                "Link watched {:?} changed to {:?} from {:?}",
                key,
                current.get(&key).unwrap_or(default),
                previous.get(&key).unwrap_or(default)
            )
        })
        .await;
}

#[tokio::main]
async fn main() {
    let mut client = SwimClientBuilder::build_with_default().await;
    let host_uri = url::Url::parse(&"ws://127.0.0.1:9001".to_string()).unwrap();
    let node_uri = "unit/foo";
    let cart_lane = "shoppingCart";
    let add_lane = "addItem";

    let path = AbsolutePath::new(host_uri.clone(), node_uri, cart_lane);

    let (map_downlink, map_recv) = client
        .map_downlink::<String, i32>(path)
        .await
        .expect("Failed to create map downlink!");

    let initial_value = map_downlink
        .view()
        .await
        .expect("Failed to retrieve initial map downlink!");

    task::spawn(did_update(map_recv, initial_value, 0));

    let path = AbsolutePath::new(host_uri, node_uri, add_lane);
    client
        .send_command(path, "FromClientCommand".to_string())
        .await
        .expect("Failed to send command!");

    delay_for(Duration::from_secs(2)).await;

    map_downlink
        .update("FromClientLink".to_string(), 25)
        .await
        .expect("Failed to send message!");

    delay_for(Duration::from_secs(2)).await;

    map_downlink
        .remove("FromClientLink".to_string())
        .await
        .expect("Failed to send message!");

    delay_for(Duration::from_secs(2)).await;

    println!("Stopping client in 2 seconds");
    delay_for(Duration::from_secs(2)).await;
}
