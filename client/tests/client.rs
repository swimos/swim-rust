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

// #[cfg(feature = "test_server")]
// mod tests {

use client::configuration::downlink::{
    BackpressureMode, ClientParams, ConfigHierarchy, DownlinkParams, OnInvalidMessage,
};
use client::connections::factory::tungstenite::TungsteniteWsFactory;
use client::interface::SwimClient;
use common::model::Value;
use common::warp::path::AbsolutePath;
use futures::StreamExt;
use tokio::time::Duration;

fn config() -> ConfigHierarchy {
    let client_params = ClientParams::new(2, Default::default()).unwrap();
    let default_params = DownlinkParams::new_queue(
        BackpressureMode::Propagate,
        5,
        Duration::from_secs(60000),
        5,
        OnInvalidMessage::Terminate,
        10000,
    )
    .unwrap();

    ConfigHierarchy::new(client_params, default_params)
}

#[tokio::test]
#[ignore]
async fn get_all() {
    let mut client = SwimClient::new(config(), TungsteniteWsFactory::new(5).await).await;
    let path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "/unit/foo",
        "random",
    );

    let (_dl, mut rcv) = client
        .value_downlink(path, Value::Int32Value(0))
        .await
        .unwrap();

    let jh2 = tokio::spawn(async move {
        while let Some(r) = rcv.next().await {
            println!("RandomLane received: {:?}", r.action());
        }
    });

    let _ = jh2.await;
}
