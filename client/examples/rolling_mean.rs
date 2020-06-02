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

use client::configuration::downlink::{
    BackpressureMode, ClientParams, ConfigHierarchy, DownlinkParams, OnInvalidMessage,
};
use client::downlink::model::map::MapAction;
use client::downlink::model::value::Action;
use client::interface::SwimClient;
use common::model::Value;
use common::sink::item::ItemSink;
use common::warp::path::AbsolutePath;
use futures::StreamExt;
use tokio::time::Duration;
use tracing::info;

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::from_default_env()
        .add_directive("client=trace".parse().unwrap())
        .add_directive("common=trace".parse().unwrap());

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_env_filter(filter)
        .init();
}

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

#[tokio::main]
async fn main() {
    init_tracing();

    let mut client = SwimClient::new(config()).await;
    // let path = AbsolutePath::new(
    //     url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
    //     "/unit/foo",
    //     "value",
    // );
    // let (mut dl1, mut receiver1) = client
    //     .untyped_value_downlink(path, Value::Extant)
    //     .await
    //     .unwrap();

    // let jh1 = tokio::spawn(async move {
    //     while let Some(r) = receiver1.next().await {
    //         info!("rcv1: Received: {:?}", r);
    //     }
    // });
    //
    // for v in 1..=10i32 {
    //     let _res = dl1.send_item(Action::set(v.into())).await;
    // }

    let path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "/unit/foo",
        "map",
    );
    let (mut dl2, mut receiver2) = client.untyped_map_downlink(path).await.unwrap();

    let jh2 = tokio::spawn(async move {
        while let Some(r) = receiver2.next().await {
            info!("rcv1: Received: {:?}", r);
        }
    });

    for v in 1..=10i32 {
        info!("Inserting {}", v);
        let _res = dl2.send_item(MapAction::insert(v.into(), v.into())).await;
        info!("Inserted {}", v);
    }

    // let _a = jh1.await;
    let _b = jh2.await;
}
