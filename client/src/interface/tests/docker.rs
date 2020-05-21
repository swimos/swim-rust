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

use crate::configuration::downlink::{
    BackpressureMode, ClientParams, ConfigHierarchy, DownlinkParams, OnInvalidMessage,
};
use crate::downlink::model::value::Action;
use crate::interface::SwimClient;
use common::model::Value;
use common::sink::item::ItemSink;
use common::topic::Topic;
use common::warp::path::AbsolutePath;
use futures::StreamExt;
use test_server::clients::Cli;
use test_server::Docker;
use test_server::SwimTestServer;
use tokio::time::Duration;
use tracing::info;

fn config() -> ConfigHierarchy {
    let client_params = ClientParams::new(2, Default::default()).unwrap();
    let default_params = DownlinkParams::new_queue(
        BackpressureMode::Propagate,
        5,
        Duration::from_secs(60000),
        5,
        OnInvalidMessage::Terminate,
    )
    .unwrap();

    ConfigHierarchy::new(client_params, default_params)
}

#[tokio::test]
#[ignore]
async fn init() {
    let docker = Cli::default();
    let container = docker.run(SwimTestServer);
    let port = container.get_host_port(9001).unwrap();
    let host = format!("ws://127.0.0.1:{}", port);

    let mut client = SwimClient::new(config()).await;
    let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "/unit/foo", "info");

    let (mut dl, mut receiver) = client
        .untyped_value_downlink(path, Value::Extant)
        .await
        .unwrap();

    let jh = tokio::spawn(async move {
        while let Some(r) = receiver.next().await {
            info!("rcv1: Received: {:?}", r);
        }
    });

    let mut rcv2 = dl.subscribe().await.unwrap();

    let jh2 = tokio::spawn(async move {
        while let Some(r) = rcv2.next().await {
            info!("rcv2: Received: {:?}", r);
        }
    });

    for v in 0..100i32 {
        let _res = dl.send_item(Action::set(v.into())).await;
    }

    let _r1 = jh.await;
    let _r2 = jh2.await;

    docker.stop(container.id());

    println!("Finished sending");
}
