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
use tokio::time::Duration;
use tracing::info;

use crate::configuration::downlink::{
    BackpressureMode, ClientParams, ConfigHierarchy, DownlinkParams, OnInvalidMessage,
};

use crate::connections::factory::tungstenite::TungsteniteWsFactory;
use crate::interface::SwimClient;
use common::model::{Attr, Item, Value};
use common::sink::item::ItemSink;
use common::topic::Topic;
use common::warp::path::AbsolutePath;
use form::Form;
use utilities::trace::init_trace;

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

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::from_default_env()
        .add_directive("client=trace".parse().unwrap())
        .add_directive("common=trace".parse().unwrap());

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_env_filter(filter)
        .init();
}

#[tokio::test]
#[ignore]
async fn client_test() {
    init_tracing();

    let mut client = SwimClient::new(config(), TungsteniteWsFactory::new(5).await).await;
    let path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "/unit/foo",
        "info",
    );

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

    // for v in 0..100i32 {
    //     let _res = dl.send_item(Action::set(v.into())).await;
    // }

    let _a = jh.await;
    let _b = jh2.await;

    println!("Finished sending");
}

#[tokio::test]
#[ignore]
async fn test_send_untyped_value_command() {
    init_trace(vec!["client::router=trace"]);

    let mut client = SwimClient::new(config()).await;
    let path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "unit/foo",
        "publish",
    );
    let mut command_dl = client.untyped_command_downlink(path).await.unwrap();

    tokio::time::delay_for(Duration::from_secs(1)).await;
    command_dl.send_item(13.into()).await.unwrap();

    tokio::time::delay_for(Duration::from_secs(3)).await;
}

#[tokio::test]
#[ignore]
async fn test_send_typed_value_command_valid() {
    init_trace(vec!["client::router=trace"]);

    let mut client = SwimClient::new(config()).await;
    let path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "unit/foo",
        "publish",
    );
    let mut command_dl = client.command_downlink::<i32>(path).await.unwrap();

    tokio::time::delay_for(Duration::from_secs(1)).await;
    command_dl.send_item(13).await.unwrap();

    tokio::time::delay_for(Duration::from_secs(3)).await;
}

#[tokio::test]
#[ignore]
async fn test_send_untyped_map_command() {
    init_trace(vec!["client::router=trace"]);

    let mut client = SwimClient::new(config()).await;
    let path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "unit/foo",
        "shoppingCart",
    );
    let mut command_dl = client.untyped_command_downlink(path).await.unwrap();

    tokio::time::delay_for(Duration::from_secs(1)).await;

    let insert =
        UntypedMapModification::Insert("milk".to_string().into_value(), 6.into_value()).as_value();

    command_dl.send_item(insert).await.unwrap();

    tokio::time::delay_for(Duration::from_secs(3)).await;
}

#[tokio::test]
#[ignore]
async fn test_send_typed_map_command() {
    init_trace(vec!["client::router=trace"]);

    let mut client = SwimClient::new(config()).await;
    let path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "unit/foo",
        "shoppingCart",
    );
    let mut command_dl = client
        .command_downlink::<MapModification<String, i32>>(path)
        .await
        .unwrap();

    tokio::time::delay_for(Duration::from_secs(1)).await;

    let insert = MapModification::Insert("milk".to_string(), 6);

    command_dl.send_item(insert).await.unwrap();

    tokio::time::delay_for(Duration::from_secs(3)).await;
}

#[tokio::test]
#[ignore]
async fn test_recv_untyped_value_event() {
    init_trace(vec!["client::router=trace"]);

    let mut client = SwimClient::new(config()).await;
    let event_path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "unit/foo",
        "info",
    );

    let command_path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "unit/foo",
        "info",
    );

    let mut event_dl = client.untyped_event_downlink(event_path).await.unwrap();
    tokio::time::delay_for(Duration::from_secs(1)).await;

    let mut command_dl = client.untyped_command_downlink(command_path).await.unwrap();
    command_dl
        .send_item("Hello, from Rust!".into())
        .await
        .unwrap();

    let incoming = event_dl.recv().await.unwrap();

    assert_eq!(incoming, Value::Text("Hello, from Rust!".to_string()));
    tokio::time::delay_for(Duration::from_secs(3)).await;
}

#[tokio::test]
#[ignore]
async fn test_recv_typed_value_event_valid() {
    init_trace(vec!["client::router=trace"]);

    let mut client = SwimClient::new(config()).await;
    let event_path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "unit/foo",
        "info",
    );

    let command_path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "unit/foo",
        "info",
    );

    let mut event_dl = client
        .event_downlink::<String>(event_path, Default::default())
        .await
        .unwrap();
    tokio::time::delay_for(Duration::from_secs(1)).await;

    let mut command_dl = client.untyped_command_downlink(command_path).await.unwrap();
    command_dl
        .send_item("Hello, from Rust!".into())
        .await
        .unwrap();

    let incoming = event_dl.recv().await.unwrap();

    assert_eq!(incoming, "Hello, from Rust!");

    tokio::time::delay_for(Duration::from_secs(3)).await;
}

#[tokio::test]
#[ignore]
async fn test_recv_typed_value_event_invalid() {
    init_trace(vec!["client::router=trace"]);

    let mut client = SwimClient::new(config()).await;
    let event_path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "unit/foo",
        "info",
    );

    let command_path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "unit/foo",
        "info",
    );

    let mut event_dl = client
        .event_downlink::<i32>(event_path, Default::default())
        .await
        .unwrap();
    tokio::time::delay_for(Duration::from_secs(1)).await;

    let mut command_dl = client.untyped_command_downlink(command_path).await.unwrap();
    command_dl
        .send_item("Hello, from Rust!".into())
        .await
        .unwrap();

    let incoming = event_dl.recv().await;

    assert_eq!(incoming, None);

    tokio::time::delay_for(Duration::from_secs(3)).await;
}

#[tokio::test]
#[ignore]
async fn test_recv_untyped_map_event() {
    init_trace(vec!["client::router=trace"]);

    let mut client = SwimClient::new(config()).await;
    let event_path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "unit/foo",
        "shoppingCart",
    );

    let command_path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "unit/foo",
        "shoppingCart",
    );

    let mut event_dl = client.untyped_event_downlink(event_path).await.unwrap();
    tokio::time::delay_for(Duration::from_secs(1)).await;

    let mut command_dl = client.untyped_command_downlink(command_path).await.unwrap();
    command_dl
        .send_item(
            UntypedMapModification::Insert("milk".to_string().into_value(), 6.into_value())
                .as_value(),
        )
        .await
        .unwrap();

    let incoming = event_dl.recv().await.unwrap();

    let header = Attr::of(("update", Value::record(vec![Item::slot("key", "milk")])));
    let body = Item::of(6);
    let expected = Value::Record(vec![header], vec![body]);

    assert_eq!(incoming, expected);
    tokio::time::delay_for(Duration::from_secs(3)).await;
}

#[tokio::test]
#[ignore]
async fn test_recv_typed_map_event_valid() {
    init_trace(vec!["client::router=trace"]);

    let mut client = SwimClient::new(config()).await;
    let event_path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "unit/foo",
        "shoppingCart",
    );

    let command_path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "unit/foo",
        "shoppingCart",
    );

    let mut event_dl = client
        .event_downlink::<MapModification<String, i32>>(event_path, Default::default())
        .await
        .unwrap();

    tokio::time::delay_for(Duration::from_secs(1)).await;

    let mut command_dl = client.untyped_command_downlink(command_path).await.unwrap();
    command_dl
        .send_item(
            UntypedMapModification::Insert("milk".to_string().into_value(), 6.into_value())
                .as_value(),
        )
        .await
        .unwrap();

    let incoming = event_dl.recv().await.unwrap();

    assert_eq!(incoming, MapModification::Insert("milk".to_string(), 6));

    tokio::time::delay_for(Duration::from_secs(3)).await;
}

#[tokio::test]
#[ignore]
async fn test_recv_typed_map_event_invalid_key() {
    init_trace(vec!["client::router=trace"]);

    let mut client = SwimClient::new(config()).await;
    let event_path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "unit/foo",
        "shoppingCart",
    );

    let command_path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "unit/foo",
        "shoppingCart",
    );

    let mut event_dl = client
        .event_downlink::<MapModification<i32, i32>>(event_path, Default::default())
        .await
        .unwrap();

    tokio::time::delay_for(Duration::from_secs(1)).await;

    let mut command_dl = client.untyped_command_downlink(command_path).await.unwrap();
    command_dl
        .send_item(
            UntypedMapModification::Insert("milk".to_string().into_value(), 6.into_value())
                .as_value(),
        )
        .await
        .unwrap();

    let incoming = event_dl.recv().await;

    assert_eq!(incoming, None);

    tokio::time::delay_for(Duration::from_secs(3)).await;
}

#[tokio::test]
#[ignore]
async fn test_recv_typed_map_event_invalid_value() {
    init_trace(vec!["client::router=trace"]);

    let mut client = SwimClient::new(config()).await;
    let event_path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "unit/foo",
        "shoppingCart",
    );

    let command_path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "unit/foo",
        "shoppingCart",
    );

    let mut event_dl = client
        .event_downlink::<MapModification<String, String>>(event_path, Default::default())
        .await
        .unwrap();

    tokio::time::delay_for(Duration::from_secs(1)).await;

    let mut command_dl = client.untyped_command_downlink(command_path).await.unwrap();
    command_dl
        .send_item(
            UntypedMapModification::Insert("milk".to_string().into_value(), 6.into_value())
                .as_value(),
        )
        .await
        .unwrap();

    let incoming = event_dl.recv().await;

    assert_eq!(incoming, None);

    tokio::time::delay_for(Duration::from_secs(3)).await;
}
