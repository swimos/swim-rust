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

use std::{thread, time};

use crate::configuration::router::RouterParamBuilder;
use crate::connections::factory::tungstenite::TungsteniteWsFactory;
use crate::connections::SwimConnPool;
use crate::router::{Router, SwimRouter};
use common::model::Value;
use common::sink::item::ItemSink;
use common::warp::envelope::Envelope;
use common::warp::path::AbsolutePath;
use std::sync::Once;
use tracing::Level;
use tracing_subscriber::EnvFilter;

static INIT: Once = Once::new();

fn init_trace() {
    INIT.call_once(|| {
        extern crate tracing;

        let filter =
            EnvFilter::from_default_env().add_directive("client::router=trace".parse().unwrap());

        let _ = tracing_subscriber::fmt()
            .with_max_level(Level::TRACE)
            .with_env_filter(filter)
            .init();
    });
}

#[tokio::test(core_threads = 2)]
#[ignore]
async fn normal_receive() {
    init_trace();

    let config = RouterParamBuilder::default().build();
    let pool = SwimConnPool::new(5, TungsteniteWsFactory::new(5).await);

    let mut router = SwimRouter::new(config, pool);

    let path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "/unit/foo",
        "info",
    );
    let (mut sink, _stream) = router.connection_for(&path).await.unwrap();

    let sync = Envelope::sync(String::from("/unit/foo"), String::from("info"));

    sink.send_item(sync).await.unwrap();

    thread::sleep(time::Duration::from_secs(5));
    let _ = router.close().await;
    thread::sleep(time::Duration::from_secs(5));
}

#[tokio::test(core_threads = 2)]
#[ignore]
async fn not_interested_receive() {
    init_trace();

    let config = RouterParamBuilder::default().build();
    let pool = SwimConnPool::new(5, TungsteniteWsFactory::new(5).await);
    let mut router = SwimRouter::new(config, pool);

    let path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "foo",
        "bar",
    );
    let (mut sink, _stream) = router.connection_for(&path).await.unwrap();

    let sync = Envelope::sync(String::from("/unit/foo"), String::from("info"));

    sink.send_item(sync).await.unwrap();

    thread::sleep(time::Duration::from_secs(5));
    let _ = router.close().await;
    thread::sleep(time::Duration::from_secs(5));
}

#[tokio::test(core_threads = 2)]
#[ignore]
async fn not_found_receive() {
    init_trace();

    let config = RouterParamBuilder::default().build();
    let pool = SwimConnPool::new(5, TungsteniteWsFactory::new(5).await);
    let mut router = SwimRouter::new(config, pool);

    let path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "foo",
        "bar",
    );
    let (mut sink, _stream) = router.connection_for(&path).await.unwrap();

    let sync = Envelope::sync(String::from("non_existent"), String::from("non_existent"));

    sink.send_item(sync).await.unwrap();

    thread::sleep(time::Duration::from_secs(5));
    let _ = router.close().await;
    thread::sleep(time::Duration::from_secs(5));
}

#[tokio::test(core_threads = 2)]
#[ignore]
async fn send_commands() {
    init_trace();

    let config = RouterParamBuilder::default().build();
    let pool = SwimConnPool::new(5, TungsteniteWsFactory::new(5).await);
    let mut router = SwimRouter::new(config, pool);

    let url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();

    let first_message = Envelope::command(
        String::from("/unit/foo"),
        String::from("publishInfo"),
        Some(Value::text("Hello, World!")),
    );

    let second_message = Envelope::command(
        String::from("/unit/foo"),
        String::from("publishInfo"),
        Some(Value::text("Test message")),
    );

    let third_message = Envelope::command(
        String::from("/unit/foo"),
        String::from("publishInfo"),
        Some(Value::text("Bye, World!")),
    );

    let mut router_sink = router.general_sink();

    router_sink
        .send_item((url.clone(), first_message))
        .await
        .unwrap();

    thread::sleep(time::Duration::from_secs(1));

    router_sink
        .send_item((url.clone(), second_message))
        .await
        .unwrap();

    thread::sleep(time::Duration::from_secs(1));

    router_sink.send_item((url, third_message)).await.unwrap();

    thread::sleep(time::Duration::from_secs(1));

    thread::sleep(time::Duration::from_secs(1));
    let _ = router.close().await;
    thread::sleep(time::Duration::from_secs(5));
}

#[tokio::test(core_threads = 2)]
#[ignore]
pub async fn server_stops_between_requests() {
    init_trace();

    let config = RouterParamBuilder::default().build();
    let pool = SwimConnPool::new(5, TungsteniteWsFactory::new(5).await);
    let mut router = SwimRouter::new(config, pool);

    let path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "/unit/foo",
        "info",
    );
    let (mut sink, _stream) = router.connection_for(&path).await.unwrap();
    let sync = Envelope::sync(String::from("/unit/foo"), String::from("info"));

    println!("Sending item");
    sink.send_item(sync).await.unwrap();
    println!("Sent item");

    //Terminate the remote server while waiting here
    thread::sleep(time::Duration::from_secs(10));

    let sync = Envelope::sync(String::from("/unit/foo"), String::from("info"));

    println!("Sending second item");
    let _ = sink.send_item(sync).await;
    println!("Sent second item");
    thread::sleep(time::Duration::from_secs(10));
    let _ = router.close().await;
    thread::sleep(time::Duration::from_secs(5));
}
