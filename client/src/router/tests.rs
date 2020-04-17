use std::{thread, time};

use common::sink::item::ItemSink;
use common::warp::envelope::Envelope;
use common::warp::path::AbsolutePath;

use crate::router::{Router, SwimRouter};
use tracing::Level;

#[tokio::test(core_threads = 2)]
async fn foo() {
    extern crate tracing;

    let _ = tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .init();

    let mut router = SwimRouter::new(5).await;
    let path = AbsolutePath::new("ws://127.0.0.1:9001", "foo", "bar");
    let (mut sink, _stream) = router.connection_for(&path).await;
    let sync = Envelope::sync(String::from("node_uri"), String::from("lane_uri"));

    println!("Sending item");
    sink.send_item(sync).await.unwrap();
    println!("Sent item");

    //Terminate the remote server while waiting here
    thread::sleep(time::Duration::from_secs(10));

    // println!("Sending item");
    // let sync = Envelope::sync(String::from("node_uri"), String::from("lane_uri"));
    // sink.send_item(sync).await.unwrap();
    // println!("Sent item");
    //
    // thread::sleep(time::Duration::from_secs(1));

    let sync = Envelope::sync(String::from("node_uri"), String::from("lane_uri"));

    println!("Sending second item");
    sink.send_item(sync).await.unwrap();
    println!("Sent second item");
    thread::sleep(time::Duration::from_secs(50));
    router.close().await;
    thread::sleep(time::Duration::from_secs(5));
}
