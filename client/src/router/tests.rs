use crate::router::{Router, SwimRouter};
use crate::sink::item::ItemSink;
use common::warp::envelope::{LaneAddressed, Envelope};
use common::warp::path::AbsolutePath;
use tokio::sync::mpsc;
use std::{thread, time};

#[tokio::test(core_threads = 4)]
async fn foo() {
    let mut router = SwimRouter::new(5);

    let path = AbsolutePath::new("ws://127.0.0.1:9001", "foo", "bar");
    let (mut sink, mut stream) = router.connection_for(&path).await;

    let lane_addressed = LaneAddressed {
        node_uri: String::from("node_uri"),
        lane_uri: String::from("lane_uri"),
        body: None,
    };

    let envelope = Envelope::EventMessage(lane_addressed);



    thread::sleep(time::Duration::from_secs(5));
    sink.send_item(envelope).await;

    let (payload_tx, mut payload_rx) = mpsc::channel::<String>(5);
    payload_rx.recv().await;
}

