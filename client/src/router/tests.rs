use crate::router::{Router, SwimRouter};
use common::sink::item::{ItemSender, ItemSink};
use common::warp::envelope::{Envelope, LaneAddressed};
use common::warp::path::AbsolutePath;
use std::{thread, time};
use tokio::sync::mpsc;

#[tokio::test(core_threads = 2)]
async fn foo() {
    let mut router = SwimRouter::new(5).await;

    let path = AbsolutePath::new("ws://127.0.0.1:9001", "foo", "bar");
    let (mut sink, mut stream) = router.connection_for(&path).await;

    let lane_addressed = LaneAddressed {
        node_uri: String::from("node_uri"),
        lane_uri: String::from("lane_uri"),
        body: None,
    };

    let envelope = Envelope::EventMessage(lane_addressed);

    // thread::sleep(time::Duration::from_secs(5));
    sink.send_item(envelope).await;
    thread::sleep(time::Duration::from_secs(5));
}
