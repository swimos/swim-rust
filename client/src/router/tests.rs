use crate::connections::{ConnectionError, ConnectionPool, ConnectionReceiver, ConnectionSender};
use crate::router::{Router, RouterEvent, SwimRouter};
use common::model::Value;
use common::request::request_future::RequestError;
use common::sink::item::ItemSink;
use common::warp::envelope::Envelope;
use common::warp::path::AbsolutePath;
use futures::future::{ready, Ready};
use std::collections::HashMap;
use std::sync::Once;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::protocol::Message;
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

async fn get_message(conn_pool: &TestPool, host_url: &url::Url) -> Option<String> {
    if let Some((_, receiver)) = conn_pool.handlers.lock().unwrap().get_mut(host_url) {
        Some(
            timeout(Duration::from_millis(10), receiver.recv())
                .await
                .unwrap()
                .unwrap()
                .to_string(),
        )
    } else {
        None
    }
}

async fn send_message(conn_pool: &TestPool, host_url: &url::Url, message: Message) {
    if let Some((sender, _)) = conn_pool.handlers.lock().unwrap().get_mut(host_url) {
        timeout(Duration::from_millis(10), sender.send(message))
            .await
            .unwrap()
            .unwrap();
    }
}

fn get_requests(conn_pool: &TestPool) -> HashMap<(url::Url, bool), usize> {
    conn_pool.connection_requests.lock().unwrap().clone()
}

fn get_request_count(conn_pool: &TestPool) -> usize {
    conn_pool.connection_requests.lock().unwrap().values().sum()
}

#[tokio::test]
async fn test_create_router() {
    let pool = TestPool::new();
    let router = SwimRouter::new(Default::default(), pool.clone());

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 0);
    assert_eq!(pool.connections.lock().unwrap().len(), 0);
}

#[tokio::test]
async fn test_create_single_downlink() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let node = "foo";
    let lane = "bar";

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let _ = router
        .connection_for(&AbsolutePath::new(url, node, lane))
        .await
        .unwrap();

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 0);
    assert_eq!(pool.connections.lock().unwrap().len(), 0);
}

#[tokio::test]
async fn test_create_multiple_downlinks_same_host() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let node = "foo";
    let lane = "bar";

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let _ = router
        .connection_for(&AbsolutePath::new(url.clone(), node, lane))
        .await
        .unwrap();

    let _ = router
        .connection_for(&AbsolutePath::new(url, node, lane))
        .await
        .unwrap();

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 0);
    assert_eq!(pool.connections.lock().unwrap().len(), 0);
}

#[tokio::test]
async fn test_create_multiple_downlinks_different_hosts() {
    let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let first_node = "first_foo";
    let first_lane = "first_bar";

    let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();
    let second_node = "second_foo";
    let second_lane = "second_bar";

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let _ = router
        .connection_for(&AbsolutePath::new(first_url, first_node, first_lane))
        .await
        .unwrap();

    let _ = router
        .connection_for(&AbsolutePath::new(second_url, second_node, second_lane))
        .await
        .unwrap();

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 0);
    assert_eq!(pool.connections.lock().unwrap().len(), 0);
}

#[tokio::test(core_threads = 2)]
async fn test_route_single_outgoing_message_to_single_downlink() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let node = "foo";
    let lane = "bar";

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut sink, _) = router
        .connection_for(&AbsolutePath::new(url.clone(), node, lane))
        .await
        .unwrap();

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = sink.send_item(envelope).await.unwrap();

    while get_request_count(&pool) != 1 {}

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 1);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);
    assert_eq!(pool.connections.lock().unwrap().len(), 1);
    assert_eq!(
        get_message(&pool, &url).await.unwrap(),
        "@sync(node:foo,lane:bar)"
    );
}

#[tokio::test(core_threads = 2)]
async fn test_route_single_outgoing_message_to_multiple_downlinks_same_host() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let node = "foo_node";
    let lane = "foo_lane";

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, _) = router
        .connection_for(&AbsolutePath::new(url.clone(), node, lane))
        .await
        .unwrap();

    let (mut second_sink, _) = router
        .connection_for(&AbsolutePath::new(url.clone(), node, lane))
        .await
        .unwrap();

    let env = Envelope::command(
        String::from("oof"),
        String::from("rab"),
        Some(Value::text("bye")),
    );

    let _ = first_sink.send_item(env.clone()).await.unwrap();
    let _ = second_sink.send_item(env).await.unwrap();

    while get_request_count(&pool) != 2 {}

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 2);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 2);

    assert_eq!(get_requests(&pool), expected_requests);

    assert_eq!(pool.connections.lock().unwrap().len(), 1);
    assert_eq!(
        get_message(&pool, &url).await.unwrap(),
        "@command(node:oof,lane:rab){bye}"
    );
    assert_eq!(
        get_message(&pool, &url).await.unwrap(),
        "@command(node:oof,lane:rab){bye}"
    );
}

#[tokio::test(core_threads = 2)]
async fn test_route_single_outgoing_message_to_multiple_downlinks_different_hosts() {
    let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let first_node = "first_foo";
    let first_lane = "first_bar";

    let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();
    let second_node = "second_foo";
    let second_lane = "second_bar";

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, _) = router
        .connection_for(&AbsolutePath::new(
            first_url.clone(),
            first_node,
            first_lane,
        ))
        .await
        .unwrap();

    let (mut second_sink, _) = router
        .connection_for(&AbsolutePath::new(
            second_url.clone(),
            second_node,
            second_lane,
        ))
        .await
        .unwrap();

    let env = Envelope::command(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("hello")),
    );

    let _ = first_sink.send_item(env.clone()).await.unwrap();
    let _ = second_sink.send_item(env).await.unwrap();

    while get_request_count(&pool) != 2 {}

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 2);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((first_url.clone(), false), 1);
    expected_requests.insert((second_url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);

    assert_eq!(pool.connections.lock().unwrap().len(), 2);
    assert_eq!(
        get_message(&pool, &first_url).await.unwrap(),
        "@command(node:foo,lane:bar){hello}"
    );
    assert_eq!(
        get_message(&pool, &second_url).await.unwrap(),
        "@command(node:foo,lane:bar){hello}"
    );
}

#[tokio::test(core_threads = 2)]
async fn test_route_multiple_outgoing_messages_to_single_downlink() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let node = "foo";
    let lane = "bar";

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut sink, _) = router
        .connection_for(&AbsolutePath::new(url.clone(), node, lane))
        .await
        .unwrap();

    let first_env = Envelope::command(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("First_Downlink")),
    );
    let _ = sink.send_item(first_env).await.unwrap();

    let second_env = Envelope::command(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("Second_Downlink")),
    );
    let _ = sink.send_item(second_env).await.unwrap();

    while get_request_count(&pool) != 2 {}

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 2);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 2);

    assert_eq!(get_requests(&pool), expected_requests);

    assert_eq!(pool.connections.lock().unwrap().len(), 1);

    assert_eq!(
        get_message(&pool, &url).await.unwrap(),
        "@command(node:foo,lane:bar){First_Downlink}"
    );
    assert_eq!(
        get_message(&pool, &url).await.unwrap(),
        "@command(node:foo,lane:bar){Second_Downlink}"
    );
}

#[tokio::test(core_threads = 2)]
async fn test_route_multiple_outgoing_messages_to_multiple_downlinks_same_host() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let node = "foo_node";
    let lane = "foo_lane";

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, _) = router
        .connection_for(&AbsolutePath::new(url.clone(), node, lane))
        .await
        .unwrap();

    let (mut second_sink, _) = router
        .connection_for(&AbsolutePath::new(url.clone(), node, lane))
        .await
        .unwrap();

    let first_env = Envelope::command(
        String::from("first_foo"),
        String::from("first_bar"),
        Some(Value::text("first_body")),
    );

    let second_env = Envelope::command(
        String::from("second_foo"),
        String::from("second_bar"),
        Some(Value::text("second_body")),
    );

    let third_env = Envelope::command(
        String::from("third_foo"),
        String::from("third_bar"),
        Some(Value::text("third_body")),
    );

    let _ = first_sink.send_item(first_env).await.unwrap();
    let _ = first_sink.send_item(second_env).await.unwrap();
    let _ = second_sink.send_item(third_env).await.unwrap();

    while get_request_count(&pool) != 3 {}

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 3);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 3);

    assert_eq!(get_requests(&pool), expected_requests);

    assert_eq!(pool.connections.lock().unwrap().len(), 1);
    assert_eq!(
        get_message(&pool, &url).await.unwrap(),
        "@command(node:first_foo,lane:first_bar){first_body}"
    );
    assert_eq!(
        get_message(&pool, &url).await.unwrap(),
        "@command(node:second_foo,lane:second_bar){second_body}"
    );
    assert_eq!(
        get_message(&pool, &url).await.unwrap(),
        "@command(node:third_foo,lane:third_bar){third_body}"
    );
}

#[tokio::test(core_threads = 2)]
async fn test_route_multiple_outgoing_messages_to_multiple_downlinks_different_hosts() {
    let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let first_node = "foo_node";
    let first_lane = "foo_lane";

    let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();
    let second_node = "foo_node";
    let second_lane = "foo_lane";

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, _) = router
        .connection_for(&AbsolutePath::new(
            first_url.clone(),
            first_node,
            first_lane,
        ))
        .await
        .unwrap();

    let (mut second_sink, _) = router
        .connection_for(&AbsolutePath::new(
            second_url.clone(),
            second_node,
            second_lane,
        ))
        .await
        .unwrap();

    let first_env = Envelope::command(
        String::from("first_foo"),
        String::from("first_bar"),
        Some(Value::text("first_body")),
    );

    let second_env = Envelope::command(
        String::from("second_foo"),
        String::from("second_bar"),
        Some(Value::text("second_body")),
    );

    let third_env = Envelope::command(
        String::from("third_foo"),
        String::from("third_bar"),
        Some(Value::text("third_body")),
    );

    let _ = first_sink.send_item(first_env).await.unwrap();
    let _ = first_sink.send_item(second_env).await.unwrap();
    let _ = second_sink.send_item(third_env).await.unwrap();

    while get_request_count(&pool) != 3 {}

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 3);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((first_url.clone(), false), 2);
    expected_requests.insert((second_url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);

    assert_eq!(pool.connections.lock().unwrap().len(), 2);
    assert_eq!(
        get_message(&pool, &first_url).await.unwrap(),
        "@command(node:first_foo,lane:first_bar){first_body}"
    );
    assert_eq!(
        get_message(&pool, &first_url).await.unwrap(),
        "@command(node:second_foo,lane:second_bar){second_body}"
    );
    assert_eq!(
        get_message(&pool, &second_url).await.unwrap(),
        "@command(node:third_foo,lane:third_bar){third_body}"
    );
}

#[tokio::test(core_threads = 2)]
async fn test_route_single_incoming_message_to_single_downlink_before_outgoing() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let node = "foo";
    let lane = "bar";

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (_, mut stream) = router
        .connection_for(&AbsolutePath::new(url.clone(), node, lane))
        .await
        .unwrap();

    send_message(
        &pool,
        &url,
        Message::Text("@command(node:foo,lane:bar){Hello}".to_string()),
    )
    .await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 0);
    assert_eq!(pool.connections.lock().unwrap().len(), 0);

    assert_eq!(stream.recv().await.unwrap(), RouterEvent::Stopping);
}

#[tokio::test(core_threads = 2)]
async fn test_route_single_incoming_message_to_single_downlink_after_outgoing() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let node = "foo";
    let lane = "bar";

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut sink, mut stream) = router
        .connection_for(&AbsolutePath::new(url.clone(), node, lane))
        .await
        .unwrap();

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));

    let _ = sink.send_item(envelope.clone()).await.unwrap();

    while get_request_count(&pool) != 1 {}

    send_message(
        &pool,
        &url,
        Message::Text("@command(node:foo,lane:bar){Hello}".to_string()),
    )
    .await;

    let expected_env = Envelope::command(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("Hello")),
    );

    assert_eq!(
        stream.recv().await.unwrap(),
        RouterEvent::Envelope(expected_env)
    );

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 1);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);
    assert_eq!(pool.connections.lock().unwrap().len(), 1);

    assert_eq!(stream.recv().await.unwrap(), RouterEvent::Stopping);
}

#[tokio::test(core_threads = 2)]
async fn test_route_single_incoming_message_to_multiple_downlinks_same_host_same_path() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let node = "foo";
    let lane = "bar";

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) = router
        .connection_for(&AbsolutePath::new(url.clone(), node, lane))
        .await
        .unwrap();

    let (_, mut second_stream) = router
        .connection_for(&AbsolutePath::new(url.clone(), node, lane))
        .await
        .unwrap();

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = first_sink.send_item(envelope.clone()).await.unwrap();

    while get_request_count(&pool) != 1 {}

    send_message(
        &pool,
        &url,
        Message::Text("@command(node:foo,lane:bar){Goodbye}".to_string()),
    )
    .await;

    let expected_env = Envelope::command(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("Goodbye")),
    );

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::Envelope(expected_env.clone())
    );

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Envelope(expected_env)
    );

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 1);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);
    assert_eq!(pool.connections.lock().unwrap().len(), 1);

    assert_eq!(first_stream.recv().await.unwrap(), RouterEvent::Stopping);
    assert_eq!(second_stream.recv().await.unwrap(), RouterEvent::Stopping);
}

#[tokio::test(core_threads = 2)]
async fn test_route_single_incoming_message_to_multiple_downlinks_same_host_different_paths() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let first_node = "oof";
    let first_lane = "rab";

    let second_node = "foo";
    let second_lane = "bar";

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) = router
        .connection_for(&AbsolutePath::new(url.clone(), first_node, first_lane))
        .await
        .unwrap();

    let (_, mut second_stream) = router
        .connection_for(&AbsolutePath::new(url.clone(), second_node, second_lane))
        .await
        .unwrap();

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = first_sink.send_item(envelope.clone()).await.unwrap();

    while get_request_count(&pool) != 1 {}

    send_message(
        &pool,
        &url,
        Message::Text("@command(node:foo,lane:bar){tseT}".to_string()),
    )
    .await;

    let expected_env = Envelope::command(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("tseT")),
    );

    assert!(timeout(Duration::from_millis(10), first_stream.recv())
        .await
        .is_err());

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Envelope(expected_env)
    );

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 1);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);
    assert_eq!(pool.connections.lock().unwrap().len(), 1);

    assert_eq!(first_stream.recv().await.unwrap(), RouterEvent::Stopping);
    assert_eq!(second_stream.recv().await.unwrap(), RouterEvent::Stopping);
}

type PoolHandler = (mpsc::Sender<Message>, mpsc::Receiver<Message>);

#[derive(Clone)]
struct TestPool {
    connection_requests: Arc<Mutex<HashMap<(url::Url, bool), usize>>>,
    connections: Arc<Mutex<HashMap<url::Url, mpsc::Sender<Message>>>>,
    handlers: Arc<Mutex<HashMap<url::Url, PoolHandler>>>,
}

impl TestPool {
    fn new() -> Self {
        TestPool {
            connection_requests: Arc::new(Mutex::new(HashMap::new())),
            connections: Arc::new(Mutex::new(HashMap::new())),
            handlers: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl ConnectionPool for TestPool {
    type ConnFut = Ready<
        Result<
            Result<(ConnectionSender, Option<ConnectionReceiver>), ConnectionError>,
            RequestError,
        >,
    >;

    type CloseFut = Ready<Result<Result<(), ConnectionError>, ConnectionError>>;

    //Todo refactor this
    fn request_connection(&mut self, host_url: url::Url, recreate: bool) -> Self::ConnFut {
        if recreate {
            let (sender_tx, sender_rx) = mpsc::channel(5);
            let (receiver_tx, receiver_rx) = mpsc::channel(5);

            self.connections
                .lock()
                .unwrap()
                .insert(host_url.clone(), sender_tx.clone());

            self.handlers
                .lock()
                .unwrap()
                .insert(host_url.clone(), (receiver_tx, sender_rx));

            *self
                .connection_requests
                .lock()
                .unwrap()
                .entry((host_url.clone(), recreate.clone()))
                .or_insert(0) += 1;

            ready(Ok(Ok((
                ConnectionSender { tx: sender_tx },
                Some(receiver_rx),
            ))))
        } else {
            if self.connections.lock().unwrap().get(&host_url).is_none() {
                let (sender_tx, sender_rx) = mpsc::channel(5);
                let (receiver_tx, receiver_rx) = mpsc::channel(5);

                self.connections
                    .lock()
                    .unwrap()
                    .insert(host_url.clone(), sender_tx.clone());

                self.handlers
                    .lock()
                    .unwrap()
                    .insert(host_url.clone(), (receiver_tx, sender_rx));

                *self
                    .connection_requests
                    .lock()
                    .unwrap()
                    .entry((host_url.clone(), recreate.clone()))
                    .or_insert(0) += 1;

                ready(Ok(Ok((
                    ConnectionSender { tx: sender_tx },
                    Some(receiver_rx),
                ))))
            } else {
                let sender_tx = self
                    .connections
                    .lock()
                    .unwrap()
                    .get(&host_url)
                    .unwrap()
                    .clone();

                *self
                    .connection_requests
                    .lock()
                    .unwrap()
                    .entry((host_url.clone(), recreate.clone()))
                    .or_insert(0) += 1;

                ready(Ok(Ok((ConnectionSender { tx: sender_tx }, None))))
            }
        }
    }

    fn close(self) -> Result<Self::CloseFut, ConnectionError> {
        Ok(ready(Ok(Ok(()))))
    }
}
