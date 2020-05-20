use crate::connections::{ConnectionError, ConnectionPool, ConnectionReceiver, ConnectionSender};
use crate::router::{Router, RouterEvent, SwimRouter};
use common::model::Value;
use common::request::request_future::RequestError;
use common::sink::item::ItemSink;
use common::warp::envelope::Envelope;
use common::warp::path::AbsolutePath;
use futures::future::{ready, Ready};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{delay_for, timeout};
use tokio_tungstenite::tungstenite::protocol::Message;

async fn get_message(conn_pool: &TestPool, host_url: &url::Url) -> Option<Message> {
    if let Some((_, receiver)) = conn_pool.handlers.lock().unwrap().get_mut(host_url) {
        Some(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
        )
    } else {
        None
    }
}

async fn send_message(conn_pool: &TestPool, host_url: &url::Url, message: &str) {
    let message = Message::Text(message.to_string());

    if let Some((sender, _)) = conn_pool.handlers.lock().unwrap().get_mut(host_url) {
        timeout(Duration::from_secs(1), sender.send(message))
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

async fn wait_for_n_requests(pool: &TestPool, n: usize) {
    while get_request_count(pool) != n {
        delay_for(Duration::from_millis(10)).await;
    }
}

async fn open_connection(
    router: &mut SwimRouter,
    url: &url::Url,
    node: &str,
    lane: &str,
) -> (
    <SwimRouter as Router>::ConnectionSink,
    <SwimRouter as Router>::ConnectionStream,
) {
    router
        .connection_for(&AbsolutePath::new(url.clone(), node, lane))
        .await
        .unwrap()
}

#[tokio::test]
async fn test_create_downlinks() {
    let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let _ = open_connection(&mut router, &first_url, "foo", "bar").await;
    let _ = open_connection(&mut router, &first_url, "foo", "bar").await;
    let _ = open_connection(&mut router, &second_url, "oof", "rab").await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 0);
    assert_eq!(pool.connections.lock().unwrap().len(), 0);
}

#[tokio::test]
async fn test_route_single_outgoing_message_to_single_downlink() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut sink, _) = open_connection(&mut router, &url, "foo", "bar").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = sink.send_item(envelope).await.unwrap();

    wait_for_n_requests(&pool, 1).await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 1);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);
    assert_eq!(pool.connections.lock().unwrap().len(), 1);
    assert_eq!(
        get_message(&pool, &url).await.unwrap(),
        Message::text("@sync(node:foo,lane:bar)")
    );
}

#[tokio::test]
async fn test_route_single_outgoing_message_to_multiple_downlinks_same_host() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, _) = open_connection(&mut router, &url, "foo_node", "foo_lane").await;
    let (mut second_sink, _) = open_connection(&mut router, &url, "foo_node", "foo_lane").await;

    let env = Envelope::command(
        String::from("oof"),
        String::from("rab"),
        Some(Value::text("bye")),
    );

    let _ = first_sink.send_item(env.clone()).await.unwrap();
    let _ = second_sink.send_item(env).await.unwrap();

    wait_for_n_requests(&pool, 2).await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 2);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 2);

    assert_eq!(get_requests(&pool), expected_requests);
    assert_eq!(pool.connections.lock().unwrap().len(), 1);
    assert_eq!(
        get_message(&pool, &url).await.unwrap(),
        Message::text("@command(node:oof,lane:rab){bye}")
    );
    assert_eq!(
        get_message(&pool, &url).await.unwrap(),
        Message::text("@command(node:oof,lane:rab){bye}")
    );
}

#[tokio::test]
async fn test_route_single_outgoing_message_to_multiple_downlinks_different_hosts() {
    let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, _) =
        open_connection(&mut router, &first_url, "first_foo", "first_bar").await;
    let (mut second_sink, _) =
        open_connection(&mut router, &second_url, "second_foo", "second_bar").await;

    let env = Envelope::command(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("hello")),
    );

    let _ = first_sink.send_item(env.clone()).await.unwrap();
    let _ = second_sink.send_item(env).await.unwrap();

    wait_for_n_requests(&pool, 2).await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 2);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((first_url.clone(), false), 1);
    expected_requests.insert((second_url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);
    assert_eq!(pool.connections.lock().unwrap().len(), 2);
    assert_eq!(
        get_message(&pool, &first_url).await.unwrap(),
        Message::text("@command(node:foo,lane:bar){hello}")
    );
    assert_eq!(
        get_message(&pool, &second_url).await.unwrap(),
        Message::text("@command(node:foo,lane:bar){hello}")
    );
}

#[tokio::test]
async fn test_route_multiple_outgoing_messages_to_single_downlink() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut sink, _) = open_connection(&mut router, &url, "foo", "bar").await;

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

    wait_for_n_requests(&pool, 2).await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 2);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 2);

    assert_eq!(get_requests(&pool), expected_requests);
    assert_eq!(pool.connections.lock().unwrap().len(), 1);

    assert_eq!(
        get_message(&pool, &url).await.unwrap(),
        Message::text("@command(node:foo,lane:bar){First_Downlink}")
    );
    assert_eq!(
        get_message(&pool, &url).await.unwrap(),
        Message::text("@command(node:foo,lane:bar){Second_Downlink}")
    );
}

#[tokio::test]
async fn test_route_multiple_outgoing_messages_to_multiple_downlinks_same_host() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, _) = open_connection(&mut router, &url, "foo_node", "foo_lane").await;
    let (mut second_sink, _) = open_connection(&mut router, &url, "foo_node", "foo_lane").await;

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

    wait_for_n_requests(&pool, 3).await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 3);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 3);

    assert_eq!(get_requests(&pool), expected_requests);

    assert_eq!(pool.connections.lock().unwrap().len(), 1);
    assert_eq!(
        get_message(&pool, &url).await.unwrap(),
        Message::text("@command(node:first_foo,lane:first_bar){first_body}")
    );
    assert_eq!(
        get_message(&pool, &url).await.unwrap(),
        Message::text("@command(node:second_foo,lane:second_bar){second_body}")
    );
    assert_eq!(
        get_message(&pool, &url).await.unwrap(),
        Message::text("@command(node:third_foo,lane:third_bar){third_body}")
    );
}

#[tokio::test]
async fn test_route_multiple_outgoing_messages_to_multiple_downlinks_different_hosts() {
    let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, _) =
        open_connection(&mut router, &first_url, "foo_node", "foo_lane").await;
    let (mut second_sink, _) =
        open_connection(&mut router, &second_url, "foo_node", "foo_lane").await;

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

    wait_for_n_requests(&pool, 3).await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 3);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((first_url.clone(), false), 2);
    expected_requests.insert((second_url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);

    assert_eq!(pool.connections.lock().unwrap().len(), 2);
    assert_eq!(
        get_message(&pool, &first_url).await.unwrap(),
        Message::text("@command(node:first_foo,lane:first_bar){first_body}")
    );
    assert_eq!(
        get_message(&pool, &first_url).await.unwrap(),
        Message::text("@command(node:second_foo,lane:second_bar){second_body}")
    );
    assert_eq!(
        get_message(&pool, &second_url).await.unwrap(),
        Message::text("@command(node:third_foo,lane:third_bar){third_body}")
    );
}

#[tokio::test]
async fn test_route_single_incoming_message_to_single_downlink_before_outgoing() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (_, mut stream) = open_connection(&mut router, &url, "foo", "bar").await;

    send_message(&pool, &url, "@command(node:foo,lane:bar){Hello}").await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 0);
    assert_eq!(pool.connections.lock().unwrap().len(), 0);

    assert_eq!(stream.recv().await.unwrap(), RouterEvent::Stopping);
}

#[tokio::test]
async fn test_route_single_incoming_message_to_single_downlink_after_outgoing() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut sink, mut stream) = open_connection(&mut router, &url, "foo", "bar").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));

    let _ = sink.send_item(envelope.clone()).await.unwrap();

    wait_for_n_requests(&pool, 1).await;

    send_message(&pool, &url, "@command(node:foo,lane:bar){Hello}").await;

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

#[tokio::test]
async fn test_route_single_incoming_message_to_multiple_downlinks_same_host_same_path() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) = open_connection(&mut router, &url, "foo", "bar").await;
    let (_, mut second_stream) = open_connection(&mut router, &url, "foo", "bar").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = first_sink.send_item(envelope).await.unwrap();

    wait_for_n_requests(&pool, 1).await;

    send_message(&pool, &url, "@command(node:foo,lane:bar){Goodbye}").await;

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

#[tokio::test]
async fn test_route_single_incoming_message_to_multiple_downlinks_same_host_different_paths() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) = open_connection(&mut router, &url, "oof", "rab").await;
    let (_, mut second_stream) = open_connection(&mut router, &url, "foo", "bar").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = first_sink.send_item(envelope.clone()).await.unwrap();

    wait_for_n_requests(&pool, 1).await;

    send_message(&pool, &url, "@command(node:foo,lane:bar){tseT}").await;

    let expected_env = Envelope::command(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("tseT")),
    );

    assert!(timeout(Duration::from_secs(1), first_stream.recv())
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

#[tokio::test]
async fn test_route_single_incoming_message_to_multiple_downlinks_different_hosts_same_path() {
    let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) =
        open_connection(&mut router, &first_url, "foo", "bar").await;
    let (mut second_sink, mut second_stream) =
        open_connection(&mut router, &second_url, "foo", "bar").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = first_sink.send_item(envelope.clone()).await.unwrap();
    let _ = second_sink.send_item(envelope).await.unwrap();

    wait_for_n_requests(&pool, 2).await;

    send_message(
        &pool,
        &first_url,
        "@command(node:foo,lane:bar){\"First Hello\"}",
    )
    .await;

    send_message(
        &pool,
        &second_url,
        "@command(node:foo,lane:bar){\"Second Hello\"}",
    )
    .await;

    let first_env = Envelope::command(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("First Hello")),
    );

    let second_env = Envelope::command(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("Second Hello")),
    );

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::Envelope(first_env)
    );

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Envelope(second_env)
    );

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 2);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((first_url.clone(), false), 1);
    expected_requests.insert((second_url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);
    assert_eq!(pool.connections.lock().unwrap().len(), 2);

    assert_eq!(first_stream.recv().await.unwrap(), RouterEvent::Stopping);
    assert_eq!(second_stream.recv().await.unwrap(), RouterEvent::Stopping);
}

#[tokio::test]
async fn test_route_single_incoming_message_to_multiple_downlinks_different_hosts_different_paths()
{
    let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();
    let third_url = url::Url::parse("ws://127.0.0.3/").unwrap();

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) =
        open_connection(&mut router, &first_url, "foo", "bar").await;

    let (mut second_sink, mut second_stream) =
        open_connection(&mut router, &second_url, "oof", "rab").await;

    let (mut third_sink, mut third_stream) =
        open_connection(&mut router, &third_url, "ofo", "abr").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = first_sink.send_item(envelope.clone()).await.unwrap();
    let _ = second_sink.send_item(envelope.clone()).await.unwrap();
    let _ = third_sink.send_item(envelope).await.unwrap();

    wait_for_n_requests(&pool, 3).await;

    send_message(
        &pool,
        &first_url,
        "@command(node:foo,lane:bar){\"Hello First\"}",
    )
    .await;

    send_message(
        &pool,
        &second_url,
        "@command(node:oof,lane:rab){\"Hello Second\"}",
    )
    .await;

    send_message(
        &pool,
        &third_url,
        "@command(node:ofo,lane:abr){\"Hello Third\"}",
    )
    .await;

    let first_env = Envelope::command(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("Hello First")),
    );

    let second_env = Envelope::command(
        String::from("oof"),
        String::from("rab"),
        Some(Value::text("Hello Second")),
    );

    let third_env = Envelope::command(
        String::from("ofo"),
        String::from("abr"),
        Some(Value::text("Hello Third")),
    );

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::Envelope(first_env)
    );

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Envelope(second_env)
    );

    assert_eq!(
        third_stream.recv().await.unwrap(),
        RouterEvent::Envelope(third_env)
    );

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 3);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((first_url.clone(), false), 1);
    expected_requests.insert((second_url.clone(), false), 1);
    expected_requests.insert((third_url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);
    assert_eq!(pool.connections.lock().unwrap().len(), 3);

    assert_eq!(first_stream.recv().await.unwrap(), RouterEvent::Stopping);
    assert_eq!(second_stream.recv().await.unwrap(), RouterEvent::Stopping);
    assert_eq!(third_stream.recv().await.unwrap(), RouterEvent::Stopping);
}

#[tokio::test]
async fn test_route_multiple_incoming_messages_to_single_downlink_before_outgoing() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (_, mut stream) = open_connection(&mut router, &url, "foo", "bar").await;

    send_message(&pool, &url, "@command(node:foo,lane:bar){First}").await;
    send_message(&pool, &url, "@command(node:foo,lane:bar){Second}").await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 0);
    assert_eq!(pool.connections.lock().unwrap().len(), 0);

    assert_eq!(stream.recv().await.unwrap(), RouterEvent::Stopping);
}

#[tokio::test]
async fn test_route_multiple_incoming_messages_to_single_downlink_after_outgoing() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut sink, mut stream) = open_connection(&mut router, &url, "foo", "bar").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = sink.send_item(envelope.clone()).await.unwrap();

    wait_for_n_requests(&pool, 1).await;

    send_message(&pool, &url, "@command(node:foo,lane:bar){\"First!\"}").await;
    send_message(&pool, &url, "@command(node:foo,lane:bar){\"Second!\"}").await;

    let first_env = Envelope::command(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("First!")),
    );

    let second_env = Envelope::command(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("Second!")),
    );

    assert_eq!(
        stream.recv().await.unwrap(),
        RouterEvent::Envelope(first_env)
    );

    assert_eq!(
        stream.recv().await.unwrap(),
        RouterEvent::Envelope(second_env)
    );

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 1);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);
    assert_eq!(pool.connections.lock().unwrap().len(), 1);

    assert_eq!(stream.recv().await.unwrap(), RouterEvent::Stopping);
}

#[tokio::test]
async fn test_route_multiple_incoming_messages_to_multiple_downlinks_same_host_same_path() {
    let url = url::Url::parse("ws://192.168.0.1/").unwrap();

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) =
        open_connection(&mut router, &url, "room", "five").await;
    let (_, mut second_stream) = open_connection(&mut router, &url, "room", "five").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = first_sink.send_item(envelope).await.unwrap();

    wait_for_n_requests(&pool, 1).await;

    send_message(&pool, &url, "@command(node:room,lane:five){\"John Doe\"}").await;
    send_message(&pool, &url, "@command(node:room,lane:five){\"Jane Doe\"}").await;

    let first_env = Envelope::command(
        String::from("room"),
        String::from("five"),
        Some(Value::text("John Doe")),
    );

    let second_env = Envelope::command(
        String::from("room"),
        String::from("five"),
        Some(Value::text("Jane Doe")),
    );

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::Envelope(first_env.clone())
    );

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::Envelope(second_env.clone())
    );

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Envelope(first_env)
    );

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Envelope(second_env)
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

#[tokio::test]
async fn test_route_multiple_incoming_messages_to_multiple_downlinks_same_host_different_paths() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) =
        open_connection(&mut router, &url, "room", "five").await;
    let (_, mut second_stream) = open_connection(&mut router, &url, "room", "six").await;

    let envelope = Envelope::sync(String::from("room"), String::from("seven"));
    let _ = first_sink.send_item(envelope.clone()).await.unwrap();

    wait_for_n_requests(&pool, 1).await;

    send_message(&pool, &url, "@command(node:room,lane:five){\"John Doe\"}").await;
    send_message(&pool, &url, "@command(node:room,lane:six){\"Jane Doe\"}").await;

    let first_env = Envelope::command(
        String::from("room"),
        String::from("five"),
        Some(Value::text("John Doe")),
    );

    let second_env = Envelope::command(
        String::from("room"),
        String::from("six"),
        Some(Value::text("Jane Doe")),
    );

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::Envelope(first_env)
    );

    assert!(timeout(Duration::from_secs(1), first_stream.recv())
        .await
        .is_err());

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Envelope(second_env)
    );

    assert!(timeout(Duration::from_secs(1), second_stream.recv())
        .await
        .is_err());

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 1);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);
    assert_eq!(pool.connections.lock().unwrap().len(), 1);

    assert_eq!(first_stream.recv().await.unwrap(), RouterEvent::Stopping);
    assert_eq!(second_stream.recv().await.unwrap(), RouterEvent::Stopping);
}

#[tokio::test]
async fn test_route_multiple_incoming_message_to_multiple_downlinks_different_hosts_same_path() {
    let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) =
        open_connection(&mut router, &first_url, "building", "1").await;

    let (mut second_sink, mut second_stream) =
        open_connection(&mut router, &second_url, "building", "1").await;

    let envelope = Envelope::sync(String::from("building"), String::from("1"));
    let _ = first_sink.send_item(envelope.clone()).await.unwrap();
    let _ = second_sink.send_item(envelope).await.unwrap();

    wait_for_n_requests(&pool, 2).await;

    send_message(
        &pool,
        &first_url,
        "@command(node:building,lane:\"1\"){\"Room 101\"}",
    )
    .await;

    send_message(
        &pool,
        &first_url,
        "@command(node:building,lane:\"1\"){\"Room 102\"}",
    )
    .await;

    send_message(
        &pool,
        &second_url,
        "@command(node:building,lane:\"1\"){\"Room 201\"}",
    )
    .await;

    send_message(
        &pool,
        &second_url,
        "@command(node:building,lane:\"1\"){\"Room 202\"}",
    )
    .await;

    let env_101 = Envelope::command(
        String::from("building"),
        String::from("1"),
        Some(Value::text("Room 101")),
    );

    let env_102 = Envelope::command(
        String::from("building"),
        String::from("1"),
        Some(Value::text("Room 102")),
    );

    let env_201 = Envelope::command(
        String::from("building"),
        String::from("1"),
        Some(Value::text("Room 201")),
    );

    let env_202 = Envelope::command(
        String::from("building"),
        String::from("1"),
        Some(Value::text("Room 202")),
    );

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::Envelope(env_101)
    );

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::Envelope(env_102)
    );

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Envelope(env_201)
    );

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Envelope(env_202)
    );

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 2);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((first_url.clone(), false), 1);
    expected_requests.insert((second_url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);
    assert_eq!(pool.connections.lock().unwrap().len(), 2);

    assert_eq!(first_stream.recv().await.unwrap(), RouterEvent::Stopping);
    assert_eq!(second_stream.recv().await.unwrap(), RouterEvent::Stopping);
}

#[tokio::test]
async fn test_route_multiple_incoming_message_to_multiple_downlinks_different_hosts_different_paths(
) {
    let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();
    let third_url = url::Url::parse("ws://127.0.0.3/").unwrap();

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) =
        open_connection(&mut router, &first_url, "building", "1").await;

    let (mut second_sink, mut second_stream) =
        open_connection(&mut router, &second_url, "room", "2").await;

    let (mut third_sink, mut third_stream) =
        open_connection(&mut router, &third_url, "building", "3").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = first_sink.send_item(envelope.clone()).await.unwrap();
    let _ = second_sink.send_item(envelope.clone()).await.unwrap();
    let _ = third_sink.send_item(envelope).await.unwrap();

    wait_for_n_requests(&pool, 3).await;

    send_message(
        &pool,
        &first_url,
        "@command(node:building,lane:\"1\"){\"Building 101\"}",
    )
    .await;

    send_message(
        &pool,
        &first_url,
        "@command(node:building,lane:\"1\"){\"Building 102\"}",
    )
    .await;

    send_message(
        &pool,
        &second_url,
        "@command(node:room,lane:\"2\"){\"Room 201\"}",
    )
    .await;

    send_message(
        &pool,
        &second_url,
        "@command(node:room,lane:\"2\"){\"Room 202\"}",
    )
    .await;

    send_message(
        &pool,
        &second_url,
        "@command(node:room,lane:\"2\"){\"Room 203\"}",
    )
    .await;

    send_message(
        &pool,
        &third_url,
        "@command(node:building,lane:\"3\"){\"Building 301\"}",
    )
    .await;

    send_message(
        &pool,
        &third_url,
        "@command(node:building,lane:\"3\"){\"Building 302\"}",
    )
    .await;

    let building_101 = Envelope::command(
        String::from("building"),
        String::from("1"),
        Some(Value::text("Building 101")),
    );

    let building_102 = Envelope::command(
        String::from("building"),
        String::from("1"),
        Some(Value::text("Building 102")),
    );

    let room_201 = Envelope::command(
        String::from("room"),
        String::from("2"),
        Some(Value::text("Room 201")),
    );

    let room_202 = Envelope::command(
        String::from("room"),
        String::from("2"),
        Some(Value::text("Room 202")),
    );

    let room_203 = Envelope::command(
        String::from("room"),
        String::from("2"),
        Some(Value::text("Room 203")),
    );

    let building_301 = Envelope::command(
        String::from("building"),
        String::from("3"),
        Some(Value::text("Building 301")),
    );

    let building_302 = Envelope::command(
        String::from("building"),
        String::from("3"),
        Some(Value::text("Building 302")),
    );

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::Envelope(building_101)
    );

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::Envelope(building_102)
    );

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Envelope(room_201)
    );

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Envelope(room_202)
    );

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Envelope(room_203)
    );

    assert_eq!(
        third_stream.recv().await.unwrap(),
        RouterEvent::Envelope(building_301)
    );

    assert_eq!(
        third_stream.recv().await.unwrap(),
        RouterEvent::Envelope(building_302)
    );

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 3);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((first_url.clone(), false), 1);
    expected_requests.insert((second_url.clone(), false), 1);
    expected_requests.insert((third_url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);
    assert_eq!(pool.connections.lock().unwrap().len(), 3);

    assert_eq!(first_stream.recv().await.unwrap(), RouterEvent::Stopping);
    assert_eq!(second_stream.recv().await.unwrap(), RouterEvent::Stopping);
    assert_eq!(third_stream.recv().await.unwrap(), RouterEvent::Stopping);
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

    fn create_connection(
        &mut self,
        host_url: url::Url,
        recreate: bool,
    ) -> (mpsc::Sender<Message>, mpsc::Receiver<Message>) {
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

        self.log_request(host_url, recreate);

        (sender_tx, receiver_rx)
    }

    fn log_request(&mut self, host_url: url::Url, recreate: bool) {
        *self
            .connection_requests
            .lock()
            .unwrap()
            .entry((host_url.clone(), recreate.clone()))
            .or_insert(0) += 1;
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

    fn request_connection(&mut self, host_url: url::Url, recreate: bool) -> Self::ConnFut {
        if !recreate && self.connections.lock().unwrap().get(&host_url).is_some() {
            let sender_tx = self
                .connections
                .lock()
                .unwrap()
                .get(&host_url)
                .unwrap()
                .clone();

            self.log_request(host_url, recreate);

            ready(Ok(Ok((ConnectionSender::new(sender_tx), None))))
        } else {
            let (sender_tx, receiver_rx) = self.create_connection(host_url, recreate);

            ready(Ok(Ok((
                ConnectionSender::new(sender_tx),
                Some(receiver_rx),
            ))))
        }
    }

    fn close(self) -> Result<Self::CloseFut, ConnectionError> {
        Ok(ready(Ok(Ok(()))))
    }
}
