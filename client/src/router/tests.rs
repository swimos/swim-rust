use crate::connections::{ConnectionPool, ConnectionReceiver, ConnectionSender};
use crate::router::{Router, RouterEvent, SwimRouter};
use common::connections::error::{ConnectionError, ConnectionErrorKind};
use common::connections::WsMessage;
use common::model::Value;
use common::request::request_future::RequestError;
use common::sink::item::ItemSink;
use common::warp::envelope::Envelope;
use common::warp::path::AbsolutePath;
use futures::future::{ready, Ready};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;

async fn get_message(
    pool_handlers: &mut HashMap<url::Url, PoolHandler>,
    host_url: &url::Url,
) -> Option<WsMessage> {
    if let Some((_, receiver)) = pool_handlers.get_mut(host_url) {
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

async fn send_message(
    pool_handlers: &mut HashMap<url::Url, PoolHandler>,
    host_url: &url::Url,
    message: &str,
) {
    let message = message.into();

    if let Some((sender, _)) = pool_handlers.get_mut(host_url) {
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

async fn open_connection(
    router: &mut SwimRouter<TestPool>,
    url: &url::Url,
    node: &str,
    lane: &str,
) -> (
    <SwimRouter<TestPool> as Router>::ConnectionSink,
    <SwimRouter<TestPool> as Router>::ConnectionStream,
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

    let (pool, _) = TestPool::new();
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

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut sink, _) = open_connection(&mut router, &url, "foo", "bar").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = sink.send_item(envelope).await.unwrap();

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(1).collect().await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 1);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);
    assert_eq!(pool.connections.lock().unwrap().len(), 1);
    assert_eq!(
        get_message(&mut pool_handlers, &url).await.unwrap(),
        "@sync(node:foo,lane:bar)".into()
    );
}

#[tokio::test]
async fn test_route_single_outgoing_message_to_multiple_downlinks_same_host() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, _) = open_connection(&mut router, &url, "foo_node", "foo_lane").await;
    let (mut second_sink, _) = open_connection(&mut router, &url, "foo_node", "foo_lane").await;

    let env = Envelope::make_event(
        String::from("oof"),
        String::from("rab"),
        Some(Value::text("bye")),
    );

    let _ = first_sink.send_item(env.clone()).await.unwrap();
    let _ = second_sink.send_item(env).await.unwrap();

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(1).collect().await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 2);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 2);

    assert_eq!(get_requests(&pool), expected_requests);
    assert_eq!(pool.connections.lock().unwrap().len(), 1);
    assert_eq!(
        get_message(&mut pool_handlers, &url).await.unwrap(),
        "@event(node:oof,lane:rab){bye}".into()
    );
    assert_eq!(
        get_message(&mut pool_handlers, &url).await.unwrap(),
        "@event(node:oof,lane:rab){bye}".into()
    );
}

#[tokio::test]
async fn test_route_single_outgoing_message_to_multiple_downlinks_different_hosts() {
    let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, _) =
        open_connection(&mut router, &first_url, "first_foo", "first_bar").await;
    let (mut second_sink, _) =
        open_connection(&mut router, &second_url, "second_foo", "second_bar").await;

    let env = Envelope::make_event(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("hello")),
    );

    let _ = first_sink.send_item(env.clone()).await.unwrap();
    let _ = second_sink.send_item(env).await.unwrap();

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(2).collect().await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 2);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((first_url.clone(), false), 1);
    expected_requests.insert((second_url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);
    assert_eq!(pool.connections.lock().unwrap().len(), 2);
    assert_eq!(
        get_message(&mut pool_handlers, &first_url).await.unwrap(),
        "@event(node:foo,lane:bar){hello}".into()
    );
    assert_eq!(
        get_message(&mut pool_handlers, &second_url).await.unwrap(),
        "@event(node:foo,lane:bar){hello}".into()
    );
}

#[tokio::test]
async fn test_route_multiple_outgoing_messages_to_single_downlink() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut sink, _) = open_connection(&mut router, &url, "foo", "bar").await;

    let first_env = Envelope::make_event(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("First_Downlink")),
    );
    let _ = sink.send_item(first_env).await.unwrap();

    let second_env = Envelope::make_event(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("Second_Downlink")),
    );
    let _ = sink.send_item(second_env).await.unwrap();

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(1).collect().await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 2);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 2);

    assert_eq!(get_requests(&pool), expected_requests);
    assert_eq!(pool.connections.lock().unwrap().len(), 1);

    assert_eq!(
        get_message(&mut pool_handlers, &url).await.unwrap(),
        "@event(node:foo,lane:bar){First_Downlink}".into()
    );
    assert_eq!(
        get_message(&mut pool_handlers, &url).await.unwrap(),
        "@event(node:foo,lane:bar){Second_Downlink}".into()
    );
}

#[tokio::test]
async fn test_route_multiple_outgoing_messages_to_multiple_downlinks_same_host() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, _) = open_connection(&mut router, &url, "foo_node", "foo_lane").await;
    let (mut second_sink, _) = open_connection(&mut router, &url, "foo_node", "foo_lane").await;

    let first_env = Envelope::make_event(
        String::from("first_foo"),
        String::from("first_bar"),
        Some(Value::text("first_body")),
    );

    let second_env = Envelope::make_event(
        String::from("second_foo"),
        String::from("second_bar"),
        Some(Value::text("second_body")),
    );

    let third_env = Envelope::make_event(
        String::from("third_foo"),
        String::from("third_bar"),
        Some(Value::text("third_body")),
    );

    let _ = first_sink.send_item(first_env).await.unwrap();
    let _ = first_sink.send_item(second_env).await.unwrap();
    let _ = second_sink.send_item(third_env).await.unwrap();

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(1).collect().await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 3);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 3);

    assert_eq!(get_requests(&pool), expected_requests);

    assert_eq!(pool.connections.lock().unwrap().len(), 1);
    assert_eq!(
        get_message(&mut pool_handlers, &url).await.unwrap(),
        "@event(node:first_foo,lane:first_bar){first_body}".into()
    );
    assert_eq!(
        get_message(&mut pool_handlers, &url).await.unwrap(),
        "@event(node:second_foo,lane:second_bar){second_body}".into()
    );
    assert_eq!(
        get_message(&mut pool_handlers, &url).await.unwrap(),
        "@event(node:third_foo,lane:third_bar){third_body}".into()
    );
}

#[tokio::test]
async fn test_route_multiple_outgoing_messages_to_multiple_downlinks_different_hosts() {
    let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, _) =
        open_connection(&mut router, &first_url, "foo_node", "foo_lane").await;
    let (mut second_sink, _) =
        open_connection(&mut router, &second_url, "foo_node", "foo_lane").await;

    let first_env = Envelope::make_event(
        String::from("first_foo"),
        String::from("first_bar"),
        Some(Value::text("first_body")),
    );

    let second_env = Envelope::make_event(
        String::from("second_foo"),
        String::from("second_bar"),
        Some(Value::text("second_body")),
    );

    let third_env = Envelope::make_event(
        String::from("third_foo"),
        String::from("third_bar"),
        Some(Value::text("third_body")),
    );

    let _ = first_sink.send_item(first_env).await.unwrap();
    let _ = first_sink.send_item(second_env).await.unwrap();
    let _ = second_sink.send_item(third_env).await.unwrap();

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(2).collect().await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 3);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((first_url.clone(), false), 2);
    expected_requests.insert((second_url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);

    assert_eq!(pool.connections.lock().unwrap().len(), 2);
    assert_eq!(
        get_message(&mut pool_handlers, &first_url).await.unwrap(),
        "@event(node:first_foo,lane:first_bar){first_body}".into()
    );
    assert_eq!(
        get_message(&mut pool_handlers, &first_url).await.unwrap(),
        "@event(node:second_foo,lane:second_bar){second_body}".into()
    );
    assert_eq!(
        get_message(&mut pool_handlers, &second_url).await.unwrap(),
        "@event(node:third_foo,lane:third_bar){third_body}".into()
    );
}

#[tokio::test]
async fn test_route_single_incoming_message_to_single_downlink() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut sink, mut stream) = open_connection(&mut router, &url, "foo", "bar").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));

    let _ = sink.send_item(envelope.clone()).await.unwrap();

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(1).collect().await;

    send_message(&mut pool_handlers, &url, "@event(node:foo,lane:bar){Hello}").await;

    let expected_env = Envelope::make_event(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("Hello")),
    );

    assert_eq!(
        stream.recv().await.unwrap(),
        RouterEvent::Message(expected_env.into_incoming().unwrap())
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

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) = open_connection(&mut router, &url, "foo", "bar").await;
    let (_, mut second_stream) = open_connection(&mut router, &url, "foo", "bar").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = first_sink.send_item(envelope).await.unwrap();

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(1).collect().await;

    send_message(
        &mut pool_handlers,
        &url,
        "@event(node:foo,lane:bar){Goodbye}",
    )
    .await;

    let expected_env = Envelope::make_event(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("Goodbye")),
    );

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::Message(expected_env.clone().into_incoming().unwrap())
    );

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Message(expected_env.into_incoming().unwrap())
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

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) = open_connection(&mut router, &url, "oof", "rab").await;
    let (_, mut second_stream) = open_connection(&mut router, &url, "foo", "bar").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = first_sink.send_item(envelope.clone()).await.unwrap();

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(1).collect().await;

    send_message(&mut pool_handlers, &url, "@event(node:foo,lane:bar){tseT}").await;

    let expected_env = Envelope::make_event(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("tseT")),
    );

    assert!(timeout(Duration::from_secs(1), first_stream.recv())
        .await
        .is_err());

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Message(expected_env.into_incoming().unwrap())
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

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) =
        open_connection(&mut router, &first_url, "foo", "bar").await;
    let (mut second_sink, mut second_stream) =
        open_connection(&mut router, &second_url, "foo", "bar").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = first_sink.send_item(envelope.clone()).await.unwrap();
    let _ = second_sink.send_item(envelope).await.unwrap();

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(2).collect().await;

    send_message(
        &mut pool_handlers,
        &first_url,
        "@event(node:foo,lane:bar){\"First Hello\"}",
    )
    .await;

    send_message(
        &mut pool_handlers,
        &second_url,
        "@event(node:foo,lane:bar){\"Second Hello\"}",
    )
    .await;

    let first_env = Envelope::make_event(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("First Hello")),
    );

    let second_env = Envelope::make_event(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("Second Hello")),
    );

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::Message(first_env.into_incoming().unwrap())
    );

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Message(second_env.into_incoming().unwrap())
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

    let (pool, pool_handlers_rx) = TestPool::new();
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

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(3).collect().await;

    send_message(
        &mut pool_handlers,
        &first_url,
        "@event(node:foo,lane:bar){\"Hello First\"}",
    )
    .await;

    send_message(
        &mut pool_handlers,
        &second_url,
        "@event(node:oof,lane:rab){\"Hello Second\"}",
    )
    .await;

    send_message(
        &mut pool_handlers,
        &third_url,
        "@event(node:ofo,lane:abr){\"Hello Third\"}",
    )
    .await;

    let first_env = Envelope::make_event(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("Hello First")),
    );

    let second_env = Envelope::make_event(
        String::from("oof"),
        String::from("rab"),
        Some(Value::text("Hello Second")),
    );

    let third_env = Envelope::make_event(
        String::from("ofo"),
        String::from("abr"),
        Some(Value::text("Hello Third")),
    );

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::Message(first_env.into_incoming().unwrap())
    );

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Message(second_env.into_incoming().unwrap())
    );

    assert_eq!(
        third_stream.recv().await.unwrap(),
        RouterEvent::Message(third_env.into_incoming().unwrap())
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
async fn test_route_multiple_incoming_messages_to_single_downlink() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut sink, mut stream) = open_connection(&mut router, &url, "foo", "bar").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = sink.send_item(envelope.clone()).await.unwrap();

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(1).collect().await;

    send_message(
        &mut pool_handlers,
        &url,
        "@event(node:foo,lane:bar){\"First!\"}",
    )
    .await;
    send_message(
        &mut pool_handlers,
        &url,
        "@event(node:foo,lane:bar){\"Second!\"}",
    )
    .await;

    let first_env = Envelope::make_event(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("First!")),
    );

    let second_env = Envelope::make_event(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("Second!")),
    );

    assert_eq!(
        stream.recv().await.unwrap(),
        RouterEvent::Message(first_env.into_incoming().unwrap())
    );

    assert_eq!(
        stream.recv().await.unwrap(),
        RouterEvent::Message(second_env.into_incoming().unwrap())
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

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) =
        open_connection(&mut router, &url, "room", "five").await;
    let (_, mut second_stream) = open_connection(&mut router, &url, "room", "five").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = first_sink.send_item(envelope).await.unwrap();

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(1).collect().await;

    send_message(
        &mut pool_handlers,
        &url,
        "@event(node:room,lane:five){\"John Doe\"}",
    )
    .await;
    send_message(
        &mut pool_handlers,
        &url,
        "@event(node:room,lane:five){\"Jane Doe\"}",
    )
    .await;

    let first_env = Envelope::make_event(
        String::from("room"),
        String::from("five"),
        Some(Value::text("John Doe")),
    );

    let second_env = Envelope::make_event(
        String::from("room"),
        String::from("five"),
        Some(Value::text("Jane Doe")),
    );

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::Message(first_env.clone().into_incoming().unwrap())
    );

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::Message(second_env.clone().into_incoming().unwrap())
    );

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Message(first_env.into_incoming().unwrap())
    );

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Message(second_env.into_incoming().unwrap())
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

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) =
        open_connection(&mut router, &url, "room", "five").await;
    let (_, mut second_stream) = open_connection(&mut router, &url, "room", "six").await;

    let envelope = Envelope::sync(String::from("room"), String::from("seven"));
    let _ = first_sink.send_item(envelope.clone()).await.unwrap();

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(1).collect().await;

    send_message(
        &mut pool_handlers,
        &url,
        "@event(node:room,lane:five){\"John Doe\"}",
    )
    .await;

    send_message(
        &mut pool_handlers,
        &url,
        "@event(node:room,lane:six){\"Jane Doe\"}",
    )
    .await;

    let first_env = Envelope::make_event(
        String::from("room"),
        String::from("five"),
        Some(Value::text("John Doe")),
    );

    let second_env = Envelope::make_event(
        String::from("room"),
        String::from("six"),
        Some(Value::text("Jane Doe")),
    );

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::Message(first_env.into_incoming().unwrap())
    );

    assert!(timeout(Duration::from_secs(1), first_stream.recv())
        .await
        .is_err());

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Message(second_env.into_incoming().unwrap())
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

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) =
        open_connection(&mut router, &first_url, "building", "1").await;

    let (mut second_sink, mut second_stream) =
        open_connection(&mut router, &second_url, "building", "1").await;

    let envelope = Envelope::sync(String::from("building"), String::from("1"));
    let _ = first_sink.send_item(envelope.clone()).await.unwrap();
    let _ = second_sink.send_item(envelope).await.unwrap();

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(2).collect().await;

    send_message(
        &mut pool_handlers,
        &first_url,
        "@event(node:building,lane:\"1\"){\"Room 101\"}",
    )
    .await;

    send_message(
        &mut pool_handlers,
        &first_url,
        "@event(node:building,lane:\"1\"){\"Room 102\"}",
    )
    .await;

    send_message(
        &mut pool_handlers,
        &second_url,
        "@event(node:building,lane:\"1\"){\"Room 201\"}",
    )
    .await;

    send_message(
        &mut pool_handlers,
        &second_url,
        "@event(node:building,lane:\"1\"){\"Room 202\"}",
    )
    .await;

    let env_101 = Envelope::make_event(
        String::from("building"),
        String::from("1"),
        Some(Value::text("Room 101")),
    );

    let env_102 = Envelope::make_event(
        String::from("building"),
        String::from("1"),
        Some(Value::text("Room 102")),
    );

    let env_201 = Envelope::make_event(
        String::from("building"),
        String::from("1"),
        Some(Value::text("Room 201")),
    );

    let env_202 = Envelope::make_event(
        String::from("building"),
        String::from("1"),
        Some(Value::text("Room 202")),
    );

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::Message(env_101.into_incoming().unwrap())
    );

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::Message(env_102.into_incoming().unwrap())
    );

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Message(env_201.into_incoming().unwrap())
    );

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Message(env_202.into_incoming().unwrap())
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

    let (pool, pool_handlers_rx) = TestPool::new();
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

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(3).collect().await;

    send_message(
        &mut pool_handlers,
        &first_url,
        "@event(node:building,lane:\"1\"){\"Building 101\"}",
    )
    .await;

    send_message(
        &mut pool_handlers,
        &first_url,
        "@event(node:building,lane:\"1\"){\"Building 102\"}",
    )
    .await;

    send_message(
        &mut pool_handlers,
        &second_url,
        "@event(node:room,lane:\"2\"){\"Room 201\"}",
    )
    .await;

    send_message(
        &mut pool_handlers,
        &second_url,
        "@event(node:room,lane:\"2\"){\"Room 202\"}",
    )
    .await;

    send_message(
        &mut pool_handlers,
        &second_url,
        "@event(node:room,lane:\"2\"){\"Room 203\"}",
    )
    .await;

    send_message(
        &mut pool_handlers,
        &third_url,
        "@event(node:building,lane:\"3\"){\"Building 301\"}",
    )
    .await;

    send_message(
        &mut pool_handlers,
        &third_url,
        "@event(node:building,lane:\"3\"){\"Building 302\"}",
    )
    .await;

    let building_101 = Envelope::make_event(
        String::from("building"),
        String::from("1"),
        Some(Value::text("Building 101")),
    );

    let building_102 = Envelope::make_event(
        String::from("building"),
        String::from("1"),
        Some(Value::text("Building 102")),
    );

    let room_201 = Envelope::make_event(
        String::from("room"),
        String::from("2"),
        Some(Value::text("Room 201")),
    );

    let room_202 = Envelope::make_event(
        String::from("room"),
        String::from("2"),
        Some(Value::text("Room 202")),
    );

    let room_203 = Envelope::make_event(
        String::from("room"),
        String::from("2"),
        Some(Value::text("Room 203")),
    );

    let building_301 = Envelope::make_event(
        String::from("building"),
        String::from("3"),
        Some(Value::text("Building 301")),
    );

    let building_302 = Envelope::make_event(
        String::from("building"),
        String::from("3"),
        Some(Value::text("Building 302")),
    );

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::Message(building_101.into_incoming().unwrap())
    );

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::Message(building_102.into_incoming().unwrap())
    );

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Message(room_201.into_incoming().unwrap())
    );

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Message(room_202.into_incoming().unwrap())
    );

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Message(room_203.into_incoming().unwrap())
    );

    assert_eq!(
        third_stream.recv().await.unwrap(),
        RouterEvent::Message(building_301.into_incoming().unwrap())
    );

    assert_eq!(
        third_stream.recv().await.unwrap(),
        RouterEvent::Message(building_302.into_incoming().unwrap())
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
async fn test_route_incoming_unsopported_message() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut sink, mut stream) = open_connection(&mut router, &url, "foo", "bar").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = sink.send_item(envelope.clone()).await.unwrap();

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(1).collect().await;

    send_message(&mut pool_handlers, &url, "@auth()").await;

    assert!(timeout(Duration::from_secs(1), stream.recv())
        .await
        .is_err());

    send_message(&mut pool_handlers, &url, "@event(node:foo,lane:bar){Hello}").await;

    let expected_env = Envelope::make_event(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("Hello")),
    );

    assert_eq!(
        stream.recv().await.unwrap(),
        RouterEvent::Message(expected_env.into_incoming().unwrap())
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
async fn test_route_incoming_message_of_no_interest() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut sink, mut stream) = open_connection(&mut router, &url, "foo", "bar").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = sink.send_item(envelope.clone()).await.unwrap();

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(1).collect().await;

    send_message(
        &mut pool_handlers,
        &url,
        "@event(node:building,lane:swim){Second}",
    )
    .await;

    assert!(timeout(Duration::from_secs(1), stream.recv())
        .await
        .is_err());

    send_message(&mut pool_handlers, &url, "@event(node:foo,lane:bar){Hello}").await;

    let expected_env = Envelope::make_event(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("Hello")),
    );

    assert_eq!(
        stream.recv().await.unwrap(),
        RouterEvent::Message(expected_env.into_incoming().unwrap())
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
async fn test_single_direct_message_existing_connection() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut sink, _) = open_connection(&mut router, &url, "foo", "bar").await;

    let sync_env = Envelope::sync(String::from("room"), String::from("seven"));
    let _ = sink.send_item(sync_env).await.unwrap();

    let command_env = Envelope::make_event(
        String::from("room"),
        String::from("seven"),
        Some(Value::text("Test Command")),
    );

    let mut general_sink = router.general_sink();

    assert!(general_sink
        .send_item((url.clone(), command_env))
        .await
        .is_ok());

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(1).collect().await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 2);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 2);

    assert_eq!(get_requests(&pool), expected_requests);

    assert_eq!(
        get_message(&mut pool_handlers, &url).await.unwrap(),
        "@sync(node:room,lane:seven)".into()
    );

    assert_eq!(
        get_message(&mut pool_handlers, &url).await.unwrap(),
        "@event(node:room,lane:seven){\"Test Command\"}".into()
    );
}

#[tokio::test]
async fn test_single_direct_message_new_connection() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let command_env = Envelope::make_event(
        String::from("room"),
        String::from("seven"),
        Some(Value::text("Test Command")),
    );

    let mut general_sink = router.general_sink();

    assert!(general_sink
        .send_item((url.clone(), command_env))
        .await
        .is_ok());

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(1).collect().await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 1);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);
    assert_eq!(
        get_message(&mut pool_handlers, &url).await.unwrap(),
        "@event(node:room,lane:seven){\"Test Command\"}".into()
    );
}

#[tokio::test]
async fn test_multiple_direct_messages_existing_connection() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut sink, _) = open_connection(&mut router, &url, "building", "swim").await;

    let sync_env = Envelope::sync(String::from("building"), String::from("swim"));
    let _ = sink.send_item(sync_env).await.unwrap();

    let first_env = Envelope::make_event(
        String::from("building"),
        String::from("swim"),
        Some(Value::text("First")),
    );

    let second_env = Envelope::make_event(
        String::from("building"),
        String::from("swim"),
        Some(Value::text("Second")),
    );

    let mut general_sink = router.general_sink();

    assert!(general_sink
        .send_item((url.clone(), first_env))
        .await
        .is_ok());

    assert!(general_sink
        .send_item((url.clone(), second_env))
        .await
        .is_ok());

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(1).collect().await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 3);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 3);

    assert_eq!(get_requests(&pool), expected_requests);

    assert_eq!(
        get_message(&mut pool_handlers, &url).await.unwrap(),
        "@sync(node:building,lane:swim)".into()
    );

    assert_eq!(
        get_message(&mut pool_handlers, &url).await.unwrap(),
        "@event(node:building,lane:swim){First}".into()
    );

    assert_eq!(
        get_message(&mut pool_handlers, &url).await.unwrap(),
        "@event(node:building,lane:swim){Second}".into()
    );
}

#[tokio::test]
async fn test_multiple_direct_messages_new_connection() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let first_env = Envelope::make_event(
        String::from("building"),
        String::from("swim"),
        Some(Value::text("First")),
    );

    let second_env = Envelope::make_event(
        String::from("building"),
        String::from("swim"),
        Some(Value::text("Second")),
    );

    let third_env = Envelope::make_event(
        String::from("building"),
        String::from("swim"),
        Some(Value::text("Third")),
    );

    let mut general_sink = router.general_sink();

    assert!(general_sink
        .send_item((url.clone(), first_env))
        .await
        .is_ok());

    assert!(general_sink
        .send_item((url.clone(), second_env))
        .await
        .is_ok());

    assert!(general_sink
        .send_item((url.clone(), third_env))
        .await
        .is_ok());

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(1).collect().await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 3);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 3);

    assert_eq!(get_requests(&pool), expected_requests);

    assert_eq!(
        get_message(&mut pool_handlers, &url).await.unwrap(),
        "@event(node:building,lane:swim){First}".into()
    );

    assert_eq!(
        get_message(&mut pool_handlers, &url).await.unwrap(),
        "@event(node:building,lane:swim){Second}".into()
    );

    assert_eq!(
        get_message(&mut pool_handlers, &url).await.unwrap(),
        "@event(node:building,lane:swim){Third}".into()
    );
}

#[tokio::test]
async fn test_multiple_direct_messages_different_connections() {
    let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let first_env = Envelope::make_event(
        String::from("building"),
        String::from("swim"),
        Some(Value::text("First")),
    );

    let second_env = Envelope::make_event(
        String::from("building"),
        String::from("swim"),
        Some(Value::text("Second")),
    );

    let third_env = Envelope::make_event(
        String::from("building"),
        String::from("swim"),
        Some(Value::text("Third")),
    );

    let mut general_sink = router.general_sink();

    assert!(general_sink
        .send_item((first_url.clone(), first_env))
        .await
        .is_ok());

    assert!(general_sink
        .send_item((second_url.clone(), second_env))
        .await
        .is_ok());

    assert!(general_sink
        .send_item((first_url.clone(), third_env))
        .await
        .is_ok());

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(2).collect().await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 3);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((first_url.clone(), false), 2);
    expected_requests.insert((second_url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);

    assert_eq!(
        get_message(&mut pool_handlers, &first_url).await.unwrap(),
        "@event(node:building,lane:swim){First}".into()
    );

    assert_eq!(
        get_message(&mut pool_handlers, &second_url).await.unwrap(),
        "@event(node:building,lane:swim){Second}".into()
    );

    assert_eq!(
        get_message(&mut pool_handlers, &&first_url).await.unwrap(),
        "@event(node:building,lane:swim){Third}".into()
    );
}

#[tokio::test]
async fn test_router_close_ok() {
    let (pool, _) = TestPool::new();
    let router = SwimRouter::new(Default::default(), pool.clone());

    assert!(router.close().await.is_ok());
}

#[tokio::test]
async fn test_router_close_error() {
    let (pool, _) = TestPool::new();
    let router = SwimRouter::new(Default::default(), pool.clone());

    let SwimRouter {
        router_connection_request_tx,
        router_sink_tx,
        task_manager_handle,
        connection_pool,
        close_tx: _,
        configuration,
    } = router;

    let (close_tx, close_rx) = watch::channel(None);

    drop(close_rx);

    let router = SwimRouter {
        router_connection_request_tx,
        router_sink_tx,
        task_manager_handle,
        connection_pool,
        close_tx,
        configuration,
    };

    assert!(router.close().await.is_err());
}

#[tokio::test]
async fn test_route_incoming_parse_message_error() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut sink, mut stream) = open_connection(&mut router, &url, "foo", "bar").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = sink.send_item(envelope.clone()).await.unwrap();

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(1).collect().await;

    send_message(&mut pool_handlers, &url, "|").await;

    assert!(timeout(Duration::from_secs(1), stream.recv())
        .await
        .is_err());

    send_message(&mut pool_handlers, &url, "@event(node:foo,lane:bar){Hello}").await;

    let expected_env = Envelope::make_event(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("Hello")),
    );

    assert_eq!(
        stream.recv().await.unwrap(),
        RouterEvent::Message(expected_env.into_incoming().unwrap())
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
async fn test_route_incoming_parse_envelope_error() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut sink, mut stream) = open_connection(&mut router, &url, "foo", "bar").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = sink.send_item(envelope.clone()).await.unwrap();

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(1).collect().await;

    send_message(&mut pool_handlers, &url, "@invalid(node:oof,lane:rab){bye}").await;

    assert!(timeout(Duration::from_secs(1), stream.recv())
        .await
        .is_err());

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 1);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);
    assert_eq!(pool.connections.lock().unwrap().len(), 1);

    assert_eq!(stream.recv().await.unwrap(), RouterEvent::Stopping);
}

#[tokio::test]
async fn test_route_incoming_unreachable_host() {
    let url = url::Url::parse("ws://unreachable/").unwrap();

    let (pool, _) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut sink, mut stream) = open_connection(&mut router, &url, "foo", "bar").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = sink.send_item(envelope.clone()).await.unwrap();

    assert_eq!(
        stream.recv().await.unwrap(),
        RouterEvent::Unreachable("An error was produced by the web socket.".to_string())
    );

    assert_eq!(get_request_count(&pool), 1);
    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 1);
    assert_eq!(get_requests(&pool), expected_requests);
}

#[tokio::test]
async fn test_route_incoming_connection_closed_single() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut sink, mut stream) = open_connection(&mut router, &url, "foo", "bar").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = sink.send_item(envelope.clone()).await.unwrap();

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(1).collect().await;

    drop(pool_handlers.remove(&url).unwrap());

    assert_eq!(stream.recv().await.unwrap(), RouterEvent::ConnectionClosed);

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 1);

    let mut expected_requests = HashMap::new();
    expected_requests.insert((url.clone(), false), 1);

    assert_eq!(get_requests(&pool), expected_requests);
    assert_eq!(pool.connections.lock().unwrap().len(), 1);

    assert_eq!(stream.recv().await.unwrap(), RouterEvent::Stopping);
}

#[tokio::test]
async fn test_route_incoming_connection_closed_multiple_same_host() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) = open_connection(&mut router, &url, "foo", "bar").await;
    let (_, mut second_stream) = open_connection(&mut router, &url, "oof", "rab").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = first_sink.send_item(envelope.clone()).await.unwrap();

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(1).collect().await;

    drop(pool_handlers.remove(&url).unwrap());

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::ConnectionClosed
    );
    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::ConnectionClosed
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
async fn test_route_incoming_connection_closed_multiple_different_hosts() {
    let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();

    let (pool, pool_handlers_rx) = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) =
        open_connection(&mut router, &first_url, "foo", "bar").await;
    let (mut second_sink, mut second_stream) =
        open_connection(&mut router, &second_url, "foo", "bar").await;

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = first_sink.send_item(envelope.clone()).await.unwrap();
    let _ = second_sink.send_item(envelope.clone()).await.unwrap();

    let mut pool_handlers: HashMap<_, _> = pool_handlers_rx.take(2).collect().await;

    drop(pool_handlers.remove(&first_url).unwrap());

    assert_eq!(
        first_stream.recv().await.unwrap(),
        RouterEvent::ConnectionClosed
    );

    assert!(timeout(Duration::from_secs(1), second_stream.recv())
        .await
        .is_err());

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

type PoolHandler = (mpsc::Sender<WsMessage>, mpsc::Receiver<WsMessage>);

#[derive(Clone)]
struct TestPool {
    connection_handlers_tx: mpsc::Sender<(url::Url, PoolHandler)>,
    connection_requests: Arc<Mutex<HashMap<(url::Url, bool), usize>>>,
    connections: Arc<Mutex<HashMap<url::Url, mpsc::Sender<WsMessage>>>>,
    permanent_error_url: url::Url,
}

impl TestPool {
    fn new() -> (Self, mpsc::Receiver<(url::Url, PoolHandler)>) {
        let (connection_handlers_tx, connection_handlers_rx) = mpsc::channel(5);

        (
            TestPool {
                connection_handlers_tx,
                connection_requests: Arc::new(Mutex::new(HashMap::new())),
                connections: Arc::new(Mutex::new(HashMap::new())),
                permanent_error_url: url::Url::parse("ws://unreachable/").unwrap(),
            },
            connection_handlers_rx,
        )
    }

    fn create_connection(
        &mut self,
        host_url: url::Url,
        recreate: bool,
    ) -> (mpsc::Sender<WsMessage>, mpsc::Receiver<WsMessage>) {
        let (sender_tx, sender_rx) = mpsc::channel(5);
        let (receiver_tx, receiver_rx) = mpsc::channel(5);

        self.connections
            .lock()
            .unwrap()
            .insert(host_url.clone(), sender_tx.clone());

        self.connection_handlers_tx
            .try_send((host_url.clone(), (receiver_tx, sender_rx)))
            .unwrap();

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
        if host_url == self.permanent_error_url {
            self.log_request(host_url, recreate);
            return ready(Ok(Err(ConnectionError::new(
                ConnectionErrorKind::SocketError,
            ))));
        }

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
