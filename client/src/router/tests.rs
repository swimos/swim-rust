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
use tokio::time::{delay_for, timeout};
use tokio_tungstenite::tungstenite::protocol::Message;

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

async fn wait_for_n_requests(pool: &TestPool, n: usize) {
    while get_request_count(pool) != n {
        delay_for(Duration::from_millis(10)).await;
    }
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

#[tokio::test]
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

    wait_for_n_requests(&pool, 1).await;

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

#[tokio::test]
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

    wait_for_n_requests(&pool, 2).await;

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

#[tokio::test]
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
        "@command(node:foo,lane:bar){hello}"
    );
    assert_eq!(
        get_message(&pool, &second_url).await.unwrap(),
        "@command(node:foo,lane:bar){hello}"
    );
}

#[tokio::test]
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

    wait_for_n_requests(&pool, 2).await;

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

#[tokio::test]
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

    wait_for_n_requests(&pool, 3).await;

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

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
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

    wait_for_n_requests(&pool, 1).await;

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

#[tokio::test]
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
    let _ = first_sink.send_item(envelope).await.unwrap();

    wait_for_n_requests(&pool, 1).await;

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

#[tokio::test]
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

    wait_for_n_requests(&pool, 1).await;

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

#[tokio::test]
async fn test_route_single_incoming_message_to_multiple_downlinks_different_hosts_same_path() {
    let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let first_node = "foo";
    let first_lane = "bar";

    let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();
    let second_node = "foo";
    let second_lane = "bar";

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) = router
        .connection_for(&AbsolutePath::new(
            first_url.clone(),
            first_node,
            first_lane,
        ))
        .await
        .unwrap();

    let (mut second_sink, mut second_stream) = router
        .connection_for(&AbsolutePath::new(
            second_url.clone(),
            second_node,
            second_lane,
        ))
        .await
        .unwrap();

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = first_sink.send_item(envelope.clone()).await.unwrap();
    let _ = second_sink.send_item(envelope).await.unwrap();

    wait_for_n_requests(&pool, 2).await;

    send_message(
        &pool,
        &first_url,
        Message::Text("@command(node:foo,lane:bar){\"First Hello\"}".to_string()),
    )
    .await;

    send_message(
        &pool,
        &second_url,
        Message::Text("@command(node:foo,lane:bar){\"Second Hello\"}".to_string()),
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
    let first_node = "foo";
    let first_lane = "bar";

    let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();
    let second_node = "oof";
    let second_lane = "rab";

    let third_url = url::Url::parse("ws://127.0.0.3/").unwrap();
    let third_node = "ofo";
    let third_lane = "abr";

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) = router
        .connection_for(&AbsolutePath::new(
            first_url.clone(),
            first_node,
            first_lane,
        ))
        .await
        .unwrap();

    let (mut second_sink, mut second_stream) = router
        .connection_for(&AbsolutePath::new(
            second_url.clone(),
            second_node,
            second_lane,
        ))
        .await
        .unwrap();

    let (mut third_sink, mut third_stream) = router
        .connection_for(&AbsolutePath::new(
            third_url.clone(),
            third_node,
            third_lane,
        ))
        .await
        .unwrap();

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = first_sink.send_item(envelope.clone()).await.unwrap();
    let _ = second_sink.send_item(envelope.clone()).await.unwrap();
    let _ = third_sink.send_item(envelope).await.unwrap();

    wait_for_n_requests(&pool, 3).await;

    send_message(
        &pool,
        &first_url,
        Message::Text("@command(node:foo,lane:bar){\"Hello First\"}".to_string()),
    )
    .await;

    send_message(
        &pool,
        &second_url,
        Message::Text("@command(node:oof,lane:rab){\"Hello Second\"}".to_string()),
    )
    .await;

    send_message(
        &pool,
        &third_url,
        Message::Text("@command(node:ofo,lane:abr){\"Hello Third\"}".to_string()),
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
        Message::Text("@command(node:foo,lane:bar){First}".to_string()),
    )
    .await;

    send_message(
        &pool,
        &url,
        Message::Text("@command(node:foo,lane:bar){Second}".to_string()),
    )
    .await;

    assert!(router.close().await.is_ok());
    assert_eq!(get_request_count(&pool), 0);
    assert_eq!(pool.connections.lock().unwrap().len(), 0);

    assert_eq!(stream.recv().await.unwrap(), RouterEvent::Stopping);
}

#[tokio::test]
async fn test_route_multiple_incoming_messages_to_single_downlink_after_outgoing() {
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

    wait_for_n_requests(&pool, 1).await;

    send_message(
        &pool,
        &url,
        Message::Text("@command(node:foo,lane:bar){\"First!\"}".to_string()),
    )
    .await;

    send_message(
        &pool,
        &url,
        Message::Text("@command(node:foo,lane:bar){\"Second!\"}".to_string()),
    )
    .await;

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
    let node = "room";
    let lane = "five";

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
    let _ = first_sink.send_item(envelope).await.unwrap();

    wait_for_n_requests(&pool, 1).await;

    send_message(
        &pool,
        &url,
        Message::Text("@command(node:room,lane:five){\"John Doe\"}".to_string()),
    )
    .await;

    send_message(
        &pool,
        &url,
        Message::Text("@command(node:room,lane:five){\"Jane Doe\"}".to_string()),
    )
    .await;

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
    let first_node = "room";
    let first_lane = "five";

    let second_node = "room";
    let second_lane = "six";

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

    let envelope = Envelope::sync(String::from("room"), String::from("seven"));
    let _ = first_sink.send_item(envelope.clone()).await.unwrap();

    wait_for_n_requests(&pool, 1).await;

    send_message(
        &pool,
        &url,
        Message::Text("@command(node:room,lane:five){\"John Doe\"}".to_string()),
    )
    .await;

    send_message(
        &pool,
        &url,
        Message::Text("@command(node:room,lane:six){\"Jane Doe\"}".to_string()),
    )
    .await;

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

    assert!(timeout(Duration::from_millis(10), first_stream.recv())
        .await
        .is_err());

    assert_eq!(
        second_stream.recv().await.unwrap(),
        RouterEvent::Envelope(second_env)
    );

    assert!(timeout(Duration::from_millis(10), second_stream.recv())
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
    let first_node = "building";
    let first_lane = "1";

    let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();
    let second_node = "building";
    let second_lane = "1";

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) = router
        .connection_for(&AbsolutePath::new(
            first_url.clone(),
            first_node,
            first_lane,
        ))
        .await
        .unwrap();

    let (mut second_sink, mut second_stream) = router
        .connection_for(&AbsolutePath::new(
            second_url.clone(),
            second_node,
            second_lane,
        ))
        .await
        .unwrap();

    let envelope = Envelope::sync(String::from("building"), String::from("1"));
    let _ = first_sink.send_item(envelope.clone()).await.unwrap();
    let _ = second_sink.send_item(envelope).await.unwrap();

    wait_for_n_requests(&pool, 2).await;

    send_message(
        &pool,
        &first_url,
        Message::Text("@command(node:building,lane:\"1\"){\"Room 101\"}".to_string()),
    )
    .await;

    send_message(
        &pool,
        &first_url,
        Message::Text("@command(node:building,lane:\"1\"){\"Room 102\"}".to_string()),
    )
    .await;

    send_message(
        &pool,
        &second_url,
        Message::Text("@command(node:building,lane:\"1\"){\"Room 201\"}".to_string()),
    )
    .await;

    send_message(
        &pool,
        &second_url,
        Message::Text("@command(node:building,lane:\"1\"){\"Room 202\"}".to_string()),
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
    let first_node = "building";
    let first_lane = "1";

    let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();
    let second_node = "room";
    let second_lane = "2";

    let third_url = url::Url::parse("ws://127.0.0.3/").unwrap();
    let third_node = "building";
    let third_lane = "3";

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut first_sink, mut first_stream) = router
        .connection_for(&AbsolutePath::new(
            first_url.clone(),
            first_node,
            first_lane,
        ))
        .await
        .unwrap();

    let (mut second_sink, mut second_stream) = router
        .connection_for(&AbsolutePath::new(
            second_url.clone(),
            second_node,
            second_lane,
        ))
        .await
        .unwrap();

    let (mut third_sink, mut third_stream) = router
        .connection_for(&AbsolutePath::new(
            third_url.clone(),
            third_node,
            third_lane,
        ))
        .await
        .unwrap();

    let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = first_sink.send_item(envelope.clone()).await.unwrap();
    let _ = second_sink.send_item(envelope.clone()).await.unwrap();
    let _ = third_sink.send_item(envelope).await.unwrap();

    wait_for_n_requests(&pool, 3).await;

    send_message(
        &pool,
        &first_url,
        Message::Text("@command(node:building,lane:\"1\"){\"Building 101\"}".to_string()),
    )
    .await;

    send_message(
        &pool,
        &first_url,
        Message::Text("@command(node:building,lane:\"1\"){\"Building 102\"}".to_string()),
    )
    .await;

    send_message(
        &pool,
        &second_url,
        Message::Text("@command(node:room,lane:\"2\"){\"Room 201\"}".to_string()),
    )
    .await;

    send_message(
        &pool,
        &second_url,
        Message::Text("@command(node:room,lane:\"2\"){\"Room 202\"}".to_string()),
    )
    .await;

    send_message(
        &pool,
        &second_url,
        Message::Text("@command(node:room,lane:\"2\"){\"Room 203\"}".to_string()),
    )
    .await;

    send_message(
        &pool,
        &third_url,
        Message::Text("@command(node:building,lane:\"3\"){\"Building 301\"}".to_string()),
    )
    .await;

    send_message(
        &pool,
        &third_url,
        Message::Text("@command(node:building,lane:\"3\"){\"Building 302\"}".to_string()),
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

            ready(Ok(Ok((ConnectionSender { tx: sender_tx }, None))))
        } else {
            let (sender_tx, receiver_rx) = self.create_connection(host_url, recreate);

            ready(Ok(Ok((
                ConnectionSender { tx: sender_tx },
                Some(receiver_rx),
            ))))
        }
    }

    fn close(self) -> Result<Self::CloseFut, ConnectionError> {
        Ok(ready(Ok(Ok(()))))
    }
}
