use crate::connections::{ConnectionError, ConnectionPool, ConnectionReceiver, ConnectionSender};
use crate::router::{Router, SwimRouter};
use common::model::Value;
use common::request::request_future::RequestError;
use common::sink::item::ItemSink;
use common::warp::envelope::Envelope;
use common::warp::path::AbsolutePath;
use futures::future::{ready, Ready};
use std::collections::HashMap;
use std::sync::Once;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
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

async fn get_message(conn_pool: &TestPool, host_url: &url::Url) -> String {
    conn_pool
        .handlers
        .lock()
        .unwrap()
        .get_mut(host_url)
        .unwrap()
        .1
        .recv()
        .await
        .unwrap()
        .to_string()
}

fn get_request(conn_pool: &TestPool, index: usize) -> (url::Url, bool) {
    conn_pool
        .connection_requests
        .lock()
        .unwrap()
        .get(index)
        .unwrap()
        .clone()
}

#[tokio::test]
async fn test_create_single_downlink() {
    let url = url::Url::parse("ws://127.0.0.2/").unwrap();
    let node = "foo";
    let lane = "bar";

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let _ = router
        .connection_for(&AbsolutePath::new(url.clone(), node, lane))
        .await
        .unwrap();

    assert!(router.close().await.is_ok());
    assert_eq!(pool.connection_requests.lock().unwrap().len(), 0);
    assert_eq!(pool.connections.lock().unwrap().len(), 0);
}

#[tokio::test]
async fn test_route_single_outgoing_message_to_single_downlink() {
    init_trace();

    let url = url::Url::parse("ws://127.0.0.2/").unwrap();
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

    assert!(router.close().await.is_ok());
    assert_eq!(pool.connection_requests.lock().unwrap().len(), 1);
    assert_eq!(get_request(&pool, 0), (url.clone(), false));
    assert_eq!(pool.connections.lock().unwrap().len(), 1);
    assert_eq!(get_message(&pool, &url).await, "@sync(node:foo,lane:bar)");
}

#[tokio::test(core_threads = 3)]
async fn test_route_single_outgoing_message_to_multiple_downlinks_same_host() {
    init_trace();

    let url = url::Url::parse("ws://127.0.0.2/").unwrap();
    let node = "foo";
    let lane = "bar";

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
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("First_Downlink")),
    );
    let _ = first_sink.send_item(first_env).await.unwrap();

    let second_env = Envelope::command(
        String::from("foo"),
        String::from("bar"),
        Some(Value::text("Second_Downlink")),
    );
    let _ = second_sink.send_item(second_env).await.unwrap();

    while pool.connection_requests.lock().unwrap().len() != 2 {}

    assert!(router.close().await.is_ok());
    assert_eq!(pool.connection_requests.lock().unwrap().len(), 2);
    assert_eq!(get_request(&pool, 0), (url.clone(), false));
    assert_eq!(get_request(&pool, 1), (url.clone(), false));
    assert_eq!(pool.connections.lock().unwrap().len(), 1);
    assert_eq!(
        get_message(&pool, &url).await,
        "@command(node:foo,lane:bar){First_Downlink}"
    );
    assert_eq!(
        get_message(&pool, &url).await,
        "@command(node:foo,lane:bar){Second_Downlink}"
    );
}

type PoolHandler = (mpsc::Sender<Message>, mpsc::Receiver<Message>);

#[derive(Clone)]
struct TestPool {
    connection_requests: Arc<Mutex<Vec<(url::Url, bool)>>>,
    connections: Arc<Mutex<HashMap<url::Url, mpsc::Sender<Message>>>>,
    handlers: Arc<Mutex<HashMap<url::Url, PoolHandler>>>,
}

impl TestPool {
    fn new() -> Self {
        TestPool {
            connection_requests: Arc::new(Mutex::new(Vec::new())),
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
        self.connection_requests
            .lock()
            .unwrap()
            .push((host_url.clone(), recreate.clone()));

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

                ready(Ok(Ok((ConnectionSender { tx: sender_tx }, None))))
            }
        }
    }

    fn close(self) -> Result<Self::CloseFut, ConnectionError> {
        Ok(ready(Ok(Ok(()))))
    }
}
