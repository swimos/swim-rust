use crate::connections::{ConnectionError, ConnectionPool, ConnectionReceiver, ConnectionSender};
use crate::router::{Router, SwimRouter};
use common::request::request_future::RequestError;
use common::sink::item::ItemSink;
use common::warp::envelope::Envelope;
use common::warp::path::AbsolutePath;
use futures::future::{ready, Ready};
use std::collections::HashMap;
use std::sync::Once;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::sync::oneshot::Receiver;
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

//Todo change this to use channels

#[tokio::test(core_threads = 3)]
async fn connect_no_send() {
    init_trace();
    // let (pool, handler) = TestPool::new();
    let url = url::Url::parse("ws://127.0.0.2/").unwrap();
    let node = "foo";
    let lane = "bar";

    let pool = TestPool::new();
    let mut router = SwimRouter::new(Default::default(), pool.clone());

    let (mut sink, stream) = router
        .connection_for(&AbsolutePath::new(url.clone(), node, lane))
        .await
        .unwrap();

    let sync = Envelope::sync(String::from("foo"), String::from("bar"));
    let _ = sink.send_item(sync).await;

    loop {
        assert_eq!(pool.connection_requests.lock().unwrap().len(), 0);
    }
}

type ConnHandler = (mpsc::Sender<Message>, mpsc::Receiver<Message>);

#[derive(Clone)]
struct TestPool {
    connection_requests: Arc<Mutex<Vec<(url::Url, bool)>>>,
    connections: Arc<Mutex<HashMap<url::Url, (mpsc::Sender<Message>, ConnHandler)>>>,
}

impl TestPool {
    fn new() -> Self {
        TestPool {
            connection_requests: Arc::new(Mutex::new(Vec::new())),
            connections: Arc::new(Mutex::new(HashMap::new())),
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

    fn request_connection(&mut self, host_url: url::Url, recreate: bool) -> Self::ConnFut {
        self.connection_requests
            .lock()
            .unwrap()
            .push((host_url.clone(), recreate.clone()));

        if recreate {
            let (sender_tx, sender_rx) = mpsc::channel(5);
            let (receiver_tx, receiver_rx) = mpsc::channel(5);

            self.connections.lock().unwrap().insert(
                host_url.clone(),
                (sender_tx.clone(), (receiver_tx, sender_rx)),
            );

            ready(Ok(Ok((
                ConnectionSender { tx: sender_tx },
                Some(receiver_rx),
            ))))
        } else {
            if self.connections.lock().unwrap().get(&host_url).is_none() {
                let (sender_tx, sender_rx) = mpsc::channel(5);
                let (receiver_tx, receiver_rx) = mpsc::channel(5);

                self.connections.lock().unwrap().insert(
                    host_url.clone(),
                    (sender_tx.clone(), (receiver_tx, sender_rx)),
                );

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
                    .0
                    .clone();

                ready(Ok(Ok((ConnectionSender { tx: sender_tx }, None))))
            }
        }
    }

    fn close(self) -> Result<Self::CloseFut, ConnectionError> {
        Ok(ready(Ok(Ok(()))))
    }
}
