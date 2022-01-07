// Copyright 2015-2021 Swim Inc.
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

use std::collections::HashMap;
use std::sync::Arc;

use futures::future::{join, BoxFuture};
use futures::FutureExt;
use http::Uri;
use parking_lot::Mutex;
use swim_async_runtime::time::timeout;
use swim_form::Form;
use swim_model::path::RelativePath;
use swim_recon::printer::print_recon_compact;
use swim_utilities::algebra::non_zero_usize;
use swim_utilities::future::retryable::{Quantity, RetryStrategy};
use swim_utilities::routing::uri::{BadRelativeUri, RelativeUri, UriIsAbsolute};
use swim_utilities::trigger;
use swim_utilities::trigger::promise;
use swim_warp::envelope::Envelope;
use tokio::sync::{mpsc, oneshot};

use crate::error::ConnectionDropped;
use crate::error::{
    CloseError, CloseErrorKind, ConnectionError, IoError, ProtocolError, ResolutionError,
};
use crate::remote::config::RemoteConnectionsConfig;
use crate::remote::router::{
    BidirectionalReceiverRequest, ResolutionErrorReplacement, RoutingError,
};
use crate::remote::task::{ConnectionTask, DispatchError};
use crate::remote::test_fixture::{LocalRoutes, RouteTable};
use crate::routing::{Route, RoutingAddr, TaggedEnvelope, TaggedSender};
use crate::ws::{AutoWebSocket, WsMessage};
use futures::io::ErrorKind;
use ratchet::{NoExt, SplittableExtension};
use ratchet_fixture::duplex::websocket_pair;
use ratchet_fixture::ratchet_failing_ext::FailingExt;
use slab::Slab;
use std::num::NonZeroUsize;
use std::time::Duration;
use swim_model::Value;
use tokio::io::DuplexStream;

#[test]
fn dispatch_error_display() {
    let bad_uri: Uri = "swim://localhost/hello".parse().unwrap();
    let string =
        DispatchError::BadNodeUri(BadRelativeUri::Absolute(UriIsAbsolute(bad_uri))).to_string();
    assert_eq!(
        string,
        "Invalid relative URI: ''swim://localhost/hello' is an absolute URI.'"
    );

    let string = DispatchError::Unresolvable(ResolutionError::router_dropped()).to_string();
    assert_eq!(
        string,
        "Could not resolve a router endpoint: 'The router channel was dropped.'"
    );

    let string = DispatchError::RoutingProblem(RoutingError::RouterDropped).to_string();
    assert_eq!(string, "Could not find a router endpoint: 'Router dropped'");

    let string = DispatchError::Dropped(ConnectionDropped::Closed).to_string();
    assert_eq!(
        string,
        "The routing channel was dropped: 'The connection was explicitly closed.'"
    );
}

fn envelope(path: RelativePath, body: &str) -> Envelope {
    let RelativePath { node, lane } = path;
    Envelope::command()
        .node_uri(node)
        .lane_uri(lane)
        .body(body)
        .done()
}

#[tokio::test]
async fn try_dispatch_in_map() {
    let (tx, mut rx) = mpsc::channel(8);
    let (_drop_tx, drop_rx) = promise::promise();
    let addr = RoutingAddr::remote(0);
    let mut resolved = HashMap::new();
    let mut bidirectional_connections = Slab::new();
    let path = RelativePath::new("/node", "/lane");
    resolved.insert(
        path.clone(),
        Route::new(TaggedSender::new(addr, tx), drop_rx),
    );

    let (router, _jh) = LocalRoutes::from_table(RouteTable::new(addr));
    let mut tagged_router = router.tagged(addr);

    let env = envelope(path, "a");

    let result = super::try_dispatch_envelope(
        &mut tagged_router,
        &mut bidirectional_connections,
        &mut resolved,
        env.clone(),
    )
    .await;

    assert!(result.is_ok());

    let received = rx.recv().now_or_never();
    assert_eq!(received, Some(Some(TaggedEnvelope(addr, env))));
}

#[tokio::test]
async fn try_dispatch_from_router() {
    let addr = RoutingAddr::remote(0);
    let mut table = RouteTable::new(addr);
    let mut rx = table.add("/node".parse().unwrap());
    let (router, _router_task) = LocalRoutes::from_table(table);
    let mut tagged_router = router.tagged(addr);

    let mut resolved = HashMap::new();
    let mut bidirectional_connections = Slab::new();
    let path = RelativePath::new("/node", "/lane");

    let env = envelope(path.clone(), "a");

    let result = super::try_dispatch_envelope(
        &mut tagged_router,
        &mut bidirectional_connections,
        &mut resolved,
        env.clone(),
    )
    .await;
    println!("{:?}", result);
    assert!(result.is_ok());

    let received = rx.recv().now_or_never();
    assert_eq!(received, Some(Some(TaggedEnvelope(addr, env))));

    assert!(resolved.contains_key(&path));
}

#[tokio::test]
async fn try_dispatch_to_bidirectional() {
    let addr = RoutingAddr::remote(0);
    let mut table = RouteTable::new(addr);
    let mut resolved = HashMap::new();
    let mut bidirectional_connections = Slab::new();

    let (conn_tx, mut conn_rx) = mpsc::channel(8);
    bidirectional_connections.insert(TaggedSender::new(addr, conn_tx));

    let path = RelativePath::new("/node", "/lane");

    let mut rx = table.add("/node".parse().unwrap());
    let (router, _jh) = LocalRoutes::from_table(table);
    let mut tagged_router = router.tagged(addr);

    let env = Envelope::event()
        .node_uri(&path.node)
        .lane_uri(&path.lane)
        .body("a")
        .done();

    let result = super::try_dispatch_envelope(
        &mut tagged_router,
        &mut bidirectional_connections,
        &mut resolved,
        env.clone(),
    )
    .await;

    assert!(result.is_ok());
    let received = rx.recv().now_or_never();
    assert_eq!(received, None);
    assert!(!resolved.contains_key(&path));

    let received = conn_rx.recv().now_or_never();
    assert_eq!(received, Some(Some(TaggedEnvelope(addr, env))));
}

#[tokio::test]
async fn try_dispatch_closed_sender() {
    let addr = RoutingAddr::remote(0);
    let (router, _jh) = LocalRoutes::from_table(RouteTable::new(addr));
    let mut tagged_router = router.tagged(addr);

    let mut resolved = HashMap::new();
    let mut bidirectional_connections = Slab::new();
    let path = RelativePath::new("/node", "/lane");

    let env = envelope(path.clone(), "a");

    let result = super::try_dispatch_envelope(
        &mut tagged_router,
        &mut bidirectional_connections,
        &mut resolved,
        env.clone(),
    )
    .await;

    if let Err((return_env, err)) = result {
        let expected_uri: RelativeUri = "/node".parse().unwrap();
        assert_eq!(return_env, env);
        assert!(
            matches!(err, DispatchError::RoutingProblem(RoutingError::Resolution(ResolutionErrorReplacement::NoAgentAtRoute(uri))) if uri == expected_uri)
        );
    } else {
        panic!("Unexpected success.")
    }
}

#[tokio::test]
async fn try_dispatch_fail_on_dropped() {
    let (tx, rx) = mpsc::channel(8);
    let (drop_tx, drop_rx) = promise::promise();
    let addr = RoutingAddr::remote(0);
    let mut bidirectional_connections = Slab::new();
    let mut table = RouteTable::new(addr);
    let mut router_rx = table.add("/node".parse().unwrap());
    let (router, _jh) = LocalRoutes::from_table(table);
    let mut tagged_router = router.tagged(addr);

    let path = RelativePath::new("/node", "/lane");
    let mut resolved = HashMap::new();
    resolved.insert(
        path.clone(),
        Route::new(TaggedSender::new(addr, tx), drop_rx),
    );

    let env = envelope(path, "a");

    drop(rx);
    drop(drop_tx);

    let result = super::try_dispatch_envelope(
        &mut tagged_router,
        &mut bidirectional_connections,
        &mut resolved,
        env.clone(),
    )
    .await;

    assert!(result.is_ok());

    let received = router_rx.recv().now_or_never();
    assert_eq!(received, Some(Some(TaggedEnvelope(addr, env))));
}

#[tokio::test]
async fn try_dispatch_fail_on_no_route() {
    let addr = RoutingAddr::remote(0);
    let (router, _jh) = LocalRoutes::from_table(RouteTable::new(addr));
    let mut tagged_router = router.tagged(addr);
    let mut resolved = HashMap::new();
    let mut bidirectional_connections = Slab::new();
    let path = RelativePath::new("/node", "/lane");

    let env = envelope(path.clone(), "a");

    let result = super::try_dispatch_envelope(
        &mut tagged_router,
        &mut bidirectional_connections,
        &mut resolved,
        env.clone(),
    )
    .await;

    if let Err((return_env, err)) = result {
        let expected_uri: RelativeUri = "/node".parse().unwrap();
        assert_eq!(return_env, env);
        assert!(
            matches!(err, DispatchError::RoutingProblem(RoutingError::Resolution(ResolutionErrorReplacement::NoAgentAtRoute(uri))) if uri == expected_uri)
        );
    } else {
        panic!("Unexpected success.")
    }
}

#[tokio::test]
async fn dispatch_immediate_success() {
    let addr = RoutingAddr::remote(0);
    let mut table = RouteTable::new(addr);
    let mut resolved = HashMap::new();
    let mut bidirectional_connections = Slab::new();
    let path = RelativePath::new("/node", "/lane");

    let mut rx = table.add("/node".parse().unwrap());
    let (router, _jh) = LocalRoutes::from_table(table);
    let mut tagged_router = router.tagged(addr);

    let env = envelope(path.clone(), "a");

    let delays = Arc::new(Mutex::new(vec![]));

    let result = super::dispatch_envelope(
        &mut tagged_router,
        &mut bidirectional_connections,
        &mut resolved,
        env.clone(),
        RetryStrategy::none(),
        |dur| {
            let delays_cpy = delays.clone();
            async move {
                delays_cpy.lock().push(dur);
            }
        },
    )
    .await;

    assert!(delays.lock().is_empty());

    assert!(result.is_ok());

    let received = rx.recv().now_or_never();
    assert_eq!(received, Some(Some(TaggedEnvelope(addr, env))));

    assert!(resolved.contains_key(&path));
}

fn retries() -> NonZeroUsize {
    non_zero_usize!(100)
}

#[tokio::test]
async fn dispatch_immediate_failure() {
    let addr = RoutingAddr::remote(0);
    let (router, _jh) = LocalRoutes::from_table(RouteTable::new(addr));
    let mut tagged_router = router.tagged(addr);
    let mut resolved = HashMap::new();
    let mut bidirectional_connections = Slab::new();
    let path = RelativePath::new("/node", "/lane");

    let env = envelope(path.clone(), "a");

    let delays = Arc::new(Mutex::new(vec![]));

    let result = super::dispatch_envelope(
        &mut tagged_router,
        &mut bidirectional_connections,
        &mut resolved,
        env.clone(),
        RetryStrategy::interval(Duration::from_secs(1), Quantity::Finite(retries())),
        |dur| {
            let delays_cpy = delays.clone();
            async move {
                delays_cpy.lock().push(dur);
            }
        },
    )
    .await;

    assert!(delays.lock().is_empty());

    if let Err((err_env, err)) = result {
        let expected_uri: RelativeUri = "/node".parse().unwrap();
        assert!(
            matches!(err, DispatchError::RoutingProblem(RoutingError::Resolution(ResolutionErrorReplacement::NoAgentAtRoute(uri))) if uri == expected_uri
            )
        );
        assert_eq!(err_env, env);
    } else {
        panic!("Unexpected success.")
    }
}

#[tokio::test]
async fn dispatch_after_retry() {
    let addr = RoutingAddr::remote(0);
    let mut table = RouteTable::new(addr);
    let mut resolved = HashMap::new();
    let mut bidirectional_connections = Slab::new();
    let path = RelativePath::new("/node", "/lane");

    let mut rx = table.add_with_countdown("/node".parse().unwrap(), 1);
    let (router, _jh) = LocalRoutes::from_table(table);
    let mut tagged_router = router.tagged(addr);
    let env = envelope(path.clone(), "a");

    let delays = Arc::new(Mutex::new(vec![]));

    let result = super::dispatch_envelope(
        &mut tagged_router,
        &mut bidirectional_connections,
        &mut resolved,
        env.clone(),
        RetryStrategy::interval(Duration::from_secs(1), Quantity::Finite(retries())),
        |dur| {
            let delays_cpy = delays.clone();
            async move {
                delays_cpy.lock().push(dur);
            }
        },
    )
    .await;

    assert_eq!(&*delays.lock(), &vec![Duration::from_secs(1)]);

    assert!(result.is_ok());

    let received = rx.recv().now_or_never();
    assert_eq!(received, Some(Some(TaggedEnvelope(addr, env))));

    assert!(resolved.contains_key(&path));
}

#[tokio::test]
async fn dispatch_after_immediate_retry() {
    let addr = RoutingAddr::remote(0);
    let mut table = RouteTable::new(addr);
    let mut resolved = HashMap::new();
    let mut bidirectional_connections = Slab::new();
    let path = RelativePath::new("/node", "/lane");

    let mut rx = table.add_with_countdown("/node".parse().unwrap(), 1);
    let (router, _jh) = LocalRoutes::from_table(table);
    let mut tagged_router = router.tagged(addr);
    let env = envelope(path.clone(), "a");

    let delays = Arc::new(Mutex::new(vec![]));

    let result = super::dispatch_envelope(
        &mut tagged_router,
        &mut bidirectional_connections,
        &mut resolved,
        env.clone(),
        RetryStrategy::immediate(retries()),
        |dur| {
            let delays_cpy = delays.clone();
            async move {
                delays_cpy.lock().push(dur);
            }
        },
    )
    .await;

    assert!(delays.lock().is_empty());

    assert!(result.is_ok());

    let received = rx.recv().now_or_never();
    assert_eq!(received, Some(Some(TaggedEnvelope(addr, env))));

    assert!(resolved.contains_key(&path));
}

// todo add ws extension type param so failing test fixtures
struct TaskFixture<E> {
    task: BoxFuture<'static, ConnectionDropped>,
    websocket_peer: AutoWebSocket<DuplexStream, E>,
    envelope_tx: mpsc::Sender<TaggedEnvelope>,
    bidirectional_tx: mpsc::Sender<BidirectionalReceiverRequest>,
    stop_trigger: trigger::Sender,
}

const BUFFER_SIZE: usize = 8;

impl<E> TaskFixture<E>
where
    E: SplittableExtension + Send + Sync + Clone + 'static,
{
    fn new(ext: E, table: RouteTable) -> Self {
        let addr = table.addr();

        let (local_websocket, websocket_peer) = websocket_pair(ext.clone(), ext);
        let (env_tx, env_rx) = mpsc::channel(BUFFER_SIZE);
        let (bidirectional_tx, bidirectional_rx) = mpsc::channel(BUFFER_SIZE);
        let (stop_tx, stop_rx) = trigger::trigger();
        let (router, _router_task) = LocalRoutes::from_table(table);

        let task = ConnectionTask::new(
            addr,
            local_websocket,
            router.tagged(addr),
            (env_tx.clone(), env_rx),
            bidirectional_rx,
            stop_rx,
            RemoteConnectionsConfig {
                router_buffer_size: non_zero_usize!(10),
                channel_buffer_size: non_zero_usize!(10),
                activity_timeout: Duration::from_secs(30),
                write_timeout: Duration::from_secs(20),
                connection_retries: RetryStrategy::immediate(non_zero_usize!(1)),
                yield_after: non_zero_usize!(256),
            },
        )
        .run()
        .boxed();

        TaskFixture {
            task,
            websocket_peer: AutoWebSocket::new(websocket_peer),
            envelope_tx: env_tx,
            bidirectional_tx,
            stop_trigger: stop_tx,
        }
    }
}

fn env_to_string(env: Envelope) -> String {
    env.into_value().to_string()
}

fn message_for(env: Envelope) -> WsMessage {
    WsMessage::Text(format!("{}", print_recon_compact(&env)))
}

#[tokio::test]
async fn task_send_message() {
    let TaskFixture {
        task,
        mut websocket_peer,
        envelope_tx,
        stop_trigger,
        bidirectional_tx: _bidirectional_tx,
    } = TaskFixture::new(NoExt, RouteTable::new(RoutingAddr::remote(0)));

    let envelope = Envelope::event()
        .node_uri("/node")
        .lane_uri("/lane")
        .body("a")
        .done();
    let env_cpy = envelope.clone();

    let test_case = async move {
        let tagged = TaggedEnvelope(RoutingAddr::plane(100), env_cpy.clone());
        assert!(envelope_tx.send(tagged).await.is_ok());

        match websocket_peer.read().await.unwrap() {
            WsMessage::Text(msg) => {
                assert_eq!(msg, env_to_string(env_cpy));
                stop_trigger.trigger();
            }
            m => {
                panic!("Expected a text message, got: {:?}", m)
            }
        }
    };

    let result = timeout::timeout(Duration::from_secs(5), join(task, test_case)).await;
    assert!(matches!(result, Ok((ConnectionDropped::Closed, _))));
}

#[tokio::test]
async fn task_send_message_bidirectional() {
    let TaskFixture {
        task,
        envelope_tx: _envelope_tx,
        mut websocket_peer,
        stop_trigger,
        bidirectional_tx,
    } = TaskFixture::new(NoExt, RouteTable::new(RoutingAddr::remote(0)));

    let envelope = Envelope::event()
        .node_uri("/node")
        .lane_uri("/lane")
        .body("a")
        .done();

    let env_cpy = envelope.clone();

    let test_case = async move {
        let (tx, rx) = oneshot::channel();
        bidirectional_tx
            .send(BidirectionalReceiverRequest::new(tx))
            .await
            .unwrap();

        let mut bidirectional_receiver = rx.await.unwrap();

        assert!(websocket_peer
            .write_text(env_to_string(env_cpy.clone()))
            .await
            .is_ok());
        assert!(
            matches!(bidirectional_receiver.recv().await, Some(TaggedEnvelope(_, env)) if env == env_cpy)
        );
        stop_trigger.trigger();
    };

    let result = timeout::timeout(Duration::from_secs(5), join(task, test_case)).await;
    assert!(matches!(result, Ok((ConnectionDropped::Closed, _))));
}

#[tokio::test]
async fn task_send_message_failure() {
    let TaskFixture {
        task,
        websocket_peer: _peer,
        envelope_tx,
        stop_trigger: _stop_trigger,
        bidirectional_tx: _bidirectional_tx,
    } = TaskFixture::new(
        FailingExt(ConnectionError::Io(IoError::new(
            ErrorKind::ConnectionReset,
            None,
        ))),
        RouteTable::new(RoutingAddr::remote(0)),
    );

    let envelope = Envelope::event()
        .node_uri("/node")
        .lane_uri("/lane")
        .body("a")
        .done();

    let env_cpy = envelope.clone();

    let test_case = async move {
        let tagged = TaggedEnvelope(RoutingAddr::plane(100), env_cpy.clone());

        assert!(envelope_tx.send(tagged).await.is_ok());
    };

    let result = timeout::timeout(Duration::from_secs(5), join(task, test_case)).await;
    let _err = ConnectionError::Io(IoError::new(ErrorKind::ConnectionReset, None));
    assert!(matches!(result, Ok((ConnectionDropped::Failed(_err), _))));
}

#[tokio::test]
async fn task_receive_message_with_route() {
    let mut table = RouteTable::new(RoutingAddr::remote(0));
    let mut rx = table.add("/node".parse().unwrap());

    let TaskFixture {
        task,
        mut websocket_peer,
        envelope_tx: _envelope_tx,
        stop_trigger,
        bidirectional_tx: _bidirectional_tx,
    } = TaskFixture::new(NoExt, table);

    let envelope = Envelope::command()
        .node_uri("/node")
        .lane_uri("/lane")
        .body("a")
        .done();

    let env_cpy = envelope.clone();

    let test_case = async move {
        assert!(websocket_peer
            .write_text(env_to_string(env_cpy.clone()))
            .await
            .is_ok());
        assert!(matches!(rx.recv().await, Some(TaggedEnvelope(_, env)) if env == env_cpy));
        stop_trigger.trigger();
    };

    let result = timeout::timeout(Duration::from_secs(5), join(task, test_case)).await;
    assert!(matches!(result, Ok((ConnectionDropped::Closed, _))));
}

#[tokio::test]
async fn task_receive_link_message_missing_node() {
    let TaskFixture {
        task,
        mut websocket_peer,
        envelope_tx: _envelope_tx,
        stop_trigger,
        bidirectional_tx: _bidirectional_tx,
    } = TaskFixture::new(NoExt, RouteTable::new(RoutingAddr::plane(0)));

    let envelope = Envelope::link()
        .node_uri("/missing")
        .lane_uri("/lane")
        .done();

    let response = Envelope::node_not_found("/missing", "/lane");

    let test_case = async move {
        assert!(websocket_peer
            .write_text(env_to_string(envelope))
            .await
            .is_ok());

        let message = websocket_peer.read().await.unwrap();
        assert_eq!(message, message_for(response));

        stop_trigger.trigger();
    };

    let result = timeout::timeout(Duration::from_secs(5), join(task, test_case)).await;
    assert!(matches!(result, Ok((ConnectionDropped::Closed, _))));
}

#[tokio::test]
async fn task_receive_sync_message_missing_node() {
    let TaskFixture {
        task,
        mut websocket_peer,
        envelope_tx: _envelope_tx,
        stop_trigger: _stop_trigger,
        bidirectional_tx: _bidirectional_tx,
    } = TaskFixture::new(NoExt, RouteTable::new(RoutingAddr::plane(0)));

    let envelope = Envelope::sync()
        .node_uri("/missing")
        .lane_uri("/lane")
        .done();

    let test_case = async move {
        assert!(websocket_peer
            .write_text(env_to_string(envelope))
            .await
            .is_ok());
        let message = websocket_peer.read().await.unwrap();
        let envelope = Envelope::node_not_found("/missing", "/lane");
        let expected = WsMessage::Text(envelope.into_value().to_string());

        assert_eq!(message, expected);
    };

    let result = timeout::timeout(Duration::from_secs(5), join(task, test_case)).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn task_receive_message_no_route() {
    let TaskFixture {
        task,
        mut websocket_peer,
        envelope_tx: _envelope_tx,
        stop_trigger,
        bidirectional_tx: _bidirectional_tx,
    } = TaskFixture::new(NoExt, RouteTable::new(RoutingAddr::plane(0)));

    let envelope = Envelope::event()
        .node_uri("/node")
        .lane_uri("/lane")
        .body("a")
        .done();

    let env_cpy = envelope.clone();

    let test_case = async move {
        assert!(websocket_peer
            .write_text(env_to_string(env_cpy.clone()))
            .await
            .is_ok());
        stop_trigger.trigger();
    };

    let result = timeout::timeout(Duration::from_secs(5), join(task, test_case)).await;
    assert!(matches!(result, Ok((ConnectionDropped::Closed, _))));
}

#[tokio::test]
async fn task_receive_error() {
    let TaskFixture {
        task,
        mut websocket_peer,
        envelope_tx: _envelope_tx,
        stop_trigger: _stop_trigger,
        bidirectional_tx: _bidirectional_tx,
    } = TaskFixture::new(NoExt, RouteTable::new(RoutingAddr::plane(0)));

    let test_case = async move {
        let envelope = Envelope::event()
            .node_uri("/node")
            .lane_uri("/lane")
            .body(Value::text("a"))
            .done();
        assert!(websocket_peer
            .write_text(env_to_string(envelope))
            .await
            .is_ok());
    };

    let result = timeout::timeout(Duration::from_secs(5), join(task, test_case)).await;
    let _err = ConnectionError::Io(IoError::new(ErrorKind::ConnectionReset, None));
    assert!(matches!(result, Ok((ConnectionDropped::Failed(_err), _))));
}

#[tokio::test]
async fn task_stopped_remotely() {
    let TaskFixture {
        task,
        websocket_peer,
        envelope_tx: _envelope_tx,
        stop_trigger: _stop_trigger,
        bidirectional_tx: _bidirectional_tx,
    } = TaskFixture::new(NoExt, RouteTable::new(RoutingAddr::plane(0)));

    let test_case = async move {
        drop(websocket_peer);
    };

    let result = timeout::timeout(Duration::from_secs(5), join(task, test_case)).await;
    let _err = ConnectionError::Closed(CloseError::new(CloseErrorKind::ClosedRemotely, None));
    assert!(matches!(result, Ok((ConnectionDropped::Failed(_err), _))));
}

#[tokio::test]
async fn task_timeout() {
    let TaskFixture {
        task,
        mut websocket_peer,
        envelope_tx,
        stop_trigger: _stop_trigger,
        bidirectional_tx: _bidirectional_tx,
    } = TaskFixture::new(NoExt, RouteTable::new(RoutingAddr::plane(0)));

    let envelope = Envelope::event()
        .node_uri("/node")
        .lane_uri("/lane")
        .body("a")
        .done();

    let env_cpy = envelope.clone();

    let test_case = async move {
        let tagged = TaggedEnvelope(RoutingAddr::plane(100), env_cpy.clone());
        tokio::time::pause();
        assert!(envelope_tx.send(tagged).await.is_ok());

        let message = websocket_peer.read().await;
        assert!(message.is_ok());
        tokio::time::advance(Duration::from_secs(31)).await;
    };

    let result = join(task, test_case).await;
    assert!(matches!(result, (ConnectionDropped::TimedOut(d), _) if d == Duration::from_secs(30)));
}

#[tokio::test]
async fn task_receive_bad_message() {
    let TaskFixture {
        task,
        mut websocket_peer,
        envelope_tx: _envelope_tx,
        stop_trigger: _stop_trigger,
        bidirectional_tx: _bidirectional_tx,
    } = TaskFixture::new(NoExt, RouteTable::new(RoutingAddr::plane(0)));

    let test_case = async move {
        assert!(websocket_peer.write_text("Boom!").await.is_ok());
    };

    let result = timeout::timeout(Duration::from_secs(5), join(task, test_case)).await;
    let _err = ConnectionError::Protocol(ProtocolError::warp(None));
    assert!(matches!(result, Ok((ConnectionDropped::Failed(_err), _))));
}
