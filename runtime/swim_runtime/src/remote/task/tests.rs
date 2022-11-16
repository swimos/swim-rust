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

use futures::future::{join, ready, BoxFuture};
use futures::FutureExt;
use parking_lot::Mutex;
use swim_form::Form;
use swim_model::path::RelativePath;
use swim_recon::printer::print_recon_compact;
use swim_utilities::algebra::non_zero_usize;
use swim_utilities::future::retryable::{Quantity, RetryStrategy};
use swim_utilities::routing::route_uri::{InvalidRouteUri, RouteUri};
use swim_utilities::trigger;
use swim_utilities::trigger::promise;
use swim_warp::envelope::Envelope;
use tokio::sync::mpsc;

use crate::error::{
    CloseError, CloseErrorKind, ConnectionError, IoError, ProtocolError, ResolutionError,
};
use crate::error::{ConnectionDropped, RouterError};
use crate::remote::config::RemoteConnectionsConfig;
use crate::remote::task::{AttachClientRouted, ConnectionTask, DispatchError};
use crate::remote::test_fixture::LocalRoutes;
use crate::routing::{Route, RoutingAddr, TaggedEnvelope, TaggedSender};
use crate::ws::{AutoWebSocket, WsMessage};
use futures::io::ErrorKind;
use ratchet::{NoExt, SplittableExtension};
use ratchet_fixture::duplex::websocket_pair;
use ratchet_fixture::ratchet_failing_ext::FailingExt;
use std::num::NonZeroUsize;
use std::time::Duration;
use swim_model::Value;
use tokio::io::DuplexStream;

const NO_RETRY: RetryStrategy = RetryStrategy::none();

#[test]
fn dispatch_error_display() {
    let string =
        DispatchError::BadNodeUri(InvalidRouteUri::new("swim://localhost/hello".to_string()))
            .to_string();
    assert_eq!(
        string,
        "Invalid route URI: ''swim://localhost/hello' is not a valid route URI.'"
    );

    let string = DispatchError::Unresolvable(ResolutionError::router_dropped()).to_string();
    assert_eq!(
        string,
        "Could not resolve a router endpoint: 'The router channel was dropped.'"
    );

    let string = DispatchError::RoutingProblem(RouterError::RouterDropped).to_string();
    assert_eq!(
        string,
        "Could not find a router endpoint: 'The router channel was dropped.'"
    );

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
    let mut router = LocalRoutes::new(addr);
    let mut resolved = HashMap::new();
    let mut bidirectional_connections = Default::default();
    let path = RelativePath::new("/node", "/lane");
    resolved.insert(
        path.clone(),
        Route::new(TaggedSender::new(addr, tx), drop_rx),
    );

    let env = envelope(path, "a");

    let result = super::dispatch_envelope(
        &mut router,
        &mut bidirectional_connections,
        &mut resolved,
        env.clone(),
        &NO_RETRY,
        |_| ready(()),
    )
    .await;

    assert!(result.is_ok());

    let received = rx.recv().now_or_never();
    assert_eq!(received, Some(Some(TaggedEnvelope(addr, env))));
}

#[tokio::test]
async fn try_dispatch_from_router() {
    let addr = RoutingAddr::remote(0);
    let mut router = LocalRoutes::new(addr);
    let mut resolved = HashMap::new();
    let mut bidirectional_connections = Default::default();
    let path = RelativePath::new("/node", "/lane");

    let mut rx = router.add("/node".parse().unwrap());

    let env = envelope(path.clone(), "a");

    let result = super::dispatch_envelope(
        &mut router,
        &mut bidirectional_connections,
        &mut resolved,
        env.clone(),
        &NO_RETRY,
        |_| ready(()),
    )
    .await;

    assert!(result.is_ok());

    let received = rx.recv().now_or_never();
    assert_eq!(received, Some(Some(TaggedEnvelope(addr, env))));

    assert!(resolved.contains_key(&path));
}

#[tokio::test]
async fn try_dispatch_closed_sender() {
    let addr = RoutingAddr::remote(0);
    let mut router = LocalRoutes::new(addr);
    let mut resolved = HashMap::new();
    let mut bidirectional_connections = Default::default();
    let path = RelativePath::new("/node", "/lane");

    let env = envelope(path.clone(), "a");

    let result = super::dispatch_envelope(
        &mut router,
        &mut bidirectional_connections,
        &mut resolved,
        env.clone(),
        &NO_RETRY,
        |_| ready(()),
    )
    .await;

    if let Err(err) = result {
        let expected_uri: RouteUri = "/node".parse().unwrap();
        assert!(
            matches!(err, DispatchError::RoutingProblem(RouterError::NoAgentAtRoute(uri)) if uri == expected_uri)
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
    let mut bidirectional_connections = Default::default();
    let mut router = LocalRoutes::new(addr);
    let mut router_rx = router.add("/node".parse().unwrap());

    let path = RelativePath::new("/node", "/lane");
    let mut resolved = HashMap::new();
    resolved.insert(
        path.clone(),
        Route::new(TaggedSender::new(addr, tx), drop_rx),
    );

    let env = envelope(path, "a");

    drop(rx);
    drop(drop_tx);

    let result = super::dispatch_envelope(
        &mut router,
        &mut bidirectional_connections,
        &mut resolved,
        env.clone(),
        &NO_RETRY,
        |_| ready(()),
    )
    .await;

    assert!(result.is_ok());

    let received = router_rx.recv().now_or_never();
    assert_eq!(received, Some(Some(TaggedEnvelope(addr, env))));
}

#[tokio::test]
async fn try_dispatch_fail_on_no_route() {
    let addr = RoutingAddr::remote(0);
    let mut router = LocalRoutes::new(addr);
    let mut resolved = HashMap::new();
    let mut bidirectional_connections = Default::default();
    let path = RelativePath::new("/node", "/lane");

    let env = envelope(path.clone(), "a");

    let result = super::dispatch_envelope(
        &mut router,
        &mut bidirectional_connections,
        &mut resolved,
        env.clone(),
        &NO_RETRY,
        |_| ready(()),
    )
    .await;

    if let Err(err) = result {
        let expected_uri: RouteUri = "/node".parse().unwrap();
        assert!(
            matches!(err, DispatchError::RoutingProblem(RouterError::NoAgentAtRoute(uri)) if uri == expected_uri)
        );
    } else {
        panic!("Unexpected success.")
    }
}

#[tokio::test]
async fn dispatch_immediate_success() {
    let addr = RoutingAddr::remote(0);
    let mut router = LocalRoutes::new(addr);
    let mut resolved = HashMap::new();
    let mut bidirectional_connections = Default::default();
    let path = RelativePath::new("/node", "/lane");

    let mut rx = router.add("/node".parse().unwrap());

    let env = envelope(path.clone(), "a");

    let delays = Arc::new(Mutex::new(vec![]));

    let result = super::dispatch_envelope(
        &mut router,
        &mut bidirectional_connections,
        &mut resolved,
        env.clone(),
        &NO_RETRY,
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
    let mut router = LocalRoutes::new(addr);
    let mut resolved = HashMap::new();
    let mut bidirectional_connections = Default::default();
    let path = RelativePath::new("/node", "/lane");

    let env = envelope(path.clone(), "a");

    let delays = Arc::new(Mutex::new(vec![]));

    let result = super::dispatch_envelope(
        &mut router,
        &mut bidirectional_connections,
        &mut resolved,
        env.clone(),
        &RetryStrategy::interval(Duration::from_secs(1), Quantity::Finite(retries())),
        |dur| {
            let delays_cpy = delays.clone();
            async move {
                delays_cpy.lock().push(dur);
            }
        },
    )
    .await;

    assert!(delays.lock().is_empty());

    if let Err(err) = result {
        let expected_uri: RouteUri = "/node".parse().unwrap();
        assert!(
            matches!(err, DispatchError::RoutingProblem(RouterError::NoAgentAtRoute(uri)) if uri == expected_uri)
        );
    } else {
        panic!("Unexpected success.")
    }
}

#[tokio::test]
async fn dispatch_after_retry() {
    let addr = RoutingAddr::remote(0);
    let mut router = LocalRoutes::new(addr);
    let mut resolved = HashMap::new();
    let mut bidirectional_connections = Default::default();
    let path = RelativePath::new("/node", "/lane");

    let mut rx = router.add_with_countdown("/node".parse().unwrap(), 1);

    let env = envelope(path.clone(), "a");

    let delays = Arc::new(Mutex::new(vec![]));

    let result = super::dispatch_envelope(
        &mut router,
        &mut bidirectional_connections,
        &mut resolved,
        env.clone(),
        &RetryStrategy::interval(Duration::from_secs(1), Quantity::Finite(retries())),
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
    let mut router = LocalRoutes::new(addr);
    let mut resolved = HashMap::new();
    let mut bidirectional_connections = Default::default();
    let path = RelativePath::new("/node", "/lane");

    let mut rx = router.add_with_countdown("/node".parse().unwrap(), 1);

    let env = envelope(path.clone(), "a");

    let delays = Arc::new(Mutex::new(vec![]));

    let result = super::dispatch_envelope(
        &mut router,
        &mut bidirectional_connections,
        &mut resolved,
        env.clone(),
        &RetryStrategy::immediate(retries()),
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
    router: LocalRoutes,
    task: BoxFuture<'static, ConnectionDropped>,
    websocket_peer: AutoWebSocket<DuplexStream, E>,
    envelope_tx: mpsc::Sender<TaggedEnvelope>,
    attach_client_tx: mpsc::Sender<AttachClientRouted>,
    stop_trigger: trigger::Sender,
}

const BUFFER_SIZE: usize = 8;

impl<E> TaskFixture<E>
where
    E: SplittableExtension + Send + Sync + Clone + 'static,
{
    fn new(ext: E) -> Self {
        let addr = RoutingAddr::remote(0);
        let router = LocalRoutes::new(addr);

        let (local_websocket, websocket_peer) = websocket_pair(ext.clone(), ext);
        let (env_tx, env_rx) = mpsc::channel(BUFFER_SIZE);
        let (attach_client_tx, attach_client_rx) = mpsc::channel(BUFFER_SIZE);
        let (stop_tx, stop_rx) = trigger::trigger();

        let task = ConnectionTask::new(
            addr,
            local_websocket,
            router.clone(),
            (env_tx.clone(), env_rx),
            attach_client_rx,
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
            router,
            task,
            websocket_peer: AutoWebSocket::new(websocket_peer),
            envelope_tx: env_tx,
            attach_client_tx,
            stop_trigger: stop_tx,
        }
    }
}

fn env_to_string(env: Envelope) -> String {
    env.into_value().to_string()
}

fn message_for(env: Envelope) -> WsMessage {
    WsMessage::from(format!("{}", print_recon_compact(&env)))
}

#[tokio::test]
async fn task_send_message() {
    let TaskFixture {
        task,
        mut websocket_peer,
        envelope_tx,
        stop_trigger,
        router: _router,
        attach_client_tx: _attach_client_tx,
    } = TaskFixture::new(NoExt);

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

    let result = tokio::time::timeout(Duration::from_secs(5), join(task, test_case)).await;
    assert!(matches!(result, Ok((ConnectionDropped::Closed, _))));
}

#[tokio::test]
async fn task_send_message_failure() {
    let TaskFixture {
        task,
        websocket_peer: _peer,
        envelope_tx,
        stop_trigger: _stop_trigger,
        router: _router,
        attach_client_tx: _attach_client_tx,
    } = TaskFixture::new(FailingExt(ConnectionError::Io(IoError::new(
        ErrorKind::ConnectionReset,
        None,
    ))));

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

    let result = tokio::time::timeout(Duration::from_secs(5), join(task, test_case)).await;
    let _err = ConnectionError::Io(IoError::new(ErrorKind::ConnectionReset, None));
    assert!(matches!(result, Ok((ConnectionDropped::Failed(_err), _))));
}

#[tokio::test]
async fn task_receive_message_with_route() {
    let TaskFixture {
        task,
        mut websocket_peer,
        envelope_tx: _envelope_tx,
        stop_trigger,
        router,
        attach_client_tx: _attach_client_tx,
    } = TaskFixture::new(NoExt);

    let mut rx = router.add("/node".parse().unwrap());
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

    let result = tokio::time::timeout(Duration::from_secs(5), join(task, test_case)).await;
    assert!(matches!(result, Ok((ConnectionDropped::Closed, _))));
}

#[tokio::test]
async fn task_receive_link_message_missing_node() {
    let TaskFixture {
        task,
        mut websocket_peer,
        envelope_tx: _envelope_tx,
        stop_trigger,
        router: _router,
        attach_client_tx: _attach_client_tx,
    } = TaskFixture::new(NoExt);

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

    let result = tokio::time::timeout(Duration::from_secs(5), join(task, test_case)).await;
    assert!(matches!(result, Ok((ConnectionDropped::Closed, _))));
}

#[tokio::test]
async fn task_receive_sync_message_missing_node() {
    let TaskFixture {
        task,
        mut websocket_peer,
        envelope_tx: _envelope_tx,
        stop_trigger: _stop_trigger,
        router: _router,
        attach_client_tx: _attach_client_tx,
    } = TaskFixture::new(NoExt);

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
        let expected = WsMessage::from(envelope.into_value().to_string());

        assert_eq!(message, expected);
    };

    let result = tokio::time::timeout(Duration::from_secs(5), join(task, test_case)).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn task_receive_message_no_route() {
    let TaskFixture {
        task,
        mut websocket_peer,
        envelope_tx: _envelope_tx,
        stop_trigger,
        router: _router,
        attach_client_tx: _attach_client_tx,
    } = TaskFixture::new(NoExt);

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

    let result = tokio::time::timeout(Duration::from_secs(5), join(task, test_case)).await;
    assert!(matches!(result, Ok((ConnectionDropped::Closed, _))));
}

#[tokio::test]
async fn task_receive_error() {
    let TaskFixture {
        task,
        mut websocket_peer,
        envelope_tx: _envelope_tx,
        stop_trigger: _stop_trigger,
        router: _router,
        attach_client_tx: _attach_client_tx,
    } = TaskFixture::new(NoExt);

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

    let result = tokio::time::timeout(Duration::from_secs(5), join(task, test_case)).await;
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
        router: _router,
        attach_client_tx: _attach_client_tx,
    } = TaskFixture::new(NoExt);

    let test_case = async move {
        drop(websocket_peer);
    };

    let result = tokio::time::timeout(Duration::from_secs(5), join(task, test_case)).await;
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
        router: _router,
        attach_client_tx: _attach_client_tx,
    } = TaskFixture::new(NoExt);

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
        router: _router,
        attach_client_tx: _attach_client_tx,
    } = TaskFixture::new(NoExt);

    let test_case = async move {
        assert!(websocket_peer.write_text("Boom!").await.is_ok());
    };

    let result = tokio::time::timeout(Duration::from_secs(5), join(task, test_case)).await;
    let _err = ConnectionError::Protocol(ProtocolError::warp(None));
    assert!(matches!(result, Ok((ConnectionDropped::Failed(_err), _))));
}
