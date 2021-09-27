// Copyright 2015-2021 SWIM.AI inc.
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

use futures::channel::mpsc as fut_mpsc;
use futures::future::{join, join4, BoxFuture};
use futures::{FutureExt, SinkExt, StreamExt};
use http::Uri;
use parking_lot::Mutex;
use tokio::sync::{mpsc, watch};

use swim_common::model::Value;
use swim_common::warp::envelope::Envelope;
use swim_common::warp::path::RelativePath;
use swim_runtime::time::timeout;
use swim_utilities::future::retryable::strategy::{Quantity, RetryStrategy};
use swim_utilities::routing::uri::{BadRelativeUri, RelativeUri, UriIsAbsolute};
use swim_utilities::trigger;
use swim_utilities::trigger::promise;

use crate::routing::error::RouterError;
use crate::routing::remote::config::ConnectionConfig;
use crate::routing::remote::task::{ConnectionTask, DispatchError};
use crate::routing::remote::test_fixture::fake_channel::TwoWayMpsc;
use crate::routing::remote::test_fixture::LocalRoutes;
use crate::routing::{ConnectionDropped, Route, RoutingAddr, TaggedEnvelope, TaggedSender};
use futures::io::ErrorKind;
use std::num::NonZeroUsize;
use std::time::Duration;
use swim_common::routing::ws::WsMessage;
use swim_common::routing::{
    CloseError, CloseErrorKind, ConnectionError, IoError, ProtocolError, ResolutionError,
};

#[test]
fn dispatch_error_display() {
    let bad_uri: Uri = "swim://localhost/hello".parse().unwrap();
    let string =
        DispatchError::BadNodeUri(BadRelativeUri::Absolute(UriIsAbsolute(bad_uri.clone())))
            .to_string();
    assert_eq!(
        string,
        "Invalid relative URI: ''swim://localhost/hello' is an absolute URI.'"
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
    Envelope::make_event(node, lane, Some(Value::text(body)))
}

#[tokio::test]
async fn try_dispatch_in_map() {
    let (tx, mut rx) = mpsc::channel(8);
    let (_drop_tx, drop_rx) = promise::promise();
    let addr = RoutingAddr::remote(0);
    let mut router = LocalRoutes::new(addr);
    let mut resolved = HashMap::new();
    let path = RelativePath::new("/node", "/lane");
    resolved.insert(
        path.clone(),
        Route::new(TaggedSender::new(addr, tx), drop_rx),
    );

    let env = envelope(path, "a");

    let result = super::try_dispatch_envelope(&mut router, &mut resolved, env.clone()).await;

    assert!(result.is_ok());

    let received = rx.recv().now_or_never();
    assert_eq!(received, Some(Some(TaggedEnvelope(addr, env))));
}

#[tokio::test]
async fn try_dispatch_from_router() {
    let addr = RoutingAddr::remote(0);
    let mut router = LocalRoutes::new(addr);
    let mut resolved = HashMap::new();
    let path = RelativePath::new("/node", "/lane");

    let mut rx = router.add("/node".parse().unwrap());

    let env = envelope(path.clone(), "a");

    let result = super::try_dispatch_envelope(&mut router, &mut resolved, env.clone()).await;

    assert!(result.is_ok());

    let received = rx.recv().now_or_never();
    assert_eq!(received, Some(Some(TaggedEnvelope(addr, env))));

    assert!(resolved.contains_key(&path));
}

#[tokio::test]
async fn try_dispatch_closed_sender() {
    let (tx, rx) = mpsc::channel(8);
    let (drop_tx, drop_rx) = promise::promise();
    let addr = RoutingAddr::remote(0);
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

    let result = super::try_dispatch_envelope(&mut router, &mut resolved, env.clone()).await;

    assert!(result.is_ok());

    let received = router_rx.recv().now_or_never();
    assert_eq!(received, Some(Some(TaggedEnvelope(addr, env))));
}

#[tokio::test]
async fn try_dispatch_fail_on_no_route() {
    let addr = RoutingAddr::remote(0);
    let mut router = LocalRoutes::new(addr);
    let mut resolved = HashMap::new();
    let path = RelativePath::new("/node", "/lane");

    let env = envelope(path.clone(), "a");

    let result = super::try_dispatch_envelope(&mut router, &mut resolved, env.clone()).await;

    if let Err((return_env, err)) = result {
        let expected_uri: RelativeUri = "/node".parse().unwrap();
        assert_eq!(return_env, env);
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
    let path = RelativePath::new("/node", "/lane");

    let mut rx = router.add("/node".parse().unwrap());

    let env = envelope(path.clone(), "a");

    let delays = Arc::new(Mutex::new(vec![]));

    let result = super::dispatch_envelope(
        &mut router,
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
    NonZeroUsize::new(100).unwrap()
}

#[tokio::test]
async fn dispatch_immediate_failure() {
    let addr = RoutingAddr::remote(0);
    let mut router = LocalRoutes::new(addr);
    let mut resolved = HashMap::new();
    let path = RelativePath::new("/node", "/lane");

    let env = envelope(path.clone(), "a");

    let delays = Arc::new(Mutex::new(vec![]));

    let result = super::dispatch_envelope(
        &mut router,
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
            matches!(err, DispatchError::RoutingProblem(RouterError::NoAgentAtRoute(uri)) if uri == expected_uri)
        );
        assert_eq!(err_env, env)
    } else {
        panic!("Unexpected success.")
    }
}

#[tokio::test]
async fn dispatch_after_retry() {
    let addr = RoutingAddr::remote(0);
    let mut router = LocalRoutes::new(addr);
    let mut resolved = HashMap::new();
    let path = RelativePath::new("/node", "/lane");

    let mut rx = router.add_with_countdown("/node".parse().unwrap(), 1);

    let env = envelope(path.clone(), "a");

    let delays = Arc::new(Mutex::new(vec![]));

    let result = super::dispatch_envelope(
        &mut router,
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
    let mut router = LocalRoutes::new(addr);
    let mut resolved = HashMap::new();
    let path = RelativePath::new("/node", "/lane");

    let mut rx = router.add_with_countdown("/node".parse().unwrap(), 1);

    let env = envelope(path.clone(), "a");

    let delays = Arc::new(Mutex::new(vec![]));

    let result = super::dispatch_envelope(
        &mut router,
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

struct TaskFixture {
    router: LocalRoutes,
    task: BoxFuture<'static, ConnectionDropped>,
    sock_in: fut_mpsc::Sender<Result<WsMessage, ConnectionError>>,
    sock_out: fut_mpsc::Receiver<WsMessage>,
    envelope_tx: mpsc::Sender<TaggedEnvelope>,
    stop_trigger: trigger::Sender,
    send_error_tx: watch::Sender<Option<ConnectionError>>,
}

const BUFFER_SIZE: usize = 8;

impl TaskFixture {
    fn new() -> Self {
        let addr = RoutingAddr::remote(0);
        let router = LocalRoutes::new(addr);
        let (tx_in, rx_in) = fut_mpsc::channel(BUFFER_SIZE);
        let (tx_out, rx_out) = fut_mpsc::channel(BUFFER_SIZE);

        let (env_tx, env_rx) = mpsc::channel(BUFFER_SIZE);
        let (stop_tx, stop_rx) = trigger::trigger();
        let (failure_tx, failure_rx) = watch::channel(None);

        let fake_socket =
            TwoWayMpsc::new(tx_out.clone(), rx_in, move |_| failure_rx.borrow().clone());
        let task = ConnectionTask::new(
            fake_socket,
            router.clone(),
            env_rx,
            env_tx.clone(),
            stop_rx,
            ConnectionConfig {
                router_buffer_size: NonZeroUsize::new(10).unwrap(),
                channel_buffer_size: NonZeroUsize::new(10).unwrap(),
                activity_timeout: Duration::from_secs(30),
                write_timeout: Duration::from_secs(20),
                connection_retries: RetryStrategy::immediate(NonZeroUsize::new(1).unwrap()),
                yield_after: NonZeroUsize::new(256).unwrap(),
            },
        )
        .run()
        .boxed();

        TaskFixture {
            router,
            task,
            sock_in: tx_in,
            sock_out: rx_out,
            envelope_tx: env_tx,
            stop_trigger: stop_tx,
            send_error_tx: failure_tx,
        }
    }
}

fn message_for(env: Envelope) -> WsMessage {
    WsMessage::Text(env.into_value().to_string())
}

#[tokio::test]
async fn task_send_message() {
    let TaskFixture {
        task,
        envelope_tx,
        mut sock_out,
        stop_trigger,
        router: _router,
        sock_in: _sock_in,
        send_error_tx: _send_error_tx,
    } = TaskFixture::new();

    let envelope = Envelope::make_event("/node", "/lane", Some(Value::text("a")));
    let env_cpy = envelope.clone();

    let test_case = async move {
        let tagged = TaggedEnvelope(RoutingAddr::local(100), env_cpy.clone());
        assert!(envelope_tx.send(tagged).await.is_ok());

        let message = sock_out.next().await;
        assert_eq!(message, Some(message_for(env_cpy)));
        stop_trigger.trigger();
    };

    let result = timeout::timeout(Duration::from_secs(5), join(task, test_case)).await;
    assert!(matches!(result, Ok((ConnectionDropped::Closed, _))));
}

#[tokio::test]
async fn task_send_message_failure() {
    let TaskFixture {
        task,
        envelope_tx,
        sock_out: _sock_out,
        stop_trigger: _stop_trigger,
        router: _router,
        sock_in: _sock_in,
        send_error_tx,
    } = TaskFixture::new();

    let envelope = Envelope::make_event("/node", "/lane", Some(Value::text("a")));
    let env_cpy = envelope.clone();

    let test_case = async move {
        let tagged = TaggedEnvelope(RoutingAddr::local(100), env_cpy.clone());

        assert!(send_error_tx
            .send(Some(ConnectionError::Io(IoError::new(
                ErrorKind::ConnectionReset,
                None
            ))))
            .is_ok());
        assert!(envelope_tx.send(tagged).await.is_ok());
    };

    let result = timeout::timeout(Duration::from_secs(5), join(task, test_case)).await;
    let _err = ConnectionError::Io(IoError::new(ErrorKind::ConnectionReset, None));
    assert!(matches!(result, Ok((ConnectionDropped::Failed(_err), _))));
}

#[tokio::test]
async fn task_receive_message_with_route() {
    let TaskFixture {
        task,
        envelope_tx: _envelope_tx,
        sock_out: _sock_out,
        stop_trigger,
        router,
        mut sock_in,
        send_error_tx: _send_error_tx,
    } = TaskFixture::new();

    let mut rx = router.add("/node".parse().unwrap());
    let envelope = Envelope::make_event("/node", "/lane", Some(Value::text("a")));
    let env_cpy = envelope.clone();

    let test_case = async move {
        assert!(sock_in.send(Ok(message_for(env_cpy.clone()))).await.is_ok());
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
        envelope_tx: _envelope_tx,
        mut sock_out,
        stop_trigger,
        router: _router,
        mut sock_in,
        send_error_tx: _send_error_tx,
    } = TaskFixture::new();

    let envelope = Envelope::link("/missing", "/lane");
    let response = Envelope::node_not_found("/missing", "/lane");

    let test_case = async move {
        assert!(sock_in.send(Ok(message_for(envelope))).await.is_ok());

        let message = sock_out.next().await;
        assert_eq!(message, Some(message_for(response)));

        stop_trigger.trigger();
    };

    let result = timeout::timeout(Duration::from_secs(5), join(task, test_case)).await;
    assert!(matches!(result, Ok((ConnectionDropped::Closed, _))));
}

#[tokio::test]
async fn task_receive_sync_message_missing_node() {
    let TaskFixture {
        task,
        envelope_tx: _envelope_tx,
        mut sock_out,
        stop_trigger: _stop_trigger,
        router: _router,
        mut sock_in,
        send_error_tx: _send_error_tx,
    } = TaskFixture::new();

    let envelope = Envelope::sync("/missing", "/lane");

    let test_case = async move {
        assert!(sock_in.send(Ok(message_for(envelope))).await.is_ok());
        let message = sock_out.next().await.unwrap();
        let expected: WsMessage = Envelope::node_not_found("/missing", "/lane").into();
        assert_eq!(message, expected);
    };

    let result = timeout::timeout(Duration::from_secs(5), join(task, test_case)).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn task_receive_message_no_route() {
    let TaskFixture {
        task,
        envelope_tx: _envelope_tx,
        sock_out: _sock_out,
        stop_trigger,
        router: _router,
        mut sock_in,
        send_error_tx: _send_error_tx,
    } = TaskFixture::new();

    let envelope = Envelope::make_event("/node", "/lane", Some(Value::text("a")));
    let env_cpy = envelope.clone();

    let test_case = async move {
        assert!(sock_in.send(Ok(message_for(env_cpy.clone()))).await.is_ok());
        stop_trigger.trigger();
    };

    let result = timeout::timeout(Duration::from_secs(5), join(task, test_case)).await;
    assert!(matches!(result, Ok((ConnectionDropped::Closed, _))));
}

#[tokio::test]
async fn task_receive_error() {
    let TaskFixture {
        task,
        envelope_tx: _envelope_tx,
        sock_out: _sock_out,
        stop_trigger: _stop_trigger,
        router: _router,
        mut sock_in,
        send_error_tx: _send_error_tx,
    } = TaskFixture::new();

    let test_case = async move {
        assert!(sock_in
            .send(Err(ConnectionError::Io(IoError::new(
                ErrorKind::ConnectionReset,
                None
            ))))
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
        envelope_tx: _envelope_tx,
        sock_out,
        stop_trigger: _stop_trigger,
        router: _router,
        sock_in,
        send_error_tx: _send_error_tx,
    } = TaskFixture::new();

    let test_case = async move {
        drop(sock_in);
        drop(sock_out);
    };

    let result = timeout::timeout(Duration::from_secs(5), join(task, test_case)).await;
    let _err = ConnectionError::Closed(CloseError::new(CloseErrorKind::ClosedRemotely, None));
    assert!(matches!(result, Ok((ConnectionDropped::Failed(_err), _))));
}

#[tokio::test]
async fn task_timeout() {
    let TaskFixture {
        task,
        envelope_tx,
        mut sock_out,
        stop_trigger: _stop_trigger,
        router: _router,
        sock_in: _sock_in,
        send_error_tx: _send_error_tx,
    } = TaskFixture::new();

    let envelope = Envelope::make_event("/node", "/lane", Some(Value::text("a")));
    let env_cpy = envelope.clone();

    let test_case = async move {
        let tagged = TaggedEnvelope(RoutingAddr::local(100), env_cpy.clone());
        tokio::time::pause();
        assert!(envelope_tx.send(tagged).await.is_ok());

        let message = sock_out.next().await;
        assert!(message.is_some());
        tokio::time::advance(Duration::from_secs(31)).await;
    };

    let result = join(task, test_case).await;
    assert!(matches!(result, (ConnectionDropped::TimedOut(d), _) if d == Duration::from_secs(30)));
}

#[tokio::test]
async fn task_receive_bad_message() {
    let TaskFixture {
        task,
        envelope_tx: _envelope_tx,
        sock_out: _sock_out,
        stop_trigger: _stop_trigger,
        router: _router,
        mut sock_in,
        send_error_tx: _send_error_tx,
    } = TaskFixture::new();

    let test_case = async move {
        assert!(sock_in
            .send(Ok(WsMessage::Text("Boom!".to_string())))
            .await
            .is_ok());
    };

    let result = timeout::timeout(Duration::from_secs(5), join(task, test_case)).await;
    let _err = ConnectionError::Protocol(ProtocolError::warp(None));
    assert!(matches!(result, Ok((ConnectionDropped::Failed(_err), _))));
}

async fn generate_writes(
    mut route_rx: mpsc::Receiver<TaggedEnvelope>,
    outgoing: mpsc::Sender<TaggedEnvelope>,
    stop_trigger: trigger::Sender,
    n: i32,
) -> Result<(), mpsc::error::SendError<TaggedEnvelope>> {
    let addr = RoutingAddr::local(7);

    for i in 0..n {
        if route_rx.recv().await.is_none() {
            return Ok(());
        }
        for j in 0..(BUFFER_SIZE * 3) {
            let envelope = Envelope::make_command(
                "/remote_node",
                "/remote_lane",
                Some(Value::text(format!("{}.{}", i, j))),
            );
            let tagged = TaggedEnvelope(addr, envelope);
            outgoing.send(tagged).await?;
        }
    }
    stop_trigger.trigger();
    Ok(())
}

#[tokio::test]
async fn read_causes_write_buffer_to_fill() {
    let TaskFixture {
        task,
        envelope_tx,
        mut sock_out,
        stop_trigger,
        router,
        sock_in,
        send_error_tx: _send_error_tx,
    } = TaskFixture::new();

    let n = 100;

    let (route_tx, route_rx) = mpsc::channel(1);

    router.add_sender("/node".parse().unwrap(), route_tx.clone());

    let outgoing = envelope_tx.clone();

    let route_task = generate_writes(route_rx, outgoing, stop_trigger, n);

    let consume_output = async move {
        loop {
            if sock_out.next().await.is_none() {
                break;
            }
        }
    };

    let mut input = sock_in.clone();

    let generate_inputs = async move {
        for i in 0..n {
            let envelope =
                Envelope::make_command("/node", "/lane", Some(Value::text(i.to_string())));
            let message = Ok(message_for(envelope));
            if input.send(message).await.is_err() {
                break;
            }
        }
    };

    let result = tokio::time::timeout(
        Duration::from_secs(60),
        join4(task, route_task, consume_output, generate_inputs),
    )
    .await;
    assert!(matches!(result, Ok((ConnectionDropped::Closed, _, _, _))));
}
