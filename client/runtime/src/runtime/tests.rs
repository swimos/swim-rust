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

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::BytesMut;
use futures_util::future::{BoxFuture, Either};
use futures_util::FutureExt;
use ratchet::{Message, NegotiatedExtension, NoExt, Role, WebSocket, WebSocketConfig};
use tokio::io::{duplex, AsyncWriteExt};
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::{mpsc, oneshot, watch, Notify};
use tokio::time::timeout;
use tokio_util::codec::Encoder;
use uuid::Uuid;

use crate::runtime::error::DownlinkErrorKind;
use crate::runtime::transport::TransportHandle;
use crate::{start_runtime, DownlinkRuntimeError, RawHandle, RemotePath, Transport};
use fixture::{MockExternalConnections, MockWs, Server, WsAction};
use swim_api::downlink::{Downlink, DownlinkConfig, DownlinkKind};
use swim_api::error::DownlinkTaskError;
use swim_downlink::lifecycle::{BasicValueDownlinkLifecycle, ValueDownlinkLifecycle};
use swim_downlink::{DownlinkTask, ValueDownlinkModel};
use swim_messages::protocol::{RawRequestMessageEncoder, RequestMessage};
use swim_model::address::{Address, RelativeAddress};
use swim_model::Text;
use swim_remote::AttachClient;
use swim_runtime::downlink::{DownlinkOptions, DownlinkRuntimeConfig};
use swim_runtime::net::{Scheme, SchemeHostPort, SchemeSocketAddr};
use swim_runtime::ws::RatchetError;
use swim_utilities::io::byte_channel::{byte_channel, ByteReader, ByteWriter};
use swim_utilities::non_zero_usize;
use swim_utilities::trigger;
use swim_utilities::trigger::{promise, Sender};

#[tokio::test]
async fn transport_opens_connection_ok() {
    let peer = SchemeHostPort::new(Scheme::Ws, "127.0.0.1".to_string(), 9001);
    let sock: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let (client, server) = duplex(128);
    let ext = MockExternalConnections::new(
        [(
            peer.clone(),
            SchemeSocketAddr::new(Scheme::Ws, sock.clone()),
        )],
        [("127.0.0.1:9001".parse().unwrap(), client)],
    );
    let ws = MockWs::new([("127.0.0.1".to_string(), WsAction::Open)]);
    let transport = Transport::new(ext, ws, non_zero_usize!(128));

    let (transport_tx, transport_rx) = mpsc::channel(128);
    let _transport_task = tokio::spawn(transport.run(transport_rx));

    let handle = TransportHandle::new(transport_tx);

    let addrs = handle.resolve(peer).await.expect("Failed to resolve peer");
    assert_eq!(addrs, vec![sock]);

    let (opened_sock, attach) = handle
        .connection_for("127.0.0.1".to_string(), vec![sock])
        .await
        .expect("Failed to open connection");
    assert_eq!(opened_sock, sock);

    let (byte_tx1, _byte_rx1) = byte_channel(non_zero_usize!(128));
    let (mut byte_tx2, byte_rx2) = byte_channel(non_zero_usize!(128));
    let (open_tx, open_rx) = oneshot::channel();

    attach
        .send(AttachClient::AttachDownlink {
            downlink_id: Uuid::nil(),
            path: RelativeAddress::new(Text::new("node"), Text::new("lane")),
            sender: byte_tx1,
            receiver: byte_rx2,
            done: open_tx,
        })
        .await
        .expect("Failed to attach downlink");
    open_rx.await.unwrap().expect("Failed to open downlink");

    let mut buf = BytesMut::new();

    RawRequestMessageEncoder
        .encode(
            RequestMessage::<Text, String>::link(
                Uuid::nil(),
                RelativeAddress::new("node".into(), "lane".into()),
            ),
            &mut buf,
        )
        .unwrap();

    byte_tx2.write_all(&mut buf).await.unwrap();

    buf.clear();

    let mut ws_server = WebSocket::from_upgraded(
        WebSocketConfig::default(),
        server,
        NegotiatedExtension::from(NoExt),
        buf,
        Role::Server,
    );

    let mut buf = BytesMut::new();
    let message = ws_server.read(&mut buf).await.unwrap();
    assert_eq!(message, Message::Text);
    let link_message = std::str::from_utf8(buf.as_ref()).unwrap();
    assert_eq!(link_message, "@link(node:node,lane:lane)");

    let (opened_sock, attach_2) = handle
        .connection_for("127.0.0.1".to_string(), vec![sock])
        .await
        .expect("Failed to open connection");
    assert_eq!(opened_sock, sock);
    assert!(attach.same_channel(&attach_2));
}

#[tokio::test]
async fn transport_opens_connection_err() {
    let peer = SchemeHostPort::new(Scheme::Ws, "127.0.0.1".to_string(), 9001);
    let sock: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let (client, _server) = duplex(128);
    let ext = MockExternalConnections::new(
        [(
            peer.clone(),
            SchemeSocketAddr::new(Scheme::Ws, sock.clone()),
        )],
        [("127.0.0.1:9001".parse().unwrap(), client)],
    );
    let ws = MockWs::new([(
        "127.0.0.1".to_string(),
        WsAction::fail(|| RatchetError::from(ratchet::Error::new(ratchet::ErrorKind::Http))),
    )]);
    let transport = Transport::new(ext, ws, non_zero_usize!(128));

    let (transport_tx, transport_rx) = mpsc::channel(128);
    let _transport_task = tokio::spawn(transport.run(transport_rx));

    let handle = TransportHandle::new(transport_tx);

    let addrs = handle.resolve(peer).await.expect("Failed to resolve peer");
    assert_eq!(addrs, vec![sock]);

    let actual_err = handle
        .connection_for("127.0.0.1".to_string(), vec![sock])
        .await
        .expect_err("Expected connection to fail");
    assert!(actual_err.is(DownlinkErrorKind::Connection));
    assert!(actual_err.downcast_ref::<RatchetError>().is_some());
}

struct TrackingDownlink<LC> {
    spawned: Arc<Notify>,
    stopped: Arc<Notify>,
    inner: DownlinkTask<ValueDownlinkModel<i32, LC>>,
}

impl<LC> TrackingDownlink<LC> {
    fn new(
        spawned: Arc<Notify>,
        stopped: Arc<Notify>,
        inner: ValueDownlinkModel<i32, LC>,
    ) -> TrackingDownlink<LC> {
        TrackingDownlink {
            spawned,
            stopped,
            inner: DownlinkTask::new(inner),
        }
    }
}

impl<LC> Downlink for TrackingDownlink<LC>
where
    LC: ValueDownlinkLifecycle<i32> + 'static,
{
    fn kind(&self) -> DownlinkKind {
        DownlinkKind::Value
    }

    fn run(
        self,
        path: Address<Text>,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>> {
        let TrackingDownlink {
            spawned,
            stopped,
            inner,
        } = self;
        let task = async move {
            spawned.notify_one();
            let result = inner.run(path, config, input, output).await;
            stopped.notify_one();
            result
        };
        Box::pin(task)
    }

    fn run_boxed(
        self: Box<Self>,
        path: Address<Text>,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>> {
        (*self).run(path, config, input, output)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum TestMessage<T> {
    Linked,
    Synced(T),
    Event(T),
    Set(Option<Arc<T>>, T),
    Unlinked,
}

fn default_lifecycle<T>(tx: mpsc::UnboundedSender<TestMessage<T>>) -> impl ValueDownlinkLifecycle<T>
where
    T: Clone + Send + Sync + 'static,
{
    BasicValueDownlinkLifecycle::<T>::default()
        .with(tx)
        .on_linked_blocking(|tx| {
            assert!(tx.send(TestMessage::Linked).is_ok());
        })
        .on_synced_blocking(|tx, v| {
            assert!(tx.send(TestMessage::Synced(v.clone())).is_ok());
        })
        .on_event_blocking(|tx, v| {
            assert!(tx.send(TestMessage::Event(v.clone())).is_ok());
        })
        .on_set_blocking(|tx, before, after| {
            assert!(tx
                .send(TestMessage::Set(before.cloned(), after.clone()))
                .is_ok());
        })
        .on_unlinked_blocking(|tx| {
            assert!(tx.send(TestMessage::Unlinked).is_ok());
        })
}

struct DownlinkContext {
    handle: RawHandle,
    spawned: Arc<Notify>,
    stopped: Arc<Notify>,
    set_tx: mpsc::Sender<i32>,
    get_rx: watch::Receiver<Arc<i32>>,
    server: Server,
    promise: promise::Receiver<Result<(), DownlinkRuntimeError>>,
    stop_tx: trigger::Sender,
}

fn start() -> (RawHandle, Sender, Server) {
    let peer = SchemeHostPort::new(Scheme::Ws, "127.0.0.1".to_string(), 80);
    let sock: SocketAddr = "127.0.0.1:80".parse().unwrap();
    let (client, server) = duplex(128);
    let ext = MockExternalConnections::new(
        [(
            peer.clone(),
            SchemeSocketAddr::new(Scheme::Ws, sock.clone()),
        )],
        [("127.0.0.1:80".parse().unwrap(), client)],
    );
    let ws = MockWs::new([("127.0.0.1".to_string(), WsAction::Open)]);

    let (handle, stop) = start_runtime(Transport::new(ext, ws, non_zero_usize!(128)));
    (handle, stop, Server::new(server))
}

async fn test_downlink<LC, F, Fut>(lifecycle: LC, test: F)
where
    LC: ValueDownlinkLifecycle<i32> + Send + Sync + 'static,
    F: FnOnce(DownlinkContext) -> Fut,
    Fut: Future,
{
    let (handle, stop_tx, server) = start();
    let TrackingContext {
        spawned,
        stopped,
        set_tx,
        get_rx,
        promise,
    } = tracking_downlink(&handle, lifecycle, DownlinkRuntimeConfig::default()).await;

    let context = DownlinkContext {
        handle,
        spawned,
        stopped,
        set_tx,
        get_rx,
        server,
        promise,
        stop_tx,
    };
    assert!(timeout(Duration::from_secs(5), test(context)).await.is_ok());
}

#[tokio::test]
async fn spawns_downlink() {
    let (msg_tx, _msg_rx) = unbounded_channel();
    test_downlink(default_lifecycle(msg_tx), |ctx| async move {
        ctx.spawned.notified().await;
    })
    .await;
}

#[tokio::test]
async fn stops_on_disconnect() {
    let (msg_tx, _msg_rx) = unbounded_channel();
    test_downlink(default_lifecycle(msg_tx), |ctx| async move {
        let DownlinkContext {
            handle: _raw,
            stop_tx: _stop_tx,
            spawned,
            stopped,
            server,
            promise,
            ..
        } = ctx;
        spawned.notified().await;
        drop(server);
        stopped.notified().await;

        assert!(promise.await.is_ok());
    })
    .await;
}

#[tokio::test]
async fn lifecycle() {
    let (msg_tx, mut msg_rx) = unbounded_channel();
    test_downlink(default_lifecycle(msg_tx), |ctx| async move {
        let DownlinkContext {
            handle: _raw,
            spawned,
            stopped,
            set_tx,
            get_rx,
            mut server,
            promise,
            stop_tx,
        } = ctx;
        spawned.notified().await;

        let mut lane = server.lane_for("node", "lane");

        lane.await_link().await;
        assert_eq!(msg_rx.recv().await.unwrap(), TestMessage::Linked);

        lane.await_sync(7).await;
        assert_eq!(msg_rx.recv().await.unwrap(), TestMessage::Synced(7));
        {
            let state = get_rx.borrow();
            assert_eq!(*state.as_ref(), 7);
        }

        set_tx.send(13).await.unwrap();
        lane.await_command(13).await;

        lane.send_unlinked().await;
        assert_eq!(msg_rx.recv().await.unwrap(), TestMessage::Unlinked);

        assert!(stop_tx.trigger());
        lane.await_closed().await;

        assert_eq!(msg_rx.recv().now_or_never().unwrap(), None);
        stopped.notified().await;
        assert!(promise.await.unwrap().is_ok());
    })
    .await;
}

async fn tracking_downlink<LC>(
    handle: &RawHandle,
    lifecycle: LC,
    config: DownlinkRuntimeConfig,
) -> TrackingContext
where
    LC: ValueDownlinkLifecycle<i32> + Send + Sync + 'static,
{
    let spawned = Arc::new(Notify::new());
    let stopped = Arc::new(Notify::new());

    let (set_tx, set_rx) = mpsc::channel(128);
    let (get_tx, get_rx) = watch::channel(Arc::new(0));

    let downlink = TrackingDownlink::new(
        spawned.clone(),
        stopped.clone(),
        ValueDownlinkModel::new(set_rx, get_tx, lifecycle),
    );

    let promise = handle
        .run_downlink(
            RemotePath::new("ws://127.0.0.1", "node", "lane"),
            config,
            Default::default(),
            DownlinkOptions::SYNC,
            downlink,
        )
        .await
        .expect("Failed to spawn downlink open request");

    TrackingContext {
        spawned,
        stopped,
        set_tx,
        get_rx,
        promise,
    }
}

struct TrackingContext {
    spawned: Arc<Notify>,
    stopped: Arc<Notify>,
    set_tx: mpsc::Sender<i32>,
    get_rx: watch::Receiver<Arc<i32>>,
    promise: promise::Receiver<Result<(), DownlinkRuntimeError>>,
}

struct State {
    fail_after_first: bool,
    tx: mpsc::UnboundedSender<TestMessage<i32>>,
    expected_events: Vec<i32>,
}

impl State {
    pub fn new(fail_after_first: bool, tx: mpsc::UnboundedSender<TestMessage<i32>>) -> State {
        State {
            fail_after_first,
            tx,
            expected_events: vec![1, 2, 3],
        }
    }

    fn make_lifecycle(
        tx: mpsc::UnboundedSender<TestMessage<i32>>,
        fail_after_first: bool,
    ) -> impl ValueDownlinkLifecycle<i32> {
        BasicValueDownlinkLifecycle::default()
            .with(State::new(fail_after_first, tx))
            .on_linked_blocking(|state| {
                assert!(state.tx.send(TestMessage::Linked).is_ok());
            })
            .on_synced_blocking(|state, v| {
                assert!(state.tx.send(TestMessage::Synced(*v)).is_ok());
            })
            .on_event_blocking(|state, v| {
                if state.fail_after_first && state.expected_events.len() != 3 {
                    panic!()
                } else if state.expected_events.len() != 0 {
                    let i = state.expected_events.remove(0);
                    assert_eq!(*v, i);
                    assert!(state.tx.send(TestMessage::Event(i)).is_ok());
                } else {
                    panic!()
                }
            })
            .on_set_blocking(|state, before, after| {
                assert!(state
                    .tx
                    .send(TestMessage::Set(before.cloned(), after.clone()))
                    .is_ok());
            })
            .on_unlinked_blocking(|state| {
                assert!(state.tx.send(TestMessage::Unlinked).is_ok());
            })
    }
}

pub struct Bias<L, R> {
    towards: L,
    other: R,
}

fn bias<L, R>(towards: L, other: R) -> impl Future<Output = Either<L::Output, R::Output>>
where
    L: Future + Unpin,
    R: Future + Unpin,
{
    Bias { towards, other }
}

impl<L, R> Future for Bias<L, R>
where
    L: Future + Unpin,
    R: Future + Unpin,
{
    type Output = Either<L::Output, R::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Poll::Ready(l) = Pin::new(&mut self.towards).poll(cx) {
            Poll::Ready(Either::Left(l))
        } else {
            Pin::new(&mut self.other).poll(cx).map(Either::Right)
        }
    }
}

struct ClosableDownlink<LC> {
    trigger: trigger::Receiver,
    stopped: Arc<Notify>,
    inner: DownlinkTask<ValueDownlinkModel<i32, LC>>,
}

impl<LC> ClosableDownlink<LC> {
    pub fn new(
        trigger: trigger::Receiver,
        stopped: Arc<Notify>,
        inner: ValueDownlinkModel<i32, LC>,
    ) -> ClosableDownlink<LC> {
        ClosableDownlink {
            trigger,
            stopped,
            inner: DownlinkTask::new(inner),
        }
    }
}

impl<LC> Downlink for ClosableDownlink<LC>
where
    LC: ValueDownlinkLifecycle<i32> + 'static,
{
    fn kind(&self) -> DownlinkKind {
        DownlinkKind::Value
    }

    fn run(
        self,
        path: Address<Text>,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>> {
        let ClosableDownlink {
            stopped,
            trigger,
            inner,
        } = self;
        let task = async move {
            match bias(trigger, inner.run(path, config, input, output)).await {
                Either::Left(Ok(())) => {
                    stopped.notify_one();
                    Ok(())
                }
                Either::Left(Err(e)) => {
                    panic!("{:?}", e)
                }
                Either::Right(out) => {
                    panic!("Downlink completed before trigger. Output: {:?}", out)
                }
            }
        };
        Box::pin(task)
    }

    fn run_boxed(
        self: Box<Self>,
        path: Address<Text>,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>> {
        (self).run(path, config, input, output)
    }
}

struct ClosableContext {
    stopped: Arc<Notify>,
    shutdown: trigger::Sender,
    _set_tx: mpsc::Sender<i32>,
    get_rx: watch::Receiver<Arc<i32>>,
    promise: promise::Receiver<Result<(), DownlinkRuntimeError>>,
}

async fn closable_downlink<LC>(
    handle: &RawHandle,
    lifecycle: LC,
    config: DownlinkRuntimeConfig,
) -> ClosableContext
where
    LC: ValueDownlinkLifecycle<i32> + Send + Sync + 'static,
{
    let stopped = Arc::new(Notify::new());

    let (set_tx, set_rx) = mpsc::channel(128);
    let (get_tx, get_rx) = watch::channel(Arc::new(0));

    let (shutdown_tx, shutdown_rx) = trigger::trigger();

    let downlink = ClosableDownlink::new(
        shutdown_rx,
        stopped.clone(),
        ValueDownlinkModel::new(set_rx, get_tx, lifecycle),
    );

    let promise = handle
        .run_downlink(
            RemotePath::new("ws://127.0.0.1", "node", "lane"),
            config,
            Default::default(),
            DownlinkOptions::SYNC,
            downlink,
        )
        .await
        .expect("Failed to spawn downlink open request");

    ClosableContext {
        stopped,
        shutdown: shutdown_tx,
        _set_tx: set_tx,
        get_rx,
        promise,
    }
}

/// Tests that disjoint runtime configurations start different runtimes for the same host
#[tokio::test]
async fn different_configurations() {
    let (handle, _stop_tx, mut server) = start();

    let (succeeding_tx, succeeding_rx) = mpsc::unbounded_channel();
    let succeeding_lifecycle = State::make_lifecycle(succeeding_tx, false);

    let (failing_tx, failing_rx) = mpsc::unbounded_channel();
    let failing_lifecycle = State::make_lifecycle(failing_tx, true);

    let succeeding_context = tracking_downlink(
        &handle,
        succeeding_lifecycle,
        DownlinkRuntimeConfig {
            empty_timeout: Duration::from_secs(30),
            ..Default::default()
        },
    )
    .await;
    let trigger_context = closable_downlink(
        &handle,
        failing_lifecycle,
        DownlinkRuntimeConfig {
            empty_timeout: Duration::from_secs(2),
            ..Default::default()
        },
    )
    .await;

    let mut receivers = (succeeding_rx, failing_rx);

    let mut lane = server.lane_for("node", "lane");
    lane.await_link().await;

    type Channel = mpsc::UnboundedReceiver<TestMessage<i32>>;
    async fn op_receivers<'f, Fun, Fut>(pair: &'f mut (Channel, Channel), f: Fun)
    where
        Fun: Fn(&'f mut Channel) -> Fut,
        Fut: Future<Output = ()> + 'f,
    {
        f(&mut pair.0).await;
        f(&mut pair.1).await;
    }

    op_receivers(&mut receivers, |rx| async move {
        assert_eq!(rx.recv().await.unwrap(), TestMessage::Linked);
    })
    .await;

    lane.await_sync(7).await;

    op_receivers(&mut receivers, |rx| async move {
        assert_eq!(rx.recv().await.unwrap(), TestMessage::Synced(7));
    })
    .await;

    {
        let state = succeeding_context.get_rx.borrow();
        assert_eq!(*state.as_ref(), 7);
    }
    {
        let state = trigger_context.get_rx.borrow();
        assert_eq!(*state.as_ref(), 7);
    }

    lane.send_event(1).await;

    op_receivers(&mut receivers, |rx| async move {
        assert_eq!(rx.recv().await.unwrap(), TestMessage::Event(1));
    })
    .await;

    op_receivers(&mut receivers, |rx| async move {
        assert_eq!(
            rx.recv().await.unwrap(),
            TestMessage::Set(Some(Arc::new(7)), 1)
        );
    })
    .await;

    assert!(trigger_context.shutdown.trigger());
    trigger_context.stopped.notified().await;

    assert!(receivers.0.recv().now_or_never().is_none());
    assert!(succeeding_context
        .stopped
        .notified()
        .now_or_never()
        .is_none());
    assert!(succeeding_context.promise.now_or_never().is_none());

    assert!(trigger_context.promise.await.is_ok());
}

#[tokio::test]
async fn failed_handshake() {
    let peer = SchemeHostPort::new(Scheme::Ws, "127.0.0.1".to_string(), 80);
    let sock: SocketAddr = "127.0.0.1:80".parse().unwrap();
    let (client, _server) = duplex(128);
    let ext = MockExternalConnections::new(
        [(
            peer.clone(),
            SchemeSocketAddr::new(Scheme::Ws, sock.clone()),
        )],
        [("127.0.0.1:80".parse().unwrap(), client)],
    );

    let ws = MockWs::new([(
        "127.0.0.1".to_string(),
        WsAction::fail(|| RatchetError::from(ratchet::Error::new(ratchet::ErrorKind::Http))),
    )]);

    let (handle, _stop) = start_runtime(Transport::new(ext, ws, non_zero_usize!(128)));

    let spawned = Arc::new(Notify::new());
    let stopped = Arc::new(Notify::new());

    let (_set_tx, set_rx) = mpsc::channel(128);
    let (get_tx, _get_rx) = watch::channel(Arc::new(0));

    let downlink = TrackingDownlink::new(
        spawned.clone(),
        stopped.clone(),
        ValueDownlinkModel::new(set_rx, get_tx, BasicValueDownlinkLifecycle::default()),
    );

    let promise = handle
        .run_downlink(
            RemotePath::new("ws://127.0.0.1", "node", "lane"),
            Default::default(),
            Default::default(),
            DownlinkOptions::SYNC,
            downlink,
        )
        .await;
    assert!(promise.is_err());
    let actual_err = promise.unwrap_err();
    assert!(actual_err.is(DownlinkErrorKind::WebsocketNegotiationFailed));
    assert!(actual_err.downcast_ref::<RatchetError>().is_some());
}
