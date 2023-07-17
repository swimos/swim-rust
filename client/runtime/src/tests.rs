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

use std::fmt::Debug;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use ratchet::{
    Message, NegotiatedExtension, NoExt, NoExtProvider, Role, WebSocket, WebSocketConfig,
};
use tokio::io::{duplex, AsyncWriteExt};
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::codec::Encoder;
use uuid::Uuid;

use crate::error::{DownlinkErrorKind, DownlinkRuntimeError};
use crate::models::RemotePath;
use crate::runtime::{start_runtime, RawHandle};
use crate::transport::{Transport, TransportHandle};
use fixture::{MockClientConnections, MockWs, Server, WsAction};
use swim_api::downlink::{Downlink, DownlinkConfig, DownlinkKind};
use swim_api::error::DownlinkTaskError;
use swim_downlink::lifecycle::{BasicValueDownlinkLifecycle, ValueDownlinkLifecycle};
use swim_downlink::{DownlinkTask, NotYetSyncedError, ValueDownlinkModel, ValueDownlinkOperation};
use swim_messages::protocol::{RawRequestMessageEncoder, RequestMessage};
use swim_model::address::{Address, RelativeAddress};
use swim_model::Text;
use swim_remote::AttachClient;
use swim_runtime::downlink::{DownlinkOptions, DownlinkRuntimeConfig};
use swim_runtime::net::{Scheme, SchemeHostPort};
use swim_runtime::ws::RatchetError;
use swim_utilities::io::byte_channel::{byte_channel, ByteReader, ByteWriter};
use swim_utilities::non_zero_usize;
use swim_utilities::trigger::{promise, trigger, Sender};

async fn get_value<T>(tx: mpsc::Sender<ValueDownlinkOperation<T>>) -> Result<T, NotYetSyncedError>
where
    T: Debug,
{
    let (callback_tx, callback_rx) = oneshot::channel();
    tx.send(ValueDownlinkOperation::Get(callback_tx))
        .await
        .unwrap();
    callback_rx.await.unwrap()
}

#[tokio::test]
async fn transport_opens_connection_ok() {
    let peer = SchemeHostPort::new(Scheme::Ws, "127.0.0.1".to_string(), 9001);
    let sock: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let (client, server) = duplex(128);
    let ext =
        MockClientConnections::new([(("127.0.0.1".to_string(), 9001), sock)], [(sock, client)]);
    let ws = MockWs::new([("127.0.0.1".to_string(), WsAction::Open)]);
    let transport = Transport::new(
        ext,
        ws,
        NoExtProvider,
        non_zero_usize!(128),
        Duration::from_secs(5),
    );

    let (transport_tx, transport_rx) = mpsc::channel(128);
    let _transport_task = tokio::spawn(transport.run(transport_rx));

    let handle = TransportHandle::new(transport_tx);

    let addrs = handle.resolve(peer).await.expect("Failed to resolve peer");
    assert_eq!(addrs, vec![sock]);

    let (opened_sock, attach) = handle
        .connection_for(Scheme::Ws, "127.0.0.1".to_string(), vec![sock])
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

    byte_tx2.write_all(&buf).await.unwrap();

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
        .connection_for(Scheme::Ws, "127.0.0.1".to_string(), vec![sock])
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
    let ext =
        MockClientConnections::new([(("127.0.0.1".to_string(), 9001), sock)], [(sock, client)]);
    let ws = MockWs::new([(
        "127.0.0.1".to_string(),
        WsAction::fail(|| RatchetError::from(ratchet::Error::new(ratchet::ErrorKind::Http))),
    )]);
    let transport = Transport::new(
        ext,
        ws,
        NoExtProvider,
        non_zero_usize!(128),
        Duration::from_secs(5),
    );

    let (transport_tx, transport_rx) = mpsc::channel(128);
    let _transport_task = tokio::spawn(transport.run(transport_rx));

    let handle = TransportHandle::new(transport_tx);

    let addrs = handle.resolve(peer).await.expect("Failed to resolve peer");
    assert_eq!(addrs, vec![sock]);

    let actual_err = handle
        .connection_for(Scheme::Ws, "127.0.0.1".to_string(), vec![sock])
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
    Set(Option<T>, T),
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
    handle_tx: mpsc::Sender<ValueDownlinkOperation<i32>>,
    server: Server,
    promise: promise::Receiver<Result<(), DownlinkRuntimeError>>,
    stop_tx: Sender,
}

struct Fixture {
    handle: RawHandle,
    stop_tx: Sender,
    server: Server,
    _jh: JoinHandle<()>,
}

fn start() -> Fixture {
    let sock: SocketAddr = "127.0.0.1:80".parse().unwrap();
    let (client, server) = duplex(128);
    let ext = MockClientConnections::new([(("127.0.0.1".to_string(), 80), sock)], [(sock, client)]);
    let ws = MockWs::new([("127.0.0.1".to_string(), WsAction::Open)]);

    let (stop_tx, stop_rx) = trigger();

    let (handle, task) = start_runtime(
        non_zero_usize!(32),
        stop_rx,
        Transport::new(
            ext,
            ws,
            NoExtProvider,
            non_zero_usize!(128),
            Duration::from_secs(5),
        ),
        non_zero_usize!(32),
    );

    Fixture {
        handle,
        stop_tx,
        server: Server::new(server),
        _jh: tokio::spawn(task),
    }
}

async fn test_downlink<LC, F, Fut>(lifecycle: LC, test: F)
where
    LC: ValueDownlinkLifecycle<i32> + Send + Sync + 'static,
    F: FnOnce(DownlinkContext) -> Fut,
    Fut: Future,
{
    let Fixture {
        handle,
        stop_tx,
        server,
        _jh,
    } = start();
    let TrackingContext {
        spawned,
        stopped,
        handle_tx,
        promise,
    } = tracking_downlink(&handle, lifecycle, DownlinkRuntimeConfig::default()).await;

    let context = DownlinkContext {
        handle,
        spawned,
        stopped,
        handle_tx,
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
            handle_tx,
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

        assert_eq!(get_value(handle_tx.clone()).await.unwrap(), 7);

        handle_tx
            .send(ValueDownlinkOperation::Set(13))
            .await
            .unwrap();

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

    let (handle_tx, handle_rx) = mpsc::channel(128);

    let downlink = TrackingDownlink::new(
        spawned.clone(),
        stopped.clone(),
        ValueDownlinkModel::new(handle_rx, lifecycle),
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
        handle_tx,
        promise,
    }
}

struct TrackingContext {
    spawned: Arc<Notify>,
    stopped: Arc<Notify>,
    handle_tx: mpsc::Sender<ValueDownlinkOperation<i32>>,
    promise: promise::Receiver<Result<(), DownlinkRuntimeError>>,
}

#[tokio::test]
async fn failed_handshake() {
    let sock: SocketAddr = "127.0.0.1:80".parse().unwrap();
    let (client, _server) = duplex(128);
    let ext = MockClientConnections::new([(("127.0.0.1".to_string(), 80), sock)], [(sock, client)]);

    let ws = MockWs::new([(
        "127.0.0.1".to_string(),
        WsAction::fail(|| RatchetError::from(ratchet::Error::new(ratchet::ErrorKind::Http))),
    )]);

    let (_stop_tx, stop_rx) = trigger();
    let (handle, task) = start_runtime(
        non_zero_usize!(128),
        stop_rx,
        Transport::new(
            ext,
            ws,
            NoExtProvider,
            non_zero_usize!(128),
            Duration::from_secs(5),
        ),
        non_zero_usize!(32),
    );
    let _jh = tokio::spawn(task);

    let spawned = Arc::new(Notify::new());
    let stopped = Arc::new(Notify::new());

    let (_handle_tx, handle_rx) = mpsc::channel(128);

    let downlink = TrackingDownlink::new(
        spawned.clone(),
        stopped.clone(),
        ValueDownlinkModel::new(handle_rx, BasicValueDownlinkLifecycle::default()),
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
