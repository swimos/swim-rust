// Copyright 2015-2023 Swim Inc.
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

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use ratchet::{
    Message, NegotiatedExtension, NoExt, NoExtProvider, Role, WebSocket, WebSocketConfig,
};
use swimos_agent_protocol::MapMessage;
use swimos_remote::net::{Scheme, SchemeHostPort};
use tokio::io::{duplex, AsyncWriteExt};
use tokio::spawn;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::{mpsc, oneshot, Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::codec::Encoder;
use uuid::Uuid;

use fixture::{MockClientConnections, MockWs, Server, WsAction};
use swimos_api::{
    address::{Address, RelativeAddress},
    agent::DownlinkKind,
    error::DownlinkTaskError,
};
use swimos_client_api::{Downlink, DownlinkConfig};
use swimos_downlink::lifecycle::{
    BasicMapDownlinkLifecycle, BasicValueDownlinkLifecycle, MapDownlinkLifecycle,
    ValueDownlinkLifecycle,
};
use swimos_downlink::{
    DownlinkTask, MapDownlinkHandle, MapDownlinkModel, ValueDownlinkModel, ValueDownlinkSet,
};
use swimos_form::Form;
use swimos_messages::protocol::{RawRequestMessageEncoder, RequestMessage};
use swimos_model::Text;
use swimos_remote::ws::RatchetError;
use swimos_remote::AttachClient;
use swimos_runtime::downlink::{DownlinkOptions, DownlinkRuntimeConfig};
use swimos_utilities::byte_channel::{byte_channel, ByteReader, ByteWriter};
use swimos_utilities::trigger::{promise, trigger};
use swimos_utilities::{non_zero_usize, trigger};

use crate::error::{DownlinkErrorKind, DownlinkRuntimeError};
use crate::models::RemotePath;
use crate::runtime::{start_runtime, RawHandle};
use crate::transport::{Transport, TransportHandle};

#[tokio::test]
async fn transport_opens_connection_ok() {
    let peer = SchemeHostPort::new(Scheme::Ws, "127.0.0.1".to_string(), 80);
    let sock: SocketAddr = "127.0.0.1:80".parse().unwrap();
    let (client, server) = duplex(128);
    let ext = MockClientConnections::new([(("127.0.0.1".to_string(), 80), sock)], [(sock, client)]);
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
            path: RelativeAddress::new(Text::new("node"), Text::new("value_lane")),
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
                RelativeAddress::new("node".into(), "value_lane".into()),
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
    assert_eq!(link_message, "@link(node:node,lane:value_lane)");

    let (opened_sock, attach_2) = handle
        .connection_for(Scheme::Ws, "127.0.0.1".to_string(), vec![sock])
        .await
        .expect("Failed to open connection");
    assert_eq!(opened_sock, sock);
    assert!(attach.same_channel(&attach_2));
}

#[tokio::test]
async fn transport_opens_connection_err() {
    let peer = SchemeHostPort::new(Scheme::Ws, "127.0.0.1".to_string(), 80);
    let sock: SocketAddr = "127.0.0.1:80".parse().unwrap();
    let (client, _server) = duplex(128);
    let ext = MockClientConnections::new([(("127.0.0.1".to_string(), 80), sock)], [(sock, client)]);
    let ws = MockWs::new([(
        "ws://127.0.0.1".to_string(),
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

struct TrackingValueDownlink<LC> {
    spawned: Arc<Notify>,
    stopped: Arc<Notify>,
    inner: DownlinkTask<ValueDownlinkModel<i32, LC>>,
}

impl<LC> TrackingValueDownlink<LC> {
    fn new(
        spawned: Arc<Notify>,
        stopped: Arc<Notify>,
        inner: ValueDownlinkModel<i32, LC>,
    ) -> TrackingValueDownlink<LC> {
        TrackingValueDownlink {
            spawned,
            stopped,
            inner: DownlinkTask::new(inner),
        }
    }
}

impl<LC> Downlink for TrackingValueDownlink<LC>
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
        let TrackingValueDownlink {
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

struct TrackingMapDownlink<LC> {
    spawned: Arc<Notify>,
    stopped: Arc<Notify>,
    inner: DownlinkTask<MapDownlinkModel<i32, i32, LC>>,
}

impl<LC> TrackingMapDownlink<LC> {
    fn new(
        spawned: Arc<Notify>,
        stopped: Arc<Notify>,
        inner: MapDownlinkModel<i32, i32, LC>,
    ) -> TrackingMapDownlink<LC> {
        TrackingMapDownlink {
            spawned,
            stopped,
            inner: DownlinkTask::new(inner),
        }
    }
}

impl<LC> Downlink for TrackingMapDownlink<LC>
where
    LC: MapDownlinkLifecycle<i32, i32> + 'static,
{
    fn kind(&self) -> DownlinkKind {
        DownlinkKind::Map
    }

    fn run(
        self,
        path: Address<Text>,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>> {
        let TrackingMapDownlink {
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
enum ValueTestMessage<T> {
    Linked,
    Synced(T),
    Event(T),
    Set(Option<T>, T),
    Unlinked,
}

fn value_lifecycle<T>(
    tx: mpsc::UnboundedSender<ValueTestMessage<T>>,
) -> impl ValueDownlinkLifecycle<T>
where
    T: Clone + Send + Sync + 'static,
{
    BasicValueDownlinkLifecycle::<T>::default()
        .with(tx)
        .on_linked_blocking(|tx| {
            assert!(tx.send(ValueTestMessage::Linked).is_ok());
        })
        .on_synced_blocking(|tx, v| {
            assert!(tx.send(ValueTestMessage::Synced(v.clone())).is_ok());
        })
        .on_event_blocking(|tx, v| {
            assert!(tx.send(ValueTestMessage::Event(v.clone())).is_ok());
        })
        .on_set_blocking(|tx, before, after| {
            assert!(tx
                .send(ValueTestMessage::Set(before.cloned(), after.clone()))
                .is_ok());
        })
        .on_unlinked_blocking(|tx| {
            assert!(tx.send(ValueTestMessage::Unlinked).is_ok());
        })
}

#[derive(Debug, PartialEq, Eq)]
enum MapTestMessage<K, V> {
    Linked,
    Synced(BTreeMap<K, V>),
    Event(MapMessage<K, V>),
    Unlinked,
}

fn map_lifecycle<K, V>(
    tx: mpsc::UnboundedSender<MapTestMessage<K, V>>,
) -> impl MapDownlinkLifecycle<K, V>
where
    K: Ord + Clone + Form + Send + Sync + Eq + Hash + 'static,
    V: Clone + Form + Send + Sync + 'static,
{
    BasicMapDownlinkLifecycle::<K, V>::default()
        .with(tx)
        .on_linked_blocking(|tx| {
            assert!(tx.send(MapTestMessage::Linked).is_ok());
        })
        .on_synced_blocking(|tx, map| {
            assert!(tx.send(MapTestMessage::Synced(map.clone())).is_ok());
        })
        .on_update_blocking(|tx, key, _, _, new_value| {
            let value: V = new_value.clone();
            assert!(tx
                .send(MapTestMessage::Event(MapMessage::Update { key, value }))
                .is_ok());
        })
        .on_removed_blocking(|tx, key, _, _| {
            assert!(tx
                .send(MapTestMessage::Event(MapMessage::Remove { key }))
                .is_ok());
        })
        .on_clear_blocking(|tx, _| {
            assert!(tx.send(MapTestMessage::Event(MapMessage::Clear)).is_ok());
        })
        .on_unlink_blocking(|tx| {
            assert!(tx.send(MapTestMessage::Unlinked).is_ok());
        })
}

struct ValueDownlinkContext {
    handle: RawHandle,
    spawned: Arc<Notify>,
    stopped: Arc<Notify>,
    handle_tx: mpsc::Sender<ValueDownlinkSet<i32>>,
    server: Server,
    promise: promise::Receiver<Result<(), Arc<DownlinkRuntimeError>>>,
    stop_tx: trigger::Sender,
}

struct Fixture {
    handle: RawHandle,
    stop_tx: trigger::Sender,
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
        true,
    );

    Fixture {
        handle,
        stop_tx,
        server: Server::new(server),
        _jh: tokio::spawn(task),
    }
}

async fn run_value_downlink<LC, F, Fut>(lifecycle: LC, test: F)
where
    LC: ValueDownlinkLifecycle<i32> + Send + Sync + 'static,
    F: FnOnce(ValueDownlinkContext) -> Fut,
    Fut: Future,
{
    let Fixture {
        handle,
        stop_tx,
        server,
        _jh,
    } = start();
    let TrackingValueContext {
        spawned,
        stopped,
        handle_tx,
        promise,
    } = tracking_value_downlink(&handle, lifecycle, DownlinkRuntimeConfig::default()).await;

    let context = ValueDownlinkContext {
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
    run_value_downlink(value_lifecycle(msg_tx), |ctx| async move {
        ctx.spawned.notified().await;
    })
    .await;
}

#[tokio::test]
async fn stops_on_disconnect() {
    let (msg_tx, _msg_rx) = unbounded_channel();
    run_value_downlink(value_lifecycle(msg_tx), |ctx| async move {
        let ValueDownlinkContext {
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
async fn test_value_lifecycle() {
    let (msg_tx, mut msg_rx) = unbounded_channel();
    run_value_downlink(value_lifecycle(msg_tx), |ctx| async move {
        let ValueDownlinkContext {
            handle: _raw,
            spawned,
            stopped,
            handle_tx,
            server,
            promise,
            stop_tx,
        } = ctx;
        spawned.notified().await;

        let mut lane = Server::lane_for(Arc::new(Mutex::new(server)), "node", "value_lane");

        lane.await_link().await;
        assert_eq!(msg_rx.recv().await.unwrap(), ValueTestMessage::Linked);

        lane.await_sync(vec![7]).await;
        assert_eq!(msg_rx.recv().await.unwrap(), ValueTestMessage::Synced(7));

        handle_tx.send(ValueDownlinkSet { to: 13 }).await.unwrap();

        lane.await_command(13).await;

        lane.send_unlinked().await;
        assert_eq!(msg_rx.recv().await.unwrap(), ValueTestMessage::Unlinked);

        assert!(stop_tx.trigger());
        lane.await_closed().await;

        assert_eq!(msg_rx.recv().now_or_never().unwrap(), None);
        stopped.notified().await;
        assert!(promise.await.unwrap().is_ok());
    })
    .await;
}

async fn tracking_value_downlink<LC>(
    handle: &RawHandle,
    lifecycle: LC,
    config: DownlinkRuntimeConfig,
) -> TrackingValueContext
where
    LC: ValueDownlinkLifecycle<i32> + Send + Sync + 'static,
{
    let spawned = Arc::new(Notify::new());
    let stopped = Arc::new(Notify::new());

    let (handle_tx, handle_rx) = mpsc::channel(128);

    let downlink = TrackingValueDownlink::new(
        spawned.clone(),
        stopped.clone(),
        ValueDownlinkModel::new(handle_rx, lifecycle),
    );

    let promise = handle
        .run_downlink(
            RemotePath::new("ws://127.0.0.1", "node", "value_lane"),
            config,
            Default::default(),
            DownlinkOptions::SYNC,
            downlink,
        )
        .await
        .expect("Failed to spawn downlink open request");

    TrackingValueContext {
        spawned,
        stopped,
        handle_tx,
        promise,
    }
}

async fn tracking_map_downlink<LC>(
    handle: &RawHandle,
    lifecycle: LC,
    config: DownlinkRuntimeConfig,
) -> TrackingMapContext
where
    LC: MapDownlinkLifecycle<i32, i32> + Send + Sync + 'static,
{
    let spawned = Arc::new(Notify::new());
    let stopped = Arc::new(Notify::new());

    let (set_tx, set_rx) = mpsc::channel(128);

    let downlink = TrackingMapDownlink::new(
        spawned.clone(),
        stopped.clone(),
        MapDownlinkModel::new(set_rx, lifecycle),
    );

    let promise = handle
        .run_downlink(
            RemotePath::new("ws://127.0.0.1", "node", "map_lane"),
            config,
            Default::default(),
            DownlinkOptions::SYNC,
            downlink,
        )
        .await
        .expect("Failed to spawn downlink open request");

    TrackingMapContext {
        spawned,
        stopped,
        tx: MapDownlinkHandle::new(set_tx),
        promise,
    }
}

struct TrackingValueContext {
    spawned: Arc<Notify>,
    stopped: Arc<Notify>,
    handle_tx: mpsc::Sender<ValueDownlinkSet<i32>>,
    promise: promise::Receiver<Result<(), Arc<DownlinkRuntimeError>>>,
}

struct TrackingMapContext {
    spawned: Arc<Notify>,
    stopped: Arc<Notify>,
    tx: MapDownlinkHandle<i32, i32>,
    promise: promise::Receiver<Result<(), Arc<DownlinkRuntimeError>>>,
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

    let (_stop_tx, stop_rx) = trigger::trigger();

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
        true,
    );

    let _task = spawn(task);

    let spawned = Arc::new(Notify::new());
    let stopped = Arc::new(Notify::new());

    let (_handle_tx, handle_rx) = mpsc::channel(128);

    let downlink = TrackingValueDownlink::new(
        spawned.clone(),
        stopped.clone(),
        ValueDownlinkModel::new(handle_rx, BasicValueDownlinkLifecycle::default()),
    );

    let promise = handle
        .run_downlink(
            RemotePath::new("ws://127.0.0.1", "node", "value_lane"),
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

async fn expect_event<T>(event_rx: &mut mpsc::UnboundedReceiver<T>, expected: T)
where
    T: Eq + Debug,
{
    assert_eq!(event_rx.recv().await, Some(expected))
}

#[tokio::test]
async fn disjoint_lanes() {
    let Fixture {
        handle,
        stop_tx: _stop,
        server,
        _jh,
    } = start();
    let (value_msg_tx, mut value_msg_rx) = unbounded_channel();

    let TrackingValueContext {
        spawned: value_spawned,
        stopped: value_stopped,
        handle_tx: _handle,
        promise: value_promise,
    } = tracking_value_downlink(
        &handle,
        value_lifecycle(value_msg_tx),
        DownlinkRuntimeConfig::default(),
    )
    .await;

    let (map_msg_tx, mut map_msg_rx) = unbounded_channel();

    let TrackingMapContext {
        spawned: map_spawned,
        stopped: map_stopped,
        tx: _map_tx,
        promise: map_promise,
    } = tracking_map_downlink(
        &handle,
        map_lifecycle(map_msg_tx),
        DownlinkRuntimeConfig::default(),
    )
    .await;

    let server = Arc::new(Mutex::new(server));
    let mut value_lane = Server::lane_for(server.clone(), "node", "value_lane");
    let mut map_lane = Server::lane_for(server, "node", "map_lane");

    {
        value_spawned.notified().await;
        value_lane.await_link().await;
        expect_event(&mut value_msg_rx, ValueTestMessage::Linked).await;

        value_lane.await_sync(vec![13]).await;
        expect_event(&mut value_msg_rx, ValueTestMessage::Synced(13)).await;

        value_lane.send_event(14).await;
        expect_event(&mut value_msg_rx, ValueTestMessage::Event(14)).await;
    }
    {
        map_spawned.notified().await;
        let map_init = BTreeMap::from([(1, 1), (2, 2), (3, 3)]);

        map_lane.await_link().await;
        expect_event(&mut map_msg_rx, MapTestMessage::Linked).await;

        let messages = map_init
            .iter()
            .map(|(key, value)| MapMessage::Update {
                key: *key,
                value: *value,
            })
            .collect::<Vec<_>>();

        map_lane.await_sync(messages).await;
        expect_event(&mut map_msg_rx, MapTestMessage::Synced(map_init)).await;

        map_lane
            .send_event(MapMessage::Update { key: 13, value: 13 })
            .await;
        expect_event(
            &mut map_msg_rx,
            MapTestMessage::Event(MapMessage::Update { key: 13, value: 13 }),
        )
        .await;
    }

    drop(value_lane);
    drop(map_lane);

    value_stopped.notified().await;
    map_stopped.notified().await;

    let value_result = value_promise.await;
    assert!(value_result.is_ok());
    assert!(value_result.unwrap().is_ok());

    let map_result = map_promise.await;
    assert!(map_result.is_ok());
    assert!(map_result.unwrap().is_ok());
}
