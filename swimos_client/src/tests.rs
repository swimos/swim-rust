// Copyright 2015-2024 Swim Inc.
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

use ratchet::NoExtProvider;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::time::Duration;
use swimos_agent_protocol::MapMessage;
use swimos_messages::remote_protocol::AttachClient;
use swimos_remote::SchemeHostPort;
use tokio::io::{duplex, AsyncWriteExt};
use tokio::spawn;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::{oneshot, Notify};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::codec::Encoder;
use uuid::Uuid;

use crate::error::{DownlinkErrorKind, DownlinkRuntimeError};
use crate::models::RemotePath;
use crate::runtime::{start_runtime, RawHandle};
use crate::transport::{Transport, TransportHandle};
use bytes::BytesMut;
use futures_util::future::{ready, BoxFuture};
use futures_util::stream::BoxStream;
use futures_util::{FutureExt, StreamExt};
use ratchet::{
    ExtensionProvider, Message, NegotiatedExtension, NoExt, PayloadType, Role, WebSocket,
    WebSocketConfig, WebSocketStream,
};
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::io;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::ops::DerefMut;
use std::sync::Arc;
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
use swimos_messages::remote_protocol::FindNode;
use swimos_model::{Text, Value};
use swimos_recon::parser::parse_recognize;
use swimos_recon::print_recon;
use swimos_remote::dns::{BoxDnsResolver, DnsResolver};
use swimos_remote::websocket::{RatchetError, WebsocketClient, WebsocketServer, WsOpenFuture};
use swimos_remote::{
    ClientConnections, ConnectionError, ConnectionResult, Listener, ListenerError, Scheme,
};
use swimos_runtime::downlink::{DownlinkOptions, DownlinkRuntimeConfig};
use swimos_utilities::byte_channel::{byte_channel, ByteReader, ByteWriter};
use swimos_utilities::trigger::{promise, trigger};
use swimos_utilities::{non_zero_usize, trigger};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream};
use tokio::sync::mpsc;
use tokio::sync::Mutex;

#[derive(Debug)]
struct Inner {
    addrs: HashMap<(String, u16), SocketAddr>,
    sockets: HashMap<SocketAddr, DuplexStream>,
}

impl Inner {
    fn new<R, S>(resolver: R, sockets: S) -> Inner
    where
        R: IntoIterator<Item = ((String, u16), SocketAddr)>,
        S: IntoIterator<Item = (SocketAddr, DuplexStream)>,
    {
        Inner {
            addrs: HashMap::from_iter(resolver),
            sockets: HashMap::from_iter(sockets),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MockClientConnections {
    inner: Arc<Mutex<Inner>>,
}

impl MockClientConnections {
    pub fn new<R, S>(resolver: R, sockets: S) -> MockClientConnections
    where
        R: IntoIterator<Item = ((String, u16), SocketAddr)>,
        S: IntoIterator<Item = (SocketAddr, DuplexStream)>,
    {
        MockClientConnections {
            inner: Arc::new(Mutex::new(Inner::new(resolver, sockets))),
        }
    }
}

impl ClientConnections for MockClientConnections {
    type ClientSocket = DuplexStream;

    fn try_open(
        &self,
        _scheme: Scheme,
        _host: Option<&str>,
        addr: SocketAddr,
    ) -> BoxFuture<'_, ConnectionResult<Self::ClientSocket>> {
        async move {
            self.inner
                .lock()
                .await
                .sockets
                .remove(&addr)
                .ok_or_else(|| ConnectionError::ConnectionFailed(ErrorKind::NotFound.into()))
        }
        .boxed()
    }

    fn dns_resolver(&self) -> BoxDnsResolver {
        Box::new(self.clone())
    }

    fn lookup(
        &self,
        host: String,
        port: u16,
    ) -> BoxFuture<'static, std::io::Result<Vec<SocketAddr>>> {
        self.resolve(host, port).boxed()
    }
}

impl DnsResolver for MockClientConnections {
    type ResolveFuture = BoxFuture<'static, io::Result<Vec<SocketAddr>>>;

    fn resolve(&self, host: String, port: u16) -> Self::ResolveFuture {
        let inner = self.inner.clone();
        async move {
            match inner.lock().await.addrs.get(&(host, port)) {
                Some(sock) => Ok(vec![*sock]),
                None => Err(io::ErrorKind::NotFound.into()),
            }
        }
        .boxed()
    }
}

pub enum WsAction {
    Open,
    Fail(Box<dyn Fn() -> RatchetError + Send + Sync + 'static>),
}

impl WsAction {
    pub fn fail<F>(with: F) -> WsAction
    where
        F: Fn() -> RatchetError + Send + Sync + 'static,
    {
        WsAction::Fail(Box::new(with))
    }
}

pub struct MockWs {
    states: HashMap<String, WsAction>,
}

impl MockWs {
    pub fn new<S>(states: S) -> MockWs
    where
        S: IntoIterator<Item = (String, WsAction)>,
    {
        MockWs {
            states: HashMap::from_iter(states),
        }
    }
}

impl WebsocketClient for MockWs {
    fn open_connection<'a, Sock, Provider>(
        &self,
        socket: Sock,
        _provider: &'a Provider,
        addr: String,
    ) -> WsOpenFuture<'a, Sock, Provider::Extension, RatchetError>
    where
        Sock: WebSocketStream + Send,
        Provider: ExtensionProvider + Send + Sync + 'static,
        Provider::Extension: Send + Sync + 'static,
    {
        let result = match self.states.get(&addr) {
            Some(WsAction::Open) => Ok(WebSocket::from_upgraded(
                WebSocketConfig::default(),
                socket,
                NegotiatedExtension::from(None),
                BytesMut::default(),
                Role::Client,
            )),
            Some(WsAction::Fail(e)) => Err(e()),
            None => Err(ratchet::Error::new(ratchet::ErrorKind::Http).into()),
        };
        ready(result).boxed()
    }
}

impl WebsocketServer for MockWs {
    type WsStream<Sock, Ext> =
        BoxStream<'static, Result<(WebSocket<Sock, Ext>, SocketAddr), ListenerError>>;

    fn wrap_listener<Sock, L, Provider>(
        &self,
        _listener: L,
        _provider: Provider,
        _find: mpsc::Sender<FindNode>,
    ) -> Self::WsStream<Sock, Provider::Extension>
    where
        Sock: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
        L: Listener<Sock> + Send + 'static,
        Provider: ExtensionProvider + Send + Sync + Unpin + 'static,
        Provider::Extension: Send + Sync + Unpin + 'static,
    {
        futures::stream::pending().boxed()
    }
}

#[derive(Clone, Debug, PartialEq, Form)]

pub enum Envelope {
    #[form(tag = "link")]
    Link {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        rate: Option<f64>,
        prio: Option<f64>,
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "sync")]
    Sync {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        rate: Option<f64>,
        prio: Option<f64>,
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "unlink")]
    Unlink {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "command")]
    Command {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "linked")]
    Linked {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        rate: Option<f64>,
        prio: Option<f64>,
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "synced")]
    Synced {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "unlinked")]
    Unlinked {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "event")]
    Event {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        #[form(body)]
        body: Option<Value>,
    },
}

pub struct Lane {
    node: String,
    lane: String,
    server: Arc<Mutex<Server>>,
}

impl Lane {
    pub async fn read(&mut self) -> Envelope {
        let Lane { server, .. } = self;
        let mut guard = server.lock().await;
        let Server { buf, transport } = &mut guard.deref_mut();

        match transport.read(buf).await.unwrap() {
            Message::Text => {}
            m => panic!("Unexpected message type: {:?}", m),
        }
        let read = String::from_utf8(buf.to_vec()).unwrap();
        buf.clear();

        parse_recognize::<Envelope>(read.as_str(), false).unwrap()
    }

    pub async fn write(&mut self, env: Envelope) {
        let Lane { server, .. } = self;
        let mut guard = server.lock().await;
        let Server { transport, .. } = &mut guard.deref_mut();

        let response = print_recon(&env);
        transport
            .write(format!("{}", response), PayloadType::Text)
            .await
            .unwrap();
    }

    pub async fn await_link(&mut self) {
        match self.read().await {
            Envelope::Link {
                node_uri, lane_uri, ..
            } => {
                assert_eq!(node_uri, self.node);
                assert_eq!(lane_uri, self.lane);
                self.write(Envelope::Linked {
                    node_uri: node_uri.clone(),
                    lane_uri: lane_uri.clone(),
                    rate: None,
                    prio: None,
                    body: None,
                })
                .await;
            }
            e => panic!("Unexpected envelope {:?}", e),
        }
    }

    pub async fn await_sync<V: Form>(&mut self, val: Vec<V>) {
        match self.read().await {
            Envelope::Sync {
                node_uri, lane_uri, ..
            } => {
                assert_eq!(node_uri, self.node);
                assert_eq!(lane_uri, self.lane);

                for v in val {
                    self.write(Envelope::Event {
                        node_uri: node_uri.clone(),
                        lane_uri: lane_uri.clone(),
                        body: Some(v.as_value()),
                    })
                    .await;
                }

                self.write(Envelope::Synced {
                    node_uri: node_uri.clone(),
                    lane_uri: lane_uri.clone(),
                    body: None,
                })
                .await;
            }
            e => panic!("Unexpected envelope {:?}", e),
        }
    }

    pub async fn await_command(&mut self, expected: i32) {
        match self.read().await {
            Envelope::Command {
                node_uri,
                lane_uri,
                body: Some(val),
            } => {
                assert_eq!(node_uri, self.node);
                assert_eq!(lane_uri, self.lane);
                assert_eq!(val, Value::Int32Value(expected));
            }
            e => panic!("Unexpected envelope {:?}", e),
        }
    }

    pub async fn send_unlinked(&mut self) {
        self.write(Envelope::Unlinked {
            node_uri: self.node.clone().into(),
            lane_uri: self.lane.clone().into(),
            body: None,
        })
        .await;
    }

    pub async fn send_event<V: Form>(&mut self, val: V) {
        self.write(Envelope::Event {
            node_uri: self.node.clone().into(),
            lane_uri: self.lane.clone().into(),
            body: Some(val.as_value()),
        })
        .await;
    }

    pub async fn await_closed(&mut self) {
        let Lane { server, .. } = self;
        let mut guard = server.lock().await;
        let Server { buf, transport } = &mut guard.deref_mut();

        match transport.borrow_mut().read(buf).await.unwrap() {
            Message::Close(_) => {}
            m => panic!("Unexpected message type: {:?}", m),
        }
    }
}

pub struct Server {
    pub buf: BytesMut,
    pub transport: WebSocket<DuplexStream, NoExt>,
}

impl Server {
    pub fn lane_for<N, L>(server: Arc<Mutex<Self>>, node: N, lane: L) -> Lane
    where
        N: ToString,
        L: ToString,
    {
        Lane {
            node: node.to_string(),
            lane: lane.to_string(),
            server,
        }
    }
}

impl Server {
    pub fn new(transport: DuplexStream) -> Server {
        Server {
            buf: BytesMut::new(),
            transport: WebSocket::from_upgraded(
                WebSocketConfig::default(),
                transport,
                NegotiatedExtension::from(NoExt),
                BytesMut::default(),
                Role::Server,
            ),
        }
    }
}

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
