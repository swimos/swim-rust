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

use std::{
    collections::{hash_map::Entry, BTreeMap, HashMap},
    pin::pin,
    sync::Arc,
    time::Duration,
};

use bytes::BytesMut;
use futures::{
    future::{join, BoxFuture, Either},
    stream::{unfold, BoxStream, FuturesUnordered, SelectAll},
    FutureExt, Stream, StreamExt,
};
use parking_lot::RwLock;
use ratchet::{
    CloseCode, CloseReason, NoExtDecoder, NoExtEncoder, NoExtProvider, ProtocolRegistry,
    WebSocketConfig,
};
use swimos_agent_protocol::MapMessage;
use swimos_form::Form;
use swimos_messages::warp::{peel_envelope_header_str, RawEnvelope};
use swimos_recon::{parser::parse_recognize, print_recon_compact};
use swimos_utilities::trigger;
use thiserror::Error;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{self, UnboundedReceiver},
        oneshot,
    },
};

use crate::{
    model::{RuntimeCommand, UIUpdate},
    shared_state::SharedState,
    ui::ViewUpdater,
    RuntimeFactory,
};

use super::ConsoleFactory;

struct DummyServer {
    stop_rx: trigger::Receiver,
    port_tx: oneshot::Sender<u16>,
    lanes: HashMap<(String, String), LaneSpec>,
    errors: Option<Box<dyn Fn(TaskError) -> bool + Send>>,
}

impl DummyServer {
    fn new(
        stop_rx: trigger::Receiver,
        port_tx: oneshot::Sender<u16>,
        lanes: HashMap<(String, String), LaneSpec>,
        errors: Option<Box<dyn Fn(TaskError) -> bool + Send>>,
    ) -> Self {
        DummyServer {
            stop_rx,
            port_tx,
            lanes,
            errors,
        }
    }
}

struct LaneSpec(Box<dyn FakeLane>);

impl LaneSpec {
    fn simple<T: Form + Clone + Send + 'static>(init: T) -> Self {
        LaneSpec(Box::new(ValueLane {
            current: init,
            delay: None,
            changes: vec![],
        }))
    }

    fn simple_map<K, V>(init: BTreeMap<K, V>) -> Self
    where
        K: Form + Clone + Ord + Eq + Send + 'static,
        V: Form + Clone + Send + 'static,
    {
        LaneSpec(Box::new(MapLane {
            current: init,
            delay: None,
            changes: vec![],
        }))
    }

    fn with_changes<T: Form + Clone + Send + 'static>(
        init: T,
        changes: Vec<T>,
        delay: Duration,
    ) -> Self {
        LaneSpec(Box::new(ValueLane {
            current: init,
            delay: Some(delay),
            changes,
        }))
    }

    fn map_with_changes<K, V>(
        init: BTreeMap<K, V>,
        changes: Vec<MapMessage<K, V>>,
        delay: Duration,
    ) -> Self
    where
        K: Form + Clone + Ord + Eq + Send + 'static,
        V: Form + Clone + Send + 'static,
    {
        LaneSpec(Box::new(MapLane {
            current: init,
            delay: Some(delay),
            changes,
        }))
    }
}

impl FakeLane for LaneSpec {
    fn into_stream(
        self,
        node: String,
        lane: String,
        rx: mpsc::UnboundedReceiver<LaneMessage>,
    ) -> BoxStream<'static, Vec<String>> {
        self.0.into_stream_boxed(node, lane, rx)
    }

    fn duplicate(&self) -> Box<dyn FakeLane> {
        self.0.duplicate()
    }

    fn into_stream_boxed(
        self: Box<Self>,
        node: String,
        lane: String,
        rx: mpsc::UnboundedReceiver<LaneMessage>,
    ) -> BoxStream<'static, Vec<String>> {
        self.0.into_stream_boxed(node, lane, rx)
    }
}

impl Clone for LaneSpec {
    fn clone(&self) -> Self {
        Self(self.0.duplicate())
    }
}

enum Event {
    NewConnection(TcpStream),
    TaskDone(Result<(), TaskError>),
}

impl DummyServer {
    async fn run_server(self) -> Result<(), ratchet::Error> {
        let DummyServer {
            mut stop_rx,
            port_tx,
            lanes,
            mut errors,
        } = self;
        let listener = TcpListener::bind("localhost:0").await?;
        let port = listener.local_addr()?.port();
        let _ = port_tx.send(port);

        let mut tasks = FuturesUnordered::new();

        loop {
            let event = tokio::select! {
                biased;
                result = tasks.next(), if !tasks.is_empty() => {
                    if let Some(r) = result {
                        Event::TaskDone(r)
                    } else {
                        continue;
                    }
                },
                _ = &mut stop_rx => break,
                result = listener.accept() => {
                    match result {
                        Ok((stream, _)) => Event::NewConnection(stream),
                        Err(err) => {
                            use std::io;
                            match err.kind() {
                                k @ (io::ErrorKind::ConnectionReset
                                | io::ErrorKind::ConnectionAborted
                                | io::ErrorKind::BrokenPipe
                                | io::ErrorKind::InvalidInput
                                | io::ErrorKind::InvalidData
                                | io::ErrorKind::TimedOut
                                | io::ErrorKind::UnexpectedEof) => {
                                    if let Some(tx) = errors.as_ref() {
                                        if !tx(TaskError::AcceptErr(k)) {
                                            errors = None;
                                        }
                                    }
                                    continue;
                                },
                                _ => return Err(err.into()),
                            }
                        }
                    }
                }
            };
            match event {
                Event::NewConnection(stream) => {
                    let subprotocols = ProtocolRegistry::new(vec!["warp0"]).unwrap();
                    let upgrader = ratchet::accept_with(
                        stream,
                        WebSocketConfig::default(),
                        NoExtProvider,
                        subprotocols,
                    )
                    .await?;
                    let ws = upgrader.upgrade().await?;
                    let (tx, rx) = ws.websocket.split()?;
                    let (con_tx, con_rx) = mpsc::unbounded_channel();
                    let (task_stop_tx, task_stop_rx) = trigger::trigger();
                    tasks.push(read_task(rx, con_tx, task_stop_rx).boxed());
                    tasks.push(send_task(tx, lanes.clone(), con_rx, task_stop_tx).boxed());
                }
                Event::TaskDone(Err(e)) => {
                    if let Some(err_tx) = errors.as_ref() {
                        if !err_tx(e) {
                            errors = None;
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[derive(Error, Debug)]
enum TaskError {
    #[error("A web socket error occurred: {0}")]
    Ws(ratchet::Error),
    #[error("Failed to accept an incoming connection: {0}")]
    AcceptErr(std::io::ErrorKind),
    #[error("Received a binary web socket frame.")]
    BadMessageType,
    #[error("Received a web socket frame containing invalid UTF-8.")]
    BadUtf8,
    #[error("Received an invalid Warp envelope: {0}")]
    BadEnvelope(String),
}

impl From<ratchet::Error> for TaskError {
    fn from(value: ratchet::Error) -> Self {
        TaskError::Ws(value)
    }
}

impl From<std::str::Utf8Error> for TaskError {
    fn from(_: std::str::Utf8Error) -> Self {
        TaskError::BadUtf8
    }
}

async fn read_task(
    mut rx: ratchet::Receiver<TcpStream, NoExtDecoder>,
    msg_tx: mpsc::UnboundedSender<ConnectionMessage>,
    mut task_stop_rx: trigger::Receiver,
) -> Result<(), TaskError> {
    let mut buffer = BytesMut::new();
    loop {
        buffer.clear();
        let message = tokio::select! {
            _ = &mut task_stop_rx => break Ok(()),
            result = rx.read(&mut buffer) => {
                result?
            }
        };
        match message {
            ratchet::Message::Text => {
                let body = std::str::from_utf8(buffer.as_ref())?;
                match peel_envelope_header_str(body) {
                    Ok(RawEnvelope::Link {
                        node_uri, lane_uri, ..
                    }) => {
                        if msg_tx
                            .send(ConnectionMessage::Link(
                                node_uri.to_string(),
                                lane_uri.to_string(),
                            ))
                            .is_err()
                        {
                            break Ok(());
                        }
                    }
                    Ok(RawEnvelope::Sync {
                        node_uri, lane_uri, ..
                    }) => {
                        if msg_tx
                            .send(ConnectionMessage::Sync(
                                node_uri.to_string(),
                                lane_uri.to_string(),
                            ))
                            .is_err()
                        {
                            break Ok(());
                        }
                    }
                    Ok(RawEnvelope::Unlink {
                        node_uri, lane_uri, ..
                    }) => {
                        if msg_tx
                            .send(ConnectionMessage::Unlink(
                                node_uri.to_string(),
                                lane_uri.to_string(),
                            ))
                            .is_err()
                        {
                            break Ok(());
                        }
                    }
                    Ok(RawEnvelope::Command {
                        node_uri,
                        lane_uri,
                        body,
                        ..
                    }) => {
                        if msg_tx
                            .send(ConnectionMessage::Command(
                                node_uri.to_string(),
                                lane_uri.to_string(),
                                body.to_string(),
                            ))
                            .is_err()
                        {
                            break Ok(());
                        }
                    }
                    _ => break Err(TaskError::BadEnvelope(body.to_string())),
                }
            }
            ratchet::Message::Close(_) => break Ok(()),
            ratchet::Message::Ping(_) | ratchet::Message::Pong(_) => {}
            _ => break Err(TaskError::BadMessageType),
        }
    }
}

async fn send_task(
    mut tx: ratchet::Sender<TcpStream, NoExtEncoder>,
    lanes: HashMap<(String, String), LaneSpec>,
    mut msg_rx: mpsc::UnboundedReceiver<ConnectionMessage>,
    task_stop: trigger::Sender,
) -> Result<(), TaskError> {
    let mut lane_tasks = SelectAll::new();
    let mut lane_senders = HashMap::new();
    loop {
        let event = tokio::select! {
            biased;
            maybe_message = msg_rx.recv() => {
                if let Some(message) = maybe_message {
                    Either::Left(message)
                } else {
                    break;
                }
            },
            outgoing = lane_tasks.next(), if !lane_tasks.is_empty() => {
                if let Some(frames) = outgoing {
                    Either::Right(frames)
                } else {
                    continue;
                }
            }
        };

        match event {
            Either::Left(ConnectionMessage::Link(node, lane)) => {
                let key = (node, lane);
                if let Some(fake_lane) = lanes.get(&key) {
                    if let Entry::Vacant(entry) = lane_senders.entry(key.clone()) {
                        let (node, lane) = key;
                        let (lane_tx, lane_rx) = mpsc::unbounded_channel();
                        let _ = lane_tx.send(LaneMessage::Link);
                        lane_tasks.push(fake_lane.clone().into_stream(node, lane, lane_rx));
                        entry.insert(lane_tx);
                    }
                }
            }
            Either::Left(ConnectionMessage::Sync(node, lane)) => {
                let key = (node, lane);
                if let Some(tx) = lane_senders.get(&key) {
                    if tx.send(LaneMessage::Sync).is_err() {
                        lane_senders.remove(&key);
                    }
                }
            }
            Either::Left(ConnectionMessage::Unlink(node, lane)) => {
                let key = (node, lane);
                if let Some(tx) = lane_senders.get(&key) {
                    let _ = tx.send(LaneMessage::Unlink);
                    if tx.send(LaneMessage::Unlink).is_err() {
                        lane_senders.remove(&key);
                    }
                }
            }
            Either::Left(ConnectionMessage::Command(node, lane, body)) => {
                let key = (node, lane);
                if let Some(tx) = lane_senders.get(&key) {
                    if tx.send(LaneMessage::Command(body)).is_err() {
                        lane_senders.remove(&key);
                    }
                }
            }
            Either::Right(frames) => {
                for frame in frames {
                    tx.write_text(frame).await?;
                }
            }
        }
    }
    let result = if tx.is_active() {
        match tx.close(CloseReason::new(CloseCode::GoingAway, None)).await {
            Ok(()) => Ok(()),
            Err(e) if e.is_close() => Ok(()),
            Err(e) => Err(e),
        }
    } else {
        Ok(())
    };
    task_stop.trigger();
    result?;
    Ok(())
}

#[derive(Clone)]
struct ValueLane<T> {
    current: T,
    delay: Option<Duration>,
    changes: Vec<T>,
}

enum LaneMessage {
    Link,
    Sync,
    Command(String),
    Unlink,
}

enum ConnectionMessage {
    Link(String, String),
    Sync(String, String),
    Command(String, String, String),
    Unlink(String, String),
}

trait FakeLane: Send {
    fn into_stream(
        self,
        node: String,
        lane: String,
        rx: mpsc::UnboundedReceiver<LaneMessage>,
    ) -> BoxStream<'static, Vec<String>>;

    fn into_stream_boxed(
        self: Box<Self>,
        node: String,
        lane: String,
        rx: mpsc::UnboundedReceiver<LaneMessage>,
    ) -> BoxStream<'static, Vec<String>>;

    fn duplicate(&self) -> Box<dyn FakeLane>;
}

impl<T> FakeLane for ValueLane<T>
where
    T: Form + Clone + Send + 'static,
{
    fn into_stream(
        self,
        node: String,
        lane: String,
        rx: mpsc::UnboundedReceiver<LaneMessage>,
    ) -> BoxStream<'static, Vec<String>> {
        StreamWrapper(self).make_stream(node, lane, rx).boxed()
    }

    fn duplicate(&self) -> Box<dyn FakeLane> {
        Box::new(self.clone())
    }

    fn into_stream_boxed(
        self: Box<Self>,
        node: String,
        lane: String,
        rx: mpsc::UnboundedReceiver<LaneMessage>,
    ) -> BoxStream<'static, Vec<String>> {
        (*self).into_stream(node, lane, rx)
    }
}

impl<K, V> FakeLane for MapLane<K, V>
where
    K: Form + Clone + Ord + Eq + Send + 'static,
    V: Form + Clone + Send + 'static,
{
    fn into_stream(
        self,
        node: String,
        lane: String,
        rx: mpsc::UnboundedReceiver<LaneMessage>,
    ) -> BoxStream<'static, Vec<String>> {
        StreamWrapper(self).make_stream(node, lane, rx).boxed()
    }

    fn duplicate(&self) -> Box<dyn FakeLane> {
        Box::new(self.clone())
    }

    fn into_stream_boxed(
        self: Box<Self>,
        node: String,
        lane: String,
        rx: mpsc::UnboundedReceiver<LaneMessage>,
    ) -> BoxStream<'static, Vec<String>> {
        (*self).into_stream(node, lane, rx)
    }
}

struct StreamWrapper<L>(L);

impl<L> StreamWrapper<L>
where
    L: LaneData + 'static,
{
    fn make_stream(
        self,
        node: String,
        lane: String,
        rx: mpsc::UnboundedReceiver<LaneMessage>,
    ) -> impl Stream<Item = Vec<String>> + Send + 'static {
        let StreamWrapper(inner) = self;
        let delay = inner.delay();
        let changes = inner.changes();
        let change_stream = delay.map(|delay| {
            Box::pin(unfold(
                (changes.into_iter(), delay),
                |(mut it, delay)| async move {
                    if let Some(t) = it.next() {
                        let sleep = pin!(tokio::time::sleep(delay));
                        sleep.await;
                        Some((t, (it, delay)))
                    } else {
                        None
                    }
                },
            ))
        });

        unfold(
            (inner, change_stream, node, lane, Some(rx)),
            |(mut state, mut change_stream, node, lane, rx)| async move {
                if let Some(mut rx) = rx {
                    loop {
                        let event = if let Some(change_stream) = change_stream.as_mut() {
                            tokio::select! {
                                biased;
                                maybe_msg = rx.recv() => Either::Left(maybe_msg),
                                maybe_update = change_stream.next() => Either::Right(maybe_update),
                            }
                        } else {
                            Either::Left(rx.recv().await)
                        };
                        match event {
                            Either::Left(Some(msg)) => match msg {
                                LaneMessage::Link => {
                                    let linked =
                                        format!("@linked(node:\"{}\",lane:{})", node, lane);
                                    break Some((
                                        vec![linked],
                                        (state, change_stream, node, lane, Some(rx)),
                                    ));
                                }
                                LaneMessage::Sync => {
                                    let events = state.sync();

                                    let mut output: Vec<String> = events
                                        .into_iter()
                                        .map(|data| {
                                            format!(
                                                "@event(node:\"{}\",lane:{}) {}",
                                                node,
                                                lane,
                                                print_recon_compact(&data)
                                            )
                                        })
                                        .collect();
                                    output
                                        .push(format!("@synced(node:\"{}\",lane:{})", node, lane));

                                    break Some((
                                        output,
                                        (state, change_stream, node, lane, Some(rx)),
                                    ));
                                }
                                LaneMessage::Unlink => {
                                    let msg = format!("@unlinked(node:\"{}\",lane:{})", node, lane);
                                    break Some((
                                        vec![msg],
                                        (state, change_stream, node, lane, None),
                                    ));
                                }
                                LaneMessage::Command(body) => {
                                    if let Ok(v) = parse_recognize(body.as_str(), false) {
                                        if let Ok(update) = L::Event::try_from_value(&v) {
                                            let event = format!(
                                                "@event(node:\"{}\",lane:{}) {}",
                                                node, lane, body
                                            );
                                            state.update(update);
                                            break Some((
                                                vec![event],
                                                (state, change_stream, node, lane, Some(rx)),
                                            ));
                                        }
                                    }
                                }
                            },
                            Either::Left(_) => break None,
                            Either::Right(Some(update)) => {
                                let event = format!(
                                    "@event(node:\"{}\",lane:{}) {}",
                                    node,
                                    lane,
                                    print_recon_compact(&update)
                                );
                                state.update(update);
                                break Some((
                                    vec![event],
                                    (state, change_stream, node, lane, Some(rx)),
                                ));
                            }
                            Either::Right(_) => {
                                change_stream = None;
                            }
                        }
                    }
                } else {
                    None
                }
            },
        )
        .boxed()
    }
}

#[derive(Debug, Clone)]
struct MapLane<K, V> {
    current: BTreeMap<K, V>,
    delay: Option<Duration>,
    changes: Vec<MapMessage<K, V>>,
}

trait LaneData: Send {
    type Event: Form + Send + 'static;

    fn sync(&self) -> Vec<Self::Event>;

    fn delay(&self) -> Option<Duration>;

    fn changes(&self) -> Vec<Self::Event>;

    fn update(&mut self, change: Self::Event);
}

impl<T> LaneData for ValueLane<T>
where
    T: Form + Clone + Send + 'static,
{
    type Event = T;

    fn sync(&self) -> Vec<Self::Event> {
        vec![self.current.clone()]
    }

    fn delay(&self) -> Option<Duration> {
        self.delay
    }

    fn changes(&self) -> Vec<Self::Event> {
        self.changes.clone()
    }

    fn update(&mut self, change: Self::Event) {
        self.current = change;
    }
}

impl<K, V> LaneData for MapLane<K, V>
where
    K: Form + Clone + Ord + Eq + Send + 'static,
    V: Form + Clone + Send + 'static,
{
    type Event = MapMessage<K, V>;

    fn sync(&self) -> Vec<Self::Event> {
        self.current
            .clone()
            .into_iter()
            .map(|(k, v)| MapMessage::Update { key: k, value: v })
            .collect()
    }

    fn delay(&self) -> Option<Duration> {
        self.delay
    }

    fn changes(&self) -> Vec<Self::Event> {
        self.changes.clone()
    }

    fn update(&mut self, change: Self::Event) {
        let MapLane { current, .. } = self;
        match change {
            MapMessage::Update { key, value } => {
                current.insert(key, value);
            }
            MapMessage::Remove { key } => {
                current.remove(&key);
            }
            MapMessage::Clear => {
                current.clear();
            }
            MapMessage::Take(n) => {
                *current = std::mem::take(current)
                    .into_iter()
                    .take(n as usize)
                    .collect();
            }
            MapMessage::Drop(n) => {
                *current = std::mem::take(current)
                    .into_iter()
                    .skip(n as usize)
                    .collect();
            }
        }
    }
}

struct DummyServerRuntimeFac<F, R> {
    dummy_server_fac: F,
    runtime_fac: R,
}

impl<F, R> DummyServerRuntimeFac<F, R>
where
    R: RuntimeFactory,
    F: Fn(
        trigger::Receiver,
        oneshot::Sender<u16>,
        Arc<dyn ViewUpdater + Send + Sync + 'static>,
    ) -> DummyServer,
{
    fn new(dummy_server_fac: F, runtime_fac: R) -> Self {
        DummyServerRuntimeFac {
            dummy_server_fac,
            runtime_fac,
        }
    }
}

impl<R, F> RuntimeFactory for DummyServerRuntimeFac<F, R>
where
    R: RuntimeFactory,
    F: Fn(
        trigger::Receiver,
        oneshot::Sender<u16>,
        Arc<dyn ViewUpdater + Send + Sync + 'static>,
    ) -> DummyServer,
{
    fn run(
        &self,
        shared_state: Arc<RwLock<SharedState>>,
        commands: mpsc::UnboundedReceiver<RuntimeCommand>,
        updater: Arc<dyn ViewUpdater + Send + Sync + 'static>,
        stop: trigger::Receiver,
    ) -> BoxFuture<'static, ()> {
        let DummyServerRuntimeFac {
            dummy_server_fac,
            runtime_fac,
        } = self;
        let runtime = runtime_fac.run(shared_state, commands, updater.clone(), stop.clone());
        let (port_tx, port_rx) = oneshot::channel();
        let dummy_server = dummy_server_fac(stop, port_tx, updater.clone());
        async move {
            let upd_cpy = updater.clone();
            let server_task = async move {
                if let Err(e) = dummy_server.run_server().await {
                    let _ = upd_cpy.update(UIUpdate::log_error(format!(
                        "Server task failed with: {}",
                        e
                    )));
                }
            };
            let runtime_task = async move {
                let r = match port_rx.await {
                    Ok(p) => updater.update(UIUpdate::log_report(format!("Bound to port: {}", p))),
                    Err(_) => updater.update(UIUpdate::log_error("Failed to bind.".to_string())),
                };
                if r.is_ok() {
                    runtime.await;
                }
            };
            join(server_task, runtime_task).await;
        }
        .boxed()
    }
}

pub fn make_dummy_runtime(
    shared_state: Arc<RwLock<SharedState>>,
    command_rx: UnboundedReceiver<RuntimeCommand>,
    updater: Arc<dyn ViewUpdater + Send + Sync + 'static>,
    stop_rx: trigger::Receiver,
) -> BoxFuture<'static, ()> {
    DummyServerRuntimeFac::new(
        |stop_rx, port_tx, updater| {
            let errors = Box::new(move |err| {
                updater
                    .update(UIUpdate::log_error(format!("Task error: {:?}", err)))
                    .is_ok()
            });
            let mut lanes = HashMap::new();
            lanes.insert(
                ("/node".to_string(), "lane1".to_string()),
                LaneSpec::simple(0),
            );
            lanes.insert(
                ("/node".to_string(), "lane2".to_string()),
                LaneSpec::with_changes(
                    "I".to_string(),
                    vec![
                        "am".to_string(),
                        "the".to_string(),
                        "very".to_string(),
                        "model".to_string(),
                        "of".to_string(),
                        "a".to_string(),
                        "modern".to_string(),
                        "major".to_string(),
                        "general.".to_string(),
                    ],
                    Duration::from_secs(5),
                ),
            );
            lanes.insert(
                ("/node".to_string(), "map1".to_string()),
                LaneSpec::simple_map(
                    [
                        (1, "aardvark".to_string()),
                        (2, "bullfrog".to_string()),
                        (3, "capybara".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                ),
            );
            lanes.insert(
                ("/node".to_string(), "map2".to_string()),
                LaneSpec::map_with_changes(
                    [
                        (2, "llama".to_string()),
                        (4, "moose".to_string()),
                        (6, "narwhal".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                    vec![
                        MapMessage::Update {
                            key: 8,
                            value: "ostrich".to_string(),
                        },
                        MapMessage::Update {
                            key: 4,
                            value: "manatee".to_string(),
                        },
                        MapMessage::Remove { key: 2 },
                        MapMessage::Clear,
                    ],
                    Duration::from_secs(10),
                ),
            );
            DummyServer::new(stop_rx, port_tx, lanes, Some(errors))
        },
        ConsoleFactory,
    )
    .run(shared_state, command_rx, updater, stop_rx)
}
