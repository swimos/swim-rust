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

use std::{collections::HashMap, net::SocketAddr, pin::pin, sync::Arc, time::Duration};

use bytes::BytesMut;
use futures::{
    future::{join, BoxFuture, Either},
    stream::{unfold, BoxStream, FuturesUnordered, SelectAll},
    FutureExt, Stream, StreamExt,
};
use parking_lot::RwLock;
use ratchet::{NoExtDecoder, NoExtEncoder, NoExtProvider, ProtocolRegistry, WebSocketConfig};
use swim::form::Form;
use swim_messages::warp::{peel_envelope_header_str, RawEnvelope};
use swim_recon::{parser::parse_value, printer::print_recon_compact};
use swim_utilities::trigger;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};

use crate::{
    model::{RuntimeCommand, UIUpdate},
    shared_state::SharedState,
    ui::ViewUpdater,
    RuntimeFactory,
};

pub struct DummyServer {
    stop_rx: trigger::Receiver,
    port_tx: oneshot::Sender<u16>,
    lanes: HashMap<(String, String), LaneSpec>,
    errors: Option<Box<dyn Fn(TaskError) -> bool + Send>>,
}

impl DummyServer {
    pub fn new(
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

pub struct LaneSpec(Box<dyn FakeLane>);

impl LaneSpec {
    pub fn simple<T: Form + Clone + Send + 'static>(init: T) -> Self {
        LaneSpec(Box::new(Lane {
            init,
            changes: None,
        }))
    }

    pub fn with_changes<T: Form + Clone + Send + 'static>(
        init: T,
        changes: Vec<T>,
        delay: Duration,
    ) -> Self {
        LaneSpec(Box::new(Lane {
            init,
            changes: Some((changes, delay)),
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
    NewConnection(TcpStream, SocketAddr),
    TaskDone(Result<(), TaskError>),
}

impl DummyServer {
    pub async fn run_server(self) -> Result<(), ratchet::Error> {
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
                    let (stream, bound_to) = result?;
                    Event::NewConnection(stream, bound_to)
                }
            };
            match event {
                Event::NewConnection(stream, _) => {
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
                    tasks.push(read_task(rx, con_tx).boxed());
                    tasks.push(send_task(tx, lanes.clone(), con_rx).boxed());
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

#[derive(Debug)]
pub enum TaskError {
    Ws(ratchet::Error),
    BadMessageType,
    BadUtf8,
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
) -> Result<(), TaskError> {
    let mut buffer = BytesMut::new();
    loop {
        buffer.clear();
        let message = rx.read(&mut buffer).await?;
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
                    break Ok(());
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
                if !lane_senders.contains_key(&key) {
                    if let Some(fake_lane) = lanes.get(&key).cloned() {
                        let (node, lane) = key.clone();
                        let (lane_tx, lane_rx) = mpsc::unbounded_channel();
                        let _ = lane_tx.send(LaneMessage::Link);
                        lane_tasks.push(fake_lane.into_stream(node, lane, lane_rx));
                        lane_senders.insert(key, lane_tx);
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
                    let _ = tx.send(LaneMessage::Unlink);
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
}

#[derive(Clone)]
struct Lane<T> {
    init: T,
    changes: Option<(Vec<T>, Duration)>,
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

impl<T> FakeLane for Lane<T>
where
    T: Form + Clone + Send + 'static,
{
    fn into_stream(
        self,
        node: String,
        lane: String,
        rx: mpsc::UnboundedReceiver<LaneMessage>,
    ) -> BoxStream<'static, Vec<String>> {
        self.make_stream(node, lane, rx).boxed()
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

impl<T> Lane<T>
where
    T: Form + Clone + Send + 'static,
{
    fn make_stream(
        self,
        node: String,
        lane: String,
        rx: mpsc::UnboundedReceiver<LaneMessage>,
    ) -> impl Stream<Item = Vec<String>> + Send + 'static {
        let Lane { init, changes } = self;

        let change_stream = changes.map(|(changes, delay)| {
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
            (init, change_stream, node, lane, Some(rx)),
            |(state, mut change_stream, node, lane, rx)| async move {
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
                                        format!("@linked(node:\"{}\",lane:{}))", node, lane);
                                    break Some((
                                        vec![linked],
                                        (state, change_stream, node, lane, Some(rx)),
                                    ));
                                }
                                LaneMessage::Sync => {
                                    let data = format!(
                                        "@event(node:\"{}\",lane:{})) {}",
                                        node,
                                        lane,
                                        print_recon_compact(&state)
                                    );
                                    let synced =
                                        format!("@synced(node:\"{}\",lane:{}))", node, lane);
                                    break Some((
                                        vec![data, synced],
                                        (state, change_stream, node, lane, Some(rx)),
                                    ));
                                }
                                LaneMessage::Unlink => {
                                    let msg =
                                        format!("@unlinked(node:\"{}\",lane:{}))", node, lane);
                                    break Some((
                                        vec![msg],
                                        (state, change_stream, node, lane, None),
                                    ));
                                }
                                LaneMessage::Command(body) => {
                                    if let Ok(v) = parse_value(&body, false) {
                                        if let Ok(update) = T::try_from_value(&v) {
                                            let event = format!(
                                                "@event(node:\"{}\",lane:{})) {}",
                                                node, lane, body
                                            );
                                            break Some((
                                                vec![event],
                                                (update, change_stream, node, lane, Some(rx)),
                                            ));
                                        }
                                    }
                                }
                            },
                            Either::Left(_) => break None,
                            Either::Right(Some(update)) => {
                                let event = format!(
                                    "@event(node:\"{}\",lane:{})) {}",
                                    node,
                                    lane,
                                    print_recon_compact(&update)
                                );
                                break Some((
                                    vec![event],
                                    (update, change_stream, node, lane, Some(rx)),
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

pub struct DummyServerRuntimeFac<F, R> {
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
    pub fn new(dummy_server_fac: F, runtime_fac: R) -> Self {
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
                    let _ = upd_cpy.update(UIUpdate::LogMessage(format!(
                        "Server task failed with: {}",
                        e
                    )));
                }
            };
            let runtime_task = async move {
                let r = match port_rx.await {
                    Ok(p) => updater.update(UIUpdate::LogMessage(format!("Bound to port: {}", p))),
                    Err(_) => updater.update(UIUpdate::LogMessage("Failed to bind.".to_string())),
                };
                if !r.is_err() {
                    runtime.await;
                }
            };
            join(server_task, runtime_task).await;
        }
        .boxed()
    }
}
