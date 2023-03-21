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

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use bytes::BytesMut;
use futures::{
    stream::{unfold, SelectAll},
    Stream, StreamExt, FutureExt,
};
use parking_lot::RwLock;
use ratchet::{
    NoExt, NoExtDecoder, NoExtEncoder, NoExtProvider, ProtocolRegistry, WebSocket, WebSocketConfig,
};
use swim::route::RouteUri;
use swim_messages::warp::{peel_envelope_header_str, RawEnvelope};
use swim_recon::{parser::MessageExtractError, printer::print_recon_compact};
use swim_utilities::{routing::route_uri::InvalidRouteUri, trigger};
use tokio::{net::TcpStream, sync::mpsc as tmpsc, task::block_in_place};

use crate::{
    model::{DisplayResponse, Endpoint, Host, RuntimeCommand, UIUpdate},
    shared_state::SharedState, ui::ViewUpdater, RuntimeFactory,
};

pub mod dummy_runtime;

const UI_DROPPED: &str = "The UI task stopped or timed out.";

#[derive(Debug, Default)]
pub struct ConsoleFactory;


impl RuntimeFactory for ConsoleFactory {
    fn run(&self,
        shared_state: Arc<RwLock<SharedState>>,
        commands: tmpsc::UnboundedReceiver<RuntimeCommand>,
        updater: Box<dyn ViewUpdater + Send + 'static>,
        stop: trigger::Receiver) -> futures::future::BoxFuture<'static, ()> {
        let runtime = Runtime::new(shared_state, commands, updater, stop);
        runtime.run().boxed()
    }
}

struct Runtime {
    shared_state: Arc<RwLock<SharedState>>,
    commands: tmpsc::UnboundedReceiver<RuntimeCommand>,
    output: Box<dyn ViewUpdater + Send + 'static>,
    stop: trigger::Receiver,
}

impl Runtime {
    fn new(
        shared_state: Arc<RwLock<SharedState>>,
        commands: tmpsc::UnboundedReceiver<RuntimeCommand>,
        output: Box<dyn ViewUpdater + Send + 'static>,
        stop: trigger::Receiver,
    ) -> Self {
        Runtime {
            shared_state,
            commands,
            output,
            stop,
        }
    }

    async fn run(self) {
        let Runtime {
            shared_state,
            mut commands,
            mut output,
            mut stop,
        } = self;

        let mut senders = HashMap::new();
        let mut receivers = SelectAll::new();
        let mut state = State::new(shared_state);

        loop {
            let event = tokio::select! {
                biased;
                _ = &mut stop => RuntimeEvent::Stop,
                maybe_cmd = commands.recv() => {
                    if let Some(cmd) = maybe_cmd {
                        RuntimeEvent::Command(cmd)
                    } else {
                        RuntimeEvent::Stop
                    }
                },
                maybe_msg = receivers.next(), if !receivers.is_empty() => {
                    match maybe_msg {
                        Some(Ok((host, body))) => RuntimeEvent::Message(host, body),
                        Some(Err(Failed(host))) => RuntimeEvent::Failed(host),
                        None => continue,
                    }
                },
            };

            match event {
                RuntimeEvent::Stop => break,
                RuntimeEvent::Command(cmd) => match cmd {
                    RuntimeCommand::Link { endpoint, response } => {
                        if let Some(id) = state.get_id(&endpoint) {
                            response.send(Ok(id));
                        } else {
                            let id = state.insert(endpoint.clone());
                            let Endpoint { remote, node, lane } = endpoint;
                            if let Some(tx) = senders.get_mut(&remote) {
                                if let Err(e) = link(tx, &node, &lane).await {
                                    senders.remove(&remote);
                                    state.remove_all(&remote);
                                    response.send(Err(e));
                                } else {
                                    response.send(Ok(id));
                                }
                            } else {
                                match open_connection(&remote).await {
                                    Ok(ws) => match ws.split() {
                                        Ok((mut tx, rx)) => {
                                            if let Err(e) = link(&mut tx, &node, &lane).await {
                                                state.remove(id);
                                                response.send(Err(e));
                                            } else {
                                                senders.insert(remote.clone(), tx);
                                                receivers.push(Box::pin(into_stream(remote, rx)));
                                                response.send(Ok(id));
                                            }
                                        }
                                        Err(e) => {
                                            state.remove(id);
                                            response.send(Err(e));
                                        }
                                    },
                                    Err(e) => response.send(Err(e)),
                                }
                            }
                        }
                    }
                    RuntimeCommand::Sync(id) => {
                        if let Some(Endpoint { remote, node, lane }) = state.get_endpoint(id) {
                            if let Some(tx) = senders.get_mut(remote) {
                                if sync(tx, node, lane).await.is_err() {
                                    state.remove_all(&remote.clone());
                                }
                            }
                        }
                    }
                    RuntimeCommand::Command(id, body) => {
                        let recon = format!("{}", print_recon_compact(&body));
                        if let Some(Endpoint { remote, node, lane }) = state.get_endpoint(id) {
                            if let Some(tx) = senders.get_mut(remote) {
                                if send_cmd(tx, node, lane, &recon).await.is_err() {
                                    state.remove_all(&remote.clone());
                                }
                            }
                        }
                    }
                    RuntimeCommand::AdHocCommand(endpoint, body) => {
                        let Endpoint { remote, node, lane } = endpoint;
                        let recon = format!("{}", print_recon_compact(&body));
                        if let Some(tx) = senders.get_mut(&remote) {
                            if !send_cmd(tx, &node, &lane, &recon).await.is_ok() {
                                senders.remove(&remote);
                            }
                        } else {
                            if let Ok((mut tx, rx)) =
                                open_connection(&remote).await.and_then(|ws| ws.split())
                            {
                                if send_cmd(&mut tx, &node, &lane, &recon).await.is_ok() {
                                    senders.insert(remote.clone(), tx);
                                    receivers.push(Box::pin(into_stream(remote, rx)));
                                }
                            }
                        }
                    }
                    RuntimeCommand::Unlink(id) => {
                        if let Some(Endpoint { remote, node, lane }) = state.get_endpoint(id) {
                            if let Some(tx) = senders.get_mut(remote) {
                                if unlink(tx, node, lane).await.is_err() {
                                    state.remove_all(&remote.clone());
                                } else {
                                    state.remove(id);
                                }
                            }
                        }
                    }
                },
                RuntimeEvent::Message(host, body) => match handle_body(&mut state, host, &body) {
                    Ok(msg) => {
                        let out_ref = &mut output;
                        block_in_place(move || out_ref.update(UIUpdate::LinkDisplay(msg)))
                            .expect(UI_DROPPED);
                    }
                    Err(BadEnvelope(error)) => {
                        let out_ref = &mut output;
                        block_in_place(move || out_ref.update(UIUpdate::LogMessage(error)))
                            .expect(UI_DROPPED);
                    }
                },
                RuntimeEvent::Failed(host) => {
                    state.remove_all(&host);
                }
            }
        }
    }
}

enum RuntimeEvent {
    Stop,
    Command(RuntimeCommand),
    Message(Host, String),
    Failed(Host),
}

type Tx = ratchet::Sender<TcpStream, NoExtEncoder>;
type Rx = ratchet::Receiver<TcpStream, NoExtDecoder>;

async fn link(writer: &mut Tx, node: &RouteUri, lane: &str) -> Result<(), ratchet::Error> {
    let envelope = format!("@link(node: \"{}\", lane: \"{}\")", node, lane);
    writer.write_text(envelope).await
}

async fn sync(writer: &mut Tx, node: &RouteUri, lane: &str) -> Result<(), ratchet::Error> {
    let envelope = format!("@sync(node: \"{}\", lane: \"{}\")", node, lane);
    writer.write_text(envelope).await
}

async fn send_cmd(
    writer: &mut Tx,
    node: &RouteUri,
    lane: &str,
    body: &str,
) -> Result<(), ratchet::Error> {
    let envelope = format!("@command(node: \"{}\", lane: \"{}\") {}", node, lane, body);
    writer.write_text(envelope).await
}

async fn unlink(writer: &mut Tx, node: &RouteUri, lane: &str) -> Result<(), ratchet::Error> {
    let envelope = format!("@unlink(node: \"{}\", lane: \"{}\")", node, lane);
    writer.write_text(envelope).await
}

struct Failed(Host);

fn into_stream(remote: Host, rx: Rx) -> impl Stream<Item = Result<(Host, String), Failed>> {
    unfold(
        (remote, Some(rx), BytesMut::new()),
        |(remote, rx, mut buffer)| async move {
            if let Some(mut rx) = rx {
                buffer.clear();
                if rx.read(&mut buffer).await.is_err() {
                    Some((Err(Failed(remote.clone())), (remote, None, buffer)))
                } else {
                    if let Ok(body) = std::str::from_utf8(buffer.as_ref()) {
                        let response = (remote.clone(), body.to_string());
                        Some((Ok(response), (remote, Some(rx), buffer)))
                    } else {
                        Some((Err(Failed(remote.clone())), (remote, None, buffer)))
                    }
                }
            } else {
                None
            }
        },
    )
}

async fn open_connection(host: &Host) -> Result<WebSocket<TcpStream, NoExt>, ratchet::Error> {
    let host_str = host.to_string();
    let socket = TcpStream::connect(&host_str).await?;
    let subprotocols = ProtocolRegistry::new(vec!["warp0"]).unwrap();
    let upgraded = ratchet::subscribe_with(
        WebSocketConfig::default(),
        socket,
        host_str,
        NoExtProvider,
        subprotocols,
    )
    .await?;
    Ok(upgraded.into_websocket())
}

struct BadEnvelope(String);

fn handle_body(state: &mut State, host: Host, body: &str) -> Result<DisplayResponse, BadEnvelope> {
    match peel_envelope_header_str(body)? {
        RawEnvelope::Linked {
            node_uri, lane_uri, ..
        } => {
            let node = node_uri.parse::<RouteUri>()?;
            let endpoint = Endpoint {
                remote: host,
                node,
                lane: lane_uri.to_string(),
            };
            let id = state.get_id(&endpoint).unwrap_or(0);
            Ok(DisplayResponse::linked(id))
        }
        RawEnvelope::Synced {
            node_uri, lane_uri, ..
        } => {
            let node = node_uri.parse::<RouteUri>()?;
            let endpoint = Endpoint {
                remote: host,
                node,
                lane: lane_uri.to_string(),
            };
            let id = state.get_id(&endpoint).unwrap_or(0);
            Ok(DisplayResponse::synced(id))
        }
        RawEnvelope::Unlinked {
            node_uri, lane_uri, ..
        } => {
            let node = node_uri.parse::<RouteUri>()?;
            let endpoint = Endpoint {
                remote: host,
                node,
                lane: lane_uri.to_string(),
            };
            let id = if let Some(id) = state.get_id(&endpoint) {
                state.remove(id);
                id
            } else {
                0
            };
            Ok(DisplayResponse::unlinked(id))
        }
        RawEnvelope::Event {
            node_uri,
            lane_uri,
            body,
            ..
        } => {
            let node = node_uri.parse::<RouteUri>()?;
            let endpoint = Endpoint {
                remote: host,
                node,
                lane: lane_uri.to_string(),
            };
            let id = state.get_id(&endpoint).unwrap_or(0);
            Ok(DisplayResponse::event(id, body.to_string()))
        }
        _ => Err(BadEnvelope(format!(
            "Invalid envelope from {}: {}",
            host, body
        ))),
    }
}

impl From<MessageExtractError> for BadEnvelope {
    fn from(value: MessageExtractError) -> Self {
        BadEnvelope(format!("Invalid envelope: {}", value))
    }
}

impl From<InvalidRouteUri> for BadEnvelope {
    fn from(value: InvalidRouteUri) -> Self {
        BadEnvelope(format!("Invalid route URI: {}", value))
    }
}

struct State {
    shared: Arc<RwLock<SharedState>>,
    links: HashMap<Endpoint, usize>,
    rev: BTreeMap<usize, Endpoint>,
}

impl State {
    fn new(shared: Arc<RwLock<SharedState>>) -> Self {
        State {
            shared,
            links: Default::default(),
            rev: Default::default(),
        }
    }

    fn get_endpoint(&self, id: usize) -> Option<&Endpoint> {
        self.rev.get(&id)
    }

    fn get_id(&self, endpoint: &Endpoint) -> Option<usize> {
        self.links.get(endpoint).copied()
    }

    fn insert(&mut self, endpoint: Endpoint) -> usize {
        let State { shared, links, rev } = self;
        let n = shared.write().insert(endpoint.clone());
        links.insert(endpoint.clone(), n);
        rev.insert(n, endpoint);
        n
    }

    fn remove(&mut self, id: usize) {
        let State { shared, links, rev } = self;
        if let Some(endpoint) = rev.remove(&id) {
            links.remove(&endpoint);
        }
        shared.write().remove(id);
    }

    fn remove_all(&mut self, remote: &Host) {
        let State { shared, links, rev } = self;
        let ids = links
            .iter()
            .filter(|(k, _)| &k.remote == remote)
            .map(|(_, v)| *v)
            .collect::<Vec<_>>();

        let mut guard = shared.write();

        for id in ids {
            if let Some(endpoint) = rev.remove(&id) {
                links.remove(&endpoint);
            }
            guard.remove(id);
        }
    }
}
