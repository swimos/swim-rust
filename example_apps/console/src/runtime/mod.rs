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
    FutureExt, Stream, StreamExt,
};
use parking_lot::RwLock;
use ratchet::{
    ErrorKind, NoExt, NoExtDecoder, NoExtEncoder, NoExtProvider, ProtocolRegistry, WebSocket,
    WebSocketConfig,
};
use swim_messages::warp::{peel_envelope_header_str, RawEnvelope};
use swim_recon::{parser::MessageExtractError, printer::print_recon_compact};
use swim_utilities::{
    routing::route_uri::{InvalidRouteUri, RouteUri},
    trigger,
};
use tokio::{net::TcpStream, sync::mpsc as tmpsc, task::block_in_place};

use crate::{
    model::{DisplayResponse, Endpoint, Host, RuntimeCommand, UIUpdate},
    shared_state::SharedState,
    ui::ViewUpdater,
    RuntimeFactory,
};

pub mod dummy_server;

const UI_DROPPED: &str = "The UI task stopped or timed out.";

#[derive(Debug, Default)]
pub struct ConsoleFactory;

impl RuntimeFactory for ConsoleFactory {
    fn run(
        &self,
        shared_state: Arc<RwLock<SharedState>>,
        commands: tmpsc::UnboundedReceiver<RuntimeCommand>,
        updater: Arc<dyn ViewUpdater + Send + Sync + 'static>,
        stop: trigger::Receiver,
    ) -> futures::future::BoxFuture<'static, ()> {
        let runtime = Runtime::new(shared_state, commands, updater, stop);
        runtime.run().boxed()
    }
}

struct Runtime {
    shared_state: Arc<RwLock<SharedState>>,
    commands: tmpsc::UnboundedReceiver<RuntimeCommand>,
    output: Arc<dyn ViewUpdater + Send + Sync + 'static>,
    stop: trigger::Receiver,
}

impl Runtime {
    fn new(
        shared_state: Arc<RwLock<SharedState>>,
        commands: tmpsc::UnboundedReceiver<RuntimeCommand>,
        output: Arc<dyn ViewUpdater + Send + Sync + 'static>,
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
            output,
            mut stop,
        } = self;

        let mut senders: HashMap<Host, RemoteHandle> = HashMap::new();
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
                                if let Err(e) = link(&mut tx.sender, &node, &lane).await {
                                    send_log(&*output, format!("Connection to {} failed.", remote));
                                    senders.remove(&remote);
                                    state.remove_all(&remote);
                                    response.send(Err(e));
                                } else {
                                    response.send(Ok(id));
                                }
                            } else {
                                match open_connection(&remote).await {
                                    Ok(ws) => {
                                        match ws.split() {
                                            Ok((mut tx, rx)) => {
                                                if let Err(e) = link(&mut tx, &node, &lane).await {
                                                    send_log(&*output, format!("Connection to remote {} failed with: {}", remote, e));
                                                    let _ = state.remove(id);
                                                    response.send(Err(e));
                                                } else {
                                                    let (recv_stop_tx, recv_stop_rx) =
                                                        trigger::trigger();
                                                    send_log(
                                                        &*output,
                                                        format!(
                                                            "Opened new connection to: {}",
                                                            remote
                                                        ),
                                                    );
                                                    senders.insert(
                                                        remote.clone(),
                                                        RemoteHandle::new(tx, recv_stop_tx),
                                                    );
                                                    receivers.push(Box::pin(
                                                        into_stream(remote, rx)
                                                            .take_until(recv_stop_rx),
                                                    ));
                                                    response.send(Ok(id));
                                                }
                                            }
                                            Err(e) => {
                                                send_log(&*output, format!("Failed to open a connection to {}, error was: {}", remote, e));
                                                let _ = state.remove(id);
                                                response.send(Err(e));
                                            }
                                        }
                                    }
                                    Err(e) => response.send(Err(e)),
                                }
                            }
                        }
                    }
                    RuntimeCommand::Sync(id) => {
                        if let Some(Endpoint { remote, node, lane }) = state.get_endpoint(id) {
                            if let Some(tx) = senders.get_mut(remote) {
                                if sync(&mut tx.sender, node, lane).await.is_err() {
                                    send_log(&*output, format!("Connection to {} failed.", remote));
                                    senders.remove(remote);
                                    state.remove_all(&remote.clone());
                                }
                            }
                        }
                    }
                    RuntimeCommand::Command(id, body) => {
                        let recon = format!("{}", print_recon_compact(&body));
                        if let Some(Endpoint { remote, node, lane }) = state.get_endpoint(id) {
                            if let Some(tx) = senders.get_mut(remote) {
                                if send_cmd(&mut tx.sender, node, lane, &recon).await.is_err() {
                                    send_log(&*output, format!("Connection to {} failed.", remote));
                                    senders.remove(remote);
                                    state.remove_all(&remote.clone());
                                }
                            }
                        }
                    }
                    RuntimeCommand::AdHocCommand(endpoint, body) => {
                        let Endpoint { remote, node, lane } = endpoint;
                        let recon = format!("{}", print_recon_compact(&body));
                        if let Some(tx) = senders.get_mut(&remote) {
                            if send_cmd(&mut tx.sender, &node, &lane, &recon)
                                .await
                                .is_err()
                            {
                                send_log(&*output, format!("Connection to {} failed.", remote));
                                senders.remove(&remote);
                            }
                        } else if let Ok((mut tx, rx)) =
                            open_connection(&remote).await.and_then(|ws| ws.split())
                        {
                            if send_cmd(&mut tx, &node, &lane, &recon).await.is_ok() {
                                let (recv_stop_tx, recv_stop_rx) = trigger::trigger();
                                senders.insert(remote.clone(), RemoteHandle::new(tx, recv_stop_tx));
                                receivers.push(Box::pin(
                                    into_stream(remote, rx).take_until(recv_stop_rx),
                                ));
                            }
                        }
                    }
                    RuntimeCommand::Unlink(id) => {
                        if let Some(Endpoint { remote, node, lane }) = state.get_endpoint(id) {
                            if let Some(tx) = senders.get_mut(remote) {
                                if unlink(&mut tx.sender, node, lane).await.is_err() {
                                    send_log(&*output, format!("Connection to {} failed.", remote));
                                    senders.remove(remote);
                                    state.remove_all(&remote.clone());
                                }
                            }
                        }
                    }
                    RuntimeCommand::UnlinkAll => {
                        for (_, Endpoint { remote, node, lane }) in state.clear().into_iter() {
                            let mut senders = std::mem::take(&mut senders);
                            if let Some(tx) = senders.get_mut(&remote) {
                                if unlink(&mut tx.sender, &node, &lane).await.is_err() {
                                    send_log(&*output, format!("Connection to {} failed.", remote));
                                    senders.remove(&remote);
                                }
                            }
                            for (host, _sender) in senders.into_iter() {
                                send_log(&*output, format!("Closed connection to: {}", host));
                            }
                        }
                    }
                },
                RuntimeEvent::Message(host, body) => {
                    match handle_body(&mut state, host, &body, &*output, &mut senders) {
                        Ok(msg) => {
                            send_link(&*output, msg);
                        }
                        Err(BadEnvelope(error)) => {
                            send_log(&*output, error);
                        }
                    }
                }
                RuntimeEvent::Failed(host) => {
                    state.remove_all(&host);
                }
            }
        }
    }
}

pub struct RemoteHandle {
    sender: Tx,
    stop_tx: Option<trigger::Sender>,
}

impl RemoteHandle {
    pub fn new(sender: Tx, stop_tx: trigger::Sender) -> Self {
        RemoteHandle {
            sender,
            stop_tx: Some(stop_tx),
        }
    }
}

impl Drop for RemoteHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.stop_tx.take() {
            tx.trigger();
        }
    }
}

fn send_link(output: &dyn ViewUpdater, line: DisplayResponse) {
    block_in_place(move || output.update(UIUpdate::LinkDisplay(line))).expect(UI_DROPPED)
}

fn send_log(output: &dyn ViewUpdater, message: String) {
    block_in_place(move || output.update(UIUpdate::LogMessage(message))).expect(UI_DROPPED)
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
                } else if let Ok(body) = std::str::from_utf8(buffer.as_ref()) {
                    let response = (remote.clone(), body.to_string());
                    Some((Ok(response), (remote, Some(rx), buffer)))
                } else {
                    Some((Err(Failed(remote.clone())), (remote, None, buffer)))
                }
            } else {
                None
            }
        },
    )
}

async fn open_connection(host: &Host) -> Result<WebSocket<TcpStream, NoExt>, ratchet::Error> {
    let socket = TcpStream::connect(&host.host_only()).await?;
    let subprotocols = ProtocolRegistry::new(vec!["warp0"]).unwrap();
    let r = ratchet::subscribe_with(
        WebSocketConfig::default(),
        socket,
        host.to_string(),
        NoExtProvider,
        subprotocols,
    )
    .await;
    match r {
        Ok(upgraded) => Ok(upgraded.into_websocket()),
        Err(e) => Err(ratchet::Error::with_cause(
            ErrorKind::Protocol,
            format!("{} - {:?}", host, e),
        )),
    }
}

struct BadEnvelope(String);

fn handle_body(
    state: &mut State,
    host: Host,
    body: &str,
    output: &dyn ViewUpdater,
    senders: &mut HashMap<Host, RemoteHandle>,
) -> Result<DisplayResponse, BadEnvelope> {
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
                if let Some(host) = state.remove(id) {
                    send_log(output, format!("Closed connection to: {}", host));
                    senders.remove(&host);
                }
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
            Ok(DisplayResponse::event(id, body.trim().to_string()))
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

    #[must_use]
    fn remove(&mut self, id: usize) -> Option<Host> {
        let State { shared, links, rev } = self;
        shared.write().remove(id);
        let removed = rev.remove(&id);
        if let Some(endpoint) = removed {
            links.remove(&endpoint);
            if links.keys().any(|e| e.remote == endpoint.remote) {
                None
            } else {
                Some(endpoint.remote)
            }
        } else {
            None
        }
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

    fn clear(&mut self) -> BTreeMap<usize, Endpoint> {
        let State { shared, links, rev } = self;
        shared.write().clear();
        links.clear();
        std::mem::take(rev)
    }
}

#[cfg(test)]
mod moo {
    use ratchet::{NoExtProvider, ProtocolRegistry, WebSocketConfig};
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn mooo() {
        let sock = TcpStream::connect("localhost:49466").await.unwrap();
        let subprotocols = ProtocolRegistry::new(vec!["warp0"]).unwrap();
        let host_str = "ws://localhost:49466".to_string();
        ratchet::subscribe_with(
            WebSocketConfig::default(),
            sock,
            host_str,
            NoExtProvider,
            subprotocols,
        )
        .await
        .unwrap();
    }
}
