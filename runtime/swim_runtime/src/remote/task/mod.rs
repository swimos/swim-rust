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

#[cfg(test)]
mod tests;

use crate::error::{
    CloseError, CloseErrorKind, ConnectionError, ProtocolError, ProtocolErrorKind, ResolutionError,
    ResolutionErrorKind,
};
use crate::error::{ConnectionDropped, RouterError};
use crate::remote::config::RemoteConnectionsConfig;
use crate::remote::router::RemoteRouter;
use crate::remote::{AttachClientRequest, RawOutRoute, RemoteRoutingRequest};
use crate::routing::{
    Route, Router, RouterFactory, RoutingAddr, TaggedEnvelope, TaggedSender, UnroutableClient,
};
use crate::ws::{into_stream, WsMessage};
use futures::future::BoxFuture;
use futures::{select_biased, FutureExt, StreamExt};
use pin_utils::pin_mut;
use ratchet::{CloseCode, CloseReason, SplittableExtension, WebSocket, WebSocketStream};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::str::FromStr;
use std::time::Duration;
use swim_form::Form;
use swim_model::path::RelativePath;
use swim_model::Text;
use swim_recon::parser::{parse_recognize, ParseError, Span};
use swim_utilities::errors::Recoverable;
use swim_utilities::future::open_ended::OpenEndedFutures;
use swim_utilities::future::retryable::RetryStrategy;
use swim_utilities::future::task::Spawner;
use swim_utilities::routing::uri::{BadRelativeUri, RelativeUri};
use swim_utilities::trigger;
use swim_utilities::trigger::promise;
use swim_warp::envelope::{Envelope, EnvelopeHeader};
use tokio::sync::mpsc;
use tokio::time::{sleep, Instant};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, Level};

const ZERO: Duration = Duration::from_secs(0);
const PEER_CLOSED: &str = "The peer closed the connection.";
const IGNORING_MESSAGE: &str = "Ignoring unexpected message.";
const ERROR_ON_CLOSE: &str = "Error whilst closing connection.";
const CLIENT_ATTACH_DROPPED: &str = "Client request was dropped.";

/// A task that manages reading from and writing to a web-sockets channel.
pub struct ConnectionTask<Sock, Ext, Router> {
    tag: RoutingAddr,
    ws_stream: WebSocket<Sock, Ext>,
    messages: mpsc::Receiver<TaggedEnvelope>,
    message_injector: mpsc::Sender<TaggedEnvelope>,
    router: Router,
    attach_client_rx: mpsc::Receiver<AttachClientRouted>,
    stop_signal: trigger::Receiver,
    config: RemoteConnectionsConfig,
}

/// Possible ways in which the task can end.
#[derive(Debug)]
enum Completion {
    Failed(ConnectionError),
    TimedOut,
    StoppedRemotely,
    StoppedLocally,
}

impl From<ParseError> for Completion {
    fn from(err: ParseError) -> Self {
        Completion::Failed(ConnectionError::Protocol(ProtocolError::new(
            ProtocolErrorKind::Warp,
            Some(err.to_string()),
        )))
    }
}

pub struct AttachClientRouted {
    pub route: RawOutRoute,
    pub request: AttachClientRequest,
}

impl AttachClientRouted {
    pub fn new(route: RawOutRoute, request: AttachClientRequest) -> Self {
        AttachClientRouted { route, request }
    }
}

enum ConnectionEvent {
    Read(Result<WsMessage, ratchet::Error>),
    Write(TaggedEnvelope),
    Request(AttachClientRouted),
    ClientTerminated(RelativePath, u64),
}

impl<Sock, Ext, R> ConnectionTask<Sock, Ext, R>
where
    Sock: WebSocketStream,
    Ext: SplittableExtension,
    R: Router,
{
    /// Create a new task.
    ///
    /// #Arguments
    ///
    /// * `tag`  - The routing address of the connection.
    /// * `ws_stream` - The joined sink/stream that implements the web sockets protocol.
    /// * `router` - Router to route incoming messages to the appropriate destination.
    /// * `messages_tx` - Allows messages to be injected into the outgoing stream.
    /// * `messages_rx`- Stream of messages to be sent into the sink.
    /// * `bidirectional_request_rx` - Stream of bidirectional requests.
    /// * `stop_signal` - Signals to the task that it should stop.
    /// * `config` - Configuration for the connection task.
    /// runtime.
    pub fn new(
        tag: RoutingAddr,
        ws_stream: WebSocket<Sock, Ext>,
        router: R,
        (messages_tx, messages_rx): (mpsc::Sender<TaggedEnvelope>, mpsc::Receiver<TaggedEnvelope>),
        attach_client_rx: mpsc::Receiver<AttachClientRouted>,
        stop_signal: trigger::Receiver,
        config: RemoteConnectionsConfig,
    ) -> Self {
        assert!(config.activity_timeout > ZERO);
        ConnectionTask {
            tag,
            ws_stream,
            messages: messages_rx,
            message_injector: messages_tx,
            router,
            attach_client_rx,
            stop_signal,
            config,
        }
    }

    pub async fn run(self) -> ConnectionDropped {
        let ConnectionTask {
            tag,
            ws_stream,
            messages,
            message_injector,
            mut router,
            attach_client_rx,
            stop_signal,
            config,
        } = self;

        let (client_close_tx, client_close_rx) = promise::promise();

        let mut outgoing_payloads = ReceiverStream::new(messages).map(Into::into).fuse();
        let mut client_req_stream = ReceiverStream::new(attach_client_rx).fuse();
        let mut client_connections = HashMap::<RelativePath, Vec<(u64, TaggedSender)>>::new();

        let (mut ws_tx, ws_rx) = match ws_stream.split() {
            Ok((tx, rx)) => (tx, rx),
            Err(_) => return ConnectionDropped::Closed,
        };

        let envelopes = into_stream(ws_rx).fuse();
        pin_mut!(envelopes);

        let mut stop_fused = stop_signal.fuse();
        let timeout = sleep(config.activity_timeout);
        pin_mut!(timeout);

        let mut resolved: HashMap<RelativePath, Route> = HashMap::new();
        let yield_mod = config.yield_after.get();
        let mut iteration_count: usize = 0;

        let client_monitor = OpenEndedFutures::new();
        let mut client_index: u64 = 0;
        pin_mut!(client_monitor);

        let completion = loop {
            timeout.as_mut().reset(
                Instant::now()
                    .checked_add(config.activity_timeout)
                    .expect("Timer overflow."),
            );
            let next: Option<ConnectionEvent> = select_biased! {
                _ = stop_fused => {
                    break Completion::StoppedLocally;
                },
                _ = (&mut timeout).fuse() => {
                    break Completion::TimedOut;
                }
                client = client_monitor.next() => client.map(|(uri, i)| ConnectionEvent::ClientTerminated(uri, i)),
                conn_request = client_req_stream.next() => conn_request.map(ConnectionEvent::Request),
                envelope = envelopes.next() => envelope.map(ConnectionEvent::Read),
                payload  = outgoing_payloads.next() => payload.map(ConnectionEvent::Write)
            };

            if let Some(event) = next {
                match event {
                    ConnectionEvent::Request(AttachClientRouted { route, request }) => {
                        let (tx, rx) = mpsc::channel(config.channel_buffer_size.get());

                        let (on_drop, on_dropped) = promise::promise();

                        let client_route =
                            UnroutableClient::new(route, rx, client_close_rx.clone(), on_drop);

                        let AttachClientRequest {
                            node,
                            lane,
                            request,
                            ..
                        } = request;

                        let key = RelativePath::new(Text::from(node.to_string()), lane);

                        if request.send_ok(client_route).is_ok() {
                            let senders = client_connections.entry(key.clone()).or_default();
                            let i = client_index;
                            client_index += 1;
                            senders.push((i, TaggedSender::new(tag, tx)));

                            let monitor = async move {
                                let _ = on_dropped.await;
                                (key, i)
                            };
                            client_monitor.push(monitor);
                        } else {
                            event!(Level::DEBUG, CLIENT_ATTACH_DROPPED);
                        }

                        //TODO Add cleanup.
                    }
                    ConnectionEvent::Read(msg) => match msg {
                        Ok(WsMessage::Text(message)) => {
                            match parse_recognize(Span::new(message.as_str()), false) {
                                Ok(envelope) => {
                                    let dispatch_result = dispatch_envelope(
                                        &mut router,
                                        &mut client_connections,
                                        &mut resolved,
                                        envelope,
                                        config.connection_retries,
                                        sleep,
                                    )
                                    .await;

                                    // Todo add router to ratchet's upgrade function to avoid this
                                    if let Err((env, _)) = dispatch_result {
                                        handle_not_found(env, &message_injector).await;
                                    }
                                }
                                Err(err) => {
                                    break err.into();
                                }
                            }
                        }
                        Ok(WsMessage::Close(reason)) => {
                            event!(Level::DEBUG, PEER_CLOSED, ?reason);
                            break Completion::StoppedRemotely;
                        }
                        Ok(message) => {
                            event!(Level::WARN, IGNORING_MESSAGE, ?message);
                        }
                        Err(e) => break Completion::Failed(e.into()),
                    },
                    ConnectionEvent::Write(envelope) => {
                        let TaggedEnvelope(_, envelope) = envelope;
                        if let Err(e) = ws_tx.write_text(envelope.into_value().to_string()).await {
                            break Completion::Failed(e.into());
                        }
                    }
                    ConnectionEvent::ClientTerminated(key, i) => {
                        if let Some(senders) = client_connections.get_mut(&key) {
                            senders.retain(|(j, _)| *j != i);
                        }
                    }
                }

                iteration_count += 1;
                if iteration_count % yield_mod == 0 {
                    tokio::task::yield_now().await;
                }
            } else {
                break Completion::StoppedRemotely;
            }
        };

        if let Some(reason) = match &completion {
            Completion::StoppedLocally => Some(CloseReason::new(
                CloseCode::GoingAway,
                Some("Stopped locally".to_string()),
            )),
            Completion::Failed(ConnectionError::Protocol(e))
                if e.kind() == ProtocolErrorKind::Warp =>
            {
                Some(CloseReason::new(CloseCode::Protocol, e.cause().clone()))
            }
            _ => None,
        } {
            if let Err(error) = ws_tx.close_with(reason).await {
                event!(Level::ERROR, ERROR_ON_CLOSE, ?error);
            }
        }

        let final_result = match completion {
            Completion::Failed(err) => ConnectionDropped::Failed(err),
            Completion::TimedOut => ConnectionDropped::TimedOut(config.activity_timeout),
            Completion::StoppedRemotely => ConnectionDropped::Failed(ConnectionError::Closed(
                CloseError::new(CloseErrorKind::ClosedRemotely, None),
            )),
            _ => ConnectionDropped::Closed,
        };
        let _ = client_close_tx.provide(final_result.clone());
        final_result
    }
}

/// Error type indicating a failure to route an incoming message.
#[derive(Debug)]
enum DispatchError {
    BadNodeUri(BadRelativeUri),
    Unresolvable(ResolutionError),
    RoutingProblem(RouterError),
    Dropped(ConnectionDropped),
}

impl Display for DispatchError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DispatchError::BadNodeUri(err) => write!(f, "Invalid relative URI: '{}'", err),
            DispatchError::Unresolvable(err) => {
                write!(f, "Could not resolve a router endpoint: '{}'", err)
            }
            DispatchError::RoutingProblem(err) => {
                write!(f, "Could not find a router endpoint: '{}'", err)
            }
            DispatchError::Dropped(err) => write!(f, "The routing channel was dropped: '{}'", err),
        }
    }
}

impl Error for DispatchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DispatchError::BadNodeUri(err) => Some(err),
            DispatchError::Unresolvable(err) => Some(err),
            DispatchError::RoutingProblem(err) => Some(err),
            _ => None,
        }
    }
}

impl Recoverable for DispatchError {
    fn is_fatal(&self) -> bool {
        match self {
            DispatchError::RoutingProblem(err) => err.is_fatal(),
            DispatchError::Dropped(reason) => !reason.is_recoverable(),
            DispatchError::Unresolvable(e) if e.kind() == ResolutionErrorKind::Unresolvable => {
                false
            }
            _ => true,
        }
    }
}

impl From<BadRelativeUri> for DispatchError {
    fn from(err: BadRelativeUri) -> Self {
        DispatchError::BadNodeUri(err)
    }
}

impl From<ResolutionError> for DispatchError {
    fn from(err: ResolutionError) -> Self {
        DispatchError::Unresolvable(err)
    }
}

impl From<RouterError> for DispatchError {
    fn from(err: RouterError) -> Self {
        DispatchError::RoutingProblem(err)
    }
}

async fn dispatch_envelope<R, F, D>(
    router: &mut R,
    client_connections: &mut HashMap<RelativePath, Vec<(u64, TaggedSender)>>,
    resolved: &mut HashMap<RelativePath, Route>,
    mut envelope: Envelope,
    mut retry_strategy: RetryStrategy,
    delay_fn: F,
) -> Result<(), (Envelope, DispatchError)>
where
    R: Router,
    F: Fn(Duration) -> D,
    D: Future<Output = ()>,
{
    loop {
        let result = try_dispatch_envelope(router, client_connections, resolved, envelope).await;
        match result {
            Err((env, err)) if !err.is_fatal() => {
                match retry_strategy.next() {
                    Some(Some(dur)) => {
                        delay_fn(dur).await;
                    }
                    None => {
                        break Err((env, err));
                    }
                    _ => {}
                }
                envelope = env;
            }
            Err((env, err)) => {
                break Err((env, err));
            }
            _ => {
                break Ok(());
            }
        }
    }
}

async fn try_dispatch_envelope<R>(
    router: &mut R,
    client_connections: &mut HashMap<RelativePath, Vec<(u64, TaggedSender)>>,
    resolved: &mut HashMap<RelativePath, Route>,
    envelope: Envelope,
) -> Result<(), (Envelope, DispatchError)>
where
    R: Router,
{
    match envelope.discriminate_header() {
        EnvelopeHeader::Response(path) => {
            if let Some(endpoints) = client_connections.get_mut(&path) {
                if let Some(((_, last), senders)) = endpoints.split_last_mut() {
                    let mut failed = HashSet::new();
                    for (i, tx) in senders.iter_mut().map(|(_, s)| s).enumerate() {
                        if tx.send_item(envelope.clone()).await.is_err() {
                            failed.insert(i);
                        }
                    }
                    if last.send_item(envelope).await.is_err() {
                        failed.insert(senders.len());
                    }
                    if !failed.is_empty() {
                        *endpoints = std::mem::take(endpoints)
                            .into_iter()
                            .enumerate()
                            .filter(|(i, _)| !failed.contains(i))
                            .map(|(_, tx)| tx)
                            .collect();
                    }
                }
            }
            Ok(())
        }
        EnvelopeHeader::Request(target) => {
            let route = if let Some(route) = resolved.get_mut(&target) {
                if route.is_closed() {
                    resolved.remove(&target);
                    insert_new_route(router, resolved, &target)
                        .await
                        .map_err(|err| (envelope.clone(), err))?
                } else {
                    route
                }
            } else {
                insert_new_route(router, resolved, &target)
                    .await
                    .map_err(|err| (envelope.clone(), err))?
            };
            if let Err(err) = route.send_item(envelope).await {
                if let Some(route) = resolved.remove(&target) {
                    let reason = route.terminated().await;
                    Err((err.0, DispatchError::Dropped(reason)))
                } else {
                    unreachable!();
                }
            } else {
                Ok(())
            }
        }
        _ => {
            panic!("Authentication envelopes not yet supported.");
        }
    }
}

#[allow(clippy::needless_lifetimes)]
async fn insert_new_route<'a, R>(
    router: &mut R,
    resolved: &'a mut HashMap<RelativePath, Route>,
    target: &RelativePath,
) -> Result<&'a mut Route, DispatchError>
where
    R: Router,
{
    let route = get_route(router, target).await;

    match route {
        Ok(route) => match resolved.entry(target.clone()) {
            Entry::Occupied(_) => unreachable!(),
            Entry::Vacant(entry) => Ok(entry.insert(route)),
        },
        Err(err) => Err(err),
    }
}

async fn get_route<R>(router: &mut R, target: &RelativePath) -> Result<Route, DispatchError>
where
    R: Router,
{
    let target_addr = router
        .lookup(None, RelativeUri::from_str(target.node.as_str())?)
        .await?;
    Ok(router.resolve_sender(target_addr).await?)
}

/// Factory to create and spawn new connection tasks.
pub struct TaskFactory<DelegateRouterFac> {
    request_tx: mpsc::Sender<RemoteRoutingRequest>,
    stop_trigger: trigger::Receiver,
    configuration: RemoteConnectionsConfig,
    delegate_router_fac: DelegateRouterFac,
}

impl<DelegateRouterFac> TaskFactory<DelegateRouterFac> {
    pub fn new(
        request_tx: mpsc::Sender<RemoteRoutingRequest>,
        stop_trigger: trigger::Receiver,
        configuration: RemoteConnectionsConfig,
        delegate_router_fac: DelegateRouterFac,
    ) -> Self {
        TaskFactory {
            request_tx,
            stop_trigger,
            configuration,
            delegate_router_fac,
        }
    }
}

impl<DelegateRouterFac> TaskFactory<DelegateRouterFac>
where
    DelegateRouterFac: RouterFactory + 'static,
{
    pub fn spawn_connection_task<Sock, Ext, Sp>(
        &self,
        ws_stream: WebSocket<Sock, Ext>,
        tag: RoutingAddr,
        spawner: &Sp,
    ) -> (
        mpsc::Sender<TaggedEnvelope>,
        mpsc::Sender<AttachClientRouted>,
    )
    where
        Ext: SplittableExtension + Send + 'static,
        Sock: WebSocketStream + Send + 'static,
        Sp: Spawner<BoxFuture<'static, (RoutingAddr, ConnectionDropped)>>,
    {
        let TaskFactory {
            request_tx,
            stop_trigger,
            configuration,
            delegate_router_fac,
        } = self;
        let (msg_tx, msg_rx) = mpsc::channel(configuration.channel_buffer_size.get());
        let (bidirectional_request_tx, bidirectional_request_rx) =
            mpsc::channel(configuration.channel_buffer_size.get());

        let task = ConnectionTask::new(
            tag,
            ws_stream,
            RemoteRouter::new(tag, delegate_router_fac.create_for(tag), request_tx.clone()),
            (msg_tx.clone(), msg_rx),
            bidirectional_request_rx,
            stop_trigger.clone(),
            *configuration,
        );

        spawner.add(
            async move {
                let result = task.run().await;
                (tag, result)
            }
            .boxed(),
        );
        (msg_tx, bidirectional_request_tx)
    }
}

//Get the target path only for link and sync messages (for creating the "not found" response).
fn link_or_sync(env: Envelope) -> Option<RelativePath> {
    match env {
        Envelope::Link {
            node_uri, lane_uri, ..
        }
        | Envelope::Sync {
            node_uri, lane_uri, ..
        } => Some(RelativePath::new(node_uri, lane_uri)),
        _ => None,
    }
}

// Dummy origing for not found messages.
const NOT_FOUND_ADDR: RoutingAddr = RoutingAddr::plane(0);

// For a link or sync message that cannot be routed, send back a "not found" message.
async fn handle_not_found(env: Envelope, sender: &mpsc::Sender<TaggedEnvelope>) {
    if let Some(RelativePath { node, lane }) = link_or_sync(env) {
        let not_found = Envelope::node_not_found(node, lane);
        //An error here means the web socket connection has failed and will produce an error
        //the next time it is polled so it is fine to discard this error.
        let _ = sender.send(TaggedEnvelope(NOT_FOUND_ADDR, not_found)).await;
    }
}
