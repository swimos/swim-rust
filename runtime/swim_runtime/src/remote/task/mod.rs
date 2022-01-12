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
};
use crate::error::{ConnectionDropped, RoutingError};
use crate::remote::config::RemoteConnectionsConfig;
use crate::remote::router::{BidirectionalReceiverRequest, Router, TaggedRouter};
use crate::routing::{Route, RoutingAddr, TaggedEnvelope, TaggedSender};
use crate::ws::{into_stream, WsMessage};
use futures::future::join_all;
use futures::future::BoxFuture;
use futures::{select_biased, FutureExt, StreamExt};
use pin_utils::pin_mut;
use ratchet::{CloseCode, CloseReason, SplittableExtension, WebSocket, WebSocketStream};
use slab::Slab;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::str::FromStr;
use std::time::Duration;
use swim_form::Form;
use swim_model::path::{Addressable, RelativePath};
use swim_recon::parser::{parse_recognize, ParseError, Span};
use swim_utilities::errors::Recoverable;
use swim_utilities::future::retryable::RetryStrategy;
use swim_utilities::future::task::Spawner;
use swim_utilities::routing::uri::{BadRelativeUri, RelativeUri};
use swim_utilities::trigger;
use swim_warp::envelope::{Envelope, EnvelopeHeader};
use tokio::sync::mpsc;
use tokio::time::{sleep, Instant};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, Level};

const ZERO: Duration = Duration::from_secs(0);
const PEER_CLOSED: &str = "The peer closed the connection.";
const IGNORING_MESSAGE: &str = "Ignoring unexpected message.";
const ERROR_ON_CLOSE: &str = "Error whilst closing connection.";
const BIDIRECTIONAL_RECEIVER_ERROR: &str = "Error whilst sending a bidirectional receiver.";

/// A task that manages reading from and writing to a web-sockets channel.
pub struct ConnectionTask<Sock, Ext, Path> {
    tag: RoutingAddr,
    ws_stream: WebSocket<Sock, Ext>,
    messages: mpsc::Receiver<TaggedEnvelope>,
    message_injector: mpsc::Sender<TaggedEnvelope>,
    router: TaggedRouter<Path>,
    bidirectional_request_rx: mpsc::Receiver<BidirectionalReceiverRequest>,
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

enum ConnectionEvent {
    Read(Result<WsMessage, ratchet::Error>),
    Write(TaggedEnvelope),
    Request(BidirectionalReceiverRequest),
}

impl<Sock, Ext, Path> ConnectionTask<Sock, Ext, Path>
where
    Sock: WebSocketStream,
    Ext: SplittableExtension,
    Path: Addressable,
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
        router: TaggedRouter<Path>,
        (messages_tx, messages_rx): (mpsc::Sender<TaggedEnvelope>, mpsc::Receiver<TaggedEnvelope>),
        bidirectional_request_rx: mpsc::Receiver<BidirectionalReceiverRequest>,
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
            bidirectional_request_rx,
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
            bidirectional_request_rx,
            stop_signal,
            config,
        } = self;

        let mut outgoing_payloads = ReceiverStream::new(messages).map(Into::into).fuse();
        let mut bidirectional_request_rx = ReceiverStream::new(bidirectional_request_rx).fuse();
        let mut bidirectional_connections = Slab::new();

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
                conn_request = bidirectional_request_rx.next() => conn_request.map(ConnectionEvent::Request),
                envelope = envelopes.next() => envelope.map(ConnectionEvent::Read),
                payload  = outgoing_payloads.next() => payload.map(ConnectionEvent::Write)
            };

            if let Some(event) = next {
                match event {
                    ConnectionEvent::Request(receiver_request) => {
                        let (tx, rx) = mpsc::channel(config.channel_buffer_size.get());
                        bidirectional_connections.insert(TaggedSender::new(tag, tx));

                        if receiver_request.send(rx).is_err() {
                            event!(Level::WARN, BIDIRECTIONAL_RECEIVER_ERROR);
                            // todo: should the connection not be removed here?
                        }
                    }
                    ConnectionEvent::Read(msg) => match msg {
                        Ok(WsMessage::Text(message)) => {
                            match parse_recognize(Span::new(message.as_str()), false) {
                                Ok(envelope) => {
                                    let dispatch_result = dispatch_envelope(
                                        &mut router,
                                        &mut bidirectional_connections,
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

        match completion {
            Completion::Failed(err) => ConnectionDropped::Failed(err),
            Completion::TimedOut => ConnectionDropped::TimedOut(config.activity_timeout),
            Completion::StoppedRemotely => ConnectionDropped::Failed(ConnectionError::Closed(
                CloseError::new(CloseErrorKind::ClosedRemotely, None),
            )),
            _ => ConnectionDropped::Closed,
        }
    }
}

/// Error type indicating a failure to route an incoming message.
#[derive(Debug)]
enum DispatchError {
    BadNodeUri(BadRelativeUri),
    RoutingProblem(RoutingError),
    Dropped(ConnectionDropped),
}

impl From<ResolutionError> for DispatchError {
    fn from(e: ResolutionError) -> Self {
        DispatchError::RoutingProblem(RoutingError::Resolution(e))
    }
}

impl Display for DispatchError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DispatchError::BadNodeUri(err) => write!(f, "Invalid relative URI: '{}'", err),
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
            _ => true,
        }
    }
}

impl From<BadRelativeUri> for DispatchError {
    fn from(err: BadRelativeUri) -> Self {
        DispatchError::BadNodeUri(err)
    }
}

impl From<RoutingError> for DispatchError {
    fn from(err: RoutingError) -> Self {
        DispatchError::RoutingProblem(err)
    }
}

async fn dispatch_envelope<Path, F, D>(
    router: &mut TaggedRouter<Path>,
    bidirectional_connections: &mut Slab<TaggedSender>,
    resolved: &mut HashMap<RelativePath, Route>,
    mut envelope: Envelope,
    mut retry_strategy: RetryStrategy,
    delay_fn: F,
) -> Result<(), (Envelope, DispatchError)>
where
    Path: Addressable,
    F: Fn(Duration) -> D,
    D: Future<Output = ()>,
{
    loop {
        let result =
            try_dispatch_envelope(router, bidirectional_connections, resolved, envelope).await;
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

async fn try_dispatch_envelope<Path>(
    router: &mut TaggedRouter<Path>,
    bidirectional_connections: &mut Slab<TaggedSender>,
    resolved: &mut HashMap<RelativePath, Route>,
    envelope: Envelope,
) -> Result<(), (Envelope, DispatchError)>
where
    Path: Addressable,
{
    match envelope.discriminate_header() {
        EnvelopeHeader::Response(_) => {
            let mut futures = vec![];

            for (idx, conn) in bidirectional_connections.iter_mut() {
                let envelope = envelope.clone();

                futures.push(async move {
                    let result = conn.send_item(envelope).await;
                    (idx, result)
                });
            }

            let results = join_all(futures).await;

            for result in results {
                if let (idx, Err(_)) = result {
                    bidirectional_connections.remove(idx);
                }
            }

            Ok(())
        }
        EnvelopeHeader::Request(target) => {
            let Route { sender, .. } = if let Some(route) = resolved.get_mut(&target) {
                if route.sender.is_closed() {
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
            if let Err(err) = sender.send_item(envelope).await {
                if let Some(Route { on_drop, .. }) = resolved.remove(&target) {
                    let reason = on_drop
                        .await
                        .map(|reason| (*reason).clone())
                        .unwrap_or(ConnectionDropped::Unknown);
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
async fn insert_new_route<'a, Path>(
    router: &mut TaggedRouter<Path>,
    resolved: &'a mut HashMap<RelativePath, Route>,
    target: &'a RelativePath,
) -> Result<&'a mut Route, DispatchError>
where
    Path: Addressable,
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

async fn get_route<Path>(
    router: &mut TaggedRouter<Path>,
    target: &RelativePath,
) -> Result<Route, DispatchError>
where
    Path: Addressable,
{
    let target_addr = router
        .lookup(RelativeUri::from_str(target.node.as_str())?)
        .await?;
    Ok(router.resolve_sender(target_addr).await?)
}

/// Factory to create and spawn new connection tasks.
pub struct TaskFactory<Path> {
    stop_trigger: trigger::Receiver,
    configuration: RemoteConnectionsConfig,
    router: Router<Path>,
}

impl<Path> TaskFactory<Path> {
    pub fn new(
        stop_trigger: trigger::Receiver,
        configuration: RemoteConnectionsConfig,
        router: Router<Path>,
    ) -> Self {
        TaskFactory {
            stop_trigger,
            configuration,
            router,
        }
    }
}

impl<Path> TaskFactory<Path>
where
    Path: Addressable,
{
    pub fn spawn_connection_task<Sock, Ext, Sp>(
        &self,
        ws_stream: WebSocket<Sock, Ext>,
        tag: RoutingAddr,
        spawner: &Sp,
    ) -> (
        mpsc::Sender<TaggedEnvelope>,
        mpsc::Sender<BidirectionalReceiverRequest>,
    )
    where
        Ext: SplittableExtension + Send + 'static,
        Sock: WebSocketStream + Send + 'static,
        Sp: Spawner<BoxFuture<'static, (RoutingAddr, ConnectionDropped)>>,
    {
        let TaskFactory {
            stop_trigger,
            configuration,
            router,
        } = self;
        let (msg_tx, msg_rx) = mpsc::channel(configuration.channel_buffer_size.get());
        let (bidirectional_request_tx, bidirectional_request_rx) =
            mpsc::channel(configuration.channel_buffer_size.get());

        let task = ConnectionTask::new(
            tag,
            ws_stream,
            router.tagged(tag),
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
