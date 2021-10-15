// Copyright 2015-2021 SWIM.AI inc.
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

use crate::routing::error::RouterError;
use crate::routing::remote::config::ConnectionConfig;
use crate::routing::remote::router::RemoteRouter;
use crate::routing::remote::RoutingRequest;
use crate::routing::{
    ConnectionDropped, Route, RoutingAddr, ServerRouter, ServerRouterFactory, TaggedEnvelope,
};
use futures::future::BoxFuture;
use futures::select_biased;
use futures::{FutureExt, StreamExt};
use pin_utils::pin_mut;
use ratchet::{CloseCode, CloseReason, SplittableExtension, WebSocket, WebSocketStream};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::str::FromStr;
use std::time::Duration;
use swim_model::path::RelativePath;
use swim_recon::parser::{parse_value, ParseError};
use swim_runtime::error::{
    CloseError, CloseErrorKind, ConnectionError, ProtocolError, ProtocolErrorKind, ResolutionError,
    ResolutionErrorKind,
};
use swim_runtime::ws::{into_stream, WsMessage};
use swim_utilities::errors::Recoverable;
use swim_utilities::future::retryable::RetryStrategy;
use swim_utilities::future::task::Spawner;
use swim_utilities::routing::uri::{BadRelativeUri, RelativeUri};
use swim_utilities::trigger;
use swim_warp::envelope::{Envelope, EnvelopeHeader, EnvelopeParseErr, OutgoingHeader};
use tokio::sync::mpsc;
use tokio::time::{sleep, Instant};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, Level};

const ZERO: Duration = Duration::from_secs(0);
const IGNORING_MESSAGE: &str = "Ignoring unexpected message.";
const ERROR_ON_CLOSE: &str = "Error whilst closing connection.";

/// A task that manages reading from and writing to a web-sockets channel.
pub struct ConnectionTask<Sock, Ext, Router> {
    ws_stream: WebSocket<Sock, Ext>,
    messages: mpsc::Receiver<TaggedEnvelope>,
    message_injector: mpsc::Sender<TaggedEnvelope>,
    router: Router,
    stop_signal: trigger::Receiver,
    config: ConnectionConfig,
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

impl From<EnvelopeParseErr> for Completion {
    fn from(err: EnvelopeParseErr) -> Self {
        Completion::Failed(ConnectionError::Protocol(ProtocolError::new(
            ProtocolErrorKind::Warp,
            Some(err.to_string()),
        )))
    }
}

enum WsEvent {
    Read(Result<WsMessage, ratchet::Error>),
    Write(TaggedEnvelope),
}

impl<Sock, Ext, Router> ConnectionTask<Sock, Ext, Router>
where
    Sock: WebSocketStream,
    Ext: SplittableExtension,
    Router: ServerRouter,
{
    /// Create a new task.
    ///
    /// #Arguments
    ///
    /// * `ws_stream` - The joined sink/stream that implements the web sockets protocol.
    /// * `router` - Router to route incoming messages to the appropriate destination.
    /// * `messages`- Stream of messages to be sent into the sink.
    /// * `message_injector` - Allows messages to be injected into the outgoing stream.
    /// * `stop_signal` - Signals to the task that it should stop.
    /// * `config` - Configuration for the connectino task.
    /// runtime.
    pub fn new(
        ws_stream: WebSocket<Sock, Ext>,
        router: Router,
        messages: mpsc::Receiver<TaggedEnvelope>,
        message_injector: mpsc::Sender<TaggedEnvelope>,
        stop_signal: trigger::Receiver,
        config: ConnectionConfig,
    ) -> Self {
        assert!(config.activity_timeout > ZERO);
        ConnectionTask {
            ws_stream,
            messages,
            message_injector,
            router,
            stop_signal,
            config,
        }
    }

    pub async fn run(self) -> ConnectionDropped {
        let ConnectionTask {
            ws_stream,
            messages,
            message_injector,
            mut router,
            stop_signal,
            config,
        } = self;

        let mut outgoing_payloads = ReceiverStream::new(messages).fuse();

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
            let next: Option<WsEvent> = select_biased! {
                _ = stop_fused => {
                    break Completion::StoppedLocally;
                },
                _ = (&mut timeout).fuse() => {
                    break Completion::TimedOut;
                }
                envelope = envelopes.next() => envelope.map(WsEvent::Read),
                payload  = outgoing_payloads.next() => payload.map(WsEvent::Write)
            };

            if let Some(event) = next {
                match event {
                    WsEvent::Read(msg) => match msg {
                        Ok(msg) => match msg {
                            WsMessage::Text(message) => {
                                match read_envelope(&message) {
                                    Ok(envelope) => {
                                        let dispatch_result = dispatch_envelope(
                                            &mut router,
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
                                    Err(e) => {
                                        break e;
                                    }
                                }
                            }
                            message => {
                                event!(Level::WARN, IGNORING_MESSAGE, ?message);
                            }
                        },
                        Err(err) => {
                            break Completion::Failed(err.into());
                        }
                    },
                    WsEvent::Write(envelope) => {
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

fn read_envelope(msg: &str) -> Result<Envelope, Completion> {
    Ok(Envelope::try_from(parse_value(msg)?)?)
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

async fn dispatch_envelope<Router, F, D>(
    router: &mut Router,
    resolved: &mut HashMap<RelativePath, Route>,
    mut envelope: Envelope,
    mut retry_strategy: RetryStrategy,
    delay_fn: F,
) -> Result<(), (Envelope, DispatchError)>
where
    Router: ServerRouter,
    F: Fn(Duration) -> D,
    D: Future<Output = ()>,
{
    loop {
        let result = try_dispatch_envelope(router, resolved, envelope).await;
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

async fn try_dispatch_envelope<Router>(
    router: &mut Router,
    resolved: &mut HashMap<RelativePath, Route>,
    envelope: Envelope,
) -> Result<(), (Envelope, DispatchError)>
where
    Router: ServerRouter,
{
    if let Some(target) = envelope.header.relative_path().as_ref() {
        let Route { sender, .. } = if let Some(route) = resolved.get_mut(target) {
            if route.sender.inner.is_closed() {
                resolved.remove(target);
                match insert_new_route(router, resolved, target).await {
                    Ok(route) => route,
                    Err(err) => return Err((envelope, err)),
                }
            } else {
                route
            }
        } else {
            match insert_new_route(router, resolved, target).await {
                Ok(route) => route,
                Err(err) => return Err((envelope, err)),
            }
        };
        if let Err(err) = sender.send_item(envelope).await {
            if let Some(Route { on_drop, .. }) = resolved.remove(target) {
                let reason = on_drop
                    .await
                    .map(|reason| (*reason).clone())
                    .unwrap_or(ConnectionDropped::Unknown);
                let (_, env) = err.split();
                Err((env, DispatchError::Dropped(reason)))
            } else {
                unreachable!();
            }
        } else {
            Ok(())
        }
    } else {
        panic!("Authentication envelopes not yet supported.");
    }
}

#[allow(clippy::needless_lifetimes)]
async fn insert_new_route<'a, Router>(
    router: &mut Router,
    resolved: &'a mut HashMap<RelativePath, Route>,
    target: &RelativePath,
) -> Result<&'a mut Route, DispatchError>
where
    Router: ServerRouter,
{
    let route = get_route(router, target).await?;
    match resolved.entry(target.clone()) {
        Entry::Occupied(_) => unreachable!(),
        Entry::Vacant(entry) => Ok(entry.insert(route)),
    }
}

async fn get_route<Router>(
    router: &mut Router,
    target: &RelativePath,
) -> Result<Route, DispatchError>
where
    Router: ServerRouter,
{
    let target_addr = router
        .lookup(None, RelativeUri::from_str(target.node.as_str())?)
        .await?;
    Ok(router.resolve_sender(target_addr).await?)
}

/// Factory to create and spawn new connection tasks.
pub struct TaskFactory<RouterFac> {
    request_tx: mpsc::Sender<RoutingRequest>,
    stop_trigger: trigger::Receiver,
    configuration: ConnectionConfig,
    delegate_router: RouterFac,
}

impl<RouterFac> TaskFactory<RouterFac> {
    pub fn new(
        request_tx: mpsc::Sender<RoutingRequest>,
        stop_trigger: trigger::Receiver,
        configuration: ConnectionConfig,
        delegate_router: RouterFac,
    ) -> Self {
        TaskFactory {
            request_tx,
            stop_trigger,
            configuration,
            delegate_router,
        }
    }
}
impl<RouterFac> TaskFactory<RouterFac>
where
    RouterFac: ServerRouterFactory + 'static,
{
    pub fn spawn_connection_task<Sock, Ext, Sp>(
        &self,
        ws_stream: WebSocket<Sock, Ext>,
        tag: RoutingAddr,
        spawner: &Sp,
    ) -> mpsc::Sender<TaggedEnvelope>
    where
        Ext: SplittableExtension + Send + 'static,
        Sock: WebSocketStream + Send + 'static,
        Sp: Spawner<BoxFuture<'static, (RoutingAddr, ConnectionDropped)>>,
    {
        let TaskFactory {
            request_tx,
            stop_trigger,
            configuration,
            delegate_router,
        } = self;
        let (msg_tx, msg_rx) = mpsc::channel(configuration.channel_buffer_size.get());
        let task = ConnectionTask::new(
            ws_stream,
            RemoteRouter::new(tag, delegate_router.create_for(tag), request_tx.clone()),
            msg_rx,
            msg_tx.clone(),
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
        msg_tx
    }
}

//Get the target path only for link and sync messages (for creating the "not found" response).
fn link_or_sync(env: Envelope) -> Option<RelativePath> {
    match env.header {
        EnvelopeHeader::OutgoingLink(OutgoingHeader::Link(_), path) => Some(path),
        EnvelopeHeader::OutgoingLink(OutgoingHeader::Sync(_), path) => Some(path),
        _ => None,
    }
}

// Dummy origing for not found messages.
const NOT_FOUND_ADDR: RoutingAddr = RoutingAddr::local(0);

// For a link or sync message that cannot be routed, send back a "not found" message.
async fn handle_not_found(env: Envelope, sender: &mpsc::Sender<TaggedEnvelope>) {
    if let Some(RelativePath { node, lane }) = link_or_sync(env) {
        let not_found = Envelope::node_not_found(node, lane);
        //An error here means the web socket connection has failed and will produce an error
        //the next time it is polled so it is fine to discard this error.
        let _ = sender.send(TaggedEnvelope(NOT_FOUND_ADDR, not_found)).await;
    }
}
