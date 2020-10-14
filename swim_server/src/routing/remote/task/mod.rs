// Copyright 2015-2020 SWIM.AI inc.
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

use crate::plane::error::ResolutionError;
use crate::routing::remote::ConnectionDropped;
use crate::routing::ws::{CloseReason, JoinedStreamSink, SelectorResult, WsStreamSelector};
use crate::routing::{Route, ServerRouter, TaggedEnvelope};
use futures::{select_biased, FutureExt, StreamExt};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::str::FromStr;
use std::time::Duration;
use swim_common::model::parser::{self, ParseFailure};
use swim_common::routing::RoutingError;
use swim_common::sink::item::ItemSink;
use swim_common::warp::envelope::{Envelope, EnvelopeParseErr};
use swim_common::warp::path::RelativePath;
use swim_common::ws::error::ConnectionError;
use swim_common::ws::WsMessage;
use tokio::sync::mpsc;
use tokio::time::{delay_for, Instant};
use utilities::future::retryable::strategy::RetryStrategy;
use utilities::sync::trigger;
use utilities::uri::{BadRelativeUri, RelativeUri};

pub struct ConnectionTask<Str, Router> {
    ws_stream: Str,
    messages: mpsc::Receiver<TaggedEnvelope>,
    router: Router,
    stop_signal: trigger::Receiver,
    activity_timeout: Duration,
    retry_strategy: RetryStrategy,
}

const ZERO: Duration = Duration::from_secs(0);

enum Completion {
    Failed(ConnectionError),
    ProtocolError(String),
    TimedOut,
    StoppedRemotely,
    StoppedLocally,
}

impl From<ParseFailure> for Completion {
    fn from(err: ParseFailure) -> Self {
        Completion::ProtocolError(err.to_string())
    }
}

impl From<EnvelopeParseErr> for Completion {
    fn from(err: EnvelopeParseErr) -> Self {
        Completion::ProtocolError(err.to_string())
    }
}

impl<Str, Router> ConnectionTask<Str, Router>
where
    Str: JoinedStreamSink<WsMessage, ConnectionError> + Unpin,
    Router: ServerRouter,
{
    pub fn new(
        ws_stream: Str,
        router: Router,
        messages: mpsc::Receiver<TaggedEnvelope>,
        stop_signal: trigger::Receiver,
        activity_timeout: Duration,
        retry_strategy: RetryStrategy,
    ) -> Self {
        assert!(activity_timeout > ZERO);
        ConnectionTask {
            ws_stream,
            messages,
            router,
            stop_signal,
            activity_timeout,
            retry_strategy,
        }
    }

    pub async fn run(self) -> ConnectionDropped {
        let ConnectionTask {
            mut ws_stream,
            messages,
            mut router,
            stop_signal,
            activity_timeout,
            retry_strategy,
        } = self;
        let outgoing_payloads = messages
            .map(|TaggedEnvelope(_, envelope)| WsMessage::Text(envelope.into_value().to_string()));

        let selector = WsStreamSelector::new(&mut ws_stream, outgoing_payloads);

        let mut stop_fused = stop_signal.fuse();
        let mut select_stream = selector.fuse();
        let mut timeout = delay_for(activity_timeout);

        let mut resolved: HashMap<RelativePath, Route<Router::Sender>> = HashMap::new();

        let completion = loop {
            timeout.reset(
                Instant::now()
                    .checked_add(activity_timeout)
                    .expect("Timer overflow."),
            );
            let next: Option<Result<SelectorResult<WsMessage>, ConnectionError>> = select_biased! {
                _ = stop_fused => {
                    break Completion::StoppedLocally;
                },
                _ = (&mut timeout).fuse() => {
                    break Completion::TimedOut;
                }
                event = select_stream.next() => event,
            };

            if let Some(event) = next {
                match event {
                    Ok(SelectorResult::Read(msg)) => match msg {
                        WsMessage::Text(msg) => match read_envelope(&msg) {
                            Ok(envelope) => {
                                if let Err(_err) = dispatch_envelope(
                                    &mut router,
                                    &mut resolved,
                                    envelope,
                                    retry_strategy.clone(),
                                )
                                .await
                                {
                                    //TODO Log error.
                                }
                            }
                            Err(c) => {
                                break c;
                            }
                        },
                        WsMessage::Binary(_) => {}
                    },
                    Err(err) => {
                        break Completion::Failed(err);
                    }
                    _ => {}
                }
            } else {
                break Completion::StoppedRemotely;
            }
        };

        if let Some(reason) = match &completion {
            Completion::StoppedLocally => Some(CloseReason::GoingAway),
            Completion::ProtocolError(err) => Some(CloseReason::ProtocolError(err.clone())),
            _ => None,
        } {
            if let Err(_err) = ws_stream.close(Some(reason)).await {
                //TODO Log close error.
            }
        }

        match completion {
            Completion::Failed(err) => ConnectionDropped::Failed(err),
            Completion::TimedOut => ConnectionDropped::TimedOut(activity_timeout),
            Completion::ProtocolError(_) => {
                ConnectionDropped::Failed(ConnectionError::ReceiveMessageError)
            }
            _ => ConnectionDropped::Closed,
        }
    }
}

fn read_envelope(msg: &str) -> Result<Envelope, Completion> {
    Ok(Envelope::try_from(parser::parse_single(msg)?)?)
}

enum DispatchError {
    BadNodeUri(BadRelativeUri),
    Unresolvable(ResolutionError),
    RoutingProblem(RoutingError),
    ChannelDropped(ConnectionDropped),
}

impl DispatchError {
    fn is_fatal(&self) -> bool {
        match self {
            DispatchError::RoutingProblem(err) => err.is_fatal(),
            DispatchError::ChannelDropped(reason) => !reason.is_recoverable(),
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

impl From<RoutingError> for DispatchError {
    fn from(err: RoutingError) -> Self {
        DispatchError::RoutingProblem(err)
    }
}

async fn dispatch_envelope<Router>(
    router: &mut Router,
    resolved: &mut HashMap<RelativePath, Route<Router::Sender>>,
    mut envelope: Envelope,
    mut retry_strategy: RetryStrategy,
) -> Result<(), DispatchError>
where
    Router: ServerRouter,
{
    loop {
        let result = try_dispatch_envelope(router, resolved, envelope).await;
        match result {
            Err((env, err)) if !err.is_fatal() => {
                match retry_strategy.next() {
                    Some(Some(dur)) => {
                        delay_for(dur).await;
                    }
                    None => {
                        break Err(err);
                    }
                    _ => {}
                }
                envelope = env;
            }
            Err((_, err)) => {
                break Err(err);
            }
            _ => {
                break Ok(());
            }
        }
    }
}

async fn try_dispatch_envelope<Router>(
    router: &mut Router,
    resolved: &mut HashMap<RelativePath, Route<Router::Sender>>,
    envelope: Envelope,
) -> Result<(), (Envelope, DispatchError)>
where
    Router: ServerRouter,
{
    if let Some(target) = envelope.header.relative_path().as_ref() {
        let Route { sender, .. } = if let Some(route) = resolved.get_mut(target) {
            route
        } else {
            let route = get_route(router, target).await;
            match route {
                Ok(route) => match resolved.entry(target.clone()) {
                    Entry::Occupied(_) => unreachable!(),
                    Entry::Vacant(entry) => entry.insert(route),
                },
                Err(err) => {
                    return Err((envelope, err));
                }
            }
        };
        if let Err(err) = sender.send_item(envelope).await {
            if let Some(Route { on_drop, .. }) = resolved.remove(target) {
                let reason = on_drop
                    .await
                    .map(|reason| (*reason).clone())
                    .unwrap_or(ConnectionDropped::Unknown);
                let (_, env) = err.split();
                Err((env, DispatchError::ChannelDropped(reason)))
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

async fn get_route<Router>(
    router: &mut Router,
    target: &RelativePath,
) -> Result<Route<Router::Sender>, DispatchError>
where
    Router: ServerRouter,
{
    let target_addr = router
        .resolve(None, RelativeUri::from_str(&target.node.as_str())?)
        .await?;
    Ok(router.get_sender(target_addr).await?)
}
