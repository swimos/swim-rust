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

#[cfg(test)]
mod tests;

use crate::agent::lane::channels::update::map::MapLaneUpdateTask;
use crate::agent::lane::channels::update::value::ValueLaneUpdateTask;
use crate::agent::lane::channels::update::{LaneUpdate, UpdateError};
use crate::agent::lane::channels::uplink::spawn::UplinkErrorReport;
use crate::agent::lane::channels::uplink::{MapLaneUplink, UplinkAction, ValueLaneUplink};
use crate::agent::lane::channels::{
    AgentExecutionConfig, AgentExecutionContext, InputMessage, LaneMessageHandler, OutputMessage,
    TaggedAction,
};
use crate::agent::lane::model::map::{MapLane, MapLaneEvent};
use crate::agent::lane::model::value::ValueLane;
use crate::routing::{RoutingAddr, TaggedClientEnvelope};
use either::Either;
use futures::future::{join3, BoxFuture};
use futures::{select, Stream, StreamExt};
use pin_utils::pin_mut;
use std::any::Any;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use stm::transaction::RetryManager;
use swim_common::form::Form;
use swim_common::model::Value;
use swim_common::topic::Topic;
use swim_common::warp::envelope::{OutgoingHeader, OutgoingLinkMessage};
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc;
use tracing::{event, span, Level};
use tracing_futures::Instrument;
use utilities::future::SwimStreamExt;
use utilities::sync::trigger;

/// Aggregate error report combining lane update errors and lane uplink errors.
#[derive(Debug)]
pub struct LaneIoError {
    pub route: RelativePath,
    pub update_error: Option<UpdateError>,
    pub uplink_errors: Vec<UplinkErrorReport>,
}

impl LaneIoError {
    pub fn for_update_err(route: RelativePath, err: UpdateError) -> Self {
        LaneIoError {
            route,
            update_error: Some(err),
            uplink_errors: vec![],
        }
    }

    pub fn for_uplink_errors(route: RelativePath, errs: Vec<UplinkErrorReport>) -> Self {
        assert!(!errs.is_empty());
        LaneIoError {
            route,
            update_error: None,
            uplink_errors: errs,
        }
    }

    pub fn new(route: RelativePath, upd: UpdateError, upl: Vec<UplinkErrorReport>) -> Self {
        LaneIoError {
            route,
            update_error: Some(upd),
            uplink_errors: upl,
        }
    }
}

impl Display for LaneIoError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let LaneIoError {
            route,
            update_error,
            uplink_errors,
        } = self;
        writeln!(f, "IO tasks failed for lane: \"{}\".", route)?;
        if let Some(upd) = update_error {
            writeln!(f, "- update_error = {}", upd)?;
        }

        if !uplink_errors.is_empty() {
            writeln!(f, "- uplink_errors =")?;
            for err in uplink_errors.iter() {
                writeln!(f, "* {}", err)?;
            }
        }
        Ok(())
    }
}

impl Error for LaneIoError {}

/// Encapsulates the inputs and outputs of an uplink spawner.
pub struct UplinkChannels<Top> {
    /// Event topic for the lane.
    pub events: Top,
    /// Input for passing actions to the uplink.
    pub actions: mpsc::Receiver<TaggedAction>,
    /// Output for the uplink to report failures.
    pub error_collector: mpsc::Sender<UplinkErrorReport>,
}

impl<Top> UplinkChannels<Top> {
    pub(crate) fn new(
        events: Top,
        actions: mpsc::Receiver<TaggedAction>,
        error_collector: mpsc::Sender<UplinkErrorReport>,
    ) -> Self {
        UplinkChannels {
            events,
            actions,
            error_collector,
        }
    }
}

/// Trait to abstract out spawning of lane uplinks.
pub trait LaneUplinks {
    /// Create a task that will spawn uplinks on demand.
    ///
    /// #Arguments
    ///
    /// * `message_handler` - Creates the update task for the lane and new uplink state machines.
    /// * `channels` - The inputs and outputs for the uplink spawner.
    /// * `route` - The route to the lane for creating envelopes.
    /// * `context` - Agent execution context for scheduling tasks and routing envelopes.
    ///
    /// #Type Parameters
    ///
    /// * `Handler` - Type of the lane uplink strategy.
    /// * `Top` - Type of the lane event topic.
    /// * `Context` - Type of the agent execution context.
    fn make_task<Handler, Top, Context>(
        &self,
        message_handler: Arc<Handler>,
        channels: UplinkChannels<Top>,
        route: RelativePath,
        context: &Context,
    ) -> BoxFuture<'static, ()>
    where
        Handler: LaneMessageHandler + 'static,
        OutputMessage<Handler>: Into<Value>,
        Top: Topic<Handler::Event> + Send + 'static,
        Context: AgentExecutionContext;
}

const LANE_IO_TASK: &str = "Lane IO task.";
const UPDATE_TASK: &str = "Lane update task.";
const UPLINK_SPAWN_TASK: &str = "Uplink spawn task.";
const DISPATCH_ACTION: &str = "Dispatching uplink action.";
const DISPATCH_COMMAND: &str = "Dispatching lane command.";
const UPLINK_FAILED: &str = "An uplink failed with a non-fatal error.";
const UPLINK_FATAL: &str = "An uplink failed with a fatal error.";
const TOO_MANY_FAILURES: &str = "Terminating after too many failed uplinks.";

/// Run the [`Envelope`] IO for a lane, updating the state of the lane and creating uplinks to
/// remote subscribers.
///
/// #Arguments
/// * `message_handler` - Creates the update task for the lane and new uplink state machines.
/// * `lane_uplinks` - Strategy for spawning lane uplinks.
/// * `envelopes` - The stream of incoming envelopes.
/// * `events` - The lane event topic.
/// * `config` - Agent configuration parameters.
/// * `context` - The agent execution context, providing task scheduling and outgoing envelope
/// routing.
/// * `route` - The route to this lane for outgoing envelope labelling.
pub async fn run_lane_io<Handler, Uplinks>(
    message_handler: Handler,
    lane_uplinks: Uplinks,
    envelopes: impl Stream<Item = TaggedClientEnvelope>,
    events: impl Topic<Handler::Event> + Send + 'static,
    config: AgentExecutionConfig,
    context: impl AgentExecutionContext,
    route: RelativePath,
) -> Result<Vec<UplinkErrorReport>, LaneIoError>
where
    Handler: LaneMessageHandler + 'static,
    Uplinks: LaneUplinks,
    OutputMessage<Handler>: Into<Value>,
    InputMessage<Handler>: Debug + Form,
{
    let span = span!(Level::INFO, LANE_IO_TASK, ?route);
    let _enter = span.enter();
    let envelopes = envelopes.fuse();
    let arc_handler = Arc::new(message_handler);

    let AgentExecutionConfig {
        action_buffer,
        update_buffer,
        uplink_err_buffer,
        max_fatal_uplink_errors,
        ..
    } = config;

    let (mut act_tx, act_rx) = mpsc::channel(action_buffer.get());
    let (mut upd_tx, upd_rx) = mpsc::channel(update_buffer.get());
    let (err_tx, err_rx) = mpsc::channel(uplink_err_buffer.get());

    let spawner_channels = UplinkChannels::new(events, act_rx, err_tx);

    let (upd_done_tx, upd_done_rx) = trigger::trigger();
    let updater = arc_handler.make_update().run_update(upd_rx);
    let update_task = async move {
        let result = updater.await;
        upd_done_tx.trigger();
        result
    }
    .instrument(span!(Level::INFO, UPDATE_TASK, ?route));

    let uplink_spawn_task = lane_uplinks
        .make_task(arc_handler, spawner_channels, route.clone(), &context)
        .instrument(span!(Level::INFO, UPLINK_SPAWN_TASK, ?route));

    let mut err_rx = err_rx.take_until_completes(upd_done_rx).fuse();

    let route_cpy = route.clone();

    let envelope_task = async move {
        pin_mut!(envelopes);

        let mut uplink_errors = vec![];
        let mut num_fatal: usize = 0;

        let failed: bool = loop {
            let envelope_or_err: Option<Either<TaggedClientEnvelope, UplinkErrorReport>> = select! {
                maybe_env = envelopes.next() => maybe_env.map(Either::Left),
                maybe_err = err_rx.next() => maybe_err.map(Either::Right),
            };

            match envelope_or_err {
                Some(Either::Left(envelope)) => {
                    let TaggedClientEnvelope(addr, OutgoingLinkMessage { header, body, .. }) =
                        envelope;
                    let action = match header {
                        OutgoingHeader::Link(_) => Either::Left(UplinkAction::Link),
                        OutgoingHeader::Sync(_) => Either::Left(UplinkAction::Sync),
                        OutgoingHeader::Unlink => Either::Left(UplinkAction::Unlink),
                        OutgoingHeader::Command => Either::Right(body.unwrap_or(Value::Extant)),
                    };
                    match action {
                        Either::Left(uplink_action) => {
                            event!(Level::TRACE, DISPATCH_ACTION, route = ?route_cpy, ?addr, action = ?uplink_action);
                            if act_tx
                                .send(TaggedAction(addr, uplink_action))
                                .await
                                .is_err()
                            {
                                break false;
                            }
                        }
                        Either::Right(command) => {
                            event!(Level::TRACE, DISPATCH_COMMAND, route = ?route_cpy, ?addr, ?command);
                            if upd_tx.send(Form::try_convert(command)).await.is_err() {
                                break false;
                            }
                        }
                    }
                }
                Some(Either::Right(error)) => {
                    if error.error.is_fatal() {
                        num_fatal += 1;
                        event!(Level::ERROR, UPLINK_FATAL, route = ?route_cpy, ?error);
                    } else {
                        event!(Level::WARN, UPLINK_FAILED, route = ?route_cpy, ?error);
                    }
                    uplink_errors.push(error);
                    if num_fatal > max_fatal_uplink_errors {
                        event!(Level::ERROR, TOO_MANY_FAILURES, route = ?route_cpy, num_fatal, max = max_fatal_uplink_errors);
                        break true;
                    }
                }
                _ => {
                    break false;
                }
            }
        };
        (failed, uplink_errors)
    };

    match join3(uplink_spawn_task, update_task, envelope_task).await {
        (_, Err(upd_err), (_, upl_errs)) => Err(LaneIoError::new(route, upd_err, upl_errs)),
        (_, _, (true, upl_errs)) if !upl_errs.is_empty() => {
            Err(LaneIoError::for_uplink_errors(route, upl_errs))
        }
        (_, _, (_, upl_errs)) => Ok(upl_errs),
    }
}

impl<T> LaneMessageHandler for ValueLane<T>
where
    T: Any + Send + Sync + Debug,
{
    type Event = Arc<T>;
    type Uplink = ValueLaneUplink<T>;
    type Update = ValueLaneUpdateTask<T>;

    fn make_uplink(&self, _addr: RoutingAddr) -> Self::Uplink {
        ValueLaneUplink::new((*self).clone())
    }

    fn make_update(&self) -> Self::Update {
        ValueLaneUpdateTask::new((*self).clone())
    }
}

/// Each uplink from a [`MapLane`] requires a unique ID. This handler implementation maintains
/// a monotonic counter to ensure that.
pub struct MapLaneMessageHandler<K, V, F> {
    lane: MapLane<K, V>,
    uplink_counter: AtomicU64,
    retries: F,
}

impl<K, V, F, Ret> MapLaneMessageHandler<K, V, F>
where
    F: Fn() -> Ret + Clone + Send + Sync + 'static,
    Ret: RetryManager + Send + Sync + 'static,
{
    pub fn new(lane: MapLane<K, V>, retries: F) -> Self {
        MapLaneMessageHandler {
            lane,
            uplink_counter: AtomicU64::new(1),
            retries,
        }
    }
}

impl<K, V, F, Ret> LaneMessageHandler for MapLaneMessageHandler<K, V, F>
where
    K: Any + Form + Send + Sync + Debug,
    V: Any + Send + Sync + Debug,
    F: Fn() -> Ret + Clone + Send + Sync + 'static,
    Ret: RetryManager + Send + Sync + 'static,
{
    type Event = MapLaneEvent<K, V>;
    type Uplink = MapLaneUplink<K, V, F>;
    type Update = MapLaneUpdateTask<K, V, F>;

    fn make_uplink(&self, _addr: RoutingAddr) -> Self::Uplink {
        let MapLaneMessageHandler {
            lane,
            uplink_counter,
            retries,
        } = self;
        let i = uplink_counter.fetch_add(1, Ordering::Relaxed);
        MapLaneUplink::new((*lane).clone(), i, retries.clone())
    }

    fn make_update(&self) -> Self::Update {
        let MapLaneMessageHandler { lane, retries, .. } = self;
        MapLaneUpdateTask::new((*lane).clone(), (*retries).clone())
    }
}
