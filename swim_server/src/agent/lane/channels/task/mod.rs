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

use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::channels::update::action::ActionLaneUpdateTask;
use crate::agent::lane::channels::update::command::CommandLaneUpdateTask;
use crate::agent::lane::channels::update::map::MapLaneUpdateTask;
use crate::agent::lane::channels::update::value::ValueLaneUpdateTask;
use crate::agent::lane::channels::update::{LaneUpdate, UpdateError};
use crate::agent::lane::channels::uplink::backpressure::{
    KeyedBackpressureConfig, SimpleBackpressureConfig,
};
use crate::agent::lane::channels::uplink::spawn::UplinkErrorReport;
use crate::agent::lane::channels::uplink::stateless::StatelessUplinks;
use crate::agent::lane::channels::uplink::{
    AddressedUplinkMessage, DemandMapLaneUplink, MapLaneUplink, UplinkAction, UplinkKind,
    ValueLaneUplink,
};
use crate::agent::lane::channels::{
    AgentExecutionConfig, InputMessage, LaneMessageHandler, OutputMessage, TaggedAction,
};
use crate::agent::lane::model::action::ActionLane;
use crate::agent::lane::model::command::CommandLane;
use crate::agent::lane::model::demand_map::{DemandMapLane, DemandMapLaneEvent};
use crate::agent::lane::model::map::{MapLane, MapLaneEvent};
use crate::agent::lane::model::value::ValueLane;
use crate::agent::lane::model::DeferredSubscription;
use crate::agent::Eff;
use crate::meta::metric::uplink::UplinkActionObserver;
use either::Either;
use futures::future::{join, join3, ready, BoxFuture};
use futures::{select, Stream, StreamExt};
use pin_utils::pin_mut;
use std::any::Any;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use stm::transaction::RetryManager;
use swim_common::form::structural::read::ReadError;
use swim_common::form::Form;
use swim_common::model::Value;
use swim_common::routing::{RoutingAddr, TaggedClientEnvelope};
use swim_common::warp::envelope::{OutgoingHeader, OutgoingLinkMessage};
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, span, Level};
use tracing_futures::Instrument;
use utilities::errors::Recoverable;
use utilities::sync::trigger;

#[cfg(test)]
mod tests;

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
    /// * `action_observer` - An observer for uplinks being opened and closed.
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
        action_observer: UplinkActionObserver,
    ) -> Eff
    where
        Handler: LaneMessageHandler + 'static,
        OutputMessage<Handler>: Into<Value>,
        Top: DeferredSubscription<Handler::Event>,
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

/// Run the [`swim_common::warp::envelope::Envelope`] IO for a lane, updating the state of the lane
/// and creating uplinks to remote subscribers.
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
pub async fn run_lane_io<Handler, Uplinks, Deferred>(
    message_handler: Handler,
    lane_uplinks: Uplinks,
    envelopes: impl Stream<Item = TaggedClientEnvelope>,
    events: Deferred,
    config: AgentExecutionConfig,
    context: impl AgentExecutionContext,
    route: RelativePath,
) -> Result<Vec<UplinkErrorReport>, LaneIoError>
where
    Handler: LaneMessageHandler + 'static,
    Uplinks: LaneUplinks,
    Deferred: DeferredSubscription<Handler::Event> + 'static,
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

    let (act_tx, act_rx) = mpsc::channel(action_buffer.get());
    let (upd_tx, upd_rx) = mpsc::channel(update_buffer.get());
    let (err_tx, err_rx) = mpsc::channel(uplink_err_buffer.get());
    let (event_observer, action_observer) =
        context.metrics().uplink_observer_for_path(route.clone());

    let spawner_channels = UplinkChannels::new(events, act_rx, err_tx);
    let message_stream = ReceiverStream::new(upd_rx).inspect(move |_| event_observer.on_event());

    let (upd_done_tx, upd_done_rx) = trigger::trigger();
    let updater = arc_handler.make_update().run_update(message_stream);
    let update_task = async move {
        let result = updater.await;
        upd_done_tx.trigger();
        result
    }
    .instrument(span!(Level::INFO, UPDATE_TASK, ?route));

    let uplink_spawn_task = lane_uplinks
        .make_task(
            arc_handler,
            spawner_channels,
            route.clone(),
            &context,
            action_observer.clone(),
        )
        .instrument(span!(Level::INFO, UPLINK_SPAWN_TASK, ?route));

    let mut err_rx = ReceiverStream::new(err_rx).take_until(upd_done_rx).fuse();

    let route_cpy = route.clone();

    let envelope_task = async move {
        pin_mut!(envelopes);

        let mut err_acc = UplinkErrorAcc::new(route_cpy.clone(), max_fatal_uplink_errors);

        let yield_mod = config.yield_after.get();
        let mut iteration_count: usize = 0;

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
                            action_observer.on_command();
                            if upd_tx
                                .send(Form::try_convert(command).map(|cmd| (addr, cmd)))
                                .await
                                .is_err()
                            {
                                break false;
                            }
                        }
                    }
                }
                Some(Either::Right(error)) => {
                    if err_acc.add(error).is_err() {
                        break true;
                    }
                }
                _ => {
                    break false;
                }
            }

            iteration_count += 1;
            if iteration_count % yield_mod == 0 {
                tokio::task::yield_now().await;
            }
        };
        (failed, err_acc.take_errors())
    };

    let (_, upd_res, (upl_fatal, upl_errs)) =
        join3(uplink_spawn_task, update_task, envelope_task).await;
    combine_results(route, upd_res.err(), upl_fatal, upl_errs)
}

pub struct ValueLaneMessageHandler<T> {
    lane: ValueLane<T>,
    backpressure_config: Option<SimpleBackpressureConfig>,
}

impl<T> ValueLaneMessageHandler<T> {
    pub fn new(lane: ValueLane<T>, backpressure_config: Option<SimpleBackpressureConfig>) -> Self {
        ValueLaneMessageHandler {
            lane,
            backpressure_config,
        }
    }
}

impl<T> LaneMessageHandler for ValueLaneMessageHandler<T>
where
    T: Any + Send + Sync + Debug,
{
    type Event = Arc<T>;
    type Uplink = ValueLaneUplink<T>;
    type Update = ValueLaneUpdateTask<T>;

    fn make_uplink(&self, _addr: RoutingAddr) -> Self::Uplink {
        ValueLaneUplink::new(self.lane.clone(), self.backpressure_config)
    }

    fn make_update(&self) -> Self::Update {
        ValueLaneUpdateTask::new(self.lane.clone())
    }
}

/// Each uplink from a [`MapLane`] requires a unique ID. This handler implementation maintains
/// a monotonic counter to ensure that.
pub struct MapLaneMessageHandler<K, V, F> {
    lane: MapLane<K, V>,
    uplink_counter: AtomicU64,
    retries: F,
    backpressure_config: Option<KeyedBackpressureConfig>,
}

impl<K, V, F, Ret> MapLaneMessageHandler<K, V, F>
where
    F: Fn() -> Ret + Clone + Send + Sync + 'static,
    Ret: RetryManager + Send + Sync + 'static,
{
    pub fn new(
        lane: MapLane<K, V>,
        retries: F,
        backpressure_config: Option<KeyedBackpressureConfig>,
    ) -> Self {
        MapLaneMessageHandler {
            lane,
            uplink_counter: AtomicU64::new(1),
            retries,
            backpressure_config,
        }
    }
}

impl<K, V, F, Ret> LaneMessageHandler for MapLaneMessageHandler<K, V, F>
where
    K: Any + Form + Send + Sync + Debug,
    V: Any + Form + Send + Sync + Debug,
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
            backpressure_config,
        } = self;
        let i = uplink_counter.fetch_add(1, Ordering::Relaxed);
        MapLaneUplink::new((*lane).clone(), i, retries.clone(), *backpressure_config)
    }

    fn make_update(&self) -> Self::Update {
        let MapLaneMessageHandler { lane, retries, .. } = self;
        MapLaneUpdateTask::new((*lane).clone(), (*retries).clone())
    }
}

async fn send_action(
    sender: &mut mpsc::Sender<TaggedAction>,
    route: &RelativePath,
    addr: RoutingAddr,
    action: UplinkAction,
) -> bool {
    event!(Level::TRACE, DISPATCH_ACTION, ?route, ?addr, ?action);
    sender.send(TaggedAction(addr, action)).await.is_ok()
}

async fn run_stateless_lane_io<Upd, S, Msg, Ev>(
    envelopes: impl Stream<Item = TaggedClientEnvelope>,
    config: AgentExecutionConfig,
    context: impl AgentExecutionContext,
    route: RelativePath,
    updater: Upd,
    uplinks: StatelessUplinks<S>,
    action_observer: UplinkActionObserver,
) -> Result<Vec<UplinkErrorReport>, LaneIoError>
where
    Upd: LaneUpdate<Msg = Msg>,
    S: Stream<Item = AddressedUplinkMessage<Ev>>,
    Msg: Send + Sync + Form + Debug + 'static,
    Ev: Send + Sync + Form + Debug + 'static,
{
    let (update_tx, update_rx) = mpsc::channel(config.update_buffer.get());
    let (upd_done_tx, upd_done_rx) = trigger::trigger();
    let envelopes = envelopes.take_until(upd_done_rx);

    let yield_after = config.yield_after.get();

    let (uplink_tx, uplink_rx) = mpsc::channel(config.action_buffer.get());
    let (err_tx, err_rx) = mpsc::channel(config.uplink_err_buffer.get());

    let update_task = async move {
        let result = updater.run_update(ReceiverStream::new(update_rx)).await;
        upd_done_tx.trigger();
        result
    }
    .instrument(span!(Level::INFO, UPDATE_TASK, ?route));

    let uplink_task = uplinks
        .run(
            ReceiverStream::new(uplink_rx),
            context.router_handle(),
            err_tx,
            yield_after,
        )
        .instrument(span!(Level::INFO, UPLINK_SPAWN_TASK, ?route));

    let error_handler =
        UplinkErrorHandler::collector(route.clone(), config.max_fatal_uplink_errors);
    let error_reporter = UplinkErrorReporter {
        err_rx,
        err_handler: error_handler,
    };

    let envelope_task = action_envelope_task_with_uplinks(
        route.clone(),
        envelopes,
        uplink_tx,
        error_reporter,
        OnCommandStrategy::Send(OnCommandHandler::new(update_tx, route.clone())),
        yield_after,
        action_observer,
    );

    let (upd_result, _, (uplink_fatal, uplink_errs)) =
        join3(update_task, uplink_task, envelope_task).await;
    combine_results(route, upd_result.err(), uplink_fatal, uplink_errs)
}

/// Run the [`swim_common::warp::envelope::Envelope`] IO for a command lane.
///
/// #Arguments
/// * `lane` - The command lane.
/// * `envelopes` - The stream of incoming envelopes.
/// * `config` - Agent configuration parameters.
/// * `context` - The agent execution context, providing task scheduling and outgoing envelope
/// routing.
/// * `route` - The route to this lane for outgoing envelope labelling.
pub async fn run_command_lane_io<T>(
    lane: CommandLane<T>,
    envelopes: impl Stream<Item = TaggedClientEnvelope>,
    config: AgentExecutionConfig,
    context: impl AgentExecutionContext,
    route: RelativePath,
) -> Result<Vec<UplinkErrorReport>, LaneIoError>
where
    T: Send + Sync + Form + Debug + 'static,
{
    let span = span!(Level::INFO, LANE_IO_TASK, ?route);
    let _enter = span.enter();

    let (event_observer, action_observer) =
        context.metrics().uplink_observer_for_path(route.clone());

    let (feedback_tx, feedback_rx) = mpsc::channel(config.feedback_buffer.get());
    let feedback_rx = ReceiverStream::new(feedback_rx)
        .map(|(_, message)| AddressedUplinkMessage::Broadcast(message))
        .inspect(|_| event_observer.on_event());

    let updater =
        CommandLaneUpdateTask::new(lane.clone(), Some(feedback_tx), config.cleanup_timeout);
    let uplinks = StatelessUplinks::new(
        feedback_rx,
        route.clone(),
        UplinkKind::Command,
        action_observer.clone(),
    );

    run_stateless_lane_io(
        envelopes,
        config,
        context,
        route,
        updater,
        uplinks,
        action_observer,
    )
    .await
}

/// Run the [`swim_common::warp::envelope::Envelope`] IO for an action lane. This is different to
/// the standard `run_lane_io` as the update and uplink components of an action lane are interleaved
/// and different uplinks will receive entirely different messages.
///
/// #Arguments
/// * `lane` - The action lane.
/// * `envelopes` - The stream of incoming envelopes.
/// * `config` - Agent configuration parameters.
/// * `context` - The agent execution context, providing task scheduling and outgoing envelope
/// routing.
/// * `route` - The route to this lane for outgoing envelope labelling.
pub async fn run_action_lane_io<Command, Response>(
    lane: ActionLane<Command, Response>,
    envelopes: impl Stream<Item = TaggedClientEnvelope>,
    config: AgentExecutionConfig,
    context: impl AgentExecutionContext,
    route: RelativePath,
) -> Result<Vec<UplinkErrorReport>, LaneIoError>
where
    Command: Send + Sync + Form + Debug + 'static,
    Response: Send + Sync + Form + Debug + 'static,
{
    let span = span!(Level::INFO, LANE_IO_TASK, ?route);
    let _enter = span.enter();

    let (event_observer, action_observer) =
        context.metrics().uplink_observer_for_path(route.clone());

    let (feedback_tx, feedback_rx) = mpsc::channel(config.feedback_buffer.get());
    let feedback_rx = ReceiverStream::new(feedback_rx)
        .map(|(address, message)| AddressedUplinkMessage::Addressed { message, address })
        .inspect(|_| event_observer.on_event());

    let updater =
        ActionLaneUpdateTask::new(lane.clone(), Some(feedback_tx), config.cleanup_timeout);
    let uplinks = StatelessUplinks::new(
        feedback_rx,
        route.clone(),
        UplinkKind::Action,
        action_observer.clone(),
    );

    run_stateless_lane_io(
        envelopes,
        config,
        context,
        route,
        updater,
        uplinks,
        action_observer,
    )
    .await
}

fn combine_results(
    route: RelativePath,
    update_error: Option<UpdateError>,
    uplink_fatal: bool,
    uplink_errs: Vec<UplinkErrorReport>,
) -> Result<Vec<UplinkErrorReport>, LaneIoError> {
    if let Some(upd_err) = update_error {
        Err(LaneIoError::new(route, upd_err, uplink_errs))
    } else if uplink_fatal && !uplink_errs.is_empty() {
        Err(LaneIoError::for_uplink_errors(route, uplink_errs))
    } else {
        Ok(uplink_errs)
    }
}

/// An error handling strategy for an uplink.
enum UplinkErrorHandler {
    /// Discards any errors that occur.
    Discard,
    /// Accumulates any errors that occur.
    Collect(UplinkErrorAcc),
}

impl UplinkErrorHandler {
    /// Creates a new `UplinkErrorHandler` that discards any errors that occur.
    pub fn discard() -> UplinkErrorHandler {
        UplinkErrorHandler::Discard
    }

    /// Creates a new `UplinkErrorHandler` that collects any errors that occur.
    pub fn collector(route: RelativePath, max_fatal: usize) -> UplinkErrorHandler {
        UplinkErrorHandler::Collect(UplinkErrorAcc::new(route, max_fatal))
    }

    /// Reports a new error to the underlying handler. For a discarder, the error is dropped and
    /// `Ok(())` is returned. For a collector, the error is accumulated and if maximum number of
    /// fatal errors is exceeded then an error is returned.
    pub fn add(&mut self, error: UplinkErrorReport) -> Result<(), ()> {
        match self {
            UplinkErrorHandler::Discard => Ok(()),
            UplinkErrorHandler::Collect(acc) => acc.add(error),
        }
    }

    /// Takes any accumulated errors and returns them. For a discarder, this returns an empty
    /// vector. For a collector it returns any errors that were accumulated.
    pub fn take_errors(self) -> Vec<UplinkErrorReport> {
        match self {
            UplinkErrorHandler::Discard => Vec::new(),
            UplinkErrorHandler::Collect(acc) => acc.take_errors(),
        }
    }
}

struct UplinkErrorAcc {
    route: RelativePath,
    errors: Vec<UplinkErrorReport>,
    max_fatal: usize,
    num_fatal: usize,
}

impl UplinkErrorAcc {
    fn new(route: RelativePath, max_fatal: usize) -> Self {
        UplinkErrorAcc {
            route,
            errors: vec![],
            max_fatal,
            num_fatal: 0,
        }
    }

    fn add(&mut self, error: UplinkErrorReport) -> Result<(), ()> {
        let UplinkErrorAcc {
            route,
            errors,
            max_fatal,
            num_fatal,
        } = self;
        if error.error.is_fatal() {
            *num_fatal += 1;
            event!(Level::ERROR, UPLINK_FATAL, ?route, ?error);
        } else {
            event!(Level::WARN, UPLINK_FAILED, ?route, ?error);
        }
        errors.push(error);
        if *num_fatal > *max_fatal {
            event!(
                Level::ERROR,
                TOO_MANY_FAILURES,
                ?route,
                ?num_fatal,
                ?max_fatal
            );
            Err(())
        } else {
            Ok(())
        }
    }

    fn take_errors(self) -> Vec<UplinkErrorReport> {
        self.errors
    }
}

pub async fn run_auto_lane_io<S, Item>(
    envelopes: impl Stream<Item = TaggedClientEnvelope>,
    config: AgentExecutionConfig,
    context: impl AgentExecutionContext,
    route: RelativePath,
    stream: S,
    uplink_kind: UplinkKind,
) -> Result<Vec<UplinkErrorReport>, LaneIoError>
where
    S: Stream<Item = Item> + Send + Sync + 'static,
    Item: Send + Sync + Form + Debug + 'static,
{
    let span = span!(Level::INFO, LANE_IO_TASK, ?route);
    let _enter = span.enter();

    let (uplink_tx, uplink_rx) = mpsc::channel(config.action_buffer.get());
    let (err_tx, err_rx) = mpsc::channel(config.uplink_err_buffer.get());
    let (event_observer, action_observer) =
        context.metrics().uplink_observer_for_path(route.clone());

    let stream = stream
        .map(AddressedUplinkMessage::broadcast)
        .inspect(|_| event_observer.on_event());
    let uplinks =
        StatelessUplinks::new(stream, route.clone(), uplink_kind, action_observer.clone());

    let on_command_strategy = OnCommandStrategy::<Dropping>::dropping();
    let yield_after = config.yield_after.get();

    let error_reporter = UplinkErrorReporter {
        err_rx,
        err_handler: UplinkErrorHandler::discard(),
    };

    let envelope_task = action_envelope_task_with_uplinks(
        route.clone(),
        envelopes,
        uplink_tx,
        error_reporter,
        on_command_strategy,
        yield_after,
        action_observer,
    );

    let uplink_task = uplinks
        .run(
            ReceiverStream::new(uplink_rx),
            context.router_handle(),
            err_tx,
            yield_after,
        )
        .instrument(span!(Level::INFO, UPLINK_SPAWN_TASK, ?route));

    let (_, (uplink_fatal, uplink_errs)) = join(uplink_task, envelope_task).await;

    if uplink_fatal && !uplink_errs.is_empty() {
        Err(LaneIoError::for_uplink_errors(route, uplink_errs))
    } else {
        Ok(uplink_errs)
    }
}

pub async fn run_supply_lane_io<S, Item>(
    envelopes: impl Stream<Item = TaggedClientEnvelope>,
    config: AgentExecutionConfig,
    context: impl AgentExecutionContext,
    route: RelativePath,
    stream: S,
) -> Result<Vec<UplinkErrorReport>, LaneIoError>
where
    S: Stream<Item = Item> + Send + Sync + 'static,
    Item: Send + Sync + Form + Debug + 'static,
{
    run_auto_lane_io(
        envelopes,
        config,
        context,
        route,
        stream,
        UplinkKind::Supply,
    )
    .await
}

struct UplinkErrorReporter {
    err_rx: mpsc::Receiver<UplinkErrorReport>,
    err_handler: UplinkErrorHandler,
}

async fn action_envelope_task_with_uplinks<Cmd>(
    route: RelativePath,
    envelopes: impl Stream<Item = TaggedClientEnvelope>,
    mut actions: mpsc::Sender<TaggedAction>,
    err_reporter: UplinkErrorReporter,
    mut on_command_handler: OnCommandStrategy<Cmd>,
    yield_mod: usize,
    action_observer: UplinkActionObserver,
) -> (bool, Vec<UplinkErrorReport>)
where
    Cmd: Send + Sync + Form + Debug + 'static,
{
    let UplinkErrorReporter {
        err_rx,
        mut err_handler,
    } = err_reporter;

    let envelopes = envelopes.fuse();
    pin_mut!(envelopes);

    let mut err_rx = ReceiverStream::new(err_rx).fuse();

    let mut iteration_count: usize = 0;

    let mut failed: bool = loop {
        let envelope_or_err: Option<Either<TaggedClientEnvelope, UplinkErrorReport>> = select! {
            maybe_env = envelopes.next() => maybe_env.map(Either::Left),
            maybe_err = err_rx.next() => maybe_err.map(Either::Right),
        };

        match envelope_or_err {
            Some(Either::Left(TaggedClientEnvelope(addr, envelope))) => {
                let OutgoingLinkMessage { header, body, .. } = envelope;
                let sent = match header {
                    OutgoingHeader::Link(_) => {
                        send_action(&mut actions, &route, addr, UplinkAction::Link).await
                    }
                    OutgoingHeader::Sync(_) => {
                        send_action(&mut actions, &route, addr, UplinkAction::Sync).await
                    }
                    OutgoingHeader::Unlink => {
                        send_action(&mut actions, &route, addr, UplinkAction::Unlink).await
                    }
                    OutgoingHeader::Command => match body {
                        Some(value) => {
                            action_observer.on_command();

                            let maybe_command = Cmd::try_convert(value).map(|cmd| (addr, cmd));
                            on_command_handler.on_command(maybe_command, addr).await
                        }
                        None => {
                            let maybe_command =
                                Cmd::try_convert(Value::Extant).map(|cmd| (addr, cmd));
                            on_command_handler.on_command(maybe_command, addr).await
                        }
                    },
                };
                if !sent {
                    break false;
                }
            }
            Some(Either::Right(error)) => {
                if err_handler.add(error).is_err() {
                    break true;
                }
            }
            _ => {
                break false;
            }
        }

        iteration_count += 1;
        if iteration_count % yield_mod == 0 {
            tokio::task::yield_now().await;
        }
    };

    drop(actions);

    while let Some(error) = err_rx.next().await {
        if err_handler.add(error).is_err() {
            failed = true;
        }
    }

    (failed, err_handler.take_errors())
}

#[derive(Debug, Form)]
struct Dropping;

/// A strategy for handling `OutgoingHeader::Command` messages.
enum OnCommandStrategy<V = Dropping> {
    /// Drop the message. Essentially a no-op.
    Drop,
    /// Forward the message to a given `OnCommandHandler` and return the result.
    Send(OnCommandHandler<V>),
}

impl<F> OnCommandStrategy<F>
where
    F: Send + Sync + Form + Debug + 'static,
{
    /// Creates a new `OnCommandStrategy` that will drop any messages and always succeed.
    fn dropping() -> OnCommandStrategy<Dropping> {
        OnCommandStrategy::Drop
    }

    /// Handle the message and return whether or not the operation was successful.
    async fn on_command(
        &mut self,
        maybe_command: Result<(RoutingAddr, F), ReadError>,
        address: RoutingAddr,
    ) -> bool {
        match self {
            OnCommandStrategy::Drop => true,
            OnCommandStrategy::Send(handler) => handler.send(maybe_command, address).await,
        }
    }
}

/// A handler for `OutgoingHeader::Command` messages. Forwarding any messages received to a sender.
struct OnCommandHandler<F> {
    /// The sender to forward messages to.
    sender: mpsc::Sender<Result<(RoutingAddr, F), ReadError>>,
    /// The corresponding route that this handler is operating on.
    route: RelativePath,
}

impl<F> OnCommandHandler<F>
where
    F: Send + Sync + Form + Debug + 'static,
{
    fn new(
        sender: mpsc::Sender<Result<(RoutingAddr, F), ReadError>>,
        route: RelativePath,
    ) -> OnCommandHandler<F> {
        OnCommandHandler { sender, route }
    }

    /// Forwards the value to the underlying sender and returns whether the forwarding operation was
    /// successful.
    async fn send(
        &mut self,
        maybe_command: Result<(RoutingAddr, F), ReadError>,
        address: RoutingAddr,
    ) -> bool {
        let OnCommandHandler { sender, route } = self;
        event!(
            Level::TRACE,
            DISPATCH_COMMAND,
            ?route,
            ?address,
            ?maybe_command
        );

        sender.send(maybe_command).await.is_ok()
    }
}

pub async fn run_demand_lane_io<Event>(
    envelopes: impl Stream<Item = TaggedClientEnvelope>,
    config: AgentExecutionConfig,
    context: impl AgentExecutionContext,
    route: RelativePath,
    response_rx: mpsc::Receiver<Event>,
) -> Result<Vec<UplinkErrorReport>, LaneIoError>
where
    Event: Send + Sync + Form + Debug + 'static,
{
    run_auto_lane_io(
        envelopes,
        config,
        context,
        route,
        ReceiverStream::new(response_rx),
        UplinkKind::Demand,
    )
    .await
}

/// Asynchronous stub for compliance with `LaneUpdate`. `DemandMapLane`s do not support updates.
pub struct DemandMapLaneUpdateTask<T>(PhantomData<T>);

impl<T> Default for DemandMapLaneUpdateTask<T> {
    fn default() -> Self {
        DemandMapLaneUpdateTask(PhantomData::default())
    }
}

impl<T> LaneUpdate for DemandMapLaneUpdateTask<T>
where
    T: Debug + Send + 'static,
{
    type Msg = T;

    fn run_update<Messages, Err>(
        self,
        _messages: Messages,
    ) -> BoxFuture<'static, Result<(), UpdateError>>
    where
        Messages: Stream<Item = Result<(RoutingAddr, Self::Msg), Err>> + Send + 'static,
        Err: Send,
        UpdateError: From<Err>,
    {
        Box::pin(ready(Err(UpdateError::OperationNotSupported)))
    }
}

/// A `DemandMapLane` uplink producer and update handler.
pub struct DemandMapLaneMessageHandler<Key, Value>
where
    Key: Form,
    Value: Form,
{
    lane: DemandMapLane<Key, Value>,
}

impl<Key, Value> DemandMapLaneMessageHandler<Key, Value>
where
    Key: Form,
    Value: Form,
{
    pub fn new(lane: DemandMapLane<Key, Value>) -> DemandMapLaneMessageHandler<Key, Value> {
        DemandMapLaneMessageHandler { lane }
    }
}

impl<Key, Value> LaneMessageHandler for DemandMapLaneMessageHandler<Key, Value>
where
    Key: Any + Clone + Form + Send + Sync + Debug,
    Value: Any + Clone + Form + Send + Sync + Debug,
{
    type Event = DemandMapLaneEvent<Key, Value>;
    type Uplink = DemandMapLaneUplink<Key, Value>;
    type Update = DemandMapLaneUpdateTask<Value>;

    fn make_uplink(&self, _addr: RoutingAddr) -> Self::Uplink {
        let DemandMapLaneMessageHandler { lane } = self;
        DemandMapLaneUplink::new(lane.clone())
    }

    fn make_update(&self) -> Self::Update {
        DemandMapLaneUpdateTask::default()
    }
}
