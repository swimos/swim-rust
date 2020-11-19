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

use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::channels::update::action::ActionLaneUpdateTask;
use crate::agent::lane::channels::update::map::MapLaneUpdateTask;
use crate::agent::lane::channels::update::value::ValueLaneUpdateTask;
use crate::agent::lane::channels::update::{LaneUpdate, UpdateError};
use crate::agent::lane::channels::uplink::spawn::UplinkErrorReport;
use crate::agent::lane::channels::uplink::stateless::StatelessUplinks;
use crate::agent::lane::channels::uplink::{
    AddressedUplinkMessage, MapLaneUplink, UplinkAction, UplinkKind, ValueLaneUplink,
};
use crate::agent::lane::channels::{
    AgentExecutionConfig, InputMessage, LaneMessageHandler, OutputMessage, TaggedAction,
};
use crate::agent::lane::model::action::ActionLane;
use crate::agent::lane::model::map::{MapLane, MapLaneEvent};
use crate::agent::lane::model::value::ValueLane;
use crate::agent::Eff;
use crate::routing::{RoutingAddr, TaggedClientEnvelope};
use either::Either;
use futures::future::{join, join3};
use futures::{select, Stream, StreamExt};
use pin_utils::pin_mut;
use pin_utils::pin_mut;
use std::any::Any;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use stm::transaction::RetryManager;
use swim_common::form::{Form, FormErr};
use swim_common::model::Value;
use swim_common::topic::Topic;
use swim_common::warp::envelope::{OutgoingHeader, OutgoingLinkMessage};
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc;
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
    ) -> Eff
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

    let mut err_rx = err_rx.take_until(upd_done_rx).fuse();

    let route_cpy = route.clone();

    let envelope_task = async move {
        pin_mut!(envelopes);

        let mut err_acc = UplinkErrorAcc::new(route_cpy.clone(), max_fatal_uplink_errors);

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
        };
        (failed, err_acc.take_errors())
    };

    let (_, upd_res, (upl_fatal, upl_errs)) =
        join3(uplink_spawn_task, update_task, envelope_task).await;
    combine_results(route, upd_res.err(), upl_fatal, upl_errs)
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
        } = self;
        let i = uplink_counter.fetch_add(1, Ordering::Relaxed);
        MapLaneUplink::new((*lane).clone(), i, retries.clone())
    }

    fn make_update(&self) -> Self::Update {
        let MapLaneMessageHandler { lane, retries, .. } = self;
        MapLaneUpdateTask::new((*lane).clone(), (*retries).clone())
    }
}

async fn simple_action_envelope_task<Command>(
    envelopes: impl Stream<Item = TaggedClientEnvelope>,
    mut commands: mpsc::Sender<Result<(RoutingAddr, Command), FormErr>>,
) where
    Command: Send + Sync + Form + 'static,
{
    pin_mut!(envelopes);

    while let Some(TaggedClientEnvelope(addr, envelope)) = envelopes.next().await {
        let OutgoingLinkMessage { header, body, .. } = envelope;
        let sent = match header {
            OutgoingHeader::Command => {
                let command =
                    Command::try_convert(body.unwrap_or(Value::Extant)).map(|cmd| (addr, cmd));
                commands.send(command).await.is_ok()
            }
            _ => true,
        };
        if !sent {
            break;
        }
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

/// Run the [`Envelope`] IO for an action lane. This is different to the standard `run_lane_io` as
/// the update and uplink components of an action lane are interleaved and different uplinks will
/// receive entirely different messages.
///
/// #Arguments
/// * `lane` - The action lane.
/// * `feedback` - Whether the lane provides feedback (and so can have uplinks).
/// * `envelopes` - The stream of incoming envelopes.
/// * `config` - Agent configuration parameters.
/// * `context` - The agent execution context, providing task scheduling and outgoing envelope
/// routing.
/// * `route` - The route to this lane for outgoing envelope labelling.
pub async fn run_action_lane_io<Command, Response>(
    lane: ActionLane<Command, Response>,
    feedback: bool,
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

    let (update_tx, update_rx) = mpsc::channel(config.update_buffer.get());
    let (upd_done_tx, upd_done_rx) = trigger::trigger();
    let envelopes = envelopes.take_until(upd_done_rx);

    if feedback {
        let (feedback_tx, feedback_rx) = mpsc::channel(config.feedback_buffer.get());
        let feedback_rx =
            feedback_rx.map(|(addr, item)| AddressedUplinkMessage::addressed(item, addr));
        let (uplink_tx, uplink_rx) = mpsc::channel(config.action_buffer.get());
        let (err_tx, err_rx) = mpsc::channel(config.uplink_err_buffer.get());

        let updater =
            ActionLaneUpdateTask::new(lane.clone(), Some(feedback_tx), config.cleanup_timeout);

        let update_task = async move {
            let result = updater.run_update(update_rx).await;
            upd_done_tx.trigger();
            result
        }
        .instrument(span!(Level::INFO, UPDATE_TASK, ?route));

        let uplinks = StatelessUplinks::new(feedback_rx, route.clone(), UplinkKind::Action);
        let uplink_task = uplinks
            .run(uplink_rx, context.router_handle(), err_tx)
            .instrument(span!(Level::INFO, UPLINK_SPAWN_TASK, ?route));

        let error_handler =
            UplinkErrorHandler::collector(route.clone(), config.max_fatal_uplink_errors);

        let envelope_task = action_envelope_task_with_uplinks(
            route.clone(),
            envelopes,
            uplink_tx,
            err_rx,
            error_handler,
            OnCommandStrategy::Send(OnCommandHandler::new(update_tx, route.clone())),
        );

        let (upd_result, _, (uplink_fatal, uplink_errs)) =
            join3(update_task, uplink_task, envelope_task).await;
        combine_results(route, upd_result.err(), uplink_fatal, uplink_errs)
    } else {
        let updater = ActionLaneUpdateTask::new(lane.clone(), None, config.cleanup_timeout);
        let update_task = async move {
            let result = updater.run_update(update_rx).await;
            upd_done_tx.trigger();
            result
        }
        .instrument(span!(Level::INFO, UPDATE_TASK, ?route));
        let envelope_task = simple_action_envelope_task(envelopes, update_tx);
        let (upd_result, _) = join(update_task, envelope_task).await;
        combine_results(route, upd_result.err(), false, vec![])
    }
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
    let span = span!(Level::INFO, LANE_IO_TASK, ?route);
    let _enter = span.enter();

    let (uplink_tx, uplink_rx) = mpsc::channel(config.action_buffer.get());
    let (err_tx, err_rx) = mpsc::channel(config.uplink_err_buffer.get());
    let stream = stream.map(AddressedUplinkMessage::broadcast);
    let uplinks = StatelessUplinks::new(stream, route.clone(), UplinkKind::Supply);

    let on_command_strategy = OnCommandStrategy::<Dropping>::dropping();

    let envelope_task = action_envelope_task_with_uplinks(
        route.clone(),
        envelopes,
        uplink_tx,
        err_rx,
        UplinkErrorHandler::discard(),
        on_command_strategy,
    );

    let uplink_task = uplinks
        .run(uplink_rx, context.router_handle(), err_tx)
        .instrument(span!(Level::INFO, UPLINK_SPAWN_TASK, ?route));

    let (_, (uplink_fatal, uplink_errs)) = join(uplink_task, envelope_task).await;

    if uplink_fatal && !uplink_errs.is_empty() {
        Err(LaneIoError::for_uplink_errors(route, uplink_errs))
    } else {
        Ok(uplink_errs)
    }
}

async fn action_envelope_task_with_uplinks<Cmd>(
    route: RelativePath,
    envelopes: impl Stream<Item = TaggedClientEnvelope>,
    mut actions: mpsc::Sender<TaggedAction>,
    err_rx: mpsc::Receiver<UplinkErrorReport>,
    mut err_handler: UplinkErrorHandler,
    mut on_command_handler: OnCommandStrategy<Cmd>,
) -> (bool, Vec<UplinkErrorReport>)
where
    Cmd: Send + Sync + Form + Debug + 'static,
{
    pin_mut!(envelopes);

    let envelopes = envelopes.fuse();
    let mut err_rx = err_rx.fuse();
    pin_mut!(envelopes);

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
                            let maybe_command = Cmd::try_convert(value).map(|cmd| (addr, cmd));
                            on_command_handler.on_command(maybe_command, addr).await
                        }
                        None => {
                            break false;
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
    };

    drop(actions);

    while let Some(error) = err_rx.next().await {
        if err_handler.add(error).is_err() {
            failed = true;
        }
    }

    (failed, err_handler.take_errors())
}

#[derive(Debug)]
struct Dropping;

impl Form for Dropping {
    fn as_value(&self) -> Value {
        Value::Extant
    }

    fn try_from_value(_value: &Value) -> Result<Self, FormErr> {
        Ok(Dropping)
    }
}

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
        maybe_command: Result<(RoutingAddr, F), FormErr>,
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
    sender: mpsc::Sender<Result<(RoutingAddr, F), FormErr>>,
    /// The corresponding route that this handler is operating on.
    route: RelativePath,
}

impl<F> OnCommandHandler<F>
where
    F: Send + Sync + Form + Debug + 'static,
{
    fn new(
        sender: mpsc::Sender<Result<(RoutingAddr, F), FormErr>>,
        route: RelativePath,
    ) -> OnCommandHandler<F> {
        OnCommandHandler { sender, route }
    }

    /// Forwards the value to the underlying sender and returns whether the forwarding operation was
    /// successful.
    async fn send(
        &mut self,
        maybe_command: Result<(RoutingAddr, F), FormErr>,
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
