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

pub mod error;
pub mod model;
pub(crate) mod state_machine;
pub mod subscription;
#[cfg(test)]
mod tests;
pub mod typed;
pub mod watch_adapter;

use crate::configuration::downlink::{DownlinkParams, OnInvalidMessage};
use crate::downlink::error::DownlinkError;
use crate::downlink::model::map::UntypedMapModification;
use crate::downlink::model::value::SharedValue;
use crate::downlink::state_machine::command::CommandStateMachine;
use crate::downlink::state_machine::event::EventStateMachine;
use crate::downlink::state_machine::map::MapStateMachine;
use crate::downlink::state_machine::value::ValueStateMachine;
use crate::downlink::state_machine::{
    DownlinkStateMachine, EventResult, Response, SchemaViolations,
};
use crate::downlink::typed::{
    UntypedCommandDownlink, UntypedEventDownlink, UntypedEventReceiver, UntypedMapDownlink,
    UntypedMapReceiver, UntypedValueDownlink, UntypedValueReceiver,
};
use either::Either;
use futures::future::FusedFuture;
use futures::select_biased;
use futures::stream::FusedStream;
use futures::{FutureExt, Stream, StreamExt};
use pin_utils::pin_mut;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use swim_common::model::schema::StandardSchema;
use swim_common::model::Value;
use swim_common::request::TryRequest;
use swim_common::routing::RoutingError;
use swim_common::sink::item::ItemSender;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, Level};
use utilities::errors::Recoverable;
use utilities::sync::{promise, topic};

/// Trait defining the common operations supported by all downlinks.
pub trait Downlink {
    /// True when the downlink has terminated.
    fn is_stopped(&self) -> bool;

    /// True when the downlink is still running. It is not safe to rely on the downlink still being
    /// active at any point after this call has been made.
    fn is_running(&self) -> bool {
        !self.is_stopped()
    }

    /// Get a promise that will complete when the downlink stops running.
    fn await_stopped(&self) -> promise::Receiver<Result<(), DownlinkError>>;

    /// Determine if two downlink handles represent the same downlink.
    fn same_downlink(left: &Self, right: &Self) -> bool;
}

/// A request to a downlink for a value.
pub type DownlinkRequest<T> = TryRequest<T, DownlinkError>;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DownlinkState {
    Unlinked,
    Linked,
    Synced,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Message<M> {
    Linked,
    Synced,
    Action(M),
    Unlinked,
    BadEnvelope(String),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Command<A> {
    Sync,
    Link,
    Action(A),
    Unlink,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Event<A> {
    Local(A),
    Remote(A),
}

impl<A> Event<A> {
    pub fn get_inner(self) -> A {
        match self {
            Event::Local(inner) => inner,
            Event::Remote(inner) => inner,
        }
    }

    pub fn get_inner_ref(&self) -> &A {
        match self {
            Event::Local(inner) => inner,
            Event::Remote(inner) => inner,
        }
    }

    /// Maps `Event<A>` to `Result<Event<B>, Err>` by applying a transformation function `Func`.
    pub fn try_transform<B, Err, Func>(self, mut func: Func) -> Result<Event<B>, Err>
    where
        Func: FnMut(A) -> Result<B, Err>,
    {
        match self {
            Event::Local(value) => Ok(Event::Local(func(value)?)),
            Event::Remote(value) => Ok(Event::Remote(func(value)?)),
        }
    }
}

/// Raw downlinks are the untyped core around which other types are downlink are built. Actions of
/// type `Act` can be applied to the downlink, modifying its state, and it will, in turn produce
/// events of type `Ev`.
#[derive(Debug)]
pub struct RawDownlink<Act, Ev> {
    action_sender: mpsc::Sender<Act>,
    event_topic: topic::Subscriber<Event<Ev>>,
    completed: Arc<AtomicBool>,
    task_result: promise::Receiver<Result<(), DownlinkError>>,
}

impl<Act, Ev> Clone for RawDownlink<Act, Ev> {
    fn clone(&self) -> Self {
        RawDownlink {
            action_sender: self.action_sender.clone(),
            event_topic: self.event_topic.clone(),
            task_result: self.task_result.clone(),
            completed: self.completed.clone(),
        }
    }
}

impl<Act, Ev> Downlink for RawDownlink<Act, Ev> {
    fn is_stopped(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }

    fn await_stopped(&self) -> promise::Receiver<Result<(), DownlinkError>> {
        self.task_result.clone()
    }

    fn same_downlink(left: &Self, right: &Self) -> bool {
        Arc::ptr_eq(&left.completed, &right.completed)
    }
}

impl<Act, Ev> RawDownlink<Act, Ev> {
    pub fn subscriber(&self) -> topic::Subscriber<Event<Ev>> {
        self.event_topic.clone()
    }

    pub fn subscribe(&self) -> Option<topic::Receiver<Event<Ev>>> {
        self.event_topic.subscribe().ok()
    }

    pub async fn send(&self, value: Act) -> Result<(), mpsc::error::SendError<Act>> {
        self.action_sender.send(value).await
    }

    pub fn sender(&self) -> &mpsc::Sender<Act> {
        &self.action_sender
    }
}

/// Configuration parameters for the downlink event loop.
#[derive(Clone, Copy, Debug)]
pub(in crate::downlink) struct DownlinkConfig {
    /// Buffer size for the action and event channels.
    pub buffer_size: NonZeroUsize,
    /// The downlink event loop with yield to the runtime after this many iterations.
    pub yield_after: NonZeroUsize,
    /// Strategy for handling invalid messages.
    pub on_invalid: OnInvalidMessage,
}

impl From<&DownlinkParams> for DownlinkConfig {
    fn from(conf: &DownlinkParams) -> Self {
        DownlinkConfig {
            buffer_size: conf.buffer_size,
            yield_after: conf.yield_after,
            on_invalid: conf.on_invalid,
        }
    }
}

pub type RawReceiver<Ev> = topic::Receiver<Event<Ev>>;

struct DownlinkEventLoop<SM, Updates, Action> {
    state_machine: SM,
    updates: Updates,
    actions: mpsc::Receiver<Action>,
    config: DownlinkConfig,
}

impl<SM, Updates, Action> DownlinkEventLoop<SM, Updates, Action> {
    fn new(
        state_machine: SM,
        updates: Updates,
        actions: mpsc::Receiver<Action>,
        config: DownlinkConfig,
    ) -> Self {
        DownlinkEventLoop {
            state_machine,
            updates,
            actions,
            config,
        }
    }
}

const ERR_ON_MSG: &str = "Error on incoming envelope in downlink.";
const ERR_ON_ACTION: &str = "Error on incoming action in downlink.";
const ERR_SENDING_CMD: &str = "Error sending outgoing message.";
const FAILED_TO_UNLINK: &str = "Failed to unlink a terminated downlink.";

impl<M, SM, Updates, Action> DownlinkEventLoop<SM, Updates, Action>
where
    Action: 'static,
    Updates: Stream<Item = Result<Message<M>, RoutingError>> + Send + Sync + 'static,
    SM: DownlinkStateMachine<M, Action>,
{
    pub async fn run<CmdSender>(
        self,
        mut commands: CmdSender,
        mut events: topic::Sender<Event<SM::Report>>,
    ) -> Result<(), DownlinkError>
    where
        CmdSender: ItemSender<Command<SM::Update>, RoutingError> + Send + Sync + 'static,
    {
        let DownlinkEventLoop {
            state_machine,
            updates,
            actions,
            config,
        } = self;

        let yield_mod = config.yield_after.get();
        let mut iteration_count: usize = 0;

        let (mut state, start_cmd) = state_machine.initialize();
        if let Some(cmd) = start_cmd {
            commands.send_item(cmd).await?;
        }

        let updates = updates.fuse();
        let mut actions = ReceiverStream::new(actions).fuse();
        pin_mut!(updates);

        //Ideally these would be a single enum but pending must be kept pinned and command_state
        //cannot be pinned so this is not possible.
        let pending = None;
        pin_mut!(pending);
        let mut command_state = Some(commands);

        let result = loop {
            match command_state {
                Some(commands) => {
                    match select_updates_and_actions(
                        &state_machine,
                        &mut state,
                        &mut updates,
                        &mut actions,
                        &mut events,
                        &config,
                    )
                    .await
                    {
                        SelectAnyEffect::SendCommand(cmd) => {
                            pending.set(Some(send_response(commands, cmd).fuse()));
                            command_state = None;
                        }
                        SelectAnyEffect::TerminateWithError(err) => {
                            command_state = Some(commands);
                            break Err(err);
                        }
                        SelectAnyEffect::Terminate => {
                            command_state = Some(commands);
                            break Ok(());
                        }
                        _ => {
                            command_state = Some(commands);
                        }
                    }
                }
                _ => {
                    let mut pending_write =
                        pending.as_mut().as_pin_mut().expect("Inconsistent state.");
                    match select_updates_only(
                        &state_machine,
                        &mut state,
                        &mut updates,
                        pending_write.as_mut(),
                        &mut events,
                        &config,
                    )
                    .await
                    {
                        SelectUpdatesEffect::WriteComplete(sender) => {
                            command_state = Some(sender);
                            pending.set(None);
                        }
                        SelectUpdatesEffect::WriteFailed(sender, error) => {
                            command_state = Some(sender);
                            pending.set(None);
                            break Err(error.into());
                        }
                        SelectUpdatesEffect::TerminateWithError(error) => {
                            let (sender, _) = pending_write.await;
                            command_state = Some(sender);
                            pending.set(None);
                            break Err(error);
                        }
                        SelectUpdatesEffect::Terminate => {
                            let (sender, _) = pending_write.await;
                            command_state = Some(sender);
                            pending.set(None);
                            break Ok(());
                        }
                        SelectUpdatesEffect::AttemptRestart => {
                            let (sender, _) = pending_write.await;
                            let (new_state, start_cmd) = state_machine.initialize();
                            state = new_state;
                            if let Some(cmd) = start_cmd {
                                pending.set(Some(send_response(sender, cmd).fuse()));
                            } else {
                                command_state = Some(sender);
                                pending.set(None);
                            }
                        }
                        _ => {
                            command_state = None;
                        }
                    }
                }
            }

            iteration_count += 1;
            if iteration_count % yield_mod == 0 {
                tokio::task::yield_now().await;
            }
        };
        if let Some(mut commands) = command_state {
            if let Some(cmd) = state_machine.finalize(&state) {
                if let Err(error) = commands.send_item(cmd).await {
                    event!(Level::ERROR, FAILED_TO_UNLINK, ?error);
                }
            }
        }
        result
    }
}

enum CommonEffect {
    Terminate,
    TerminateWithError(DownlinkError),
    Continue,
}

enum SelectAnyEffect<C> {
    Terminate,
    TerminateWithError(DownlinkError),
    SendCommand(Command<C>),
    Continue,
}

impl<C> From<CommonEffect> for SelectAnyEffect<C> {
    fn from(eff: CommonEffect) -> Self {
        match eff {
            CommonEffect::Terminate => SelectAnyEffect::Terminate,
            CommonEffect::TerminateWithError(error) => SelectAnyEffect::TerminateWithError(error),
            CommonEffect::Continue => SelectAnyEffect::Continue,
        }
    }
}

enum SelectUpdatesEffect<CmdSender> {
    Terminate,
    TerminateWithError(DownlinkError),
    Continue,
    WriteComplete(CmdSender),
    WriteFailed(CmdSender, RoutingError),
    AttemptRestart,
}

impl<CmdSender> From<CommonEffect> for SelectUpdatesEffect<CmdSender> {
    fn from(eff: CommonEffect) -> Self {
        match eff {
            CommonEffect::Terminate => SelectUpdatesEffect::Terminate,
            CommonEffect::TerminateWithError(error) => {
                SelectUpdatesEffect::TerminateWithError(error)
            }
            CommonEffect::Continue => SelectUpdatesEffect::Continue,
        }
    }
}

type WriteResult<CmdSender> = (CmdSender, Result<(), RoutingError>);

async fn select_updates_only<M, Action, Updates, CmdSender, Fut, SM>(
    state_machine: &SM,
    state: &mut SM::State,
    updates: &mut Updates,
    mut command_dispatch: Fut,
    events: &mut topic::Sender<Event<SM::Report>>,
    config: &DownlinkConfig,
) -> SelectUpdatesEffect<CmdSender>
where
    Updates: FusedStream<Item = Result<Message<M>, RoutingError>> + Unpin,
    Fut: FusedFuture<Output = WriteResult<CmdSender>> + Unpin,
    SM: DownlinkStateMachine<M, Action>,
{
    let next: Either<WriteResult<CmdSender>, Option<Result<Message<M>, RoutingError>>> = select_biased! {
        write_result = command_dispatch => Either::Left(write_result),
        maybe_update = updates.next() => Either::Right(maybe_update),
    };

    match next {
        Either::Left((sender, Ok(_))) => SelectUpdatesEffect::WriteComplete(sender),
        Either::Right(Some(Ok(message))) => {
            process_message(state_machine, state, events, message, config)
                .await
                .into()
        }
        Either::Left((sender, Err(error))) => {
            event!(Level::ERROR, ERR_SENDING_CMD, ?error);
            SelectUpdatesEffect::WriteFailed(sender, error)
        }
        Either::Right(Some(Err(error))) => {
            if error.is_fatal() {
                SelectUpdatesEffect::TerminateWithError(error.into())
            } else {
                SelectUpdatesEffect::AttemptRestart
            }
        }
        _ => SelectUpdatesEffect::Continue,
    }
}

async fn process_message<M, Action, SM>(
    state_machine: &SM,
    state: &mut SM::State,
    events: &mut topic::Sender<Event<SM::Report>>,
    message: Message<M>,
    config: &DownlinkConfig,
) -> CommonEffect
where
    SM: DownlinkStateMachine<M, Action>,
{
    match state_machine.handle_event(state, message) {
        EventResult {
            result: Ok(event),
            terminate,
        } => {
            let send_result = if let Some(event) = event {
                events.discarding_send(Event::Remote(event)).await
            } else {
                Ok(())
            };
            if send_result.is_err() || terminate {
                CommonEffect::Terminate
            } else {
                CommonEffect::Continue
            }
        }
        EventResult {
            result: Err(error),
            terminate,
        } => {
            if terminate {
                event!(Level::ERROR, ERR_ON_MSG, ?error);
                if config.on_invalid == OnInvalidMessage::Ignore && error.is_bad_message() {
                    CommonEffect::Continue
                } else {
                    CommonEffect::TerminateWithError(error)
                }
            } else {
                event!(Level::WARN, ERR_ON_MSG, ?error);
                CommonEffect::Continue
            }
        }
    }
}

async fn select_updates_and_actions<M, Action, Updates, Actions, SM>(
    state_machine: &SM,
    state: &mut SM::State,
    updates: &mut Updates,
    actions: &mut Actions,
    events: &mut topic::Sender<Event<SM::Report>>,
    config: &DownlinkConfig,
) -> SelectAnyEffect<SM::Update>
where
    Updates: FusedStream<Item = Result<Message<M>, RoutingError>> + Unpin,
    Actions: FusedStream<Item = Action> + Unpin + 'static,
    SM: DownlinkStateMachine<M, Action>,
{
    let next = if state_machine.handle_actions(&state) {
        select_biased! {
            maybe_upd = updates.next() => Some(maybe_upd.map(Either::Left)),
            maybe_act = actions.next() => {
                if let Some(act) = maybe_act {
                    Some(Some(Either::Right(act)))
                } else {
                    None
                }
            }
        }
    } else {
        Some(updates.next().await.map(Either::Left))
    };

    match next {
        Some(Some(Either::Left(Ok(message)))) => {
            process_message(state_machine, state, events, message, config)
                .await
                .into()
        }
        Some(Some(Either::Left(Err(e)))) => {
            if e.is_fatal() {
                SelectAnyEffect::TerminateWithError(e.into())
            } else {
                let (new_state, start_cmd) = state_machine.initialize();
                *state = new_state;
                if let Some(cmd) = start_cmd {
                    SelectAnyEffect::SendCommand(cmd)
                } else {
                    SelectAnyEffect::Continue
                }
            }
        }
        Some(Some(Either::Right(action))) => match state_machine.handle_request(state, action) {
            Ok(Response { event, command }) => {
                let event_result = if let Some(event) = event {
                    events.discarding_send(Event::Local(event)).await
                } else {
                    Ok(())
                };
                if event_result.is_err() {
                    SelectAnyEffect::Terminate
                } else if let Some(cmd) = command {
                    SelectAnyEffect::SendCommand(cmd)
                } else {
                    SelectAnyEffect::Continue
                }
            }
            Err(error) => {
                event!(Level::WARN, ERR_ON_ACTION, ?error);
                SelectAnyEffect::Continue
            }
        },
        Some(_) => SelectAnyEffect::Terminate,
        _ => SelectAnyEffect::Continue,
    }
}

async fn send_response<S, CmdSender>(
    mut command_sender: CmdSender,
    cmd: Command<S>,
) -> (CmdSender, Result<(), RoutingError>)
where
    CmdSender: ItemSender<Command<S>, RoutingError> + Send + Sync + 'static,
{
    let result = command_sender.send_item(cmd).await;
    (command_sender, result)
}

/// Create a new raw downlink, starting its event loop.
/// # Arguments
///
/// * `machine` - The downlink state machine.
/// * `update_stream` - Stream of external updates to the state.
/// * `cmd_sink` - Sink for outgoing commands to the remote lane.
/// * `config` - Configuration for the event loop.
pub(in crate::downlink) fn create_downlink<M, Act, SM, CmdSend, Updates>(
    machine: SM,
    update_stream: Updates,
    cmd_sink: CmdSend,
    config: DownlinkConfig,
) -> (RawDownlink<Act, SM::Report>, RawReceiver<SM::Report>)
where
    M: Send + 'static,
    Act: Send + 'static,
    SM: DownlinkStateMachine<M, Act> + Send + Sync + 'static,
    Updates: Stream<Item = Result<Message<M>, RoutingError>> + Send + Sync + 'static,
    CmdSend: ItemSender<Command<SM::Update>, RoutingError> + Send + Sync + 'static,
{
    let (act_tx, act_rx) = mpsc::channel(config.buffer_size.get());

    let downlink = DownlinkEventLoop::new(machine, update_stream, act_rx, config);

    let (event_tx, event_rx) = topic::channel(config.buffer_size);

    let completed = Arc::new(AtomicBool::new(false));
    let completed_cpy = completed.clone();
    let (result_tx, result_rx) = promise::promise();

    let task = async move {
        let result = downlink.run(cmd_sink, event_tx).await;
        let _ = result_tx.provide(result);
        completed_cpy.store(true, Ordering::SeqCst);
    };

    swim_runtime::task::spawn(task);
    let dl = RawDownlink {
        action_sender: act_tx,
        event_topic: event_rx.subscriber(),
        completed,
        task_result: result_rx,
    };
    (dl, event_rx)
}

pub(in crate::downlink) fn command_downlink<Commands>(
    schema: StandardSchema,
    cmd_sender: Commands,
    config: DownlinkConfig,
) -> UntypedCommandDownlink
where
    Commands: ItemSender<Command<Value>, RoutingError> + Send + Sync + 'static,
{
    let upd_stream = futures::stream::pending();

    create_downlink(
        CommandStateMachine::new(schema),
        upd_stream,
        cmd_sender,
        config,
    )
    .0
}

/// Create an event downlink.
pub(in crate::downlink) fn event_downlink<Updates, Snk>(
    schema: StandardSchema,
    violations: SchemaViolations,
    update_stream: Updates,
    cmd_sink: Snk,
    config: DownlinkConfig,
) -> (UntypedEventDownlink, UntypedEventReceiver)
where
    Updates: Stream<Item = Result<Message<Value>, RoutingError>> + Send + Sync + 'static,
    Snk: ItemSender<Command<()>, RoutingError> + Send + Sync + 'static,
{
    create_downlink(
        EventStateMachine::new(schema, violations),
        update_stream,
        cmd_sink,
        config,
    )
}

/// Typedef for map downlink stream item.
type MapItemResult = Result<Message<UntypedMapModification<Value>>, RoutingError>;

/// Create a map downlink.
pub(in crate::downlink) fn map_downlink<Updates, Commands>(
    key_schema: Option<StandardSchema>,
    value_schema: Option<StandardSchema>,
    update_stream: Updates,
    cmd_sink: Commands,
    config: DownlinkConfig,
) -> (UntypedMapDownlink, UntypedMapReceiver)
where
    Updates: Stream<Item = MapItemResult> + Send + Sync + 'static,
    Commands:
        ItemSender<Command<UntypedMapModification<Value>>, RoutingError> + Send + Sync + 'static,
{
    create_downlink(
        MapStateMachine::new(
            key_schema.unwrap_or(StandardSchema::Anything),
            value_schema.unwrap_or(StandardSchema::Anything),
        ),
        update_stream,
        cmd_sink,
        config,
    )
}

/// Typedef for value downlink stream item.
type ValueItemResult = Result<Message<Value>, RoutingError>;

/// Create a raw value downlink.
pub(in crate::downlink) fn value_downlink<Updates, Commands>(
    init: Value,
    schema: Option<StandardSchema>,
    update_stream: Updates,
    cmd_sender: Commands,
    config: DownlinkConfig,
) -> (UntypedValueDownlink, UntypedValueReceiver)
where
    Updates: Stream<Item = ValueItemResult> + Send + Sync + 'static,
    Commands: ItemSender<Command<SharedValue>, RoutingError> + Send + Sync + 'static,
{
    create_downlink(
        ValueStateMachine::new(init, schema.unwrap_or(StandardSchema::Anything)),
        update_stream,
        cmd_sender,
        config,
    )
}
