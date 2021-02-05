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

pub mod error;
pub mod model;
pub mod subscription;
#[cfg(test)]
mod tests;
pub mod typed;
pub mod watch_adapter;

use crate::configuration::downlink::{DownlinkParams, OnInvalidMessage};
use crate::downlink::error::{DownlinkError, TransitionError};
use futures::future::ready;
use futures::select_biased;
use futures::stream::once;
use futures::{Stream, StreamExt};
use pin_utils::pin_mut;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use swim_common::request::TryRequest;
use swim_common::routing::RoutingError;
use swim_common::sink::item::ItemSender;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{instrument, trace};
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
enum DownlinkState {
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

#[derive(Clone, PartialEq, Debug)]
pub enum Operation<M, A> {
    Start,
    Message(Message<M>),
    Action(A),
    Error(RoutingError),
}

#[derive(Clone, PartialEq, Eq, Debug)]
struct Response<Ev, Cmd> {
    event: Option<Event<Ev>>,
    command: Option<Command<Cmd>>,
    error: Option<TransitionError>,
    terminate: bool,
}

impl<Ev, Cmd> Response<Ev, Cmd> {
    fn none() -> Response<Ev, Cmd> {
        Response {
            event: None,
            command: None,
            error: None,
            terminate: false,
        }
    }

    fn for_event(event: Event<Ev>) -> Response<Ev, Cmd> {
        Response {
            event: Some(event),
            command: None,
            error: None,
            terminate: false,
        }
    }

    fn for_command(command: Command<Cmd>) -> Response<Ev, Cmd> {
        Response {
            event: None,
            command: Some(command),
            error: None,
            terminate: false,
        }
    }

    fn then_terminate(mut self) -> Self {
        self.terminate = true;
        self
    }
}

/// This trait defines the interface that must be implemented for the state type of a downlink.
trait StateMachine<State, Message, Action>: Sized {
    /// Type of events that will be issued to the owner of the downlink.
    type Ev: Send;
    /// Type of commands that will be sent out to the Warp connection.
    type Cmd: Send;

    /// The initial value for the state.
    fn init_state(&self) -> State;

    // The downlink state at which the machine should start
    // to process updates and actions.
    fn dl_start_state(&self) -> DownlinkState;

    /// For an operation on the downlink, generate output messages.
    fn handle_operation(
        &self,
        downlink_state: &mut DownlinkState,
        state: &mut State,
        op: Operation<Message, Action>,
    ) -> Result<Response<Self::Ev, Self::Cmd>, DownlinkError>;
}

#[derive(Clone, PartialEq, Eq, Debug)]
struct BasicResponse<Ev, Cmd> {
    event: Option<Ev>,
    command: Option<Cmd>,
    error: Option<TransitionError>,
}

impl<Ev, Cmd> BasicResponse<Ev, Cmd> {
    fn none() -> Self {
        BasicResponse {
            event: None,
            command: None,
            error: None,
        }
    }

    fn of(event: Ev, command: Cmd) -> Self {
        BasicResponse {
            event: Some(event),
            command: Some(command),
            error: None,
        }
    }

    fn with_error(mut self, err: TransitionError) -> Self {
        self.error = Some(err);
        self
    }
}

impl<Ev, Cmd> From<BasicResponse<Ev, Cmd>> for Response<Ev, Cmd> {
    fn from(basic: BasicResponse<Ev, Cmd>) -> Self {
        let BasicResponse {
            event,
            command,
            error,
        } = basic;
        Response {
            event: event.map(Event::Local),
            command: command.map(Command::Action),
            error,
            terminate: false,
        }
    }
}

/// This trait is for simple, stateful downlinks that follow the standard synchronization model.
trait SyncStateMachine<State, Message, Action> {
    /// Type of events that will be issued to the owner of the downlink.
    type Ev: Send;
    /// Type of commands that will be sent out to the Warp connection.
    type Cmd: Send;

    /// The initial value of the state.
    fn init(&self) -> State;

    /// Generate the initial event when the downlink enters the [`Synced`] state.
    fn on_sync(&self, state: &State) -> Self::Ev;

    /// Update the state with a message, received between [`Linked`] and [`Synced`'].
    fn handle_message_unsynced(
        &self,
        state: &mut State,
        message: Message,
    ) -> Result<(), DownlinkError>;

    /// Update the state with a message when in the [`Synced`] state, potentially generating an
    /// event.
    fn handle_message(
        &self,
        state: &mut State,
        message: Message,
    ) -> Result<Option<Self::Ev>, DownlinkError>;

    /// Handle a local action potentially generating an event and/or a command and/or an error.
    fn handle_action(
        &self,
        state: &mut State,
        action: Action,
    ) -> BasicResponse<Self::Ev, Self::Cmd>;
}

//Adapter to make a SyncStateMachine into a StateMachine.
impl<State, M, A, Basic> StateMachine<State, M, A> for Basic
where
    Basic: SyncStateMachine<State, M, A>,
{
    type Ev = <Basic as SyncStateMachine<State, M, A>>::Ev;
    type Cmd = <Basic as SyncStateMachine<State, M, A>>::Cmd;

    fn init_state(&self) -> State {
        self.init()
    }

    fn dl_start_state(&self) -> DownlinkState {
        DownlinkState::Synced
    }

    #[instrument(skip(self, state, data_state, op))]
    fn handle_operation(
        &self,
        state: &mut DownlinkState,
        data_state: &mut State,
        op: Operation<M, A>,
    ) -> Result<Response<Self::Ev, Self::Cmd>, DownlinkError> {
        let response = match op {
            Operation::Start => {
                if *state == DownlinkState::Synced {
                    trace!("Downlink synced");
                    Response::none()
                } else {
                    trace!("Downlink syncing");
                    Response::for_command(Command::Sync)
                }
            }
            Operation::Message(message) => match message {
                Message::Linked => {
                    trace!("Downlink linked");
                    *state = DownlinkState::Linked;
                    Response::none()
                }
                Message::Synced => {
                    let old_state = *state;
                    *state = DownlinkState::Synced;
                    if old_state == DownlinkState::Synced {
                        Response::none()
                    } else {
                        Response::for_event(Event::Remote(self.on_sync(data_state)))
                    }
                }
                Message::Action(msg) => match *state {
                    DownlinkState::Unlinked => Response::none(),
                    DownlinkState::Linked => {
                        self.handle_message_unsynced(data_state, msg)?;
                        Response::none()
                    }
                    DownlinkState::Synced => match self.handle_message(data_state, msg)? {
                        Some(ev) => Response::for_event(Event::Remote(ev)),
                        _ => Response::none(),
                    },
                },
                Message::Unlinked => {
                    trace!("Downlink unlinked");
                    *state = DownlinkState::Unlinked;
                    Response::none().then_terminate()
                }
                Message::BadEnvelope(_) => return Err(DownlinkError::MalformedMessage),
            },
            Operation::Action(action) => self.handle_action(data_state, action).into(),
            Operation::Error(e) => {
                if e.is_fatal() {
                    return Err(e.into());
                } else {
                    *state = DownlinkState::Unlinked;
                    Response::for_command(Command::Sync)
                }
            }
        };
        Ok(response)
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

/// Create a new raw downlink, starting its event loop.
/// # Arguments
///
/// * `machine` - The downlink state machine.
/// * `update_stream` - Stream of external updates to the state.
/// * `cmd_sink` - Sink for outgoing commands to the remote lane.
/// * `config` - Configuration for the event loop.
pub(in crate::downlink) fn create_downlink<M, Act, State, Machine, CmdSend, Updates>(
    machine: Machine,
    update_stream: Updates,
    cmd_sink: CmdSend,
    config: DownlinkConfig,
) -> (RawDownlink<Act, Machine::Ev>, RawReceiver<Machine::Ev>)
where
    M: Send + 'static,
    Act: Send + 'static,
    State: Send + 'static,
    Machine: StateMachine<State, M, Act> + Send + Sync + 'static,
    Updates: Stream<Item = Result<Message<M>, RoutingError>> + Send + Sync + 'static,
    CmdSend: ItemSender<Command<Machine::Cmd>, RoutingError> + Send + Sync + 'static,
{
    let (act_tx, act_rx) = mpsc::channel::<Act>(config.buffer_size.get());
    let (event_tx, event_rx) = topic::channel::<Event<Machine::Ev>>(config.buffer_size);

    let (stopped_tx, stopped_rx) = promise::promise();

    let completed = Arc::new(AtomicBool::new(false));
    let completed_cpy = completed.clone();

    // The task that maintains the internal state of the lane.
    let task: DownlinkTask<Act, Machine::Ev> = DownlinkTask {
        event_sink: event_tx,
        actions: act_rx,
        config,
    };

    let dl_task = async move {
        let result = task.run(machine, update_stream, cmd_sink).await;
        completed.store(true, Ordering::Release);
        let _ = stopped_tx.provide(result);
    };

    swim_runtime::task::spawn(dl_task);

    let sub = event_rx.subscriber();

    let dl = RawDownlink {
        action_sender: act_tx,
        event_topic: sub,
        task_result: stopped_rx,
        completed: completed_cpy,
    };
    (dl, event_rx)
}

struct DownlinkTask<Act, Ev> {
    event_sink: topic::Sender<Event<Ev>>,
    actions: mpsc::Receiver<Act>,
    config: DownlinkConfig,
}

impl<Act, Ev> DownlinkTask<Act, Ev> {
    async fn run<M, Cmd, State, Updates>(
        self,
        state_machine: impl StateMachine<State, M, Act, Cmd = Cmd, Ev = Ev>,
        updates: Updates,
        mut cmd_sink: impl ItemSender<Command<Cmd>, RoutingError>,
    ) -> Result<(), DownlinkError>
    where
        Updates: Stream<Item = Result<Message<M>, RoutingError>> + Send + Sync + 'static,
    {
        let DownlinkTask {
            mut event_sink,
            actions,
            config,
        } = self;

        let update_ops = updates.map(|r| match r {
            Ok(upd) => Operation::Message(upd),
            Err(e) => Operation::Error(e),
        });

        let start: Operation<M, Act> = Operation::Start;
        let ops = once(ready(start)).chain(update_ops);

        let mut dl_state = DownlinkState::Unlinked;
        let mut model = state_machine.init_state();
        let yield_mod = config.yield_after.get();

        let ops = ops.fuse();
        let actions = ReceiverStream::new(actions).fuse();

        pin_mut!(ops);
        pin_mut!(actions);

        let mut act_terminated = false;
        let mut events_terminated = false;
        let mut read_act = false;

        let mut iteration_count: usize = 0;

        trace!("Running downlink task");

        let result: Result<(), DownlinkError> = loop {
            let next_op: TaskInput<M, Act> =
                if dl_state == state_machine.dl_start_state() && !act_terminated {
                    if read_act {
                        read_act = false;
                        let input = select_biased! {
                            act_op = actions.next() => TaskInput::from_action(act_op),
                            upd_op = ops.next() => TaskInput::from_operation(upd_op),
                        };
                        input
                    } else {
                        read_act = true;
                        let input = select_biased! {
                            upd_op = ops.next() => TaskInput::from_operation(upd_op),
                            act_op = actions.next() => TaskInput::from_action(act_op),
                        };
                        input
                    }
                } else {
                    TaskInput::from_operation(ops.next().await)
                };

            match next_op {
                TaskInput::Op(op) => {
                    let Response {
                        event,
                        command,
                        error,
                        terminate,
                    } = match state_machine.handle_operation(&mut dl_state, &mut model, op) {
                        Ok(r) => r,
                        Err(e) => match e {
                            e @ DownlinkError::TaskPanic(_) => {
                                break Err(e);
                            }
                            _ => match config.on_invalid {
                                OnInvalidMessage::Ignore => {
                                    continue;
                                }
                                OnInvalidMessage::Terminate => {
                                    break Err(e);
                                }
                            },
                        },
                    };
                    let result = match (event, command) {
                        (Some(event), Some(cmd)) => {
                            if !events_terminated
                                && event_sink.discarding_send(event).await.is_err()
                            {
                                events_terminated = true;
                            }
                            cmd_sink.send_item(cmd).await
                        }
                        (Some(event), _) => {
                            if !events_terminated
                                && event_sink.discarding_send(event).await.is_err()
                            {
                                events_terminated = true;
                            }
                            Ok(())
                        }
                        (_, Some(command)) => cmd_sink.send_item(command).await,
                        _ => Ok(()),
                    };

                    if error.map(|e| e.is_fatal()).unwrap_or(false) {
                        break Err(DownlinkError::TransitionError);
                    } else if terminate || result.is_err() {
                        break result.map_err(Into::into);
                    } else if act_terminated && events_terminated {
                        break Ok(());
                    }
                }
                TaskInput::ActTerminated => {
                    act_terminated = true;
                    if events_terminated {
                        break Ok(());
                    }
                }
                TaskInput::Terminated => {
                    break Ok(());
                }
            }

            iteration_count += 1;
            if iteration_count % yield_mod == 0 {
                tokio::task::yield_now().await;
            }
        };
        let _ = cmd_sink.send_item(Command::Unlink).await;
        result
    }
}

enum TaskInput<M, A> {
    Op(Operation<M, A>),
    ActTerminated,
    Terminated,
}

impl<M, A> TaskInput<M, A> {
    fn from_action(maybe_action: Option<A>) -> TaskInput<M, A> {
        match maybe_action {
            Some(a) => TaskInput::Op(Operation::Action(a)),
            _ => TaskInput::ActTerminated,
        }
    }

    fn from_operation(maybe_action: Option<Operation<M, A>>) -> TaskInput<M, A> {
        match maybe_action {
            Some(op) => TaskInput::Op(op),
            _ => TaskInput::Terminated,
        }
    }
}
