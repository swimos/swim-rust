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

use tokio::sync::mpsc;

use common::sink::item;
use futures::StreamExt;
use std::fmt::{Debug, Display, Formatter};
use tokio::sync::broadcast;
use tokio::sync::watch;

pub mod action;
pub mod any;
pub mod buffered;
pub mod dropping;
pub mod model;
pub mod queue;
pub mod raw;
pub mod subscription;
pub mod topic;
pub mod typed;
pub mod watch_adapter;

pub(self) use self::raw::create_downlink;
use crate::downlink::raw::DownlinkTaskHandle;
use crate::router::RoutingError;
use common::model::schema::StandardSchema;
use common::model::Value;
use common::topic::Topic;
use futures::task::{Context, Poll};
use futures::Future;
use std::pin::Pin;

/// Shared trait for all Warp downlinks. `Act` is the type of actions that can be performed on the
/// downlink locally and `Upd` is the type of updates that an be observed on the client side.
pub trait Downlink<Upd>: Topic<Upd> {
    /// Type of the topic which can be used to subscribe to the downlink.
    type DlTopic: Topic<Upd>;

    /// Type of the sink that can be used to apply actions to the downlink.
    type DlSink;

    /// Split the downlink into a topic and sink.
    fn split(self) -> (Self::DlTopic, Self::DlSink);
}

pub(in crate::downlink) trait DownlinkInternals: Send + Sync + Debug {
    fn task_handle(&self) -> &DownlinkTaskHandle;
}

impl DownlinkInternals for DownlinkTaskHandle {
    fn task_handle(&self) -> &DownlinkTaskHandle {
        self
    }
}

/// A future that completes after a downlink task has terminated.
pub struct StoppedFuture(watch::Receiver<Option<Result<(), DownlinkError>>>);

impl Future for StoppedFuture {
    type Output = Result<(), DownlinkError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut receiver = Pin::new(&mut self.get_mut().0);
        loop {
            match receiver.poll_next_unpin(cx) {
                Poll::Ready(None) => break Poll::Ready(Err(DownlinkError::DroppedChannel)),
                Poll::Ready(Some(maybe)) => {
                    if let Some(result) = maybe {
                        break Poll::Ready(result);
                    }
                }
                Poll::Pending => break Poll::Pending,
            };
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum DownlinkError {
    DroppedChannel,
    TaskPanic,
    TransitionError,
    MalformedMessage,
    SchemaViolation(Value, StandardSchema),
}

impl From<RoutingError> for DownlinkError {
    fn from(e: RoutingError) -> Self {
        match e {
            RoutingError::RouterDropped => DownlinkError::DroppedChannel,
        }
    }
}

impl Display for DownlinkError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DownlinkError::DroppedChannel => write!(
                f,
                "An internal channel was dropped and the downlink is now closed."
            ),
            DownlinkError::TaskPanic => write!(f, "The downlink task panicked."),
            DownlinkError::TransitionError => {
                write!(f, "The downlink state machine produced and error.")
            }
            DownlinkError::SchemaViolation(value, schema) => write!(
                f,
                "Received {} but expected a value matching {}.",
                value, schema
            ),
            DownlinkError::MalformedMessage => {
                write!(f, "A message did not have the expected shape.")
            }
        }
    }
}

impl std::error::Error for DownlinkError {}

impl<T> From<mpsc::error::SendError<T>> for DownlinkError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        DownlinkError::DroppedChannel
    }
}

impl<T> From<mpsc::error::TrySendError<T>> for DownlinkError {
    fn from(_: mpsc::error::TrySendError<T>) -> Self {
        DownlinkError::DroppedChannel
    }
}

impl<T> From<watch::error::SendError<T>> for DownlinkError {
    fn from(_: watch::error::SendError<T>) -> Self {
        DownlinkError::DroppedChannel
    }
}

impl From<item::SendError> for DownlinkError {
    fn from(_: item::SendError) -> Self {
        DownlinkError::DroppedChannel
    }
}

impl<T> From<broadcast::SendError<T>> for DownlinkError {
    fn from(_: broadcast::SendError<T>) -> Self {
        DownlinkError::DroppedChannel
    }
}

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
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Command<A> {
    Sync,
    Action(A),
    Unlink,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Event<A>(pub A, pub bool);

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Operation<M, A> {
    Start,
    Message(Message<M>),
    Action(A),
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

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum TransitionError {
    ReceiverDropped,
    SideEffectFailed,
    IllegalTransition(String),
}

/// This trait defines the interface that must be implemented for the state type of a downlink.
trait StateMachine<State, Message, Action>: Sized {
    /// Type of events that will be issued to the owner of the downlink.
    type Ev;
    /// Type of commands that will be sent out to the Warp connection.
    type Cmd;

    /// The initial value for the state.
    fn init_state(&self) -> State;

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
            event: event.map(|e| Event(e, true)),
            command: command.map(Command::Action),
            error,
            terminate: false,
        }
    }
}

/// This trait is for simple, stateful downlinks that follow the standard synchronization model.
trait BasicStateMachine<State, Message, Action> {
    /// Type of events that will be issued to the owner of the downlink.
    type Ev;
    /// Type of commands that will be sent out to the Warp connection.
    type Cmd;

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

//Adapter to make a BasicStateMachine into a StateMachine.
impl<State, M, A, Basic> StateMachine<State, M, A> for Basic
where
    Basic: BasicStateMachine<State, M, A>,
{
    type Ev = <Basic as BasicStateMachine<State, M, A>>::Ev;
    type Cmd = <Basic as BasicStateMachine<State, M, A>>::Cmd;

    fn init_state(&self) -> State {
        self.init()
    }

    fn handle_operation(
        &self,
        state: &mut DownlinkState,
        data_state: &mut State,
        op: Operation<M, A>,
    ) -> Result<Response<Self::Ev, Self::Cmd>, DownlinkError> {
        let response = match op {
            Operation::Start => {
                if *state == DownlinkState::Synced {
                    Response::none()
                } else {
                    Response::for_command(Command::Sync)
                }
            }
            Operation::Message(message) => match message {
                Message::Linked => {
                    *state = DownlinkState::Linked;
                    Response::none()
                }
                Message::Synced => {
                    let old_state = *state;
                    *state = DownlinkState::Synced;
                    if old_state == DownlinkState::Synced {
                        Response::none()
                    } else {
                        Response::for_event(Event(self.on_sync(data_state), false))
                    }
                }
                Message::Action(msg) => match *state {
                    DownlinkState::Unlinked => Response::none(),
                    DownlinkState::Linked => {
                        self.handle_message_unsynced(data_state, msg)?;
                        Response::none()
                    }
                    DownlinkState::Synced => match self.handle_message(data_state, msg)? {
                        Some(ev) => Response::for_event(Event(ev, false)),
                        _ => Response::none(),
                    },
                },
                Message::Unlinked => {
                    *state = DownlinkState::Unlinked;
                    Response::none().then_terminate()
                }
            },
            Operation::Action(action) => self.handle_action(data_state, action).into(),
        };
        Ok(response)
    }
}

/// Merges a number of different channel send error types.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DroppedError;

impl Display for DroppedError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Channel dropped.")
    }
}

impl std::error::Error for DroppedError {}

impl<T> From<mpsc::error::SendError<T>> for DroppedError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        DroppedError
    }
}

impl<T> From<watch::error::SendError<T>> for DroppedError {
    fn from(_: watch::error::SendError<T>) -> Self {
        DroppedError
    }
}

impl<T> From<broadcast::SendError<T>> for DroppedError {
    fn from(_: broadcast::SendError<T>) -> Self {
        DroppedError
    }
}

impl From<common::sink::item::SendError> for DroppedError {
    fn from(_: common::sink::item::SendError) -> Self {
        DroppedError
    }
}
