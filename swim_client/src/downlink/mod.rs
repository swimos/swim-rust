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
pub mod improved;
pub mod model;
pub mod subscription;
#[cfg(test)]
mod tests;
pub mod typed;
pub mod watch_adapter;

use std::fmt::Debug;
use swim_common::request::TryRequest;
use swim_common::routing::RoutingError;
use tracing::{instrument, trace};
use utilities::errors::Recoverable;
use crate::downlink::error::{DownlinkError, TransitionError};
use utilities::sync::promise;

trait Downlink {

    fn is_stopped(&self) -> bool;

    fn is_running(&self) -> bool {
        !self.is_stopped()
    }

    /// Get a promise that will complete when the downlink stops running.
    fn await_stopped(&self) -> promise::Receiver<Result<(), DownlinkError>>;

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

