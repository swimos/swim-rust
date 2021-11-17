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

pub mod command;
pub mod event;
pub mod map;
pub mod value;

use crate::downlink::error::DownlinkError;
use crate::downlink::{Command, DownlinkState, Message};
use tracing::{trace, warn};

/// Result of processing a Warp message in a downlink state machine. If the terminate
/// flag is set the downlink will stop (using any error from the result). If the
/// result is an error but the terminate flag is not set the error will only be logged.
#[derive(Debug)]
pub struct EventResult<T> {
    pub result: Result<Option<T>, DownlinkError>,
    pub terminate: bool,
}

impl<T> EventResult<T> {
    pub fn terminate() -> EventResult<T> {
        EventResult {
            result: Ok(None),
            terminate: true,
        }
    }

    pub fn fail(err: DownlinkError) -> EventResult<T> {
        EventResult {
            result: Err(err),
            terminate: true,
        }
    }

    pub fn of(value: T) -> EventResult<T> {
        EventResult {
            result: Ok(Some(value)),
            terminate: false,
        }
    }
}

impl<T> From<Result<Option<T>, DownlinkError>> for EventResult<T> {
    fn from(result: Result<Option<T>, DownlinkError>) -> Self {
        let terminate = result.is_err();
        EventResult { result, terminate }
    }
}

impl<T> From<Result<(), DownlinkError>> for EventResult<T> {
    fn from(result: Result<(), DownlinkError>) -> Self {
        let terminate = result.is_err();
        EventResult {
            result: result.map(|_| None),
            terminate,
        }
    }
}

impl<T> Default for EventResult<T> {
    fn default() -> Self {
        EventResult {
            result: Ok(None),
            terminate: false,
        }
    }
}

/// Result of processing an action against a downlink state machine.
#[derive(PartialEq, Eq, Debug)]
pub struct Response<E, C> {
    pub event: Option<E>,
    pub command: Option<Command<C>>,
}

impl<E, C> Response<E, C> {
    pub fn command(cmd: C) -> Self {
        Response {
            event: None,
            command: Some(Command::Action(cmd)),
        }
    }
}

impl<E, C> From<(E, C)> for Response<E, C> {
    fn from((event, cmd): (E, C)) -> Self {
        Response {
            event: Some(event),
            command: Some(Command::Action(cmd)),
        }
    }
}

impl<E, C> Default for Response<E, C> {
    fn default() -> Self {
        Response {
            event: None,
            command: None,
        }
    }
}

pub type ResponseResult<E, C> = Result<Response<E, C>, DownlinkError>;

/// This trait defines the interface that must be implemented for the state type of a downlink.
pub trait DownlinkStateMachine<WarpMsg, ActionReq> {
    /// State type of the downlink.
    type State: Send + Sync;
    /// Type of commands that will be sent out to the Warp connection.
    type WarpCmd: Send;
    /// Type of events that will be issued to the observer of the downlink.
    type Report: Send + Sync;

    /// Create the initial value of the state and any command that should be sent
    /// at initialization.
    fn initialize(&self) -> (Self::State, Option<Command<Self::WarpCmd>>);

    /// Determines whether action requests should be processed, based on the current state.
    fn handle_requests(&self, _state: &Self::State) -> bool {
        true
    }

    /// Handle an incoming Warp message.
    fn handle_warp_message(
        &self,
        state: &mut Self::State,
        event: Message<WarpMsg>,
    ) -> EventResult<Self::Report>;

    /// Handle a local action request.
    fn handle_action_request(
        &self,
        state: &mut Self::State,
        request: ActionReq,
    ) -> ResponseResult<Self::Report, Self::WarpCmd>;

    /// A command to attempt to dispatch when the downlink stops.
    fn finalize(&self, _state: &Self::State) -> Option<Command<Self::WarpCmd>> {
        None
    }
}

/// This trait is for simple, stateful downlinks that follow the standard synchronization model.
pub trait SyncStateMachine<WarpMsg, ActionReq> {
    /// State type of the downlink.
    type State: Send + Sync;
    /// Type of commands that will be sent out to the Warp connection.
    type WarpCmd: Send;
    /// Type of events that will be issued to the owner of the downlink.
    type Report: Send + Sync;

    /// The initial value of the state.
    fn init(&self) -> Self::State;

    /// Generate the initial event when the downlink enters the [`Synced`] state.
    fn on_sync(&self, state: &Self::State) -> Self::Report;

    /// Update the state with a message, received between [`Linked`] and [`Synced`'].
    fn handle_message_unsynced(
        &self,
        state: &mut Self::State,
        message: WarpMsg,
    ) -> Result<(), DownlinkError>;

    /// Update the state with a message when in the [`Synced`] state, potentially generating an
    /// event.
    fn handle_message(
        &self,
        state: &mut Self::State,
        message: WarpMsg,
    ) -> Result<Option<Self::Report>, DownlinkError>;

    /// Apply an action request to the state of the downlink, potentialyl generating outgoing events
    /// and commands.
    fn apply_action_request(
        &self,
        state: &mut Self::State,
        req: ActionReq,
    ) -> ResponseResult<Self::Report, Self::WarpCmd>;
}

impl<Basic, WarpMsg, ActionReq> DownlinkStateMachine<WarpMsg, ActionReq> for Basic
where
    Basic: SyncStateMachine<WarpMsg, ActionReq>,
{
    type State = (DownlinkState, Basic::State);
    type Report = Basic::Report;
    type WarpCmd = Basic::WarpCmd;

    fn initialize(&self) -> (Self::State, Option<Command<Self::WarpCmd>>) {
        ((DownlinkState::Unlinked, self.init()), Some(Command::Sync))
    }

    fn handle_requests(&self, state: &Self::State) -> bool {
        let (dl_state, _) = state;
        *dl_state == DownlinkState::Synced
    }

    fn handle_warp_message(
        &self,
        state: &mut Self::State,
        event: Message<WarpMsg>,
    ) -> EventResult<Self::Report> {
        let (dl_state, basic_state) = state;
        match event {
            Message::Linked => {
                trace!("Downlink linked");
                *dl_state = DownlinkState::Linked;
                EventResult::default()
            }
            Message::Synced => {
                let old = *dl_state;
                *dl_state = DownlinkState::Synced;
                if old == DownlinkState::Synced {
                    EventResult::default()
                } else {
                    EventResult::of(self.on_sync(basic_state))
                }
            }
            Message::Action(event) => match dl_state {
                DownlinkState::Unlinked => EventResult::default(),
                DownlinkState::Linked => self.handle_message_unsynced(basic_state, event).into(),
                DownlinkState::Synced => self.handle_message(basic_state, event).into(),
            },
            Message::Unlinked => {
                *dl_state = DownlinkState::Unlinked;
                EventResult::terminate()
            }
            Message::BadEnvelope(details) => {
                warn!(
                    ?details,
                    "Downlink failed after receiving a malformed envelope."
                );
                EventResult::fail(DownlinkError::MalformedMessage)
            }
        }
    }

    fn handle_action_request(
        &self,
        state: &mut Self::State,
        request: ActionReq,
    ) -> ResponseResult<Basic::Report, Basic::WarpCmd> {
        let (_, basic_state) = state;
        self.apply_action_request(basic_state, request)
    }

    fn finalize(&self, state: &Self::State) -> Option<Command<Self::WarpCmd>> {
        let (dl_state, _) = state;
        if *dl_state == DownlinkState::Linked || *dl_state == DownlinkState::Synced {
            Some(Command::Unlink)
        } else {
            None
        }
    }
}
