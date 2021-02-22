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

pub mod command;
pub mod event;
pub mod map;
pub mod value;

use crate::downlink::error::DownlinkError;
use crate::downlink::{Command, DownlinkState, Message};
use tracing::trace;

pub struct EventResult<T> {
    result: Result<Option<T>, DownlinkError>,
    terminate: bool,
}

impl<T> EventResult<T> {
    fn terminate() -> EventResult<T> {
        EventResult {
            result: Ok(None),
            terminate: true,
        }
    }

    fn fail(err: DownlinkError) -> EventResult<T> {
        EventResult {
            result: Err(err),
            terminate: true,
        }
    }

    fn of(value: T) -> EventResult<T> {
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

#[derive(PartialEq, Eq, Debug)]
pub struct Response<E, C> {
    event: Option<E>,
    command: Option<Command<C>>,
}

impl<E, C> Response<E, C> {
    fn command(cmd: C) -> Self {
        Response {
            event: None,
            command: Some(Command::Action(cmd)),
        }
    }

    fn event(ev: E) -> Self {
        Response {
            event: Some(ev),
            command: None,
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

type ResponseResult<E, C> = Result<Response<E, C>, DownlinkError>;

pub trait DownlinkStateMachine<Event, Request> {
    type State;
    type Update;
    type Report;

    fn initialize(&self) -> (Self::State, Option<Command<Self::Update>>);

    fn handle_event(
        &self,
        state: &mut Self::State,
        event: Message<Event>,
    ) -> EventResult<Self::Report>;

    fn handle_request(
        &self,
        state: &mut Self::State,
        request: Request,
    ) -> ResponseResult<Self::Report, Self::Update>;
}

/// This trait is for simple, stateful downlinks that follow the standard synchronization model.
pub trait SyncStateMachine<Event, Request> {
    type State;
    type Command;
    type Report;

    /// The initial value of the state.
    fn init(&self) -> Self::State;

    /// Generate the initial event when the downlink enters the [`Synced`] state.
    fn on_sync(&self, state: &Self::State) -> Self::Report;

    /// Update the state with a message, received between [`Linked`] and [`Synced`'].
    fn handle_message_unsynced(
        &self,
        state: &mut Self::State,
        message: Event,
    ) -> Result<(), DownlinkError>;

    /// Update the state with a message when in the [`Synced`] state, potentially generating an
    /// event.
    fn handle_message(
        &self,
        state: &mut Self::State,
        message: Event,
    ) -> Result<Option<Self::Report>, DownlinkError>;

    fn apply_request(
        &self,
        state: &mut Self::State,
        req: Request,
    ) -> ResponseResult<Self::Report, Self::Command>;
}

impl<Basic, Event, Request> DownlinkStateMachine<Event, Request> for Basic
where
    Basic: SyncStateMachine<Event, Request>,
{
    type State = (DownlinkState, Basic::State);
    type Report = Basic::Report;
    type Update = Basic::Command;

    fn initialize(&self) -> (Self::State, Option<Command<Self::Update>>) {
        ((DownlinkState::Unlinked, self.init()), Some(Command::Sync))
    }

    fn handle_event(
        &self,
        state: &mut Self::State,
        event: Message<Event>,
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
            Message::BadEnvelope(_) => EventResult::fail(DownlinkError::MalformedMessage),
        }
    }

    fn handle_request(
        &self,
        state: &mut Self::State,
        request: Request,
    ) -> ResponseResult<Basic::Report, Basic::Command> {
        let (_, basic_state) = state;
        self.apply_request(basic_state, request)
    }
}
