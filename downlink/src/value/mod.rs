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

use super::*;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use futures::Stream;
use tokio::sync::mpsc;
use tokio::sync::watch;

use model::Value;
use request::Request;
use sink::item;
use std::fmt;

#[cfg(test)]
mod tests;

pub enum Action {
    Set(Value, Option<Request<()>>),
    Get(Request<Arc<Value>>),
    Update(
        Box<dyn FnOnce(&Value) -> Value + Send>,
        Option<Request<Arc<Value>>>,
    ),
}

impl Debug for Action {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Action::Set(v, r) => write!(f, "Set({:?}, {:?})", v, r.is_some()),
            Action::Get(_) => write!(f, "Get"),
            Action::Update(_, r) => write!(f, "Update(<closure>, {:?})", r.is_some()),
        }
    }
}

impl Action {
    pub fn set(val: Value) -> Action {
        Action::Set(val, None)
    }

    pub fn set_and_await(val: Value, request: Request<()>) -> Action {
        Action::Set(val, Some(request))
    }

    pub fn get(request: Request<Arc<Value>>) -> Action {
        Action::Get(request)
    }

    pub fn update<F>(f: F) -> Action
    where
        F: FnOnce(&Value) -> Value + Send + 'static,
    {
        Action::Update(Box::new(f), None)
    }

    pub fn update_box(f: Box<dyn FnOnce(&Value) -> Value + Send>) -> Action {
        Action::Update(f, None)
    }

    pub fn update_and_await<F>(f: F, request: Request<Arc<Value>>) -> Action
    where
        F: FnOnce(&Value) -> Value + Send + 'static,
    {
        Action::Update(Box::new(f), Some(request))
    }

    pub fn update_box_and_await(
        f: Box<dyn FnOnce(&Value) -> Value + Send>,
        request: Request<Arc<Value>>,
    ) -> Action {
        Action::Update(f, Some(request))
    }
}

/// Create a value downlink with back-pressure (it will only process set messages as rapidly
/// as it can write commands to the output).
pub fn create_back_pressure_downlink<Updates, Commands>(
    init: Value,
    update_stream: Updates,
    cmd_sender: mpsc::Sender<Command<Arc<Value>>>,
    buffer_size: usize,
) -> Downlink<mpsc::Sender<Action>, mpsc::Receiver<Event<Arc<Value>>>>
where
    Updates: Stream<Item = Message<Value>> + Send + 'static,
{
    let cmd_sink = item::for_mpsc_sender::<Command<Arc<Value>>, DownlinkError>(cmd_sender);
    super::create_downlink(Arc::new(init), update_stream, cmd_sink, buffer_size)
}

fn transform_err<T, Err: From<item::WatchErr<T>>>(
    result: Result<(), item::WatchErr<T>>,
) -> Result<(), Err> {
    result.map_err(|e| e.into())
}

/// Create a value downlink without back-pressure (it will process set operations as rapidly as it
/// can and some outgoing message will be dropped).
pub fn create_dropping_downlink<Updates, Commands>(
    init: Value,
    update_stream: Updates,
    cmd_sender: watch::Sender<Command<Arc<Value>>>,
    buffer_size: usize,
) -> Downlink<mpsc::Sender<Action>, mpsc::Receiver<Event<Arc<Value>>>>
where
    Updates: Stream<Item = Message<Value>> + Send + 'static,
{
    let err_trans = || transform_err::<Command<Arc<Value>>, DownlinkError>;
    let cmd_sink = item::map_err(cmd_sender, err_trans);
    super::create_downlink(Arc::new(init), update_stream, cmd_sink, buffer_size)
}

impl StateMachine<Value, Action> for Arc<Value> {
    type Ev = Arc<Value>;
    type Cmd = Arc<Value>;

    fn handle_operation(
        model: &mut Model<Self>,
        op: Operation<Value, Action>,
    ) -> Response<Self::Ev, Self::Cmd> {
        let Model { data_state, state } = model;
        match op {
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
                        Response::for_event(Event(data_state.clone(), false))
                    }
                }
                Message::Action(upd_value) => {
                    if *state != DownlinkState::Unlinked {
                        *data_state = Arc::new(upd_value);
                    }
                    if *state == DownlinkState::Synced {
                        Response::for_event(Event(data_state.clone(), false))
                    } else {
                        Response::none()
                    }
                }
                Message::Unlinked => {
                    *state = DownlinkState::Unlinked;
                    Response::none().then_terminate()
                }
            },
            Operation::Action(action) => match action {
                Action::Set(set_value, maybe_resp) => {
                    *data_state = Arc::new(set_value);
                    let resp = Response::of(
                        Event(data_state.clone(), true),
                        Command::Action(data_state.clone()),
                    );
                    match maybe_resp.and_then(|req| req.send(())) {
                        Some(_) => resp.with_error(TransitionError::ReceiverDropped),
                        _ => resp,
                    }
                }
                Action::Get(resp) => match resp.send(data_state.clone()) {
                    Some(_) => Response::none().with_error(TransitionError::ReceiverDropped),
                    _ => Response::none(),
                },
                Action::Update(upd_fn, maybe_resp) => {
                    *data_state = Arc::new(upd_fn(data_state.as_ref()));
                    let resp = Response::of(
                        Event(data_state.clone(), true),
                        Command::Action(data_state.clone()),
                    );
                    match maybe_resp.and_then(|req| req.send(data_state.clone())) {
                        Some(_) => resp.with_error(TransitionError::ReceiverDropped),
                        _ => resp,
                    }
                }
            },
            Operation::Close => Response::for_command(Command::Unlink).then_terminate(),
        }
    }
}
