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

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use futures::Stream;
use tokio::sync::mpsc;

use crate::downlink::buffered::{BufferedDownlink, BufferedReceiver};
use crate::downlink::dropping::{DroppingDownlink, DroppingReceiver};
use crate::downlink::queue::{QueueDownlink, QueueReceiver};
use crate::downlink::raw::RawDownlink;
use crate::downlink::*;
use common::model::Value;
use common::request::Request;
use common::sink::item::ItemSender;
use std::fmt;

#[cfg(test)]
mod tests;

pub type SharedValue = Arc<Value>;

pub enum Action {
    Set(Value, Option<Request<()>>),
    Get(Request<SharedValue>),
    Update(
        Box<dyn FnOnce(&Value) -> Value + Send>,
        Option<Request<SharedValue>>,
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

    pub fn get(request: Request<SharedValue>) -> Action {
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

    pub fn update_and_await<F>(f: F, request: Request<SharedValue>) -> Action
    where
        F: FnOnce(&Value) -> Value + Send + 'static,
    {
        Action::Update(Box::new(f), Some(request))
    }

    pub fn update_box_and_await(
        f: Box<dyn FnOnce(&Value) -> Value + Send>,
        request: Request<SharedValue>,
    ) -> Action {
        Action::Update(f, Some(request))
    }
}

/// Create a raw value downlink.
pub fn create_raw_downlink<Err, Updates, Commands>(
    init: Value,
    update_stream: Updates,
    cmd_sender: Commands,
    buffer_size: usize,
) -> RawDownlink<mpsc::Sender<Action>, mpsc::Receiver<Event<SharedValue>>>
where
    Err: Into<DownlinkError> + Send + 'static,
    Updates: Stream<Item = Message<Value>> + Send + 'static,
    Commands: ItemSender<Command<SharedValue>, Err> + Send + 'static,
{
    create_downlink(Arc::new(init), update_stream, cmd_sender, buffer_size)
}

/// Create a value downlink with an queue based multiplexing topic.
pub fn create_queue_downlink<Err, Updates, Commands>(
    init: Value,
    update_stream: Updates,
    cmd_sender: Commands,
    buffer_size: usize,
    queue_size: usize,
) -> (
    QueueDownlink<Action, SharedValue>,
    QueueReceiver<SharedValue>,
)
where
    Err: Into<DownlinkError> + Send + 'static,
    Updates: Stream<Item = Message<Value>> + Send + 'static,
    Commands: ItemSender<Command<SharedValue>, Err> + Send + 'static,
{
    queue::make_downlink(
        Arc::new(init),
        update_stream,
        cmd_sender,
        buffer_size,
        queue_size,
    )
}

/// Create a value downlink with a dropping multiplexing topic.
pub fn create_dropping_downlink<Err, Updates, Commands>(
    init: Value,
    update_stream: Updates,
    cmd_sender: Commands,
    buffer_size: usize,
) -> (
    DroppingDownlink<Action, SharedValue>,
    DroppingReceiver<SharedValue>,
)
where
    Err: Into<DownlinkError> + Send + 'static,
    Updates: Stream<Item = Message<Value>> + Send + 'static,
    Commands: ItemSender<Command<SharedValue>, Err> + Send + 'static,
{
    dropping::make_downlink(Arc::new(init), update_stream, cmd_sender, buffer_size)
}

/// Create a value downlink with an buffering multiplexing topic.
pub fn create_buffered_downlink<Err, Updates, Commands>(
    init: Value,
    update_stream: Updates,
    cmd_sender: Commands,
    buffer_size: usize,
    queue_size: usize,
) -> (
    BufferedDownlink<Action, SharedValue>,
    BufferedReceiver<SharedValue>,
)
where
    Err: Into<DownlinkError> + Send + 'static,
    Updates: Stream<Item = Message<Value>> + Send + 'static,
    Commands: ItemSender<Command<SharedValue>, Err> + Send + 'static,
{
    buffered::make_downlink(
        Arc::new(init),
        update_stream,
        cmd_sender,
        buffer_size,
        queue_size,
    )
}

impl StateMachine<Value, Action> for SharedValue {
    type Ev = SharedValue;
    type Cmd = SharedValue;

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
