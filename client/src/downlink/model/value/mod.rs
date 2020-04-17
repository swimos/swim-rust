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

use crate::downlink::buffered::{self, BufferedDownlink, BufferedReceiver};
use crate::downlink::dropping::{self, DroppingDownlink, DroppingReceiver};
use crate::downlink::queue::{self, QueueDownlink, QueueReceiver};
use crate::downlink::raw::RawDownlink;
use crate::downlink::{
    create_downlink, BasicResponse, BasicStateMachine, Command, Event, Message, TransitionError,
};
use crate::router::RoutingError;
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
pub fn create_raw_downlink<Updates, Commands>(
    init: Value,
    update_stream: Updates,
    cmd_sender: Commands,
    buffer_size: usize,
) -> RawDownlink<mpsc::Sender<Action>, mpsc::Receiver<Event<SharedValue>>>
where
    Updates: Stream<Item = Message<Value>> + Send + 'static,
    Commands: ItemSender<Command<SharedValue>, RoutingError> + Send + 'static,
{
    create_downlink(
        ValueModel::new(init),
        update_stream,
        cmd_sender,
        buffer_size,
    )
}

/// Create a value downlink with an queue based multiplexing topic.
pub fn create_queue_downlink<Updates, Commands>(
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
    Updates: Stream<Item = Message<Value>> + Send + 'static,
    Commands: ItemSender<Command<SharedValue>, RoutingError> + Send + 'static,
{
    queue::make_downlink(
        ValueModel::new(init),
        update_stream,
        cmd_sender,
        buffer_size,
        queue_size,
    )
}

/// Create a value downlink with a dropping multiplexing topic.
pub fn create_dropping_downlink<Updates, Commands>(
    init: Value,
    update_stream: Updates,
    cmd_sender: Commands,
    buffer_size: usize,
) -> (
    DroppingDownlink<Action, SharedValue>,
    DroppingReceiver<SharedValue>,
)
where
    Updates: Stream<Item = Message<Value>> + Send + 'static,
    Commands: ItemSender<Command<SharedValue>, RoutingError> + Send + 'static,
{
    dropping::make_downlink(
        ValueModel::new(init),
        update_stream,
        cmd_sender,
        buffer_size,
    )
}

/// Create a value downlink with an buffering multiplexing topic.
pub fn create_buffered_downlink<Updates, Commands>(
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
    Updates: Stream<Item = Message<Value>> + Send + 'static,
    Commands: ItemSender<Command<SharedValue>, RoutingError> + Send + 'static,
{
    buffered::make_downlink(
        ValueModel::new(init),
        update_stream,
        cmd_sender,
        buffer_size,
        queue_size,
    )
}

pub(in crate::downlink) struct ValueModel {
    state: SharedValue,
}

impl ValueModel {
    fn new(state: Value) -> Self {
        ValueModel {
            state: Arc::new(state),
        }
    }
}

impl BasicStateMachine<Value, Action> for ValueModel {
    type Ev = SharedValue;
    type Cmd = SharedValue;

    fn on_sync(&self) -> Self::Ev {
        self.state.clone()
    }

    fn handle_message_unsynced(&mut self, upd_value: Value) {
        self.state = Arc::new(upd_value);
    }

    fn handle_message(&mut self, upd_value: Value) -> Option<Self::Ev> {
        self.state = Arc::new(upd_value);
        Some(self.state.clone())
    }

    fn handle_action(&mut self, action: Action) -> BasicResponse<Self::Ev, Self::Cmd> {
        match action {
            Action::Get(resp) => match resp.send(self.state.clone()) {
                Err(_) => BasicResponse::none().with_error(TransitionError::ReceiverDropped),
                _ => BasicResponse::none(),
            },
            Action::Set(set_value, maybe_resp) => {
                self.state = Arc::new(set_value);
                let resp = BasicResponse::of(self.state.clone(), self.state.clone());
                match maybe_resp.and_then(|req| req.send(()).err()) {
                    Some(_) => resp.with_error(TransitionError::ReceiverDropped),
                    _ => resp,
                }
            }
            Action::Update(upd_fn, maybe_resp) => {
                self.state = Arc::new(upd_fn(self.state.as_ref()));
                let resp = BasicResponse::of(self.state.clone(), self.state.clone());
                match maybe_resp.and_then(|req| req.send(self.state.clone()).err()) {
                    None => resp,
                    Some(_) => resp.with_error(TransitionError::ReceiverDropped),
                }
            }
        }
    }
}
