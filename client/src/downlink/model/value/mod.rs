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

use crate::configuration::downlink::OnInvalidMessage;
use crate::downlink::buffered::{self, BufferedDownlink, BufferedReceiver};
use crate::downlink::dropping::{self, DroppingDownlink, DroppingReceiver};
use crate::downlink::queue::{self, QueueDownlink, QueueReceiver};
use crate::downlink::raw::RawDownlink;
use crate::downlink::{
    create_downlink, BasicResponse, BasicStateMachine, Command, DownlinkError, DownlinkRequest,
    Event, Message, TransitionError, UpdateFailure,
};
use crate::router::RoutingError;
use common::model::schema::{Schema, StandardSchema};
use common::model::Value;
use common::sink::item::ItemSender;
use either::Either;
use std::fmt;

#[cfg(test)]
mod tests;

pub type SharedValue = Arc<Value>;

pub type UpdateResult<T> = Result<T, UpdateFailure>;

pub enum Action {
    Set(Value, Option<DownlinkRequest<()>>),
    Get(DownlinkRequest<SharedValue>),
    Update(
        Box<dyn FnOnce(&Value) -> Value + Send>,
        Option<DownlinkRequest<SharedValue>>,
    ),
    TryUpdate(
        Box<dyn FnOnce(&Value) -> UpdateResult<Value> + Send>,
        Option<DownlinkRequest<UpdateResult<SharedValue>>>,
    ),
}

impl Debug for Action {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Action::Set(v, r) => write!(f, "Set({:?}, {:?})", v, r.is_some()),
            Action::Get(_) => write!(f, "Get"),
            Action::Update(_, r) => write!(f, "Update(<closure>, {:?})", r.is_some()),
            Action::TryUpdate(_, r) => write!(f, "TryUpdate(<closure>, {:?})", r.is_some()),
        }
    }
}

impl Action {
    pub fn set(val: Value) -> Action {
        Action::Set(val, None)
    }

    pub fn set_and_await(val: Value, request: DownlinkRequest<()>) -> Action {
        Action::Set(val, Some(request))
    }

    pub fn get(request: DownlinkRequest<SharedValue>) -> Action {
        Action::Get(request)
    }

    pub fn update<F>(f: F) -> Action
    where
        F: FnOnce(&Value) -> Value + Send + 'static,
    {
        Action::Update(Box::new(f), None)
    }

    pub fn try_update<F>(f: F) -> Action
    where
        F: FnOnce(&Value) -> UpdateResult<Value> + Send + 'static,
    {
        Action::TryUpdate(Box::new(f), None)
    }

    pub fn update_box(f: Box<dyn FnOnce(&Value) -> Value + Send>) -> Action {
        Action::Update(f, None)
    }

    pub fn update_and_await<F>(f: F, request: DownlinkRequest<SharedValue>) -> Action
    where
        F: FnOnce(&Value) -> Value + Send + 'static,
    {
        Action::Update(Box::new(f), Some(request))
    }

    pub fn try_update_and_await<F>(
        f: F,
        request: DownlinkRequest<UpdateResult<SharedValue>>,
    ) -> Action
    where
        F: FnOnce(&Value) -> UpdateResult<Value> + Send + 'static,
    {
        Action::TryUpdate(Box::new(f), Some(request))
    }

    pub fn update_box_and_await(
        f: Box<dyn FnOnce(&Value) -> Value + Send>,
        request: DownlinkRequest<SharedValue>,
    ) -> Action {
        Action::Update(f, Some(request))
    }
}

/// Create a raw value downlink.
pub fn create_raw_downlink<Updates, Commands>(
    init: Value,
    schema: Option<StandardSchema>,
    update_stream: Updates,
    cmd_sender: Commands,
    buffer_size: usize,
    on_invalid: OnInvalidMessage,
) -> RawDownlink<mpsc::Sender<Action>, mpsc::Receiver<Event<SharedValue>>>
where
    Updates: Stream<Item = Either<Message<Value>, RoutingError>> + Send + 'static,
    Commands: ItemSender<Command<SharedValue>, RoutingError> + Send + 'static,
{
    create_downlink(
        ValueStateMachine::new(init, schema.unwrap_or(StandardSchema::Anything)),
        update_stream,
        cmd_sender,
        buffer_size,
        on_invalid,
    )
}

/// Create a value downlink with an queue based multiplexing topic.
pub fn create_queue_downlink<Updates, Commands>(
    init: Value,
    schema: Option<StandardSchema>,
    update_stream: Updates,
    cmd_sender: Commands,
    buffer_size: usize,
    queue_size: usize,
    on_invalid: OnInvalidMessage,
) -> (
    QueueDownlink<Action, SharedValue>,
    QueueReceiver<SharedValue>,
)
where
    Updates: Stream<Item = Either<Message<Value>, RoutingError>> + Send + 'static,
    Commands: ItemSender<Command<SharedValue>, RoutingError> + Send + 'static,
{
    queue::make_downlink(
        ValueStateMachine::new(init, schema.unwrap_or(StandardSchema::Anything)),
        update_stream,
        cmd_sender,
        buffer_size,
        queue_size,
        on_invalid,
    )
}

/// Create a value downlink with a dropping multiplexing topic.
pub fn create_dropping_downlink<Updates, Commands>(
    init: Value,
    schema: Option<StandardSchema>,
    update_stream: Updates,
    cmd_sender: Commands,
    buffer_size: usize,
    on_invalid: OnInvalidMessage,
) -> (
    DroppingDownlink<Action, SharedValue>,
    DroppingReceiver<SharedValue>,
)
where
    Updates: Stream<Item = Either<Message<Value>, RoutingError>> + Send + 'static,
    Commands: ItemSender<Command<SharedValue>, RoutingError> + Send + 'static,
{
    dropping::make_downlink(
        ValueStateMachine::new(init, schema.unwrap_or(StandardSchema::Anything)),
        update_stream,
        cmd_sender,
        buffer_size,
        on_invalid,
    )
}

/// Create a value downlink with an buffering multiplexing topic.
pub fn create_buffered_downlink<Updates, Commands>(
    init: Value,
    schema: Option<StandardSchema>,
    update_stream: Updates,
    cmd_sender: Commands,
    buffer_size: usize,
    queue_size: usize,
    on_invalid: OnInvalidMessage,
) -> (
    BufferedDownlink<Action, SharedValue>,
    BufferedReceiver<SharedValue>,
)
where
    Updates: Stream<Item = Either<Message<Value>, RoutingError>> + Send + 'static,
    Commands: ItemSender<Command<SharedValue>, RoutingError> + Send + 'static,
{
    buffered::make_downlink(
        ValueStateMachine::new(init, schema.unwrap_or(StandardSchema::Anything)),
        update_stream,
        cmd_sender,
        buffer_size,
        queue_size,
        on_invalid,
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

pub struct ValueStateMachine {
    init: Value,
    schema: StandardSchema,
}

impl ValueStateMachine {
    pub fn unvalidated(init: Value) -> Self {
        ValueStateMachine::new(init, StandardSchema::Anything)
    }

    pub fn new(init: Value, schema: StandardSchema) -> Self {
        if !schema.matches(&init) {
            panic!("Initial value {} inconsistent with schema {}", init, schema)
        }
        ValueStateMachine { init, schema }
    }
}

impl BasicStateMachine<ValueModel, Value, Action> for ValueStateMachine {
    type Ev = SharedValue;
    type Cmd = SharedValue;

    fn init(&self) -> ValueModel {
        ValueModel::new(self.init.clone())
    }

    fn on_sync(&self, state: &ValueModel) -> Self::Ev {
        state.state.clone()
    }

    fn handle_message_unsynced(
        &self,
        state: &mut ValueModel,
        upd_value: Value,
    ) -> Result<(), DownlinkError> {
        if self.schema.matches(&upd_value) {
            state.state = Arc::new(upd_value);
            Ok(())
        } else {
            Err(DownlinkError::SchemaViolation(
                upd_value,
                self.schema.clone(),
            ))
        }
    }

    fn handle_message(
        &self,
        state: &mut ValueModel,
        upd_value: Value,
    ) -> Result<Option<Self::Ev>, DownlinkError> {
        if self.schema.matches(&upd_value) {
            state.state = Arc::new(upd_value);
            Ok(Some(state.state.clone()))
        } else {
            Err(DownlinkError::SchemaViolation(
                upd_value,
                self.schema.clone(),
            ))
        }
    }

    fn handle_action(
        &self,
        state: &mut ValueModel,
        action: Action,
    ) -> BasicResponse<Self::Ev, Self::Cmd> {
        match action {
            Action::Get(resp) => match resp.send_ok(state.state.clone()) {
                Err(_) => BasicResponse::none().with_error(TransitionError::ReceiverDropped),
                _ => BasicResponse::none(),
            },
            Action::Set(set_value, maybe_resp) => {
                apply_set(state, &self.schema, set_value, maybe_resp, |_| ())
            }
            Action::Update(upd_fn, maybe_resp) => {
                let new_value = upd_fn(state.state.as_ref());
                apply_set(state, &self.schema, new_value, maybe_resp, |s| s.clone())
            }
            Action::TryUpdate(upd_fn, maybe_resp) => try_apply_set(
                state,
                &self.schema,
                upd_fn(state.state.as_ref()),
                maybe_resp,
            ),
        }
    }
}

fn apply_set<F, T>(
    state: &mut ValueModel,
    schema: &StandardSchema,
    set_value: Value,
    maybe_resp: Option<DownlinkRequest<T>>,
    to_output: F,
) -> BasicResponse<SharedValue, SharedValue>
where
    F: FnOnce(&SharedValue) -> T,
{
    if schema.matches(&set_value) {
        let with_old = maybe_resp.map(|req| (req, to_output(&state.state)));
        state.state = Arc::new(set_value);
        let resp = BasicResponse::of(state.state.clone(), state.state.clone());
        match with_old.and_then(|(req, old)| req.send_ok(old).err()) {
            Some(_) => resp.with_error(TransitionError::ReceiverDropped),
            _ => resp,
        }
    } else {
        send_error(maybe_resp, set_value, schema.clone())
    }
}

fn try_apply_set(
    state: &mut ValueModel,
    schema: &StandardSchema,
    maybe_set_value: UpdateResult<Value>,
    maybe_resp: Option<DownlinkRequest<UpdateResult<SharedValue>>>,
) -> BasicResponse<SharedValue, SharedValue> {
    match maybe_set_value {
        Ok(set_value) => {
            if schema.matches(&set_value) {
                let with_old = maybe_resp.map(|req| (req, state.state.clone()));
                state.state = Arc::new(set_value);
                let resp = BasicResponse::of(state.state.clone(), state.state.clone());
                match with_old.and_then(|(req, old)| req.send_ok(Ok(old)).err()) {
                    Some(_) => resp.with_error(TransitionError::ReceiverDropped),
                    _ => resp,
                }
            } else {
                send_error(maybe_resp, set_value, schema.clone())
            }
        }
        Err(err) => {
            let resp = BasicResponse::none();
            match maybe_resp.and_then(|req| req.send_ok(Err(err)).err()) {
                Some(_) => resp.with_error(TransitionError::ReceiverDropped),
                _ => resp,
            }
        }
    }
}

fn send_error<T, Ev, Cmd>(
    maybe_resp: Option<DownlinkRequest<T>>,
    set_value: Value,
    schema: StandardSchema,
) -> BasicResponse<Ev, Cmd> {
    let resp = BasicResponse::none();
    let err = DownlinkError::SchemaViolation(set_value, schema);
    match maybe_resp.and_then(|req| req.send_err(err).err()) {
        Some(_) => resp.with_error(TransitionError::ReceiverDropped),
        _ => resp,
    }
}
