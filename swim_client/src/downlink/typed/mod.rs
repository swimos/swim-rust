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

use crate::downlink::model::map::{MapAction, ViewWithEvent};
use crate::downlink::model::value::{Action, SharedValue, UpdateResult};
use crate::downlink::{error::DownlinkError, error::UpdateFailure, Event, RawDownlink};
use std::fmt::{Display, Formatter};
use swim_form::Form;
use swim_model::Value;
use swim_schema::ValueSchema;
use swim_utilities::sync::topic;
use tokio::sync::oneshot;

pub mod command;
pub mod event;
pub mod map;
pub mod value;

pub type UntypedValueDownlink = RawDownlink<Action, SharedValue>;
pub type UntypedValueReceiver = topic::Receiver<Event<SharedValue>>;
pub type UntypedMapDownlink = RawDownlink<MapAction, ViewWithEvent>;
pub type UntypedMapReceiver = topic::Receiver<Event<ViewWithEvent>>;
pub type UntypedCommandDownlink = RawDownlink<Value, ()>;
pub type UntypedEventDownlink = RawDownlink<(), Value>;
pub type UntypedEventReceiver = topic::Receiver<Event<Value>>;

#[derive(Debug, Clone)]
pub enum ViewMode {
    ReadOnly,
    WriteOnly,
}

impl Display for ViewMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ViewMode::ReadOnly => write!(f, "Read Only"),
            ViewMode::WriteOnly => write!(f, "Write Only"),
        }
    }
}

fn wrap_update_fn<T, F>(update_fn: F) -> impl FnOnce(&Value) -> UpdateResult<Value>
where
    T: Form,
    F: FnOnce(T) -> T,
{
    move |value: &Value| match Form::try_from_value(value) {
        Ok(t) => Ok(update_fn(t).into_value()),
        Err(e) => Err(UpdateFailure(e.to_string())),
    }
}

async fn await_fallible<T: Form + ValueSchema>(
    rx: oneshot::Receiver<Result<UpdateResult<SharedValue>, DownlinkError>>,
) -> Result<T, DownlinkError> {
    let value = rx
        .await
        .map_err(|_| DownlinkError::DroppedChannel)??
        .map_err(|_| DownlinkError::InvalidAction)?;
    Form::try_from_value(value.as_ref()).map_err(|_| {
        let schema = T::schema();
        DownlinkError::SchemaViolation((*value).clone(), schema)
    })
}

async fn await_value<T: Form + ValueSchema>(
    rx: oneshot::Receiver<Result<SharedValue, DownlinkError>>,
) -> Result<T, DownlinkError> {
    let value = rx.await.map_err(|_| DownlinkError::DroppedChannel)??;
    Form::try_from_value(value.as_ref()).map_err(|_| {
        let schema = T::schema();
        DownlinkError::SchemaViolation((*value).clone(), schema)
    })
}

async fn await_optional<T: Form + ValueSchema>(
    rx: oneshot::Receiver<Result<Option<SharedValue>, DownlinkError>>,
) -> Result<Option<T>, DownlinkError> {
    let maybe_value = rx.await.map_err(|_| DownlinkError::DroppedChannel)??;
    match maybe_value {
        Some(value) => Form::try_from_value(value.as_ref())
            .map_err(|_| {
                let schema = <T as ValueSchema>::schema();
                DownlinkError::SchemaViolation((*value).clone(), schema)
            })
            .map(Some),
        _ => Ok(None),
    }
}

async fn await_discard<T>(
    rx: oneshot::Receiver<Result<T, DownlinkError>>,
) -> Result<(), DownlinkError> {
    rx.await.map_err(|_| DownlinkError::DroppedChannel)??;
    Ok(())
}
