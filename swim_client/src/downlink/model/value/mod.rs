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

use crate::downlink::{error::UpdateFailure, DownlinkRequest, Message};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use swim_model::Value;
use swim_runtime::error::RoutingError;

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

/// Typedef for value downlink stream item.
pub type ValueItemResult = Result<Message<Value>, RoutingError>;
