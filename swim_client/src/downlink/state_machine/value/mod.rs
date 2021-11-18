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

#[cfg(test)]
mod tests;

use crate::downlink::error::DownlinkError;
use crate::downlink::model::value::{Action, SharedValue, UpdateResult};
use crate::downlink::state_machine::{Response, ResponseResult, SyncStateMachine};
use crate::downlink::DownlinkRequest;
use swim_model::Value;
use swim_schema::schema::Schema;
use swim_schema::schema::StandardSchema;

/// State machine for value downlinks.
pub struct ValueStateMachine {
    init: Value,
    schema: StandardSchema,
}

impl ValueStateMachine {
    pub fn new(init: Value, schema: StandardSchema) -> Self {
        assert!(
            schema.matches(&init),
            "Initial value does not satisfy the schema."
        );
        ValueStateMachine { init, schema }
    }
}

impl SyncStateMachine<Value, Action> for ValueStateMachine {
    type State = SharedValue;
    type WarpCmd = SharedValue;
    type Report = SharedValue;

    fn init(&self) -> Self::State {
        SharedValue::new(self.init.clone())
    }

    fn on_sync(&self, state: &Self::State) -> Self::Report {
        state.clone()
    }

    fn handle_message_unsynced(
        &self,
        state: &mut Self::State,
        message: Value,
    ) -> Result<(), DownlinkError> {
        if self.schema.matches(&message) {
            *state = SharedValue::new(message);
            Ok(())
        } else if message == Value::Extant {
            Ok(())
        } else {
            Err(DownlinkError::SchemaViolation(message, self.schema.clone()))
        }
    }

    fn handle_message(
        &self,
        state: &mut Self::State,
        message: Value,
    ) -> Result<Option<Self::Report>, DownlinkError> {
        if self.schema.matches(&message) {
            *state = SharedValue::new(message);
            Ok(Some(state.clone()))
        } else {
            Err(DownlinkError::SchemaViolation(message, self.schema.clone()))
        }
    }

    fn apply_action_request(
        &self,
        state: &mut Self::State,
        action: Action,
    ) -> ResponseResult<SharedValue, SharedValue> {
        match action {
            Action::Get(resp) => {
                let _ = resp.send_ok(state.clone());
                Ok(Response::default())
            }
            Action::Set(set_value, maybe_resp) => Ok(apply_set(
                state,
                &self.schema,
                set_value,
                maybe_resp,
                |_| (),
            )),
            Action::Update(upd_fn, maybe_resp) => {
                let new_value = upd_fn(&**state);
                Ok(apply_set(state, &self.schema, new_value, maybe_resp, |s| {
                    s.clone()
                }))
            }
            Action::TryUpdate(upd_fn, maybe_resp) => Ok(try_apply_set(
                state,
                &self.schema,
                upd_fn(&**state),
                maybe_resp,
            )),
        }
    }
}

fn apply_set<F, T>(
    state: &mut SharedValue,
    schema: &StandardSchema,
    set_value: Value,
    maybe_resp: Option<DownlinkRequest<T>>,
    to_output: F,
) -> Response<SharedValue, SharedValue>
where
    F: FnOnce(&SharedValue) -> T,
{
    if schema.matches(&set_value) {
        let with_old = maybe_resp.map(|req| (req, to_output(state)));
        *state = SharedValue::new(set_value);
        if let Some((req, old)) = with_old {
            let _ = req.send_ok(old);
        }
        ((*state).clone(), (*state).clone()).into()
    } else {
        if let Some(req) = maybe_resp {
            let _ = req.send_err(DownlinkError::SchemaViolation(set_value, schema.clone()));
        }
        Response::default()
    }
}

fn try_apply_set(
    state: &mut SharedValue,
    schema: &StandardSchema,
    maybe_set_value: UpdateResult<Value>,
    maybe_resp: Option<DownlinkRequest<UpdateResult<SharedValue>>>,
) -> Response<SharedValue, SharedValue> {
    match maybe_set_value {
        Ok(set_value) => {
            if schema.matches(&set_value) {
                let with_old = maybe_resp.map(|req| (req, state.clone()));
                *state = SharedValue::new(set_value);
                if let Some((req, old)) = with_old {
                    let _ = req.send_ok(Ok(old));
                }
                ((*state).clone(), (*state).clone()).into()
            } else {
                if let Some(req) = maybe_resp {
                    let _ = req.send_err(DownlinkError::SchemaViolation(set_value, schema.clone()));
                }
                Response::default()
            }
        }
        Err(err) => {
            if let Some(req) = maybe_resp {
                let _ = req.send_ok(Err(err));
            }
            Response::default()
        }
    }
}
