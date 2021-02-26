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

#[cfg(test)]
mod tests;

use crate::downlink::error::DownlinkError;
use crate::downlink::model::map::{MapAction, UntypedMapModification, ValMap, ViewWithEvent};
use crate::downlink::state_machine::{Response, ResponseResult, SyncStateMachine};
use crate::downlink::DownlinkRequest;
use std::sync::Arc;
use swim_common::model::schema::{Schema, StandardSchema};
use swim_common::model::Value;

pub struct MapStateMachine {
    key_schema: StandardSchema,
    value_schema: StandardSchema,
}

impl MapStateMachine {
    pub fn unvalidated() -> Self {
        MapStateMachine {
            key_schema: StandardSchema::Anything,
            value_schema: StandardSchema::Anything,
        }
    }

    pub fn new(key_schema: StandardSchema, value_schema: StandardSchema) -> Self {
        MapStateMachine {
            key_schema,
            value_schema,
        }
    }
}

impl SyncStateMachine<UntypedMapModification<Value>, MapAction> for MapStateMachine {
    type State = ValMap;
    type Command = UntypedMapModification<Value>;
    type Report = ViewWithEvent;

    fn init(&self) -> Self::State {
        ValMap::new()
    }

    fn on_sync(&self, state: &Self::State) -> Self::Report {
        ViewWithEvent::initial(&*state)
    }

    fn handle_message_unsynced(
        &self,
        state: &mut Self::State,
        message: UntypedMapModification<Value>,
    ) -> Result<(), DownlinkError> {
        match message {
            UntypedMapModification::Update(k, v) => {
                if self.key_schema.matches(&k) {
                    if self.value_schema.matches(&v) {
                        state.insert(k, v);
                        Ok(())
                    } else {
                        Err(DownlinkError::SchemaViolation(
                            (*v).clone(),
                            self.value_schema.clone(),
                        ))
                    }
                } else {
                    Err(DownlinkError::SchemaViolation(k, self.key_schema.clone()))
                }
            }
            UntypedMapModification::Remove(k) => {
                if self.key_schema.matches(&k) {
                    state.remove(&k);
                    Ok(())
                } else {
                    Err(DownlinkError::SchemaViolation(k, self.key_schema.clone()))
                }
            }
            UntypedMapModification::Take(n) => {
                *state = state.take(n);
                Ok(())
            }
            UntypedMapModification::Drop(n) => {
                *state = state.skip(n);
                Ok(())
            }
            UntypedMapModification::Clear => {
                state.clear();
                Ok(())
            }
        }
    }

    fn handle_message(
        &self,
        state: &mut Self::State,
        message: UntypedMapModification<Value>,
    ) -> Result<Option<Self::Report>, DownlinkError> {
        match message {
            UntypedMapModification::Update(k, v) => {
                if self.key_schema.matches(&k) {
                    if self.value_schema.matches(&v) {
                        state.insert(k.clone(), v);
                        Ok(Some(ViewWithEvent::update(state, k)))
                    } else {
                        Err(DownlinkError::SchemaViolation(
                            (*v).clone(),
                            self.value_schema.clone(),
                        ))
                    }
                } else {
                    Err(DownlinkError::SchemaViolation(k, self.key_schema.clone()))
                }
            }
            UntypedMapModification::Remove(k) => {
                if self.key_schema.matches(&k) {
                    state.remove(&k);
                    Ok(Some(ViewWithEvent::remove(state, k)))
                } else {
                    Err(DownlinkError::SchemaViolation(k, self.key_schema.clone()))
                }
            }
            UntypedMapModification::Take(n) => {
                *state = state.take(n);
                Ok(Some(ViewWithEvent::take(state, n)))
            }
            UntypedMapModification::Drop(n) => {
                *state = state.skip(n);
                Ok(Some(ViewWithEvent::skip(state, n)))
            }
            UntypedMapModification::Clear => {
                state.clear();
                Ok(Some(ViewWithEvent::clear(state)))
            }
        }
    }

    fn apply_request(
        &self,
        state: &mut Self::State,
        action: MapAction,
    ) -> ResponseResult<Self::Report, Self::Command> {
        process_action(&self.key_schema, &self.value_schema, state, action)
    }
}

fn update_and_notify<Upd>(
    data_state: &mut ValMap,
    update: Upd,
    request: Option<DownlinkRequest<ValMap>>,
) where
    Upd: FnOnce(&mut ValMap),
{
    match request {
        Some(req) => {
            let prev = data_state.clone();
            update(data_state);
            let _ = req.send_ok(prev);
        }
        _ => {
            update(data_state);
        }
    }
}

fn update_and_notify_prev<Upd>(
    data_state: &mut ValMap,
    key: &Value,
    update: Upd,
    request: Option<DownlinkRequest<Option<Arc<Value>>>>,
) where
    Upd: FnOnce(&mut ValMap),
{
    match request {
        Some(req) => {
            let prev = data_state.get(key).cloned();
            update(data_state);
            let _ = req.send_ok(prev);
        }
        _ => {
            update(data_state);
        }
    }
}

fn process_action(
    key_schema: &StandardSchema,
    val_schema: &StandardSchema,
    data_state: &mut ValMap,
    action: MapAction,
) -> ResponseResult<ViewWithEvent, UntypedMapModification<Value>> {
    match action {
        MapAction::Update { key, value, old } => {
            if !key_schema.matches(&key) {
                send_error(old, key, key_schema.clone());
                Ok(Response::default())
            } else if !val_schema.matches(&value) {
                send_error(old, value, val_schema.clone());
                Ok(Response::default())
            } else {
                let v_arc = Arc::new(value);
                update_and_notify_prev(
                    data_state,
                    &key,
                    |map| {
                        map.insert(key.clone(), v_arc.clone());
                    },
                    old,
                );
                let response = (
                    ViewWithEvent::update(data_state, key.clone()),
                    UntypedMapModification::Update(key, v_arc),
                );
                Ok(response.into())
            }
        }
        MapAction::Remove { key, old } => {
            if !key_schema.matches(&key) {
                send_error(old, key, key_schema.clone());
                Ok(Response::default())
            } else {
                let did_rem = if let Some(req) = old {
                    let prev = data_state.remove(&key);
                    let did_remove = prev.is_some();
                    let _ = req.send_ok(prev);
                    did_remove
                } else {
                    let old = data_state.remove(&key);
                    old.is_some()
                };
                if did_rem {
                    let response = (
                        ViewWithEvent::remove(data_state, key.clone()),
                        UntypedMapModification::Remove(key),
                    );
                    Ok(response.into())
                } else {
                    Ok(Response::default())
                }
            }
        }
        MapAction::Take { n, before, after } => {
            update_and_notify(
                data_state,
                |map| {
                    *map = map.take(n);
                },
                before,
            );
            if let Some(req) = after {
                let _ = req.send_ok(data_state.clone());
            }
            let response = (
                ViewWithEvent::take(data_state, n),
                UntypedMapModification::Take(n),
            );
            Ok(response.into())
        }
        MapAction::Skip { n, before, after } => {
            update_and_notify(
                data_state,
                |map| {
                    *map = map.skip(n);
                },
                before,
            );
            if let Some(req) = after {
                let _ = req.send_ok(data_state.clone());
            }
            let response = (
                ViewWithEvent::skip(data_state, n),
                UntypedMapModification::Drop(n),
            );
            Ok(response.into())
        }
        MapAction::Clear { before } => {
            if let Some(req) = before {
                let prev = std::mem::take(data_state);
                let _ = req.send_ok(prev);
            } else {
                data_state.clear();
            }
            let response = (
                ViewWithEvent::clear(data_state),
                UntypedMapModification::Clear,
            );
            Ok(response.into())
        }
        MapAction::Get { request } => {
            let _ = request.send_ok(data_state.clone());
            Ok(Response::default())
        }
        MapAction::GetByKey { key, request } => {
            let _ = request.send_ok(data_state.get(&key).cloned());
            Ok(Response::default())
        }
        MapAction::Modify {
            key,
            f,
            before,
            after,
        } => {
            if !key_schema.matches(&key) {
                modify_key_schema_errors(key_schema, key, before, after);
                Ok(Response::default())
            } else {
                let prev = get_and_deref(data_state, &key);
                let maybe_new_val = f(&prev);
                match maybe_new_val {
                    Some(v) if !val_schema.matches(&v) => {
                        send_error(before, v.clone(), val_schema.clone());
                        send_error(after, v, val_schema.clone());
                        Ok(Response::default())
                    }
                    validated => {
                        let had_existing = prev.is_some();
                        handle_modify(
                            data_state,
                            key,
                            had_existing,
                            validated,
                            before,
                            after,
                            |v| v,
                        )
                    }
                }
            }
        }
        MapAction::TryModify {
            key,
            f,
            before,
            after,
        } => {
            if !key_schema.matches(&key) {
                modify_key_schema_errors(key_schema, key, before, after);
                Ok(Response::default())
            } else {
                let prev = get_and_deref(data_state, &key);
                match f(&prev) {
                    Ok(maybe_new_val) => match maybe_new_val {
                        Some(v) if !val_schema.matches(&v) => {
                            send_error(before, v.clone(), val_schema.clone());
                            send_error(after, v, val_schema.clone());
                            Ok(Response::default())
                        }
                        validated => {
                            let had_existing = prev.is_some();
                            handle_modify(
                                data_state,
                                key,
                                had_existing,
                                validated,
                                before,
                                after,
                                Ok,
                            )
                        }
                    },
                    Err(e) => {
                        match (before, after) {
                            (Some(r1), Some(r2)) => {
                                let _ = r1.send_ok(Err(e.clone()));
                                let _ = r2.send_ok(Err(e));
                            }
                            (Some(r), _) => {
                                let _ = r.send_ok(Err(e));
                            }
                            (_, Some(r)) => {
                                let _ = r.send_ok(Err(e));
                            }
                            _ => {}
                        }
                        Ok(Response::default())
                    }
                }
            }
        }
    }
}

fn handle_modify<F, T>(
    data_state: &mut ValMap,
    key: Value,
    had_existing: bool,
    maybe_new_val: Option<Value>,
    old: Option<DownlinkRequest<T>>,
    replacement: Option<DownlinkRequest<T>>,
    to_event: F,
) -> ResponseResult<ViewWithEvent, UntypedMapModification<Value>>
where
    F: Fn(Option<Arc<Value>>) -> T + Send + 'static,
{
    match maybe_new_val {
        Some(new_val) => {
            let v_arc = Arc::new(new_val);
            let replaced = data_state.insert(key.clone(), v_arc.clone());
            if let Some(req) = old {
                let _ = req.send_ok(to_event(replaced));
            }
            if let Some(req) = replacement {
                let _ = req.send_ok(to_event(Some(v_arc.clone())));
            }
            let response = (
                ViewWithEvent::update(data_state, key.clone()),
                UntypedMapModification::Update(key, v_arc),
            );
            Ok(response.into())
        }
        _ if had_existing => {
            let removed = data_state.remove(&key);
            if let Some(req) = old {
                let _ = req.send_ok(to_event(removed));
            }
            if let Some(req) = replacement {
                let _ = req.send_ok(to_event(None));
            }
            let response = (
                ViewWithEvent::remove(data_state, key.clone()),
                UntypedMapModification::Remove(key),
            );
            Ok(response.into())
        }
        _ => Ok(Response::default()),
    }
}

fn send_error<T>(maybe_resp: Option<DownlinkRequest<T>>, value: Value, schema: StandardSchema) {
    if let Some(req) = maybe_resp {
        let err = DownlinkError::SchemaViolation(value, schema);
        let _ = req.send_err(err);
    }
}

fn modify_key_schema_errors<T>(
    key_schema: &StandardSchema,
    key: Value,
    before: Option<DownlinkRequest<T>>,
    after: Option<DownlinkRequest<T>>,
) {
    let _ = send_error(before, key.clone(), key_schema.clone());
    let _ = send_error(after, key, key_schema.clone());
}

fn get_and_deref<'a>(data_state: &'a ValMap, key: &Value) -> Option<&'a Value> {
    data_state.get(key).map(|arc| arc.as_ref())
}
