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
use im::ordmap::OrdMap;
use tokio::sync::mpsc;

use swim_common::model::schema::{Schema, StandardSchema};
use swim_common::model::Value;
use swim_common::sink::item::ItemSender;

use crate::configuration::downlink::{DownlinkParams, OnInvalidMessage};
use crate::downlink::buffered::{self, BufferedDownlink, BufferedReceiver};
use crate::downlink::dropping::{self, DroppingDownlink, DroppingReceiver};
use crate::downlink::model::value::UpdateResult;
use crate::downlink::queue::{self, QueueDownlink, QueueReceiver};
use crate::downlink::raw::RawDownlink;
use crate::downlink::{
    BasicResponse, Command, DownlinkError, DownlinkRequest, Event, Message, SyncStateMachine,
    TransitionError,
};
use std::num::NonZeroUsize;
use swim_common::routing::RoutingError;
use swim_warp::model::map::MapUpdate;

#[cfg(test)]
mod tests;

pub type MapModification<K, V> = MapUpdate<K, V>;

pub type UntypedMapModification<V> = MapModification<Value, V>;

pub enum MapAction {
    Update {
        key: Value,
        value: Value,
        old: Option<DownlinkRequest<Option<Arc<Value>>>>,
    },
    Remove {
        key: Value,
        old: Option<DownlinkRequest<Option<Arc<Value>>>>,
    },
    Take {
        n: usize,
        before: Option<DownlinkRequest<ValMap>>,
        after: Option<DownlinkRequest<ValMap>>,
    },
    Skip {
        n: usize,
        before: Option<DownlinkRequest<ValMap>>,
        after: Option<DownlinkRequest<ValMap>>,
    },
    Clear {
        before: Option<DownlinkRequest<ValMap>>,
    },
    Get {
        request: DownlinkRequest<ValMap>,
    },
    GetByKey {
        key: Value,
        request: DownlinkRequest<Option<Arc<Value>>>,
    },
    Modify {
        key: Value,
        f: Box<dyn FnOnce(&Option<&Value>) -> Option<Value> + Send>,
        before: Option<DownlinkRequest<Option<Arc<Value>>>>,
        after: Option<DownlinkRequest<Option<Arc<Value>>>>,
    },
    TryModify {
        key: Value,
        f: Box<dyn FnOnce(&Option<&Value>) -> UpdateResult<Option<Value>> + Send>,
        before: Option<DownlinkRequest<UpdateResult<Option<Arc<Value>>>>>,
        after: Option<DownlinkRequest<UpdateResult<Option<Arc<Value>>>>>,
    },
}

impl MapAction {
    pub fn update(key: Value, value: Value) -> MapAction {
        MapAction::Update {
            key,
            value,
            old: None,
        }
    }

    pub fn update_and_await(
        key: Value,
        value: Value,
        request: DownlinkRequest<Option<Arc<Value>>>,
    ) -> MapAction {
        MapAction::Update {
            key,
            value,
            old: Some(request),
        }
    }

    pub fn remove(key: Value) -> MapAction {
        MapAction::Remove { key, old: None }
    }

    pub fn remove_and_await(key: Value, request: DownlinkRequest<Option<Arc<Value>>>) -> MapAction {
        MapAction::Remove {
            key,
            old: Some(request),
        }
    }

    pub fn take(n: usize) -> MapAction {
        MapAction::Take {
            n,
            before: None,
            after: None,
        }
    }

    pub fn take_and_await(
        n: usize,
        map_before: DownlinkRequest<ValMap>,
        map_after: DownlinkRequest<ValMap>,
    ) -> MapAction {
        MapAction::Take {
            n,
            before: Some(map_before),
            after: Some(map_after),
        }
    }

    pub fn skip(n: usize) -> MapAction {
        MapAction::Skip {
            n,
            before: None,
            after: None,
        }
    }

    pub fn skip_and_await(
        n: usize,
        map_before: DownlinkRequest<ValMap>,
        map_after: DownlinkRequest<ValMap>,
    ) -> MapAction {
        MapAction::Skip {
            n,
            before: Some(map_before),
            after: Some(map_after),
        }
    }

    pub fn clear() -> MapAction {
        MapAction::Clear { before: None }
    }

    pub fn clear_and_await(map_before: DownlinkRequest<ValMap>) -> MapAction {
        MapAction::Clear {
            before: Some(map_before),
        }
    }

    pub fn get_map(request: DownlinkRequest<ValMap>) -> MapAction {
        MapAction::Get { request }
    }

    pub fn get(key: Value, request: DownlinkRequest<Option<Arc<Value>>>) -> MapAction {
        MapAction::GetByKey { key, request }
    }

    pub fn modify<F>(key: Value, f: F) -> MapAction
    where
        F: FnOnce(&Option<&Value>) -> Option<Value> + Send + 'static,
    {
        MapAction::Modify {
            key,
            f: Box::new(f),
            before: None,
            after: None,
        }
    }

    pub fn try_modify<F>(key: Value, f: F) -> MapAction
    where
        F: FnOnce(&Option<&Value>) -> UpdateResult<Option<Value>> + Send + 'static,
    {
        MapAction::TryModify {
            key,
            f: Box::new(f),
            before: None,
            after: None,
        }
    }

    pub fn modify_box(
        key: Value,
        f: Box<dyn FnOnce(&Option<&Value>) -> Option<Value> + Send>,
    ) -> MapAction {
        MapAction::Modify {
            key,
            f: Box::new(f),
            before: None,
            after: None,
        }
    }

    pub fn modify_and_await<F>(
        key: Value,
        f: F,
        val_before: DownlinkRequest<Option<Arc<Value>>>,
        val_after: DownlinkRequest<Option<Arc<Value>>>,
    ) -> MapAction
    where
        F: FnOnce(&Option<&Value>) -> Option<Value> + Send + 'static,
    {
        MapAction::Modify {
            key,
            f: Box::new(f),
            before: Some(val_before),
            after: Some(val_after),
        }
    }

    pub fn try_modify_and_await<F>(
        key: Value,
        f: F,
        val_before: DownlinkRequest<UpdateResult<Option<Arc<Value>>>>,
        val_after: DownlinkRequest<UpdateResult<Option<Arc<Value>>>>,
    ) -> MapAction
    where
        F: FnOnce(&Option<&Value>) -> UpdateResult<Option<Value>> + Send + 'static,
    {
        MapAction::TryModify {
            key,
            f: Box::new(f),
            before: Some(val_before),
            after: Some(val_after),
        }
    }

    pub fn modify_box_and_await(
        key: Value,
        f: Box<dyn FnOnce(&Option<&Value>) -> Option<Value> + Send>,
        val_before: DownlinkRequest<Option<Arc<Value>>>,
        val_after: DownlinkRequest<Option<Arc<Value>>>,
    ) -> MapAction {
        MapAction::Modify {
            key,
            f: Box::new(f),
            before: Some(val_before),
            after: Some(val_after),
        }
    }
}

impl Debug for MapAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MapAction::Update { key, value, old } => {
                write!(f, "Update({:?} => {:?}, {:?})", key, value, old)
            }
            MapAction::Remove { key, old } => write!(f, "Remove({:?}, {:?})", key, old),
            MapAction::Take { n, before, after } => {
                write!(f, "Take({:?}, {:?}, {:?})", n, before, after)
            }
            MapAction::Skip { n, before, after } => {
                write!(f, "Skip({:?}, {:?}, {:?})", n, before, after)
            }
            MapAction::Clear { before } => write!(f, "Clear({:?})", before),
            MapAction::Get { request } => write!(f, "Get({:?})", request),
            MapAction::GetByKey { key, request } => write!(f, "GetByKey({:?}, {:?})", key, request),
            MapAction::Modify {
                key, before, after, ..
            } => write!(f, "Modify({:?}, <closure>, {:?}, {:?})", key, before, after),
            MapAction::TryModify {
                key, before, after, ..
            } => write!(
                f,
                "TryModify({:?}, <closure>, {:?}, {:?})",
                key, before, after
            ),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MapEvent<K> {
    Initial,
    Update(K),
    Remove(K),
    Take(usize),
    Skip(usize),
    Clear,
}

pub type ValMap = OrdMap<Value, Arc<Value>>;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ViewWithEvent {
    pub view: ValMap,
    pub event: MapEvent<Value>,
}

impl ViewWithEvent {
    fn initial(map: &ValMap) -> ViewWithEvent {
        ViewWithEvent {
            view: map.clone(),
            event: MapEvent::Initial,
        }
    }

    fn update(map: &ValMap, key: Value) -> ViewWithEvent {
        ViewWithEvent {
            view: map.clone(),
            event: MapEvent::Update(key),
        }
    }

    fn remove(map: &ValMap, key: Value) -> ViewWithEvent {
        ViewWithEvent {
            view: map.clone(),
            event: MapEvent::Remove(key),
        }
    }

    fn take(map: &ValMap, n: usize) -> ViewWithEvent {
        ViewWithEvent {
            view: map.clone(),
            event: MapEvent::Take(n),
        }
    }

    fn skip(map: &ValMap, n: usize) -> ViewWithEvent {
        ViewWithEvent {
            view: map.clone(),
            event: MapEvent::Skip(n),
        }
    }

    fn clear(map: &ValMap) -> ViewWithEvent {
        ViewWithEvent {
            view: map.clone(),
            event: MapEvent::Clear,
        }
    }
}

/// Typedef for map downlink stream item.
type MapItemResult = Result<Message<UntypedMapModification<Value>>, RoutingError>;

/// Create a map downlink.
pub fn create_raw_downlink<Updates, Commands>(
    key_schema: Option<StandardSchema>,
    value_schema: Option<StandardSchema>,
    update_stream: Updates,
    cmd_sink: Commands,
    buffer_size: NonZeroUsize,
    yield_after: NonZeroUsize,
    on_invalid: OnInvalidMessage,
) -> RawDownlink<MapAction, mpsc::Receiver<Event<ViewWithEvent>>>
where
    Updates: Stream<Item = MapItemResult> + Send + 'static,
    Commands:
        ItemSender<Command<UntypedMapModification<Value>>, RoutingError> + Send + 'static,
{
    crate::downlink::create_downlink(
        MapStateMachine::new(
            key_schema.unwrap_or(StandardSchema::Anything),
            value_schema.unwrap_or(StandardSchema::Anything),
        ),
        update_stream,
        cmd_sink,
        buffer_size,
        yield_after,
        on_invalid,
    )
}

/// Create a map downlink with an queue based multiplexing topic.
pub fn create_queue_downlink<Updates, Commands>(
    key_schema: Option<StandardSchema>,
    value_schema: Option<StandardSchema>,
    update_stream: Updates,
    cmd_sink: Commands,
    queue_size: NonZeroUsize,
    config: &DownlinkParams,
) -> (
    QueueDownlink<MapAction, ViewWithEvent>,
    QueueReceiver<ViewWithEvent>,
)
where
    Updates: Stream<Item = MapItemResult> + Send + 'static,
    Commands:
        ItemSender<Command<UntypedMapModification<Value>>, RoutingError> + Send + 'static,
{
    queue::make_downlink(
        MapStateMachine::new(
            key_schema.unwrap_or(StandardSchema::Anything),
            value_schema.unwrap_or(StandardSchema::Anything),
        ),
        update_stream,
        cmd_sink,
        queue_size,
        &config,
    )
}

/// Create a value downlink with an dropping multiplexing topic.
pub fn create_dropping_downlink<Updates, Commands>(
    key_schema: Option<StandardSchema>,
    value_schema: Option<StandardSchema>,
    update_stream: Updates,
    cmd_sink: Commands,
    config: &DownlinkParams,
) -> (
    DroppingDownlink<MapAction, ViewWithEvent>,
    DroppingReceiver<ViewWithEvent>,
)
where
    Updates: Stream<Item = MapItemResult> + Send + 'static,
    Commands:
        ItemSender<Command<UntypedMapModification<Value>>, RoutingError> + Send + 'static,
{
    dropping::make_downlink(
        MapStateMachine::new(
            key_schema.unwrap_or(StandardSchema::Anything),
            value_schema.unwrap_or(StandardSchema::Anything),
        ),
        update_stream,
        cmd_sink,
        &config,
    )
}

/// Create a value downlink with an buffered multiplexing topic.
pub fn create_buffered_downlink<Updates, Commands>(
    key_schema: Option<StandardSchema>,
    value_schema: Option<StandardSchema>,
    update_stream: Updates,
    cmd_sink: Commands,
    queue_size: NonZeroUsize,
    config: &DownlinkParams,
) -> (
    BufferedDownlink<MapAction, ViewWithEvent>,
    BufferedReceiver<ViewWithEvent>,
)
where
    Updates: Stream<Item = MapItemResult> + Send + 'static,
    Commands:
        ItemSender<Command<UntypedMapModification<Value>>, RoutingError> + Send + 'static,
{
    buffered::make_downlink(
        MapStateMachine::new(
            key_schema.unwrap_or(StandardSchema::Anything),
            value_schema.unwrap_or(StandardSchema::Anything),
        ),
        update_stream,
        cmd_sink,
        queue_size,
        &config,
    )
}

pub(in crate::downlink) struct MapModel {
    state: ValMap,
}

impl MapModel {
    fn new() -> Self {
        MapModel {
            state: ValMap::new(),
        }
    }
}

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

impl SyncStateMachine<MapModel, UntypedMapModification<Value>, MapAction> for MapStateMachine {
    type Ev = ViewWithEvent;
    type Cmd = UntypedMapModification<Value>;

    fn init(&self) -> MapModel {
        MapModel::new()
    }

    fn on_sync(&self, state: &MapModel) -> Self::Ev {
        ViewWithEvent::initial(&state.state)
    }

    fn handle_message_unsynced(
        &self,
        state: &mut MapModel,
        message: UntypedMapModification<Value>,
    ) -> Result<(), DownlinkError> {
        match message {
            UntypedMapModification::Update(k, v) => {
                if self.key_schema.matches(&k) {
                    if self.value_schema.matches(&v) {
                        state.state.insert(k, v);
                        Ok(())
                    } else {
                        Err(DownlinkError::SchemaViolation((*v).clone(), self.value_schema.clone()))
                    }
                } else {
                    Err(DownlinkError::SchemaViolation(k, self.key_schema.clone()))
                }
            }
            UntypedMapModification::Remove(k) => {
                if self.key_schema.matches(&k) {
                    state.state.remove(&k);
                    Ok(())
                } else {
                    Err(DownlinkError::SchemaViolation(k, self.key_schema.clone()))
                }
            }
            UntypedMapModification::Take(n) => {
                state.state = state.state.take(n);
                Ok(())
            }
            UntypedMapModification::Drop(n) => {
                state.state = state.state.skip(n);
                Ok(())
            }
            UntypedMapModification::Clear => {
                state.state.clear();
                Ok(())
            }
        }
    }

    fn handle_message(
        &self,
        state: &mut MapModel,
        message: UntypedMapModification<Value>,
    ) -> Result<Option<Self::Ev>, DownlinkError> {
        match message {
            UntypedMapModification::Update(k, v) => {
                if self.key_schema.matches(&k) {
                    if self.value_schema.matches(&v) {
                        state.state.insert(k.clone(), v);
                        Ok(Some(ViewWithEvent::update(&state.state, k)))
                    } else {
                        Err(DownlinkError::SchemaViolation((*v).clone(), self.value_schema.clone()))
                    }
                } else {
                    Err(DownlinkError::SchemaViolation(k, self.key_schema.clone()))
                }
            }
            UntypedMapModification::Remove(k) => {
                if self.key_schema.matches(&k) {
                    state.state.remove(&k);
                    Ok(Some(ViewWithEvent::remove(&state.state, k)))
                } else {
                    Err(DownlinkError::SchemaViolation(k, self.key_schema.clone()))
                }
            }
            UntypedMapModification::Take(n) => {
                state.state = state.state.take(n);
                Ok(Some(ViewWithEvent::take(&state.state, n)))
            }
            UntypedMapModification::Drop(n) => {
                state.state = state.state.skip(n);
                Ok(Some(ViewWithEvent::skip(&state.state, n)))
            }
            UntypedMapModification::Clear => {
                state.state.clear();
                Ok(Some(ViewWithEvent::clear(&state.state)))
            }
        }
    }

    fn handle_action(
        &self,
        state: &mut MapModel,
        action: MapAction,
    ) -> BasicResponse<Self::Ev, Self::Cmd> {
        process_action(
            &self.key_schema,
            &self.value_schema,
            &mut state.state,
            action,
        )
    }
}

fn update_and_notify<Upd>(
    data_state: &mut ValMap,
    update: Upd,
    request: Option<DownlinkRequest<ValMap>>,
) -> Result<(), ()>
where
    Upd: FnOnce(&mut ValMap),
{
    match request {
        Some(req) => {
            let prev = data_state.clone();
            update(data_state);
            req.send_ok(prev)
        }
        _ => {
            update(data_state);
            Ok(())
        }
    }
}

fn update_and_notify_prev<Upd>(
    data_state: &mut ValMap,
    key: &Value,
    update: Upd,
    request: Option<DownlinkRequest<Option<Arc<Value>>>>,
) -> Result<(), ()>
where
    Upd: FnOnce(&mut ValMap),
{
    match request {
        Some(req) => {
            let prev = data_state.get(key).cloned();
            update(data_state);
            req.send_ok(prev)
        }
        _ => {
            update(data_state);
            Ok(())
        }
    }
}

fn process_action(
    key_schema: &StandardSchema,
    val_schema: &StandardSchema,
    data_state: &mut ValMap,
    action: MapAction,
) -> BasicResponse<ViewWithEvent, UntypedMapModification<Value>> {
    let (resp, err) = match action {
        MapAction::Update { key, value, old } => {
            if !key_schema.matches(&key) {
                (
                    BasicResponse::none(),
                    send_error(old, key, key_schema.clone()),
                )
            } else if !val_schema.matches(&value) {
                (
                    BasicResponse::none(),
                    send_error(old, value, val_schema.clone()),
                )
            } else {
                let v_arc = Arc::new(value);
                let err = update_and_notify_prev(
                    data_state,
                    &key,
                    |map| {
                        map.insert(key.clone(), v_arc.clone());
                    },
                    old,
                );
                (
                    BasicResponse::of(
                        ViewWithEvent::update(data_state, key.clone()),
                        UntypedMapModification::Update(key, v_arc),
                    ),
                    err.is_err(),
                )
            }
        }
        MapAction::Remove { key, old } => {
            if !key_schema.matches(&key) {
                (
                    BasicResponse::none(),
                    send_error(old, key, key_schema.clone()),
                )
            } else {
                let (err, did_rem) = if let Some(req) = old {
                    let prev = data_state.remove(&key);
                    let did_remove = prev.is_some();
                    (req.send_ok(prev), did_remove)
                } else {
                    let old = data_state.remove(&key);
                    (Ok(()), old.is_some())
                };
                if did_rem {
                    (
                        BasicResponse::of(
                            ViewWithEvent::remove(data_state, key.clone()),
                            UntypedMapModification::Remove(key),
                        ),
                        err.is_err(),
                    )
                } else {
                    (BasicResponse::none(), err.is_err())
                }
            }
        }
        MapAction::Take { n, before, after } => {
            let err1 = update_and_notify(
                data_state,
                |map| {
                    *map = map.take(n);
                },
                before,
            );
            let err2 = match after {
                None => Ok(()),
                Some(req) => req.send_ok(data_state.clone()),
            };
            (
                BasicResponse::of(
                    ViewWithEvent::take(data_state, n),
                    UntypedMapModification::Take(n),
                ),
                err1.is_err() || err2.is_err(),
            )
        }
        MapAction::Skip { n, before, after } => {
            let err1 = update_and_notify(
                data_state,
                |map| {
                    *map = map.skip(n);
                },
                before,
            );
            let err2 = match after {
                None => Ok(()),
                Some(req) => req.send_ok(data_state.clone()),
            };
            (
                BasicResponse::of(
                    ViewWithEvent::skip(data_state, n),
                    UntypedMapModification::Drop(n),
                ),
                err1.is_err() || err2.is_err(),
            )
        }
        MapAction::Clear { before } => {
            let err = if let Some(req) = before {
                let prev = std::mem::take(data_state);
                req.send_ok(prev)
            } else {
                data_state.clear();
                Ok(())
            };
            (
                BasicResponse::of(
                    ViewWithEvent::clear(data_state),
                    UntypedMapModification::Clear,
                ),
                err.is_err(),
            )
        }
        MapAction::Get { request } => {
            let err = request.send_ok(data_state.clone());
            (BasicResponse::none(), err.is_err())
        }
        MapAction::GetByKey { key, request } => {
            let err = request.send_ok(data_state.get(&key).cloned());
            (BasicResponse::none(), err.is_err())
        }
        MapAction::Modify {
            key,
            f,
            before,
            after,
        } => {
            if !key_schema.matches(&key) {
                modify_key_schema_errors(key_schema, key, before, after)
            } else {
                let prev = get_and_deref(data_state, &key);
                let maybe_new_val = f(&prev);
                match maybe_new_val {
                    Some(v) if !val_schema.matches(&v) => {
                        let result_before = send_error(before, v.clone(), val_schema.clone());
                        let result_after = send_error(after, v, val_schema.clone());
                        (BasicResponse::none(), result_before || result_after)
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
                modify_key_schema_errors(key_schema, key, before, after)
            } else {
                let prev = get_and_deref(data_state, &key);
                match f(&prev) {
                    Ok(maybe_new_val) => match maybe_new_val {
                        Some(v) if !val_schema.matches(&v) => {
                            let result_before = send_error(before, v.clone(), val_schema.clone());
                            let result_after = send_error(after, v, val_schema.clone());
                            (BasicResponse::none(), result_before || result_after)
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
                        let result_before = before
                            .map(|req| req.send_ok(Err(e.clone())).is_err())
                            .unwrap_or(false);
                        let result_after = after
                            .map(|req| req.send_ok(Err(e)).is_err())
                            .unwrap_or(false);
                        (BasicResponse::none(), result_before || result_after)
                    }
                }
            }
        }
    };
    if err {
        resp.with_error(TransitionError::ReceiverDropped)
    } else {
        resp
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
) -> (
    BasicResponse<ViewWithEvent, UntypedMapModification<Value>>,
    bool,
)
where
    F: Fn(Option<Arc<Value>>) -> T + Send + 'static,
{
    match maybe_new_val {
        Some(new_val) => {
            let v_arc = Arc::new(new_val);
            let replaced = data_state.insert(key.clone(), v_arc.clone());
            let err1 = old
                .map(|req| req.send_ok(to_event(replaced)).is_err())
                .unwrap_or(false);
            let err2 = replacement
                .map(|req| req.send_ok(to_event(Some(v_arc.clone()))).is_err())
                .unwrap_or(false);
            (
                BasicResponse::of(
                    ViewWithEvent::update(data_state, key.clone()),
                    UntypedMapModification::Update(key, v_arc),
                ),
                err1 || err2,
            )
        }
        _ if had_existing => {
            let removed = data_state.remove(&key);
            let err1 = old
                .map(|req| req.send_ok(to_event(removed)).is_err())
                .unwrap_or(false);
            let err2 = replacement
                .map(|req| req.send_ok(to_event(None)).is_err())
                .unwrap_or(false);
            (
                BasicResponse::of(
                    ViewWithEvent::remove(data_state, key.clone()),
                    UntypedMapModification::Remove(key),
                ),
                err1 || err2,
            )
        }
        _ => (BasicResponse::none(), false),
    }
}

fn send_error<T>(
    maybe_resp: Option<DownlinkRequest<T>>,
    value: Value,
    schema: StandardSchema,
) -> bool {
    if let Some(req) = maybe_resp {
        let err = DownlinkError::SchemaViolation(value, schema);
        req.send_err(err).is_err()
    } else {
        false
    }
}

fn modify_key_schema_errors<Ev, Cmd, T>(
    key_schema: &StandardSchema,
    key: Value,
    before: Option<DownlinkRequest<T>>,
    after: Option<DownlinkRequest<T>>,
) -> (BasicResponse<Ev, Cmd>, bool) {
    let result_before = send_error(before, key.clone(), key_schema.clone());
    let result_after = send_error(after, key, key_schema.clone());
    (BasicResponse::none(), result_before || result_after)
}

fn get_and_deref<'a>(data_state: &'a ValMap, key: &Value) -> Option<&'a Value> {
    data_state.get(key).map(|arc| arc.as_ref())
}
