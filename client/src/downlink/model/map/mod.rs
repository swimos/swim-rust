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

use std::sync::Arc;

use im::ordmap::OrdMap;
use tokio::sync::mpsc;

use common::model::{Attr, Item, Value, ValueKind};
use common::request::Request;

use crate::downlink::buffered::{self, BufferedDownlink, BufferedReceiver};
use crate::downlink::dropping::{self, DroppingDownlink, DroppingReceiver};
use crate::downlink::queue::{self, QueueDownlink, QueueReceiver};
use crate::downlink::raw::RawDownlink;
use crate::downlink::{
    BasicResponse, BasicStateMachine, Command, DownlinkError, Event, Message, TransitionError,
};
use crate::router::RoutingError;
use common::model::schema::{AttrSchema, FieldSpec, Schema, SlotSchema, StandardSchema};
use common::sink::item::ItemSender;
use deserialize::FormDeserializeErr;
use form::{Form, ValidatedForm};
use futures::Stream;
use std::convert::TryInto;
use std::fmt::{Debug, Formatter};
use std::ops::Deref;

#[cfg(test)]
mod tests;

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MapModification<V> {
    Insert(Value, V),
    Remove(Value),
    Take(usize),
    Skip(usize),
    Clear,
}

const INSERT_NAME: &str = "insert";
const REMOVE_NAME: &str = "remove";
const TAKE_NAME: &str = "take";
const SKIP_NAME: &str = "drop";
const CLEAR_NAME: &str = "clear";
const KEY_FIELD: &str = "key";

impl Form for MapModification<Value> {
    fn as_value(&self) -> Value {
        match self {
            MapModification::Insert(key, value) => insert(key.clone(), value.clone()),
            MapModification::Remove(key) => remove(key.clone()),
            MapModification::Take(n) => take(*n),
            MapModification::Skip(n) => skip(*n),
            MapModification::Clear => clear(),
        }
    }

    fn into_value(self) -> Value {
        match self {
            MapModification::Insert(key, value) => insert(key, value),
            MapModification::Remove(key) => remove(key),
            MapModification::Take(n) => take(n),
            MapModification::Skip(n) => skip(n),
            MapModification::Clear => clear(),
        }
    }

    fn try_from_value(body: &Value) -> Result<Self, FormDeserializeErr> {
        use Value::*;
        if let Record(attrs, items) = body {
            let mut attr_it = attrs.iter();
            let head = attr_it.next();
            let has_more = attr_it.next().is_some();
            match head {
                Some(first) if !has_more && items.is_empty() => match first {
                    Attr {
                        name,
                        value: Extant,
                    } if *name == CLEAR_NAME => Ok(MapModification::Clear),
                    Attr {
                        name,
                        value: Int32Value(n),
                    } => extract_take_or_skip(&name, *n),
                    Attr { name, value } if name == REMOVE_NAME => {
                        extract_key(value.clone()).map(MapModification::Remove)
                    }
                    Attr { name, value } if name == INSERT_NAME => {
                        extract_key(value.clone()).map(|key| MapModification::Insert(key, Extant))
                    }
                    _ => Err(FormDeserializeErr::Malformatted),
                },
                Some(Attr { name, value }) if *name == INSERT_NAME => extract_key(value.clone())
                    .map(|key| {
                        let insert_value = if !has_more && items.len() < 2 {
                            match items.first() {
                                Some(Item::ValueItem(single)) => single.clone(),
                                _ => Value::record(items.clone()),
                            }
                        } else {
                            Record(attrs.iter().skip(1).cloned().collect(), items.clone())
                        };
                        MapModification::Insert(key, insert_value)
                    }),
                _ => Err(FormDeserializeErr::Malformatted),
            }
        } else {
            Err(FormDeserializeErr::IncorrectType(
                "Invalid structure for map action.".to_string(),
            ))
        }
    }

    fn try_convert(body: Value) -> Result<Self, FormDeserializeErr> {
        use Value::*;
        if let Record(attrs, items) = body {
            let single_attr = items.is_empty() && attrs.len() < 2;
            let mut attr_it = attrs.into_iter().fuse();
            let head = attr_it.next();

            match head {
                Some(Attr {
                    name,
                    value: Extant,
                }) if name == CLEAR_NAME && single_attr => Ok(MapModification::Clear),
                Some(Attr {
                    name,
                    value: Int32Value(n),
                }) if single_attr => extract_take_or_skip(&name, n),
                Some(Attr { name, value }) if name == REMOVE_NAME && single_attr => {
                    extract_key(value).map(MapModification::Remove)
                }
                Some(Attr { name, value }) if name == INSERT_NAME => {
                    let attr_tail = attr_it.collect::<Vec<_>>();
                    let insert_value = if attr_tail.is_empty() && items.len() < 2 {
                        match items.into_iter().next() {
                            Some(Item::ValueItem(single)) => single,
                            Some(ow) => Value::singleton(ow),
                            _ => Extant,
                        }
                    } else {
                        Record(attr_tail, items)
                    };
                    extract_key(value).map(|key| MapModification::Insert(key, insert_value))
                }
                _ => Err(FormDeserializeErr::Malformatted),
            }
        } else {
            Err(FormDeserializeErr::Malformatted)
        }
    }
}

impl ValidatedForm for MapModification<Value> {
    fn schema() -> StandardSchema {
        let clear_schema = AttrSchema::tag(CLEAR_NAME).only();

        let num_schema =
            StandardSchema::OfKind(ValueKind::Int32).and(StandardSchema::after_int(0, true));

        let take_schema = AttrSchema::named(TAKE_NAME, num_schema.clone()).only();

        let drop_schema = AttrSchema::named(SKIP_NAME, num_schema).only();

        let key_schema = FieldSpec::default(SlotSchema::new(
            StandardSchema::text(KEY_FIELD),
            StandardSchema::Anything,
        ));

        let key_body_schema = StandardSchema::NumAttrs(0).and(StandardSchema::HasSlots {
            slots: vec![key_schema],
            exhaustive: true,
        });

        let remove_schema = AttrSchema::named(REMOVE_NAME, key_body_schema.clone()).only();

        let insert_schema =
            AttrSchema::named(INSERT_NAME, key_body_schema).and_then(StandardSchema::Anything);

        StandardSchema::Or(vec![
            insert_schema,
            remove_schema,
            clear_schema,
            take_schema,
            drop_schema,
        ])
    }
}

fn extract_key(attr_body: Value) -> Result<Value, FormDeserializeErr> {
    match attr_body {
        Value::Record(attrs, items) if attrs.is_empty() && items.len() < 2 => {
            match items.into_iter().next() {
                Some(Item::Slot(Value::Text(name), key)) if name == KEY_FIELD => Ok(key),
                _ => Err(FormDeserializeErr::Message(
                    "Invalid key specifier.".to_string(),
                )),
            }
        }
        _ => Err(FormDeserializeErr::Message(
            "Invalid key specifier.".to_string(),
        )),
    }
}

fn extract_take_or_skip(name: &str, n: i32) -> Result<MapModification<Value>, FormDeserializeErr> {
    match name {
        TAKE_NAME => {
            if n >= 0 {
                Ok(MapModification::Take(n as usize))
            } else {
                Err(FormDeserializeErr::Message(format!(
                    "Invalid take size: {}",
                    n
                )))
            }
        }
        SKIP_NAME => {
            if n >= 0 {
                Ok(MapModification::Skip(n as usize))
            } else {
                Err(FormDeserializeErr::Message(format!(
                    "Invalid drop size: {}",
                    n
                )))
            }
        }
        _ => Err(FormDeserializeErr::Message(format!(
            "{} is not a map action.",
            name
        ))),
    }
}

fn clear() -> Value {
    Value::of_attr(CLEAR_NAME)
}

fn skip(n: usize) -> Value {
    let num: i32 = n.try_into().unwrap_or(i32::max_value());
    Value::of_attr((SKIP_NAME, num))
}

fn take(n: usize) -> Value {
    let num: i32 = n.try_into().unwrap_or(i32::max_value());
    Value::of_attr((TAKE_NAME, num))
}

fn remove(key: Value) -> Value {
    Value::of_attr((REMOVE_NAME, Value::singleton((KEY_FIELD, key))))
}

fn insert(key: Value, value: Value) -> Value {
    let attr = Attr::of(("insert", Value::singleton((KEY_FIELD, key))));
    match value {
        Value::Extant => Value::of_attr(attr),
        Value::Record(mut attrs, items) => {
            attrs.insert(0, attr);
            Value::Record(attrs, items)
        }
        ow => Value::Record(vec![attr], vec![Item::ValueItem(ow)]),
    }
}

impl<V: Deref<Target = Value>> MapModification<V> {
    pub fn envelope_body(self) -> Value {
        match self {
            MapModification::Insert(key, value) => insert(key, (*value).clone()),
            MapModification::Remove(key) => remove(key),
            MapModification::Take(n) => take(n),
            MapModification::Skip(n) => skip(n),
            MapModification::Clear => clear(),
        }
    }
}

pub enum MapAction {
    Insert {
        key: Value,
        value: Value,
        old: Option<Request<Option<Arc<Value>>>>,
    },
    Remove {
        key: Value,
        old: Option<Request<Option<Arc<Value>>>>,
    },
    Take {
        n: usize,
        before: Option<Request<ValMap>>,
        after: Option<Request<ValMap>>,
    },
    Skip {
        n: usize,
        before: Option<Request<ValMap>>,
        after: Option<Request<ValMap>>,
    },
    Clear {
        before: Option<Request<ValMap>>,
    },
    Get {
        request: Request<ValMap>,
    },
    GetByKey {
        key: Value,
        request: Request<Option<Arc<Value>>>,
    },
    Update {
        key: Value,
        f: Box<dyn FnOnce(&Option<&Value>) -> Option<Value> + Send>,
        before: Option<Request<Option<Arc<Value>>>>,
        after: Option<Request<Option<Arc<Value>>>>,
    },
}

impl MapAction {
    pub fn insert(key: Value, value: Value) -> MapAction {
        MapAction::Insert {
            key,
            value,
            old: None,
        }
    }

    pub fn insert_and_await(
        key: Value,
        value: Value,
        request: Request<Option<Arc<Value>>>,
    ) -> MapAction {
        MapAction::Insert {
            key,
            value,
            old: Some(request),
        }
    }

    pub fn remove(key: Value) -> MapAction {
        MapAction::Remove { key, old: None }
    }

    pub fn remove_and_await(key: Value, request: Request<Option<Arc<Value>>>) -> MapAction {
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
        map_before: Request<ValMap>,
        map_after: Request<ValMap>,
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
        map_before: Request<ValMap>,
        map_after: Request<ValMap>,
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

    pub fn clear_and_await(map_before: Request<ValMap>) -> MapAction {
        MapAction::Clear {
            before: Some(map_before),
        }
    }

    pub fn get_map(request: Request<ValMap>) -> MapAction {
        MapAction::Get { request }
    }

    pub fn get(key: Value, request: Request<Option<Arc<Value>>>) -> MapAction {
        MapAction::GetByKey { key, request }
    }

    pub fn update<F>(key: Value, f: F) -> MapAction
    where
        F: FnOnce(&Option<&Value>) -> Option<Value> + Send + 'static,
    {
        MapAction::Update {
            key,
            f: Box::new(f),
            before: None,
            after: None,
        }
    }

    pub fn update_box(
        key: Value,
        f: Box<dyn FnOnce(&Option<&Value>) -> Option<Value> + Send>,
    ) -> MapAction {
        MapAction::Update {
            key,
            f: Box::new(f),
            before: None,
            after: None,
        }
    }

    pub fn update_and_await<F>(
        key: Value,
        f: F,
        val_before: Request<Option<Arc<Value>>>,
        val_after: Request<Option<Arc<Value>>>,
    ) -> MapAction
    where
        F: FnOnce(&Option<&Value>) -> Option<Value> + Send + 'static,
    {
        MapAction::Update {
            key,
            f: Box::new(f),
            before: Some(val_before),
            after: Some(val_after),
        }
    }

    pub fn update_box_and_await(
        key: Value,
        f: Box<dyn FnOnce(&Option<&Value>) -> Option<Value> + Send>,
        val_before: Request<Option<Arc<Value>>>,
        val_after: Request<Option<Arc<Value>>>,
    ) -> MapAction {
        MapAction::Update {
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
            MapAction::Insert { key, value, old } => {
                write!(f, "Insert({:?} => {:?}, {:?})", key, value, old)
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
            MapAction::Update {
                key, before, after, ..
            } => write!(f, "Update({:?}, <closure>, {:?}, {:?})", key, before, after),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MapEvent {
    Initial,
    Insert(Value),
    Remove(Value),
    Take(usize),
    Skip(usize),
    Clear,
}

pub type ValMap = OrdMap<Value, Arc<Value>>;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ViewWithEvent {
    pub view: ValMap,
    pub event: MapEvent,
}

impl ViewWithEvent {
    fn initial(map: &ValMap) -> ViewWithEvent {
        ViewWithEvent {
            view: map.clone(),
            event: MapEvent::Initial,
        }
    }

    fn insert(map: &ValMap, key: Value) -> ViewWithEvent {
        ViewWithEvent {
            view: map.clone(),
            event: MapEvent::Insert(key),
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

/// Create a map downlink.
pub fn create_raw_downlink<Updates, Commands>(
    key_schema: Option<StandardSchema>,
    value_schema: Option<StandardSchema>,
    update_stream: Updates,
    cmd_sink: Commands,
    buffer_size: usize,
) -> RawDownlink<mpsc::Sender<MapAction>, mpsc::Receiver<Event<ViewWithEvent>>>
where
    Updates: Stream<Item = Message<MapModification<Value>>> + Send + 'static,
    Commands: ItemSender<Command<MapModification<Arc<Value>>>, RoutingError> + Send + 'static,
{
    crate::downlink::create_downlink(
        MapStateMachine::new(
            key_schema.unwrap_or(StandardSchema::Anything),
            value_schema.unwrap_or(StandardSchema::Anything),
        ),
        update_stream,
        cmd_sink,
        buffer_size,
    )
}

/// Create a map downlink with an queue based multiplexing topic.
pub fn create_queue_downlink<Updates, Commands>(
    key_schema: Option<StandardSchema>,
    value_schema: Option<StandardSchema>,
    update_stream: Updates,
    cmd_sink: Commands,
    buffer_size: usize,
    queue_size: usize,
) -> (
    QueueDownlink<MapAction, ViewWithEvent>,
    QueueReceiver<ViewWithEvent>,
)
where
    Updates: Stream<Item = Message<MapModification<Value>>> + Send + 'static,
    Commands: ItemSender<Command<MapModification<Arc<Value>>>, RoutingError> + Send + 'static,
{
    queue::make_downlink(
        MapStateMachine::new(
            key_schema.unwrap_or(StandardSchema::Anything),
            value_schema.unwrap_or(StandardSchema::Anything),
        ),
        update_stream,
        cmd_sink,
        buffer_size,
        queue_size,
    )
}

/// Create a value downlink with an dropping multiplexing topic.
pub fn create_dropping_downlink<Updates, Commands>(
    key_schema: Option<StandardSchema>,
    value_schema: Option<StandardSchema>,
    update_stream: Updates,
    cmd_sink: Commands,
    buffer_size: usize,
) -> (
    DroppingDownlink<MapAction, ViewWithEvent>,
    DroppingReceiver<ViewWithEvent>,
)
where
    Updates: Stream<Item = Message<MapModification<Value>>> + Send + 'static,
    Commands: ItemSender<Command<MapModification<Arc<Value>>>, RoutingError> + Send + 'static,
{
    dropping::make_downlink(
        MapStateMachine::new(
            key_schema.unwrap_or(StandardSchema::Anything),
            value_schema.unwrap_or(StandardSchema::Anything),
        ),
        update_stream,
        cmd_sink,
        buffer_size,
    )
}

/// Create a value downlink with an buffered multiplexing topic.
pub fn create_buffered_downlink<Updates, Commands>(
    key_schema: Option<StandardSchema>,
    value_schema: Option<StandardSchema>,
    update_stream: Updates,
    cmd_sink: Commands,
    buffer_size: usize,
    queue_size: usize,
) -> (
    BufferedDownlink<MapAction, ViewWithEvent>,
    BufferedReceiver<ViewWithEvent>,
)
where
    Updates: Stream<Item = Message<MapModification<Value>>> + Send + 'static,
    Commands: ItemSender<Command<MapModification<Arc<Value>>>, RoutingError> + Send + 'static,
{
    buffered::make_downlink(
        MapStateMachine::new(
            key_schema.unwrap_or(StandardSchema::Anything),
            value_schema.unwrap_or(StandardSchema::Anything),
        ),
        update_stream,
        cmd_sink,
        buffer_size,
        queue_size,
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

impl BasicStateMachine<MapModel, MapModification<Value>, MapAction> for MapStateMachine {
    type Ev = ViewWithEvent;
    type Cmd = MapModification<Arc<Value>>;

    fn init(&self) -> MapModel {
        MapModel::new()
    }

    fn on_sync(&self, state: &MapModel) -> Self::Ev {
        ViewWithEvent::initial(&state.state)
    }

    fn handle_message_unsynced(
        &self,
        state: &mut MapModel,
        message: MapModification<Value>,
    ) -> Option<DownlinkError> {
        match message {
            MapModification::Insert(k, v) => {
                if self.key_schema.matches(&k) {
                    if self.value_schema.matches(&v) {
                        state.state.insert(k, Arc::new(v));
                        None
                    } else {
                        Some(DownlinkError::SchemaViolation(v, self.value_schema.clone()))
                    }
                } else {
                    Some(DownlinkError::SchemaViolation(k, self.key_schema.clone()))
                }
            }
            MapModification::Remove(k) => {
                if self.key_schema.matches(&k) {
                    state.state.remove(&k);
                    None
                } else {
                    Some(DownlinkError::SchemaViolation(k, self.key_schema.clone()))
                }
            }
            MapModification::Take(n) => {
                state.state = state.state.take(n);
                None
            }
            MapModification::Skip(n) => {
                state.state = state.state.skip(n);
                None
            }
            MapModification::Clear => {
                state.state.clear();
                None
            }
        }
    }

    fn handle_message(
        &self,
        state: &mut MapModel,
        message: MapModification<Value>,
    ) -> Result<Option<Self::Ev>, DownlinkError> {
        Ok(Some(match message {
            MapModification::Insert(k, v) => {
                state.state.insert(k.clone(), Arc::new(v));
                ViewWithEvent::insert(&state.state, k)
            }
            MapModification::Remove(k) => {
                state.state.remove(&k);
                ViewWithEvent::remove(&state.state, k)
            }
            MapModification::Take(n) => {
                state.state = state.state.take(n);
                ViewWithEvent::take(&state.state, n)
            }
            MapModification::Skip(n) => {
                state.state = state.state.skip(n);
                ViewWithEvent::skip(&state.state, n)
            }
            MapModification::Clear => {
                state.state.clear();
                ViewWithEvent::clear(&state.state)
            }
        }))
    }

    fn handle_action(
        &self,
        state: &mut MapModel,
        action: MapAction,
    ) -> BasicResponse<Self::Ev, Self::Cmd> {
        process_action(&mut state.state, action)
    }
}

fn update_and_notify<Upd>(
    data_state: &mut ValMap,
    update: Upd,
    request: Option<Request<ValMap>>,
) -> Result<(), ()>
where
    Upd: FnOnce(&mut ValMap) -> (),
{
    match request {
        Some(req) => {
            let prev = data_state.clone();
            update(data_state);
            req.send(prev)
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
    request: Option<Request<Option<Arc<Value>>>>,
) -> Result<(), ()>
where
    Upd: FnOnce(&mut ValMap) -> (),
{
    match request {
        Some(req) => {
            let prev = data_state.get(key).cloned();
            update(data_state);
            req.send(prev)
        }
        _ => {
            update(data_state);
            Ok(())
        }
    }
}

fn process_action(
    data_state: &mut ValMap,
    action: MapAction,
) -> BasicResponse<ViewWithEvent, MapModification<Arc<Value>>> {
    let (resp, err) = match action {
        MapAction::Insert { key, value, old } => {
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
                    ViewWithEvent::insert(data_state, key.clone()),
                    MapModification::Insert(key, v_arc),
                ),
                err.is_err(),
            )
        }
        MapAction::Remove { key, old } => {
            let (err, did_rem) = match old {
                Some(req) => {
                    let prev = data_state.remove(&key);
                    let did_remove = prev.is_some();
                    (req.send(prev), did_remove)
                }
                _ => {
                    let old = data_state.remove(&key);
                    (Ok(()), old.is_some())
                }
            };
            if did_rem {
                (
                    BasicResponse::of(
                        ViewWithEvent::remove(data_state, key.clone()),
                        MapModification::Remove(key),
                    ),
                    err.is_err(),
                )
            } else {
                (BasicResponse::none(), err.is_err())
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
                Some(req) => req.send(data_state.clone()),
            };
            (
                BasicResponse::of(ViewWithEvent::take(data_state, n), MapModification::Take(n)),
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
                Some(req) => req.send(data_state.clone()),
            };
            (
                BasicResponse::of(ViewWithEvent::skip(data_state, n), MapModification::Skip(n)),
                err1.is_err() || err2.is_err(),
            )
        }
        MapAction::Clear { before } => {
            let err = match before {
                Some(req) => {
                    let prev = std::mem::take(data_state);
                    req.send(prev)
                }
                _ => {
                    data_state.clear();
                    Ok(())
                }
            };
            (
                BasicResponse::of(ViewWithEvent::clear(data_state), MapModification::Clear),
                err.is_err(),
            )
        }
        MapAction::Get { request } => {
            let err = request.send(data_state.clone());
            (BasicResponse::none(), err.is_err())
        }
        MapAction::GetByKey { key, request } => {
            let err = request.send(data_state.get(&key).cloned());
            (BasicResponse::none(), err.is_err())
        }
        MapAction::Update {
            key,
            f,
            before: old,
            after: replacement,
        } => {
            let prev = data_state.get(&key).map(|arc| arc.as_ref());
            let maybe_new_val = f(&prev);
            match maybe_new_val {
                Some(new_val) => {
                    let v_arc = Arc::new(new_val);
                    let replaced = data_state.insert(key.clone(), v_arc.clone());
                    let err1 = old.and_then(|req| req.send(replaced).err());
                    let err2 = replacement.and_then(|req| req.send(Some(v_arc.clone())).err());
                    (
                        BasicResponse::of(
                            ViewWithEvent::insert(data_state, key.clone()),
                            MapModification::Insert(key, v_arc),
                        ),
                        err1.is_some() || err2.is_some(),
                    )
                }
                _ if prev.is_some() => {
                    let removed = data_state.remove(&key);
                    let err1 = old.and_then(|req| req.send(removed).err());
                    let err2 = replacement.and_then(|req| req.send(None).err());
                    (
                        BasicResponse::of(
                            ViewWithEvent::remove(data_state, key.clone()),
                            MapModification::Remove(key),
                        ),
                        err1.is_some() || err2.is_some(),
                    )
                }
                _ => (BasicResponse::none(), false),
            }
        }
    };
    if err {
        resp.with_error(TransitionError::ReceiverDropped)
    } else {
        resp
    }
}
