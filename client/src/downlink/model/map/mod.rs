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

use std::convert::TryInto;
use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::sync::Arc;

use futures::Stream;
use im::ordmap::OrdMap;
use tokio::sync::mpsc;

use common::model::schema::{AttrSchema, FieldSpec, Schema, SlotSchema, StandardSchema};
use common::model::{Attr, Item, Value, ValueKind};
use common::sink::item::ItemSender;
use swim_form_old::FormDeserializeErr;
use swim_form_old::{Form, ValidatedForm};

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
use crate::router::RoutingError;
use std::num::NonZeroUsize;

#[cfg(test)]
mod tests;

const INSERT_NAME: &str = "update";
const REMOVE_NAME: &str = "remove";
const TAKE_NAME: &str = "take";
const SKIP_NAME: &str = "drop";
const CLEAR_NAME: &str = "clear";
const KEY_FIELD: &str = "key";

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MapModification<K, V> {
    Insert(K, V),
    Remove(K),
    Take(usize),
    Skip(usize),
    Clear,
}

impl<K: ValidatedForm, V: ValidatedForm> Form for MapModification<K, V> {
    fn as_value(&self) -> Value {
        match self {
            MapModification::Insert(key, value) => insert(key.as_value(), value.as_value()),
            MapModification::Remove(key) => remove(key.as_value()),
            MapModification::Take(n) => take(*n),
            MapModification::Skip(n) => skip(*n),
            MapModification::Clear => clear(),
        }
    }

    fn into_value(self) -> Value {
        match self {
            MapModification::Insert(key, value) => insert(key.into_value(), value.into_value()),
            MapModification::Remove(key) => remove(key.into_value()),
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
                        let key = extract_key(value.clone())?;
                        Ok(MapModification::Insert(key, V::try_convert(Extant)?))
                    }
                    _ => Err(FormDeserializeErr::Malformatted),
                },
                Some(Attr { name, value }) if *name == INSERT_NAME => {
                    let key = extract_key(value.clone())?;

                    let insert_value = if !has_more && items.len() < 2 {
                        match items.first() {
                            Some(Item::ValueItem(single)) => single.clone(),
                            _ => Value::record(items.clone()),
                        }
                    } else {
                        Record(attrs.iter().skip(1).cloned().collect(), items.clone())
                    };
                    Ok(MapModification::Insert(key, V::try_convert(insert_value)?))
                }

                _ => Err(FormDeserializeErr::Malformatted),
            }
        } else {
            Err(FormDeserializeErr::IncorrectType(
                "Invalid structure for map action.".to_string(),
            ))
        }
    }
}

impl<K: ValidatedForm, V: ValidatedForm> ValidatedForm for MapModification<K, V> {
    fn schema() -> StandardSchema {
        let clear_schema = AttrSchema::tag(CLEAR_NAME).only();

        let num_schema =
            StandardSchema::OfKind(ValueKind::Int32).and(StandardSchema::after_int(0, true));

        let take_schema = AttrSchema::named(TAKE_NAME, num_schema.clone()).only();

        let drop_schema = AttrSchema::named(SKIP_NAME, num_schema).only();

        let key_schema = FieldSpec::default(SlotSchema::new(
            StandardSchema::text(KEY_FIELD),
            K::schema(),
        ));

        let key_body_schema = StandardSchema::NumAttrs(0).and(StandardSchema::HasSlots {
            slots: vec![key_schema],
            exhaustive: true,
        });

        let remove_schema = AttrSchema::named(REMOVE_NAME, key_body_schema.clone()).only();

        let insert_schema = AttrSchema::named(INSERT_NAME, key_body_schema).and_then(V::schema());

        StandardSchema::Or(vec![
            insert_schema,
            remove_schema,
            clear_schema,
            take_schema,
            drop_schema,
        ])
    }
}

pub type UntypedMapModification<V> = MapModification<Value, V>;

fn extract_key<K: ValidatedForm>(attr_body: Value) -> Result<K, FormDeserializeErr> {
    match attr_body {
        Value::Record(attrs, items) if attrs.is_empty() && items.len() < 2 => {
            match items.into_iter().next() {
                Some(Item::Slot(Value::Text(name), key)) if name == KEY_FIELD => {
                    Ok(K::try_convert(key)?)
                }
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

fn extract_take_or_skip<K: ValidatedForm, V: ValidatedForm>(
    name: &str,
    n: i32,
) -> Result<MapModification<K, V>, FormDeserializeErr> {
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
    let attr = Attr::of((INSERT_NAME, Value::singleton((KEY_FIELD, key))));
    match value {
        Value::Extant => Value::of_attr(attr),
        Value::Record(mut attrs, items) => {
            attrs.insert(0, attr);
            Value::Record(attrs, items)
        }
        ow => Value::Record(vec![attr], vec![Item::ValueItem(ow)]),
    }
}

impl<V: Deref<Target = Value>> UntypedMapModification<V> {
    pub fn envelope_body(self) -> Value {
        match self {
            UntypedMapModification::Insert(key, value) => insert(key, (*value).clone()),
            UntypedMapModification::Remove(key) => remove(key),
            UntypedMapModification::Take(n) => take(n),
            UntypedMapModification::Skip(n) => skip(n),
            UntypedMapModification::Clear => clear(),
        }
    }
}

pub enum MapAction {
    Insert {
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
    Update {
        key: Value,
        f: Box<dyn FnOnce(&Option<&Value>) -> Option<Value> + Send>,
        before: Option<DownlinkRequest<Option<Arc<Value>>>>,
        after: Option<DownlinkRequest<Option<Arc<Value>>>>,
    },
    TryUpdate {
        key: Value,
        f: Box<dyn FnOnce(&Option<&Value>) -> UpdateResult<Option<Value>> + Send>,
        before: Option<DownlinkRequest<UpdateResult<Option<Arc<Value>>>>>,
        after: Option<DownlinkRequest<UpdateResult<Option<Arc<Value>>>>>,
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
        request: DownlinkRequest<Option<Arc<Value>>>,
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

    pub fn try_update<F>(key: Value, f: F) -> MapAction
    where
        F: FnOnce(&Option<&Value>) -> UpdateResult<Option<Value>> + Send + 'static,
    {
        MapAction::TryUpdate {
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
        val_before: DownlinkRequest<Option<Arc<Value>>>,
        val_after: DownlinkRequest<Option<Arc<Value>>>,
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

    pub fn try_update_and_await<F>(
        key: Value,
        f: F,
        val_before: DownlinkRequest<UpdateResult<Option<Arc<Value>>>>,
        val_after: DownlinkRequest<UpdateResult<Option<Arc<Value>>>>,
    ) -> MapAction
    where
        F: FnOnce(&Option<&Value>) -> UpdateResult<Option<Value>> + Send + 'static,
    {
        MapAction::TryUpdate {
            key,
            f: Box::new(f),
            before: Some(val_before),
            after: Some(val_after),
        }
    }

    pub fn update_box_and_await(
        key: Value,
        f: Box<dyn FnOnce(&Option<&Value>) -> Option<Value> + Send>,
        val_before: DownlinkRequest<Option<Arc<Value>>>,
        val_after: DownlinkRequest<Option<Arc<Value>>>,
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
            MapAction::TryUpdate {
                key, before, after, ..
            } => write!(
                f,
                "TryUpdate({:?}, <closure>, {:?}, {:?})",
                key, before, after
            ),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MapEvent<K> {
    Initial,
    Insert(K),
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
) -> RawDownlink<mpsc::Sender<MapAction>, mpsc::Receiver<Event<ViewWithEvent>>>
where
    Updates: Stream<Item = MapItemResult> + Send + 'static,
    Commands:
        ItemSender<Command<UntypedMapModification<Arc<Value>>>, RoutingError> + Send + 'static,
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
        ItemSender<Command<UntypedMapModification<Arc<Value>>>, RoutingError> + Send + 'static,
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
        ItemSender<Command<UntypedMapModification<Arc<Value>>>, RoutingError> + Send + 'static,
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
        ItemSender<Command<UntypedMapModification<Arc<Value>>>, RoutingError> + Send + 'static,
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
    type Cmd = UntypedMapModification<Arc<Value>>;

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
            UntypedMapModification::Insert(k, v) => {
                if self.key_schema.matches(&k) {
                    if self.value_schema.matches(&v) {
                        state.state.insert(k, Arc::new(v));
                        Ok(())
                    } else {
                        Err(DownlinkError::SchemaViolation(v, self.value_schema.clone()))
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
            UntypedMapModification::Skip(n) => {
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
            UntypedMapModification::Insert(k, v) => {
                if self.key_schema.matches(&k) {
                    if self.value_schema.matches(&v) {
                        state.state.insert(k.clone(), Arc::new(v));
                        Ok(Some(ViewWithEvent::insert(&state.state, k)))
                    } else {
                        Err(DownlinkError::SchemaViolation(v, self.value_schema.clone()))
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
            UntypedMapModification::Skip(n) => {
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
    Upd: FnOnce(&mut ValMap) -> (),
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
    Upd: FnOnce(&mut ValMap) -> (),
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
) -> BasicResponse<ViewWithEvent, UntypedMapModification<Arc<Value>>> {
    let (resp, err) = match action {
        MapAction::Insert { key, value, old } => {
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
                        ViewWithEvent::insert(data_state, key.clone()),
                        UntypedMapModification::Insert(key, v_arc),
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
                    UntypedMapModification::Skip(n),
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
        MapAction::Update {
            key,
            f,
            before,
            after,
        } => {
            if !key_schema.matches(&key) {
                update_key_schema_errors(key_schema, key, before, after)
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
                        handle_update(
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
        MapAction::TryUpdate {
            key,
            f,
            before,
            after,
        } => {
            if !key_schema.matches(&key) {
                update_key_schema_errors(key_schema, key, before, after)
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
                            handle_update(
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

fn handle_update<F, T>(
    data_state: &mut ValMap,
    key: Value,
    had_existing: bool,
    maybe_new_val: Option<Value>,
    old: Option<DownlinkRequest<T>>,
    replacement: Option<DownlinkRequest<T>>,
    to_event: F,
) -> (
    BasicResponse<ViewWithEvent, UntypedMapModification<Arc<Value>>>,
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
                    ViewWithEvent::insert(data_state, key.clone()),
                    UntypedMapModification::Insert(key, v_arc),
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

fn update_key_schema_errors<Ev, Cmd, T>(
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
