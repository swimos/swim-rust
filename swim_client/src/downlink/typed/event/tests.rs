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

use crate::configuration::downlink::OnInvalidMessage;
use crate::downlink::model::map::{MapEvent, ValMap, ViewWithEvent};
use crate::downlink::model::SchemaViolations;
use crate::downlink::typed::event::{EventDownlinkReceiver, EventViewError, TypedEventDownlink};
use crate::downlink::typed::map::events::{TypedMapView, TypedViewWithEvent};
use crate::downlink::DownlinkConfig;
use crate::downlink::{Command, Message};
use im::OrdMap;
use std::collections::{BTreeMap, HashMap};
use std::convert::TryInto;
use std::num::NonZeroUsize;
use std::sync::Arc;
use swim_common::form::FormErr;
use swim_common::form::ValidatedForm;
use swim_common::model::schema::StandardSchema;
use swim_common::model::Value;
use swim_common::routing::RoutingError;
use swim_common::sink::item::ItemSender;
use tokio::sync::mpsc;

fn make_raw() -> ValMap {
    let mut map = ValMap::new();

    map.insert(Value::from(1), Arc::new(Value::from(2)));
    map.insert(Value::from(2), Arc::new(Value::from(4)));
    map.insert(Value::from(3), Arc::new(Value::from(6)));
    map
}

fn make_view() -> TypedMapView<i32, i32> {
    TypedMapView::new(make_raw())
}

#[test]
fn typed_map_view_get() {
    let view = make_view();

    assert_eq!(view.get(&1), Some(2));
    assert_eq!(view.get(&7), None);
}

#[test]
fn typed_map_view_len() {
    let view = make_view();

    assert_eq!(view.len(), 3);
}

#[test]
fn typed_map_view_is_empty() {
    let view = make_view();

    assert_eq!(view.is_empty(), false);
}

#[test]
fn typed_map_view_entries() {
    let view = make_view();

    let entries = view.iter().collect::<Vec<_>>();

    assert_eq!(entries, vec![(1, 2), (2, 4), (3, 6)]);
}

#[test]
fn typed_map_view_keys() {
    let view = make_view();

    let entries = view.keys().collect::<Vec<_>>();

    assert_eq!(entries, vec![1, 2, 3]);
}

#[test]
fn typed_map_view_to_hashmap() {
    let view = make_view();

    let map = view.as_hash_map();

    let mut expected = HashMap::new();
    expected.insert(1, 2);
    expected.insert(2, 4);
    expected.insert(3, 6);

    assert_eq!(map, expected);
}

#[test]
fn typed_map_view_to_btreemap() {
    let view = make_view();

    let map = view.as_btree_map();

    let mut expected = BTreeMap::new();
    expected.insert(1, 2);
    expected.insert(2, 4);
    expected.insert(3, 6);

    assert_eq!(map, expected);
}

#[test]
fn typed_map_view_to_ordmap() {
    let view = make_view();

    let map = view.as_ord_map();

    let mut expected = OrdMap::new();
    expected.insert(1, 2);
    expected.insert(2, 4);
    expected.insert(3, 6);

    assert_eq!(map, expected);
}

#[test]
fn typed_view_with_event_initial() {
    let raw = ViewWithEvent {
        view: make_raw(),
        event: MapEvent::Initial,
    };

    let typed: Result<TypedViewWithEvent<i32, i32>, FormErr> = raw.try_into();

    assert_eq!(
        typed,
        Ok(TypedViewWithEvent {
            view: make_view(),
            event: MapEvent::Initial
        })
    );
}

#[test]
fn typed_view_with_event_clear() {
    let raw = ViewWithEvent {
        view: make_raw(),
        event: MapEvent::Clear,
    };

    let typed: Result<TypedViewWithEvent<i32, i32>, FormErr> = raw.try_into();

    assert_eq!(
        typed,
        Ok(TypedViewWithEvent {
            view: make_view(),
            event: MapEvent::Clear
        })
    );
}

#[test]
fn typed_view_with_event_take() {
    let raw = ViewWithEvent {
        view: make_raw(),
        event: MapEvent::Take(1),
    };

    let typed: Result<TypedViewWithEvent<i32, i32>, FormErr> = raw.try_into();

    assert_eq!(
        typed,
        Ok(TypedViewWithEvent {
            view: make_view(),
            event: MapEvent::Take(1)
        })
    );
}

#[test]
fn typed_view_with_event_skip() {
    let raw = ViewWithEvent {
        view: make_raw(),
        event: MapEvent::Skip(1),
    };

    let typed: Result<TypedViewWithEvent<i32, i32>, FormErr> = raw.try_into();

    assert_eq!(
        typed,
        Ok(TypedViewWithEvent {
            view: make_view(),
            event: MapEvent::Skip(1)
        })
    );
}

#[test]
fn typed_view_with_event_good_update() {
    let raw = ViewWithEvent {
        view: make_raw(),
        event: MapEvent::Update(Value::Int32Value(2)),
    };

    let typed: Result<TypedViewWithEvent<i32, i32>, FormErr> = raw.try_into();

    assert_eq!(
        typed,
        Ok(TypedViewWithEvent {
            view: make_view(),
            event: MapEvent::Update(2)
        })
    );
}

#[test]
fn typed_view_with_event_bad_update() {
    let raw = ViewWithEvent {
        view: make_raw(),
        event: MapEvent::Update(Value::text("hello")),
    };

    let typed: Result<TypedViewWithEvent<i32, i32>, FormErr> = raw.try_into();

    assert!(typed.is_err());
}

#[test]
fn typed_view_with_event_good_remove() {
    let raw = ViewWithEvent {
        view: make_raw(),
        event: MapEvent::Remove(Value::Int32Value(2)),
    };

    let typed: Result<TypedViewWithEvent<i32, i32>, FormErr> = raw.try_into();

    assert_eq!(
        typed,
        Ok(TypedViewWithEvent {
            view: make_view(),
            event: MapEvent::Remove(2)
        })
    );
}

#[test]
fn typed_view_with_event_bad_remove() {
    let raw = ViewWithEvent {
        view: make_raw(),
        event: MapEvent::Remove(Value::text("hello")),
    };

    let typed: Result<TypedViewWithEvent<i32, i32>, FormErr> = raw.try_into();

    assert!(typed.is_err());
}

struct Components<T> {
    downlink: TypedEventDownlink<T>,
    receiver: EventDownlinkReceiver<T>,
    update_tx: mpsc::Sender<Result<Message<Value>, RoutingError>>,
    command_rx: mpsc::Receiver<Command<Value>>,
}

fn make_event_downlink<T: ValidatedForm>() -> Components<T> {
    let (update_tx, update_rx) = mpsc::channel(8);
    let (command_tx, command_rx) = mpsc::channel(8);
    let sender = swim_common::sink::item::for_mpsc_sender(command_tx).map_err_into();

    let (dl, rx) = crate::downlink::model::event::create_downlink(
        T::schema(),
        SchemaViolations::Report,
        update_rx,
        sender,
        DownlinkConfig {
            buffer_size: NonZeroUsize::new(8).unwrap(),
            yield_after: NonZeroUsize::new(2048).unwrap(),
            on_invalid: OnInvalidMessage::Terminate,
        },
    );
    let downlink = TypedEventDownlink::new(Arc::new(dl));
    let receiver = EventDownlinkReceiver::new(rx);

    Components {
        downlink,
        receiver,
        update_tx,
        command_rx,
    }
}

#[tokio::test]
async fn subscriber_covariant_cast() {
    let Components {
        downlink,
        receiver: _receiver,
        update_tx: _update_tx,
        command_rx: _command_rx,
    } = make_event_downlink::<i32>();

    let sub = downlink.subscriber();

    assert!(sub.clone().covariant_cast::<i32>().is_ok());
    assert!(sub.clone().covariant_cast::<Value>().is_ok());
    assert!(sub.clone().covariant_cast::<String>().is_err());
}

#[test]
fn event_view_error_display() {
    let err = EventViewError {
        existing: StandardSchema::Nothing,
        requested: StandardSchema::Anything,
    };
    let str = err.to_string();

    assert_eq!(str, format!("A Read Only view of an event downlink with schema {} was requested but the original event downlink is running with schema {}.", StandardSchema::Anything, StandardSchema::Nothing));
}
