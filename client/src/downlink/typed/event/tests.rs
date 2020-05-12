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

use crate::downlink::model::map::{MapEvent, ValMap, ViewWithEvent};
use crate::downlink::typed::event::{TypedMapView, TypedViewWithEvent};
use common::model::Value;
use deserialize::FormDeserializeErr;
use hamcrest2::assert_that;
use hamcrest2::prelude::*;
use im::OrdMap;
use std::collections::{BTreeMap, HashMap};
use std::convert::TryInto;
use std::sync::Arc;

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

    assert_that!(view.get(&1), eq(Some(2)));
    assert_that!(view.get(&7), eq(None));
}

#[test]
fn typed_map_view_len() {
    let view = make_view();

    assert_that!(view.len(), eq(3));
}

#[test]
fn typed_map_view_is_empty() {
    let view = make_view();

    assert_that!(view.is_empty(), eq(false));
}

#[test]
fn typed_map_view_entries() {
    let view = make_view();

    let entries = view.iter().collect::<Vec<_>>();

    assert_that!(entries, eq(vec![(1, 2), (2, 4), (3, 6)]));
}

#[test]
fn typed_map_view_keys() {
    let view = make_view();

    let entries = view.keys().collect::<Vec<_>>();

    assert_that!(entries, eq(vec![1, 2, 3]));
}

#[test]
fn typed_map_view_to_hashmap() {
    let view = make_view();

    let map = view.as_hash_map();

    let mut expected = HashMap::new();
    expected.insert(1, 2);
    expected.insert(2, 4);
    expected.insert(3, 6);

    assert_that!(map, eq(expected));
}

#[test]
fn typed_map_view_to_btreemap() {
    let view = make_view();

    let map = view.as_btree_map();

    let mut expected = BTreeMap::new();
    expected.insert(1, 2);
    expected.insert(2, 4);
    expected.insert(3, 6);

    assert_that!(map, eq(expected));
}

#[test]
fn typed_map_view_to_ordmap() {
    let view = make_view();

    let map = view.as_ord_map();

    let mut expected = OrdMap::new();
    expected.insert(1, 2);
    expected.insert(2, 4);
    expected.insert(3, 6);

    assert_that!(map, eq(expected));
}

#[test]
fn typed_view_with_event_initial() {
    let raw = ViewWithEvent {
        view: make_raw(),
        event: MapEvent::Initial,
    };

    let typed: Result<TypedViewWithEvent<i32, i32>, FormDeserializeErr> = raw.try_into();

    assert_that!(
        typed,
        eq(Ok(TypedViewWithEvent {
            view: make_view(),
            event: MapEvent::Initial
        }))
    );
}

#[test]
fn typed_view_with_event_clear() {
    let raw = ViewWithEvent {
        view: make_raw(),
        event: MapEvent::Clear,
    };

    let typed: Result<TypedViewWithEvent<i32, i32>, FormDeserializeErr> = raw.try_into();

    assert_that!(
        typed,
        eq(Ok(TypedViewWithEvent {
            view: make_view(),
            event: MapEvent::Clear
        }))
    );
}

#[test]
fn typed_view_with_event_take() {
    let raw = ViewWithEvent {
        view: make_raw(),
        event: MapEvent::Take(1),
    };

    let typed: Result<TypedViewWithEvent<i32, i32>, FormDeserializeErr> = raw.try_into();

    assert_that!(
        typed,
        eq(Ok(TypedViewWithEvent {
            view: make_view(),
            event: MapEvent::Take(1)
        }))
    );
}

#[test]
fn typed_view_with_event_skip() {
    let raw = ViewWithEvent {
        view: make_raw(),
        event: MapEvent::Skip(1),
    };

    let typed: Result<TypedViewWithEvent<i32, i32>, FormDeserializeErr> = raw.try_into();

    assert_that!(
        typed,
        eq(Ok(TypedViewWithEvent {
            view: make_view(),
            event: MapEvent::Skip(1)
        }))
    );
}

#[test]
fn typed_view_with_event_good_insert() {
    let raw = ViewWithEvent {
        view: make_raw(),
        event: MapEvent::Insert(Value::Int32Value(2)),
    };

    let typed: Result<TypedViewWithEvent<i32, i32>, FormDeserializeErr> = raw.try_into();

    assert_that!(
        typed,
        eq(Ok(TypedViewWithEvent {
            view: make_view(),
            event: MapEvent::Insert(2)
        }))
    );
}

#[test]
fn typed_view_with_event_bad_insert() {
    let raw = ViewWithEvent {
        view: make_raw(),
        event: MapEvent::Insert(Value::text("hello")),
    };

    let typed: Result<TypedViewWithEvent<i32, i32>, FormDeserializeErr> = raw.try_into();

    assert_that!(typed, err());
}

#[test]
fn typed_view_with_event_good_remove() {
    let raw = ViewWithEvent {
        view: make_raw(),
        event: MapEvent::Remove(Value::Int32Value(2)),
    };

    let typed: Result<TypedViewWithEvent<i32, i32>, FormDeserializeErr> = raw.try_into();

    assert_that!(
        typed,
        eq(Ok(TypedViewWithEvent {
            view: make_view(),
            event: MapEvent::Remove(2)
        }))
    );
}

#[test]
fn typed_view_with_event_bad_remove() {
    let raw = ViewWithEvent {
        view: make_raw(),
        event: MapEvent::Remove(Value::text("hello")),
    };

    let typed: Result<TypedViewWithEvent<i32, i32>, FormDeserializeErr> = raw.try_into();

    assert_that!(typed, err());
}
