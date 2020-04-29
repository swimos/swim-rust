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
use std::marker::PhantomData;
use form::Form;
use std::hash::Hash;
use std::collections::{HashMap, BTreeMap};
use im::OrdMap;
use std::convert::TryFrom;
use deserialize::FormDeserializeErr;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TypedViewWithEvent<K, V> {
    pub view: TypedMapView<K, V>,
    pub event: MapEvent<K>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TypedMapView<K, V> {
    inner: ValMap,
    _entry_type: PhantomData<(K, V)>,
}

impl<K, V> TypedMapView<K, V> {

    pub fn new(inner: ValMap) -> Self {
        TypedMapView {
            inner,
            _entry_type: PhantomData,
        }
    }
}

impl<K: Form, V: Form> TypedMapView<K, V> {

    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(&key.as_value()).and_then(|value| {
            <V as Form>::try_from_value(value.as_ref()).ok()
        })
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (K, V)> + '_ {
        self.inner.iter().filter_map(|(key, value)| {
            match (<K as Form>::try_from_value(key), <V as Form>::try_from_value(value.as_ref())) {
                (Ok(k), Ok(v)) => Some((k, v)),
                _ => None
            }
        })
    }

    pub fn keys(&self) -> impl Iterator<Item = K> + '_ {
        self.inner.keys().filter_map(|key| <K as Form>::try_from_value(key).ok())
    }

}

impl<K: Form + Hash + Eq, V: Form> TypedMapView<K, V> {

    pub fn as_hash_map<S>(&self) -> HashMap<K, V> {
        let mut map = HashMap::new();
        for (key, value) in self.iter() {
            map.insert(key, value);
        }
        map
    }

}

impl<K: Form + Ord, V: Form> TypedMapView<K, V> {

    pub fn as_btree_map<S>(&self) -> BTreeMap<K, V> {
        let mut map = BTreeMap::new();
        for (key, value) in self.iter() {
            map.insert(key, value);
        }
        map
    }

}

impl<K: Form + Ord + Clone, V: Form + Clone> TypedMapView<K, V> {

    pub fn as_ord_map<S>(&self) -> OrdMap<K, V> {
        let mut map = OrdMap::new();
        for (key, value) in self.iter() {
            map.insert(key, value);
        }
        map
    }

}

impl<K: Form, V: Form> TryFrom<ViewWithEvent> for TypedViewWithEvent<K, V> {
    type Error = FormDeserializeErr;

    fn try_from(view: ViewWithEvent) -> Result<Self, Self::Error> {
        let ViewWithEvent {
            view, event
        } = view;
        let typed_view = TypedMapView::new(view);
        let typed_event = event.typed();
        typed_event.map(|ev| TypedViewWithEvent {
            view: typed_view,
            event: ev
        })
    }
}