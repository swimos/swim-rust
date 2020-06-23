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

use common::model::Value;
use swim_form::{Form, FormDeserializeErr};
use im::HashMap;
use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;
use stm::stm::Stm;
use stm::var::TVar;

#[derive(Debug)]
pub enum EntryModification<V> {
    Update(Arc<V>),
    Remove,
}

impl<V> Clone for EntryModification<V> {
    fn clone(&self) -> Self {
        match self {
            EntryModification::Update(v) => EntryModification::Update(v.clone()),
            EntryModification::Remove => EntryModification::Remove,
        }
    }
}

#[derive(Debug)]
pub struct TransactionSummary<K: Hash + Eq, V> {
    clear: bool,
    changes: HashMap<K, EntryModification<V>>,
}

pub enum MapLaneEvent<K, V> {
    Clear,
    Update(K, Arc<V>),
    Remove(K),
}

impl<V> MapLaneEvent<Value, V> {
    pub fn try_into_typed<K: Form>(self) -> Result<MapLaneEvent<K, V>, FormDeserializeErr> {
        match self {
            MapLaneEvent::Clear => Ok(MapLaneEvent::Clear),
            MapLaneEvent::Update(k, v) => Ok(MapLaneEvent::Update(K::try_convert(k)?, v)),
            MapLaneEvent::Remove(k) => Ok(MapLaneEvent::Remove(K::try_convert(k)?)),
        }
    }
}

impl<K: Hash + Eq + Clone, V> TransactionSummary<K, V> {
    pub fn to_events(&self) -> Vec<MapLaneEvent<K, V>> {
        let TransactionSummary { clear, changes } = self;
        let mut n = changes.len();
        if *clear {
            n += 1;
        }
        let mut events = Vec::with_capacity(n);
        if *clear {
            events.push(MapLaneEvent::Clear);
        }
        for (k, modification) in changes.iter() {
            let key = k.clone();
            let event = match modification {
                EntryModification::Update(v) => MapLaneEvent::Update(key, v.clone()),
                EntryModification::Remove => MapLaneEvent::Remove(key),
            };
            events.push(event);
        }
        events
    }
}

pub fn clear_summary<V: Any + Send + Sync>(
    summary: &TVar<TransactionSummary<Value, V>>,
) -> impl Stm<Result = ()> {
    summary.put(TransactionSummary::clear())
}

pub fn update_summary<'a, V: Any + Send + Sync>(
    summary: &'a TVar<TransactionSummary<Value, V>>,
    key: Value,
    value: Arc<V>,
) -> impl Stm<Result = ()> + 'a {
    summary
        .get()
        .and_then(move |sum| summary.put(sum.update(key.clone(), value.clone())))
}

pub fn remove_summary<'a, V: Any + Send + Sync>(
    summary: &'a TVar<TransactionSummary<Value, V>>,
    key: Value,
) -> impl Stm<Result = ()> + 'a {
    summary
        .get()
        .and_then(move |sum| summary.put(sum.remove(key.clone())))
}

impl<V> TransactionSummary<Value, V> {
    pub fn clear() -> Self {
        TransactionSummary {
            clear: true,
            changes: Default::default(),
        }
    }

    pub fn make_update(key: Value, value: Arc<V>) -> Self {
        let mut map = HashMap::new();
        map.insert(key, EntryModification::Update(value));
        TransactionSummary {
            clear: false,
            changes: map,
        }
    }

    pub fn make_removal(key: Value) -> Self {
        let mut map = HashMap::new();
        map.insert(key, EntryModification::Remove);
        TransactionSummary {
            clear: false,
            changes: map,
        }
    }

    fn update(&self, key: Value, value: Arc<V>) -> Self {
        let TransactionSummary { clear, changes } = self;
        TransactionSummary {
            clear: *clear,
            changes: changes.update(key, EntryModification::Update(value)),
        }
    }

    fn remove(&self, key: Value) -> Self {
        let TransactionSummary { clear, changes } = self;
        TransactionSummary {
            clear: *clear,
            changes: changes.update(key, EntryModification::Remove),
        }
    }
}

impl<K: Hash + Eq, V> Default for TransactionSummary<K, V> {
    fn default() -> Self {
        TransactionSummary {
            clear: false,
            changes: HashMap::default(),
        }
    }
}
