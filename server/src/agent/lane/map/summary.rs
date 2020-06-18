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
use common::model::Value;
use im::HashMap;
use stm::var::TVar;
use stm::stm::Stm;
use std::any::Any;

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
pub struct TransactionSummary<V> {
    clear: bool,
    changes: HashMap<Value, EntryModification<V>>,
}

pub fn clear_summary<V: Any + Send + Sync>(
    summary: &TVar<TransactionSummary<V>>,
) -> impl Stm<Result = ()> {
    summary.put(TransactionSummary::clear())
}

pub fn update_summary<'a, V: Any + Send + Sync>(
    summary: &'a TVar<TransactionSummary<V>>,
    key: Value,
    value: Arc<V>,
) -> impl Stm<Result = ()> + 'a {
    summary
        .get()
        .and_then(move |sum| summary.put(sum.update(key.clone(), value.clone())))
}

pub fn remove_summary<'a, V: Any + Send + Sync>(
    summary: &'a TVar<TransactionSummary<V>>,
    key: Value,
) -> impl Stm<Result = ()> + 'a {
    summary
        .get()
        .and_then(move |sum| summary.put(sum.remove(key.clone())))
}


impl<V> TransactionSummary<V> {
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

impl<V> Default for TransactionSummary<V> {
    fn default() -> Self {
        TransactionSummary {
            clear: false,
            changes: HashMap::default(),
        }
    }
}
