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

use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;

use im::OrdMap;

use common::model::Value;
use form::Form;
use stm::local::TLocal;
use stm::stm::{left, right, Constant, Stm, UNIT};
use stm::transaction::{atomically, RetryManager, TransactionError};
use stm::var::TVar;
use summary::{clear_summary, remove_summary, update_summary};

use crate::agent::lane::map::summary::TransactionSummary;

mod summary;

pub struct MapLane<K, V> {
    map_state: TVar<OrdMap<Value, TVar<V>>>,
    summary: TVar<TransactionSummary<V>>,
    transaction_started: TLocal<bool>,
    _key_type: PhantomData<K>,
}

impl<K: Form, V: Any + Send + Sync> MapLane<K, V> {
    pub fn update_direct(&self, key: K, value: Arc<V>) -> DirectUpdate<'_, K, V> {
        DirectUpdate {
            lane: self,
            key,
            value,
        }
    }

    pub fn remove_direct(&self, key: K) -> DirectRemove<'_, K, V> {
        DirectRemove { lane: self, key }
    }

    pub fn get(&self, key: K) -> impl Stm<Result = Option<Arc<V>>> + '_ {
        let k = key.into_value();
        self.map_state.get().and_then(move |map| match map.get(&k) {
            Some(var) => left(var.get().map(Option::Some)),
            _ => right(Constant(None)),
        })
    }

    pub fn contains(&self, key: K) -> impl Stm<Result = bool> + '_ {
        let k = key.into_value();
        self.map_state.get().map(move |map| map.contains_key(&k))
    }

    pub fn len(&self) -> impl Stm<Result = usize> + '_ {
        self.map_state.get().map(move |map| map.len())
    }

    pub fn first(&self) -> impl Stm<Result = Option<Arc<V>>> + '_ {
        self.map_state
            .get()
            .and_then(move |map| match map.get_min() {
                Some((_, var)) => left(var.get().map(Option::Some)),
                _ => right(Constant(None)),
            })
    }

    pub fn last(&self) -> impl Stm<Result = Option<Arc<V>>> + '_ {
        self.map_state
            .get()
            .and_then(move |map| match map.get_max() {
                Some((_, var)) => left(var.get().map(Option::Some)),
                _ => right(Constant(None)),
            })
    }

    //pub fn snapshot(&self) -> impl Stm<Result = OrdMap<K, Arc<V>>> + '_ {
    //    unimplemented!()
    //}

    pub fn clear_direct(&self) -> DirectClear<'_, K, V> {
        DirectClear(self)
    }

    pub fn update(&self, key: K, value: Arc<V>) -> impl Stm<Result = ()> + '_ {
        let MapLane {
            map_state,
            summary,
            transaction_started,
            ..
        } = self;
        let key_value = key.into_value();
        let state_update = update_lane(map_state, key_value.clone(), value.clone());
        let apply_to_summary = update_summary(summary, key_value, value);
        let action = state_update.followed_by(apply_to_summary);

        compound_map_transaction(transaction_started, summary, action)
    }

    pub fn remove(&self, key: K) -> impl Stm<Result = ()> + '_ {
        let MapLane {
            map_state,
            summary,
            transaction_started,
            ..
        } = self;
        let key_value = key.into_value();
        let state_update = remove_lane(map_state, key_value.clone());
        let apply_to_summary = remove_summary(summary, key_value);
        let action = state_update.followed_by(apply_to_summary);

        compound_map_transaction(transaction_started, summary, action)
    }

    pub fn clear(&self) -> impl Stm<Result = ()> + '_ {
        let MapLane {
            map_state,
            summary,
            transaction_started,
            ..
        } = self;
        let state_update = clear_lane(map_state);
        let apply_to_summary = clear_summary(summary);
        let action = state_update.followed_by(apply_to_summary);

        compound_map_transaction(transaction_started, summary, action)
    }
}

fn compound_map_transaction<'a, S: Stm + 'a, V: Any + Send + Sync>(
    flag: &'a TLocal<bool>,
    summary: &'a TVar<TransactionSummary<V>>,
    then: S,
) -> impl Stm<Result = S::Result> + 'a {
    let action = Arc::new(then);
    flag.get().and_then(move |started| {
        if *started {
            left(action.clone())
        } else {
            let with_init = flag
                .put(true)
                .followed_by(summary.put(Default::default()))
                .followed_by(action.clone());
            right(with_init)
        }
    })
}

fn clear_lane<V: Any + Send + Sync>(content: &TVar<OrdMap<Value, V>>) -> impl Stm<Result = ()> {
    content.put(OrdMap::default())
}

fn update_lane<'a, V: Any + Send + Sync>(
    content: &'a TVar<OrdMap<Value, TVar<V>>>,
    key: Value,
    value: Arc<V>,
) -> impl Stm<Result = ()> + 'a {
    content.get().and_then(move |map| match map.get(&key) {
        Some(var) => left(var.put_arc(value.clone())),
        _ => {
            let var = TVar::from_arc(value.clone());
            let new_map = map.update(key.clone(), var);
            right(content.put(new_map))
        }
    })
}

fn remove_lane<'a, V: Any + Send + Sync>(
    content: &'a TVar<OrdMap<Value, TVar<V>>>,
    key: Value,
) -> impl Stm<Result = ()> + 'a {
    content.get().and_then(move |map| {
        if map.contains_key(&key) {
            let new_map = map.without(&key);
            left(content.put(new_map))
        } else {
            right(UNIT)
        }
    })
}

#[must_use = "Transactions do nothing if not executed."]
pub struct DirectUpdate<'a, K, V> {
    lane: &'a MapLane<K, V>,
    key: K,
    value: Arc<V>,
}

impl<'a, K, V> DirectUpdate<'a, K, V>
where
    K: Form + Send + Sync + 'a,
    V: Any + Send + Sync + 'a,
{
    fn into_stm(self) -> impl Stm<Result = ()> + 'a {
        let DirectUpdate { lane, key, value } = self;
        let key_value = key.into_value();
        let state_update = update_lane(&lane.map_state, key_value.clone(), value.clone());
        let set_summary = lane
            .summary
            .put(TransactionSummary::make_update(key_value, value));
        state_update.followed_by(set_summary)
    }

    pub async fn apply<R: RetryManager>(self, retries: R) -> Result<(), TransactionError> {
        let stm = self.into_stm();
        atomically(&stm, retries).await
    }
}

#[must_use = "Transactions do nothing if not executed."]
pub struct DirectRemove<'a, K, V> {
    lane: &'a MapLane<K, V>,
    key: K,
}

impl<'a, K, V> DirectRemove<'a, K, V>
where
    K: Form + Send + Sync + 'a,
    V: Any + Send + Sync + 'a,
{
    fn into_stm(self) -> impl Stm<Result = ()> + 'a {
        let DirectRemove { lane, key } = self;
        let key_value = key.into_value();
        let state_update = remove_lane(&lane.map_state, key_value.clone());
        let set_summary = lane
            .summary
            .put(TransactionSummary::make_removal(key_value));
        state_update.followed_by(set_summary)
    }

    pub async fn apply<R: RetryManager>(self, retries: R) -> Result<(), TransactionError> {
        let stm = self.into_stm();
        atomically(&stm, retries).await
    }
}

#[must_use = "Transactions do nothing if not executed."]
pub struct DirectClear<'a, K, V>(&'a MapLane<K, V>);

impl<'a, K, V> DirectClear<'a, K, V>
where
    K: Form + Send + Sync + 'a,
    V: Any + Send + Sync + 'a,
{
    fn into_stm(self) -> impl Stm<Result = ()> + 'a {
        let DirectClear(lane) = self;

        let state_update = clear_lane(&lane.map_state);
        let set_summary = lane.summary.put(TransactionSummary::clear());
        state_update.followed_by(set_summary)
    }

    pub async fn apply<R: RetryManager>(self, retries: R) -> Result<(), TransactionError> {
        let stm = self.into_stm();
        atomically(&stm, retries).await
    }
}
