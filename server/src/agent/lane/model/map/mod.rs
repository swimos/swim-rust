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

#[cfg(test)]
mod tests;

use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;

use im::OrdMap;

use common::model::Value;
use stm::local::TLocal;
use stm::stm::{abort, left, right, Constant, Stm, VecStm, UNIT};
use stm::transaction::{atomically, RetryManager, TransactionError};
use stm::var::TVar;
use summary::{clear_summary, remove_summary, update_summary};
use swim_form::{Form, FormDeserializeErr};

use crate::agent::lane::model::map::summary::TransactionSummary;
use crate::agent::lane::strategy::{Buffered, ChannelObserver, Dropping, Queue};
use crate::agent::lane::{BroadcastStream, InvalidForm, LaneModel};
use futures::stream::{iter, Flatten, Iter, StreamExt};
use futures::Stream;
use std::collections::HashMap;
use std::hash::Hash;
use stm::var::observer::StaticObserver;
use tokio::sync::{broadcast, mpsc, watch};
use utilities::future::{SwimStreamExt, Transform, TransformedStream};

mod summary;

/// A lane consisting of a map from keys to values.
pub struct MapLane<K, V> {
    // Transactional variable containing the current state of the map where the value of each entry
    // is stored in its own variable. The keys are stored as recon values as the map ordering
    // needs to be determined by the Ord implementation for the Value type, not any Ord
    // implementation that might be defined or K.
    map_state: TVar<OrdMap<Value, TVar<V>>>,
    // Transactional variable that records the effect that the transaction most recently executed
    // on the lane had.
    summary: TVar<TransactionSummary<Value, V>>,
    // A local variable to coordinate the update of the summary variable for compound transactions.
    transaction_started: TLocal<bool>,
    _key_type: PhantomData<K>,
}

impl<K, V> MapLane<K, V>
where
    K: Form + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub fn new() -> MapLane<K, V> {
        MapLane {
            map_state: Default::default(),
            summary: Default::default(),
            transaction_started: TLocal::new(false),
            _key_type: PhantomData,
        }
    }
}

impl<K, V> Default for MapLane<K, V>
where
    K: Form + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> LaneModel for MapLane<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    type Event = MapLaneEvent<K, V>;

    fn same_lane(this: &Self, other: &Self) -> bool {
        TVar::same_var(&this.map_state, &other.map_state)
    }
}

impl<K, V> Clone for MapLane<K, V> {
    fn clone(&self) -> Self {
        MapLane {
            map_state: self.map_state.clone(),
            summary: self.summary.clone(),
            transaction_started: self.transaction_started.clone(),
            _key_type: PhantomData,
        }
    }
}

/// Create a new map lane with the specified watch strategy.
pub fn make_lane_model<K, V, W>(watch: W) -> (MapLane<K, V>, W::View)
where
    K: Any + Send + Sync + Form,
    V: Any + Send + Sync,
    W: MapLaneWatch<K, V>,
{
    let (observer, view) = watch.make_watch();
    let summary = TVar::new_with_observer(Default::default(), observer);
    let transaction_started = TLocal::new(false);
    let lane = MapLane {
        map_state: Default::default(),
        summary,
        transaction_started,
        _key_type: PhantomData,
    };
    (lane, view)
}

/// A single event that occurred during a transaction.
#[derive(Debug, Clone)]
pub enum MapLaneEvent<K, V> {
    /// The map as cleared.
    Clear,
    /// An entry was updated.
    Update(K, Arc<V>),
    /// An entry was removed.
    Remove(K),
}

impl<V> MapLaneEvent<Value, V> {
    /// Attempt to type the key of a [`MapLaneEvent`] using a form.
    pub fn try_into_typed<K: Form>(self) -> Result<MapLaneEvent<K, V>, FormDeserializeErr> {
        match self {
            MapLaneEvent::Clear => Ok(MapLaneEvent::Clear),
            MapLaneEvent::Update(k, v) => Ok(MapLaneEvent::Update(K::try_convert(k)?, v)),
            MapLaneEvent::Remove(k) => Ok(MapLaneEvent::Remove(K::try_convert(k)?)),
        }
    }
}

/// Adapts a watch strategy for use with a [`ValueLane`].
pub trait MapLaneWatch<K, V> {
    /// The type of the observer to watch the value of the lane.
    type Obs: StaticObserver<Arc<TransactionSummary<Value, V>>> + Send + Sync + 'static;
    /// The type of the stream of values produced by the lane.
    type View: Stream<Item = MapLaneEvent<K, V>> + Send + Sync + 'static;

    /// Create a linked observer and view stream.
    fn make_watch(&self) -> (Self::Obs, Self::View);
}

type MpscEventStream<V> = Flatten<
    TransformedStream<mpsc::Receiver<Arc<TransactionSummary<Value, V>>>, SummaryToEvents<V>>,
>;

type WatchEventStream<V> = Flatten<
    TransformedStream<watch::Receiver<Arc<TransactionSummary<Value, V>>>, SummaryToEvents<V>>,
>;

type BroadcastEventStream<V> = Flatten<
    TransformedStream<BroadcastStream<Arc<TransactionSummary<Value, V>>>, SummaryToEvents<V>>,
>;

impl<K, V> MapLaneWatch<K, V> for Dropping
where
    K: Any + Send + Sync + Form,
    V: Any + Send + Sync,
{
    type Obs = ChannelObserver<watch::Sender<Arc<TransactionSummary<Value, V>>>>;
    type View = TransformedStream<WatchEventStream<V>, TypeEvents<K>>;

    fn make_watch(&self) -> (Self::Obs, Self::View) {
        let (tx, rx) = watch::channel(Default::default());
        let observer = ChannelObserver::new(tx);
        let str = rx
            .transform(SummaryToEvents::default())
            .flatten()
            .transform(TypeEvents::default());
        (observer, str)
    }
}

impl<K, V> MapLaneWatch<K, V> for Queue
where
    K: Any + Send + Sync + Form,
    V: Any + Send + Sync,
{
    type Obs = ChannelObserver<mpsc::Sender<Arc<TransactionSummary<Value, V>>>>;
    type View = TransformedStream<MpscEventStream<V>, TypeEvents<K>>;

    fn make_watch(&self) -> (Self::Obs, Self::View) {
        let Queue(n) = self;
        let (tx, rx) = mpsc::channel(n.get());
        let observer = ChannelObserver::new(tx);
        let str = rx
            .transform(SummaryToEvents::default())
            .flatten()
            .transform(TypeEvents::default());
        (observer, str)
    }
}

impl<K, V> MapLaneWatch<K, V> for Buffered
where
    K: Any + Send + Sync + Form,
    V: Any + Send + Sync,
{
    type Obs = ChannelObserver<broadcast::Sender<Arc<TransactionSummary<Value, V>>>>;
    type View = TransformedStream<BroadcastEventStream<V>, TypeEvents<K>>;

    fn make_watch(&self) -> (Self::Obs, Self::View) {
        let Buffered(n) = self;
        let (tx, rx) = broadcast::channel(n.get());
        let observer = ChannelObserver::new(tx);
        let str = BroadcastStream(rx)
            .transform(SummaryToEvents::default())
            .flatten()
            .transform(TypeEvents::default());
        (observer, str)
    }
}

/// [`Transform`] to break a transaction summary into a sequence of events.
pub struct SummaryToEvents<V>(PhantomData<fn(V) -> V>);

impl<V> Default for SummaryToEvents<V> {
    fn default() -> Self {
        SummaryToEvents(PhantomData)
    }
}

/// [`Transform`] to apply a form implementation to the keys of an untyped event.
pub struct TypeEvents<K>(PhantomData<fn(Value) -> K>);

impl<K> Default for TypeEvents<K> {
    fn default() -> Self {
        TypeEvents(PhantomData)
    }
}

impl<V> Transform<Arc<TransactionSummary<Value, V>>> for SummaryToEvents<V> {
    type Out = Iter<std::vec::IntoIter<MapLaneEvent<Value, V>>>;

    fn transform(&self, input: Arc<TransactionSummary<Value, V>>) -> Self::Out {
        iter(input.to_events().into_iter())
    }
}

impl<K: Form, V> Transform<MapLaneEvent<Value, V>> for TypeEvents<K> {
    type Out = MapLaneEvent<K, V>;

    fn transform(&self, input: MapLaneEvent<Value, V>) -> Self::Out {
        input.try_into_typed().expect("Key form is inconsistent.")
    }
}

impl<K: Form, V: Any + Send + Sync> MapLane<K, V> {
    /// Updates (or inserts) the value of an entry in the map, in a transaction. This is more
    /// efficient than `update` but cannot be composed into a larger transaction.
    pub fn update_direct(&self, key: K, value: Arc<V>) -> DirectUpdate<'_, K, V> {
        DirectUpdate {
            lane: self,
            key,
            value,
        }
    }

    /// Modifies the value of an entry in the map, in a transaction. This is more efficient than
    /// a compound of `get`,  `update` and `remove` transactions.
    pub fn modify_direct<'a, F>(&'a self, key: K, f: F) -> DirectModify<'a, K, V, F>
    where
        F: Fn(Option<&V>) -> Option<V> + Send + Sync + Clone + 'a,
    {
        DirectModify { lane: self, key, f }
    }

    /// Modifies the value of an entry in the map, in a transaction. This is more efficient than
    /// a compound of `get` and `update` transactions.
    pub fn modify_if_defined_direct<'a, F>(
        &'a self,
        key: K,
        f: F,
    ) -> DirectModifyDefined<'a, K, V, F>
    where
        F: Fn(&V) -> V + Send + Sync + Clone + 'a,
    {
        DirectModifyDefined { lane: self, key, f }
    }

    /// Removes an entry in the map, in a transaction. This is more efficient than `remove` but
    /// cannot be composed into a larger transaction.
    pub fn remove_direct(&self, key: K) -> DirectRemove<'_, K, V> {
        DirectRemove { lane: self, key }
    }

    /// Clears the map, in a transaction. This is more efficient than `clear` but cannot be
    /// composed into a larger transaction.
    pub fn clear_direct(&self) -> DirectClear<'_, K, V> {
        DirectClear(self)
    }

    /// Get the value associated with a key in the map, in a transaction.
    pub fn get(&self, key: K) -> impl Stm<Result = Option<Arc<V>>> + '_ {
        let k = key.into_value();
        self.map_state.get().and_then(move |map| match map.get(&k) {
            Some(var) => left(var.get().map(Option::Some)),
            _ => right(Constant(None)),
        })
    }

    /// Determine if a key is contained in the map, in a transaction.
    pub fn contains(&self, key: K) -> impl Stm<Result = bool> + '_ {
        let k = key.into_value();
        self.map_state.get().map(move |map| map.contains_key(&k))
    }

    /// Get the number of entries in the map, in a transaction.
    pub fn len(&self) -> impl Stm<Result = usize> + '_ {
        self.map_state.get().map(move |map| map.len())
    }

    /// Determine if the map is empty, in a transaction.
    pub fn is_empty(&self) -> impl Stm<Result = bool> + '_ {
        self.map_state.get().map(move |map| map.is_empty())
    }

    /// Get the value associated with the first key in the map, in a transaction.
    pub fn first(&self) -> impl Stm<Result = Option<Arc<V>>> + '_ {
        self.map_state
            .get()
            .and_then(move |map| match map.get_min() {
                Some((_, var)) => left(var.get().map(Option::Some)),
                _ => right(Constant(None)),
            })
    }

    /// Get the value associated with the last key in the map, in a transaction.
    pub fn last(&self) -> impl Stm<Result = Option<Arc<V>>> + '_ {
        self.map_state
            .get()
            .and_then(move |map| match map.get_max() {
                Some((_, var)) => left(var.get().map(Option::Some)),
                _ => right(Constant(None)),
            })
    }

    /// Update (or insert) the value associated with a key in the map, in a transaction. If this
    /// does not need to be composed into a larger transaction, `update_direct` is more efficient.
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

    /// Remove an entry from the map, in a transaction. If this does not need to be composed into a
    /// larger transaction, `remove_direct` is more efficient.
    pub fn remove(&self, key: K) -> impl Stm<Result = ()> + '_ {
        let MapLane {
            map_state,
            summary,
            transaction_started,
            ..
        } = self;
        let key_value = key.into_value();
        let state_update = remove_lane(map_state, key_value.clone());

        let action = state_update.and_then(move |did_remove| {
            if did_remove {
                let apply_to_summary = remove_summary(summary, key_value.clone());
                left(apply_to_summary)
            } else {
                right(UNIT)
            }
        });

        compound_map_transaction(transaction_started, summary, action)
    }

    /// Clear the map, in a transaction. If this does not need to be composed into a larger
    /// transaction, `clear_direct` is more efficient.
    pub fn clear(&self) -> impl Stm<Result = ()> + '_ {
        let MapLane {
            map_state,
            summary,
            transaction_started,
            ..
        } = self;
        let state_update = clear_lane(map_state);
        let action = state_update.and_then(move |did_clear| {
            if did_clear {
                let apply_to_summary = clear_summary(summary);
                left(apply_to_summary)
            } else {
                right(UNIT)
            }
        });

        compound_map_transaction(transaction_started, summary, action)
    }
}

impl<K, V> MapLane<K, V>
where
    K: Form + Hash + Eq + Clone + Send + Sync,
    V: Any + Send + Sync,
{
    /// Transactionally snapshot the state of the lane.
    pub fn snapshot(&self) -> impl Stm<Result = HashMap<K, Arc<V>>> + '_ {
        self.map_state.get().and_then(|map| {
            let entry_stms = map
                .iter()
                .map(|(key, var)| {
                    let key_copy = key.clone();
                    var.get().map(move |value| (key_copy.clone(), value))
                })
                .collect::<Vec<_>>();
            let all = VecStm::new(entry_stms);
            all.and_then(|entries| {
                let map = entries
                    .into_iter()
                    .try_fold(HashMap::new(), |mut state, (k, v)| {
                        K::try_convert(k).map(move |key| {
                            state.insert(key, v);
                            state
                        })
                    });
                match map {
                    Ok(m) => left(Constant(m)),
                    Err(e) => right(abort(InvalidForm(e))),
                }
            })
        })
    }
}

//Compound transactions that mutate the state of the map lane must be coordinated to ensure that
//the value in the summary variable reflects everything that happened within the transaction but
//nothing else. This is achieved using a single boolean flag. The flag starts as false and the
//first mutating action sets it to true and clears the value of the summary. Then it, and all
//subsequent mutating actions, contribute their effect into the summary.
fn compound_map_transaction<'a, S: Stm + 'a, V: Any + Send + Sync>(
    flag: &'a TLocal<bool>,
    summary: &'a TVar<TransactionSummary<Value, V>>,
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

//Clears the underlying map state.
fn clear_lane<'a, V: Any + Send + Sync>(
    content: &'a TVar<OrdMap<Value, V>>,
) -> impl Stm<Result = bool> + 'a {
    content.get().and_then(move |map| {
        if map.is_empty() {
            left(Constant(false))
        } else {
            right(content.put(OrdMap::default()).followed_by(Constant(true)))
        }
    })
}

//Applies an update to the underlying map state.
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

//Applies a removal to the underlying map state.
fn remove_lane<'a, V: Any + Send + Sync>(
    content: &'a TVar<OrdMap<Value, TVar<V>>>,
    key: Value,
) -> impl Stm<Result = bool> + 'a {
    content.get().and_then(move |map| {
        if map.contains_key(&key) {
            let new_map = map.without(&key);
            left(content.put(new_map).followed_by(Constant(true)))
        } else {
            right(Constant(false))
        }
    })
}

/// Returned by the `update_direct` method of [`MapLane`]. This wraps the update transaction
/// so that it can only be executed and not combined into a larger transaction.
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

    /// Executes the update as a transaction.
    pub async fn apply<R: RetryManager>(self, retries: R) -> Result<(), TransactionError> {
        let stm = self.into_stm();
        atomically(&stm, retries).await
    }
}

/// Returned by the `rmove_direct` method of [`MapLane`]. This wraps the removal transaction
/// so that it can only be executed and not combined into a larger transaction.
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
        state_update.and_then(move |did_remove| {
            if did_remove {
                let set_summary = lane
                    .summary
                    .put(TransactionSummary::make_removal(key_value.clone()));
                left(set_summary)
            } else {
                right(UNIT)
            }
        })
    }

    /// Executes the removal as a transaction.
    pub async fn apply<R: RetryManager>(self, retries: R) -> Result<(), TransactionError> {
        let stm = self.into_stm();
        atomically(&stm, retries).await
    }
}

/// Returned by the `clear_direct` method of [`MapLane`]. This wraps the clear transaction
/// so that it can only be executed and not combined into a larger transaction.
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
        state_update.and_then(move |did_clear| {
            if did_clear {
                let set_summary = lane.summary.put(TransactionSummary::clear());
                left(set_summary)
            } else {
                right(UNIT)
            }
        })
    }

    /// Executes the clear as a transaction.
    pub async fn apply<R: RetryManager>(self, retries: R) -> Result<(), TransactionError> {
        let stm = self.into_stm();
        atomically(&stm, retries).await
    }
}

/// Returned by the `modify_direct` method of [`MapLane`]. This wraps the modification transaction
/// so that it can only be executed and not combined into a larger transaction.
#[must_use = "Transactions do nothing if not executed."]
pub struct DirectModify<'a, K, V, F> {
    lane: &'a MapLane<K, V>,
    key: K,
    f: F,
}

impl<'a, K, V, F> DirectModify<'a, K, V, F>
where
    K: Form + Send + Sync + 'a,
    V: Any + Send + Sync + 'a,
    F: Fn(Option<&V>) -> Option<V> + Send + Sync + Clone + 'a,
{
    fn into_stm(self) -> impl Stm<Result = ()> + 'a {
        let DirectModify { lane, key, f } = self;
        let key_value = key.into_value();

        lane.map_state.get().and_then(move |map| {
            let entry: Option<TVar<V>> = map.get(&key_value).map(Clone::clone);
            match entry {
                Some(var) => {
                    let k = key_value.clone();
                    let f = f.clone();
                    let upd = var.get().and_then(move |old| match f(Some(old.as_ref())) {
                        Some(new) => {
                            let arc_v = Arc::new(new);
                            let set_summary = lane
                                .summary
                                .put(TransactionSummary::make_update(k.clone(), arc_v.clone()));
                            left(var.put_arc(arc_v).followed_by(set_summary))
                        }
                        _ => {
                            let set_summary = lane
                                .summary
                                .put(TransactionSummary::make_removal(k.clone()));
                            right(lane.map_state.put(map.without(&k)).followed_by(set_summary))
                        }
                    });
                    left(upd)
                }
                _ => match f(None) {
                    Some(v) => {
                        let arc_v = Arc::new(v);
                        let new_var = TVar::from_arc(arc_v.clone());
                        let set_summary = lane
                            .summary
                            .put(TransactionSummary::make_update(key_value.clone(), arc_v));
                        right(left(
                            lane.map_state
                                .put(map.update(key_value.clone(), new_var))
                                .followed_by(set_summary),
                        ))
                    }
                    _ => right(right(UNIT)),
                },
            }
        })
    }

    /// Executes the modification as a transaction.
    pub async fn apply<R: RetryManager>(self, retries: R) -> Result<(), TransactionError> {
        let stm = self.into_stm();
        atomically(&stm, retries).await
    }
}

/// Returned by the `modify_direct_if_defined` method of [`MapLane`]. This wraps the modification
/// transaction so that it can only be executed and not combined into a larger transaction.
#[must_use = "Transactions do nothing if not executed."]
pub struct DirectModifyDefined<'a, K, V, F> {
    lane: &'a MapLane<K, V>,
    key: K,
    f: F,
}

impl<'a, K, V, F> DirectModifyDefined<'a, K, V, F>
where
    K: Form + Send + Sync + 'a,
    V: Any + Send + Sync + 'a,
    F: Fn(&V) -> V + Send + Sync + Clone + 'a,
{
    fn into_stm(self) -> impl Stm<Result = ()> + 'a {
        let DirectModifyDefined { lane, key, f } = self;
        let key_value = key.into_value();

        lane.map_state.get().and_then(move |map| {
            let entry: Option<TVar<V>> = map.get(&key_value).map(Clone::clone);
            match entry {
                Some(var) => {
                    let k = key_value.clone();
                    let f = f.clone();
                    let upd = var.get().and_then(move |old| {
                        let f = f.clone();
                        let new = f(old.as_ref());
                        let arc_v = Arc::new(new);
                        let set_summary = lane
                            .summary
                            .put(TransactionSummary::make_update(k.clone(), arc_v.clone()));
                        var.put_arc(arc_v).followed_by(set_summary)
                    });
                    left(upd)
                }
                _ => right(UNIT),
            }
        })
    }

    /// Executes the modification as a transaction.
    pub async fn apply<R: RetryManager>(self, retries: R) -> Result<(), TransactionError> {
        let stm = self.into_stm();
        atomically(&stm, retries).await
    }
}
