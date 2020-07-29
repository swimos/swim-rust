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

use crate::agent::lane::model::map::{MapLane, MapLaneEvent};
use common::model::Value;
use either::Either;
use futures::stream::{unfold, FusedStream};
use futures::{select_biased, FutureExt, StreamExt};
use futures::{Stream, TryFutureExt};
use futures_util::stream::FuturesUnordered;
use im::OrdMap;
use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use stm::transaction;
use stm::transaction::{RetryManager, TransactionError};
use stm::var::TVar;
use swim_form::{Form, FormDeserializeErr};
use utilities::sync::trigger;

pub enum MapLaneSyncError {
    FailedTransaction(TransactionError),
    InconsistentForm(FormDeserializeErr),
}

type EventResult<K, V> = Result<MapLaneEvent<K, V>, MapLaneSyncError>;
type Checkpoint<V> = OrdMap<Value, TVar<V>>;

/// State type for the state machine that tracks the synchronization of a [`MapLane`]. The
/// future types `CF` and `GF` are not actually free, however the actual types are not
/// nameable and it must be left to the compiler to infer them.
enum MapLaneSyncState<R, CF, GF> {
    /// Initial state, containing the retry strategy for initiating the checkpoint transaction.
    Init(R),
    /// State for while the checkpoint transaction is executing.
    Checkpointing(CF),
    /// Waiting to observe a complete, consistent view of the map, after observing the checkpoint.
    Awaiting {
        /// Pending retrievals of values from the map.
        pending: FuturesUnordered<GF>,
        /// Triggers to allow us to cancel futures in `pending` if we no longer need th results.
        cancellers: HashMap<Value, trigger::Sender>,
    },
    /// Final state which will cause the stream to terminate the next time it is polled.
    Complete,
}

type UnfoldResult<'a, R, Ev, State> = Option<(R, (&'a mut Ev, State))>;

impl<R, CF, GF> MapLaneSyncState<R, CF, GF> {
    //Create a return value for the unfold function causing the stream to yield a map event.
    fn yield_event<K, V, Ev>(
        self,
        events: &mut Ev,
        event: MapLaneEvent<K, V>,
    ) -> UnfoldResult<EventResult<K, V>, Ev, Self> {
        Some((Ok(event), (events, self)))
    }

    // Create a return value for an unfold function causing the stream to terminate with an error.
    fn yield_error<K, V, Ev>(
        self,
        events: &mut Ev,
        error: MapLaneSyncError,
    ) -> UnfoldResult<EventResult<K, V>, Ev, Self> {
        Some((Err(error), (events, self)))
    }
}

// Update the state from a checkpoint (map from keys in the map the the variables holding the values).
fn from_checkpoint<'a, Retries, V, CheckpointFut>(
    checkpoint: Checkpoint<V>,
) -> MapLaneSyncState<Retries, CheckpointFut, impl Future<Output = Option<(Value, Arc<V>)>> + 'a>
where
    V: Any + Send + Sync,
{
    let mut cancellers = HashMap::with_capacity(checkpoint.len());
    let pending = FuturesUnordered::new();
    for (key, var) in checkpoint.into_iter() {
        let (tx, rx) = trigger::trigger();
        let key_copy = key.clone();
        let fut = async move {
            let var: TVar<V> = var;
            select_biased! {
                _ = rx.fuse() => None,
                v = var.load().fuse() => Some((key_copy, v)),
            }
        };
        cancellers.insert(key, tx);
        pending.push(fut);
    }
    MapLaneSyncState::Awaiting {
        pending,
        cancellers,
    }
}

//Move from the initial state, initiatig a checkpoint transaction.
fn initialize<'a, K, V, Retries>(
    id: u64,
    lane: &'a MapLane<K, V>,
    retry: Retries,
) -> impl Future<Output = Result<Checkpoint<V>, MapLaneSyncError>> + 'a
where
    K: Form + Send + Sync,
    V: Any + Send + Sync,
    Retries: RetryManager + 'static,
{
    let chk_stm = lane.checkpoint(id);
    async move {
        let chk_stm = chk_stm;
        transaction::atomically(&chk_stm, retry).await
    }
    .map_err(MapLaneSyncError::FailedTransaction)
}

// Handle an event from the lane, cancelling the load of any values that become irrelevant.
fn handle_event<K, V>(event: &MapLaneEvent<K, V>, cancellers: &mut HashMap<Value, trigger::Sender>)
where
    K: Form + Send + Sync,
    V: Any + Send + Sync,
{
    match event {
        MapLaneEvent::Update(k, _) | MapLaneEvent::Remove(k) => {
            let key_as_value = Form::as_value(k);
            if let Some(tx) = cancellers.remove(&key_as_value) {
                tx.trigger();
            }
        }
        MapLaneEvent::Clear => {
            for (_, tx) in cancellers.drain() {
                tx.trigger();
            }
        }
        _ => {}
    }
}

/// Run the sync state machine for a map lane. The returned stream will emit updates to the lane
/// until the synchronization process is complete, after which it will terminate. When this stream
/// terminates, the sync message can be sent to the consumer.
pub fn sync_map_lane<'a, K, V, Events, Retries>(
    id: u64,
    lane: &'a MapLane<K, V>,
    events: &'a mut Events,
    retry: Retries,
) -> impl Stream<Item = EventResult<K, V>> + 'a
where
    K: Form + Send + Sync + 'static,
    V: Any + Send + Sync,
    Events: FusedStream<Item = MapLaneEvent<K, V>> + Unpin,
    Retries: RetryManager + 'static,
{
    let init = (events, MapLaneSyncState::Init(retry));

    unfold(init, move |(events, mut state)| async move {
        'outer: loop {
            state = match state {
                MapLaneSyncState::Init(retries) => {
                    MapLaneSyncState::Checkpointing(Box::pin(initialize(id, lane, retries).fuse()))
                }
                MapLaneSyncState::Checkpointing(mut chk_fut) => {
                    let event_or_cp = select_biased! {
                        maybe_event = events.next() => Either::Left(maybe_event),
                        cp = &mut chk_fut => Either::Right(cp),
                    };
                    match event_or_cp {
                        Either::Left(Some(MapLaneEvent::Checkpoint(cp_id))) if cp_id == id => {
                            match chk_fut.await {
                                Ok(cp) => from_checkpoint(cp),
                                Err(err) => {
                                    break MapLaneSyncState::Complete.yield_error(events, err);
                                }
                            }
                        }
                        Either::Left(Some(ev)) => {
                            break MapLaneSyncState::Checkpointing(chk_fut).yield_event(events, ev);
                        }
                        Either::Left(_) => {
                            //No more events.
                            break None;
                        }
                        Either::Right(Ok(cp)) => from_checkpoint(cp),
                        Either::Right(Err(err)) => {
                            break MapLaneSyncState::Complete.yield_error(events, err);
                        }
                    }
                }
                MapLaneSyncState::Awaiting {
                    mut pending,
                    mut cancellers,
                } => loop {
                    let next = select_biased! {
                        maybe_loaded = pending.next() => Either::Right(maybe_loaded),
                        maybe_event = events.next() => Either::Left(maybe_event),
                    };
                    match next {
                        Either::Left(Some(event)) => {
                            handle_event(&event, &mut cancellers);
                            let new_state = if cancellers.is_empty() {
                                MapLaneSyncState::Complete
                            } else {
                                MapLaneSyncState::Awaiting {
                                    pending,
                                    cancellers,
                                }
                            };
                            break 'outer new_state.yield_event(events, event);
                        }
                        Either::Right(Some(Some((key, value)))) => {
                            cancellers.remove(&key);
                            let result = match Form::try_convert(key) {
                                Ok(typed_key) => {
                                    let event = MapLaneEvent::Update(typed_key, value);
                                    let new_state = MapLaneSyncState::Awaiting {
                                        pending,
                                        cancellers,
                                    };
                                    new_state.yield_event(events, event)
                                }
                                Err(err) => MapLaneSyncState::Complete
                                    .yield_error(events, MapLaneSyncError::InconsistentForm(err)),
                            };
                            break 'outer result;
                        }
                        Either::Left(None) | Either::Right(None) => {
                            break 'outer None;
                        }
                        _ => {}
                    }
                },
                MapLaneSyncState::Complete => {
                    break None;
                }
            }
        }
    })
}
