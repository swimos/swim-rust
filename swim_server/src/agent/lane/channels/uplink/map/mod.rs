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
use either::Either;
use futures::stream::{unfold, FusedStream};
use futures::{select_biased, FutureExt, StreamExt};
use futures::{Stream, TryFutureExt};
use futures_util::stream::FuturesUnordered;
use im::OrdMap;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use stm::transaction;
use stm::transaction::{RetryManager, TransactionError};
use stm::var::TVar;
use swim_common::model::Value;
use swim_form::{Form, FormDeserializeErr};
use tracing::{event, span, Level};
use tracing_futures::Instrument;
use utilities::sync::trigger;

#[derive(Debug)]
pub enum MapLaneSyncError {
    FailedTransaction(TransactionError),
    InconsistentForm(FormDeserializeErr),
}

type EventResult<K, V> = Result<MapLaneEvent<K, V>, MapLaneSyncError>;
type Checkpoint<V> = OrdMap<Value, TVar<V>>;

/// State type for the state machine that tracks the synchronization of a [`MapLane`]. The
/// future types `CF` and `GF` are not actually free, however the actual types are not
/// nameable and it must be left to the compiler to infer them.
///
/// #Type Parameters
///
/// * `R` - Type of the retry strategy for the checkpoint transaction.
/// * `CF` - Type of the future that executes the checkpoint transaction against the lane.
/// * `GF` - Type of the futures that read a single value from the map with a kill switch that
/// allows them to be cancelled if they become irrelevant.
enum MapLaneSyncState<R, CF, GF> {
    /// Initial state, containing the retry strategy for initiating the checkpoint transaction.
    Init(R),
    /// State for while the checkpoint transaction is executing.
    Checkpointing(CF),
    /// Waiting to observe a complete, consistent view of the map, after observing the checkpoint.
    Awaiting {
        /// Whether the checkpoint marker has been observed in the event stream.
        seen_marker: bool,
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
    seen_marker: bool,
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
        seen_marker,
        pending,
        cancellers,
    }
}

const CHECKPOINTING: &str = "Checkpointing map lane.";
const OBSERVED_CHECKPOINT: &str = "Observed checkpoint coordination marker.";
const OBTAINED_CHECKPOINT: &str = "Obtained checkpoint of map structure.";
const CHECKPOINT_FAILED: &str = "Checkpoint transaction failed to complete.";
const LANE_STOPPED: &str = "Lane stopped providing events during synchronization.";
const SYNC_COMPLETE: &str = "Map lane synchronization complete.";
const ENTRY_PREEMPTED: &str = "The value for key was preempted.";
const MAP_CLEARED: &str = "The map was cleared whilst synchronizing.";

//Move from the initial state, initiating a checkpoint transaction.
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
    .instrument(span!(Level::TRACE, CHECKPOINTING))
}

// Handle an event from the lane, cancelling the load of any values that become irrelevant.
fn handle_event<K, V>(event: &MapLaneEvent<K, V>, cancellers: &mut HashMap<Value, trigger::Sender>)
where
    K: Form + Send + Sync + Debug,
    V: Any + Send + Sync,
{
    match event {
        MapLaneEvent::Update(k, _) | MapLaneEvent::Remove(k) => {
            event!(Level::TRACE, message = ENTRY_PREEMPTED, key = ?k);
            let key_as_value = Form::as_value(k);
            if let Some(tx) = cancellers.remove(&key_as_value) {
                tx.trigger();
            }
        }
        MapLaneEvent::Clear => {
            event!(Level::TRACE, message = MAP_CLEARED);
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
///
/// #Notes
///
/// The naive way to synchronize a map lane would be to execute a `snapshot` transaction against
/// the lane, loading the underlying map of variables and all of the values in a single transaction.
/// However, as this reads a very large number of [`TVar`]s, when the lane is under high load, this
/// transaction could take a very long time to complete or fail entirely. To avoid this, we employ
/// the following scheme:
///
/// 1. When a Sync is requested, a special `checkpoint` transaction is executed against the lane.
/// This reads the structure of the map and causes a special checkpoint [`MapLaneEvent`] to be
/// emitted on the event stream of the lane.
/// 2. After executing the `checkpoint` transaction, the Sync state machine waits for the
/// checkpoint indicator (containing its unique ID) on the event stream. It now knows that the
/// map of variables that it obtained in the transaction is consistent with its current offset
/// in the event stream.
/// 3. Now it starts concurrent, asynchronous reads of all of the variables in the map that it
/// received, adding a mechanism for them to be cancelled. Simultaneously, it continues to watch
/// the event stream.
/// 4. Then it races the reads of the variables with information received on the event stream until
/// it is aware of the state of the value for every key in the map. For example, if it is waiting
/// for the value associated with a key `k` and that key is later removed it will cancel that read.
/// In the special case that the map is cleared, all of the reads will be cancelled.
/// 5. As it receives evidence about the value for each key, records are emitted on Sync stream which
/// then terminates when a complete, consistent view of the map has been emitted.
pub fn sync_map_lane<'a, K, V, Events, Retries>(
    id: u64,
    lane: &'a MapLane<K, V>,
    events: &'a mut Events,
    retry: Retries,
) -> impl Stream<Item = EventResult<K, V>> + 'a
where
    K: Form + Send + Sync + Debug + 'static,
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
                            event!(Level::TRACE, message = OBSERVED_CHECKPOINT, id = cp_id);
                            match chk_fut.await {
                                Ok(cp) => {
                                    event!(Level::TRACE, message = OBTAINED_CHECKPOINT, id, checkpoint = ?cp);
                                    from_checkpoint(true, cp)
                                }
                                Err(error) => {
                                    event!(Level::ERROR, message = CHECKPOINT_FAILED, id, ?error);
                                    break MapLaneSyncState::Complete.yield_error(events, error);
                                }
                            }
                        }
                        Either::Left(Some(ev)) => {
                            break MapLaneSyncState::Checkpointing(chk_fut).yield_event(events, ev);
                        }
                        Either::Left(_) => {
                            //No more events.
                            event!(Level::WARN, message = LANE_STOPPED, id);
                            break None;
                        }
                        Either::Right(Ok(cp)) => {
                            event!(Level::TRACE, message = OBTAINED_CHECKPOINT, id, checkpoint = ?cp);
                            from_checkpoint(false, cp)
                        }
                        Either::Right(Err(error)) => {
                            event!(Level::ERROR, message = CHECKPOINT_FAILED, id, ?error);
                            break MapLaneSyncState::Complete.yield_error(events, error);
                        }
                    }
                }
                MapLaneSyncState::Awaiting {
                    seen_marker: false,
                    pending,
                    cancellers,
                } => match events.next().await {
                    Some(MapLaneEvent::Checkpoint(cp_id)) if cp_id == id => {
                        event!(Level::TRACE, message = OBSERVED_CHECKPOINT, id = cp_id);
                        MapLaneSyncState::Awaiting {
                            seen_marker: true,
                            pending,
                            cancellers,
                        }
                    }
                    Some(event) => {
                        let new_state = MapLaneSyncState::Awaiting {
                            seen_marker: false,
                            pending,
                            cancellers,
                        };
                        break 'outer new_state.yield_event(events, event);
                    }
                    _ => {
                        event!(Level::WARN, message = LANE_STOPPED, id);
                        break 'outer None;
                    }
                },
                MapLaneSyncState::Awaiting {
                    mut pending,
                    mut cancellers,
                    ..
                } => loop {
                    let next = select_biased! {
                        maybe_loaded = pending.next() => Either::Right(maybe_loaded),
                        maybe_event = events.next() => Either::Left(maybe_event),
                    };
                    match next {
                        Either::Left(Some(event)) => {
                            handle_event(&event, &mut cancellers);
                            let new_state = if cancellers.is_empty() {
                                event!(Level::TRACE, message = SYNC_COMPLETE, id);
                                MapLaneSyncState::Complete
                            } else {
                                MapLaneSyncState::Awaiting {
                                    seen_marker: true,
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
                                        seen_marker: true,
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
                            event!(Level::WARN, message = LANE_STOPPED, id);
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
