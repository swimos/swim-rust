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

type TResult<T> = Result<T, MapLaneSyncError>;
type Checkpoint<V> = OrdMap<Value, TVar<V>>;

enum MapLaneSyncState<Retries, CheckpointFut, GetValueFut> {
    Init(Retries),
    Checkpointing(CheckpointFut),
    Awaiting {
        pending: FuturesUnordered<GetValueFut>,
        cancellers: HashMap<Value, trigger::Sender>,
    },
    Complete,
}

fn yield_event<R, CF, GF, Ev, K, V>(
    state: MapLaneSyncState<R, CF, GF>,
    events: &mut Ev,
    event: MapLaneEvent<K, V>,
) -> Option<(
    TResult<MapLaneEvent<K, V>>,
    (&mut Ev, MapLaneSyncState<R, CF, GF>),
)> {
    Some((Ok(event), (events, state)))
}

fn yield_error<R, CF, GF, Ev, K, V>(
    state: MapLaneSyncState<R, CF, GF>,
    events: &mut Ev,
    error: MapLaneSyncError,
) -> Option<(
    TResult<MapLaneEvent<K, V>>,
    (&mut Ev, MapLaneSyncState<R, CF, GF>),
)> {
    Some((Err(error), (events, state)))
}

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

fn initialize<'a, K, V, Retries>(
    id: u64,
    lane: &'a MapLane<K, V>,
    retry: Retries,
) -> impl Future<Output = TResult<Checkpoint<V>>> + 'a
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

fn handle_event<'a, K, V>(
    event: &MapLaneEvent<K, V>,
    cancellers: &mut HashMap<Value, trigger::Sender>,
) where
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

pub fn sync_map_lane<'a, K, V, Events, Retries>(
    id: u64,
    lane: &'a MapLane<K, V>,
    events: &'a mut Events,
    retry: Retries,
) -> impl Stream<Item = TResult<MapLaneEvent<K, V>>> + 'a
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
                                    break yield_error(MapLaneSyncState::Complete, events, err);
                                }
                            }
                        }
                        Either::Left(Some(ev)) => {
                            break yield_event(
                                MapLaneSyncState::Checkpointing(chk_fut),
                                events,
                                ev,
                            );
                        }
                        Either::Left(_) => {
                            //No more events.
                            break None;
                        }
                        Either::Right(Ok(cp)) => from_checkpoint(cp),
                        Either::Right(Err(err)) => {
                            break yield_error(MapLaneSyncState::Complete, events, err);
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
                            break 'outer yield_event(new_state, events, event);
                        }
                        Either::Right(Some(Some((key, value)))) => {
                            cancellers.remove(&key);
                            let result =  match Form::try_convert(key) {
                                Ok(typed_key) => {
                                    let event = MapLaneEvent::Update(typed_key, value);
                                    let new_state = MapLaneSyncState::Awaiting {
                                        pending,
                                        cancellers,
                                    };
                                    yield_event(new_state, events, event)
                                }
                                Err(err) => {
                                    yield_error(
                                        MapLaneSyncState::Complete,
                                        events,
                                        MapLaneSyncError::InconsistentForm(err),
                                    )
                                }
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
