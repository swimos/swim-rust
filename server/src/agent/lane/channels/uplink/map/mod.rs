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
use futures::future::FusedFuture;
use futures::stream::{unfold, FusedStream};
use futures::Stream;
use futures::{select_biased, FutureExt, StreamExt};
use futures_util::stream::FuturesUnordered;
use im::OrdMap;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use stm::transaction;
use stm::transaction::{RetryManager, TransactionError};
use stm::var::TVar;
use swim_form::Form;
use utilities::sync::trigger;
use std::any::Any;

type TResult<T> = Result<T, TransactionError>;
type Checkpoint<V> = OrdMap<Value, TVar<V>>;

type CheckpointFuture<'a, V> = Pin<Box<dyn FusedFuture<Output = TResult<Checkpoint<V>>> + 'a>>;
type ValueRetFuture<'a, V> = Pin<Box<dyn FusedFuture<Output = Option<(Value, Arc<V>)>> + 'a>>;

enum MapLaneSyncState<'a, Retries, V> {
    Init(Option<Retries>),
    Checkpointing(CheckpointFuture<'a, V>),
    Awaiting {
        pending: FuturesUnordered<ValueRetFuture<'a, V>>,
        cancellers: HashMap<Value, trigger::Sender>,
    },
    Complete,
}

impl<'a, Retries, V> MapLaneSyncState<'a, Retries, V>
where V: Any + Send + Sync,
{
    fn from_checkpoint(checkpoint: Checkpoint<V>) -> Self {
        let mut cancellers = HashMap::with_capacity(checkpoint.len());
        let mut pending: FuturesUnordered<ValueRetFuture<'a, V>> = FuturesUnordered::new();
        for (key, var) in checkpoint.into_iter() {
            let (tx, rx) = trigger::trigger();
            let key_copy = key.clone();
            let fut = async move {
                let var: TVar<V> = var;
                select_biased! {
                    _ = rx.fuse() => None,
                    v = var.load().fuse() => Some((key_copy, v)),
                }
            }.fuse();
            cancellers.insert(key, tx);
            pending.push(Box::pin(fut));
        }
        MapLaneSyncState::Awaiting { pending, cancellers }
    }
}

pub(crate) fn sync_map_lane<'a, K, V, Events, Retries>(
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
    let init: (&'a mut Events, MapLaneSyncState<'a, Retries, V>) =
        (events, MapLaneSyncState::Init(Some(retry)));

    unfold(init, move |(events, mut state)| async move {
        'outer: loop {
            state = match state {
                MapLaneSyncState::Init(mut maybe_ret) => {
                    let chk_stm = lane.checkpoint(id);
                    let ret = maybe_ret.take().expect("Boom!");
                    let mut chk_fut = Box::pin(
                        async move {
                            let chk_stm = chk_stm;
                            transaction::atomically(&chk_stm, ret).await
                        }
                        .fuse(),
                    );
                    MapLaneSyncState::Checkpointing(chk_fut)
                }
                MapLaneSyncState::Checkpointing(mut chk_fut) => {
                    let event_or_cp: Either<Option<MapLaneEvent<K, V>>, TResult<Checkpoint<V>>> = select_biased! {
                        maybe_event = events.next() => Either::Left(maybe_event),
                        cp = &mut chk_fut => Either::Right(cp),
                    };
                    match event_or_cp {
                        Either::Left(Some(MapLaneEvent::Checkpoint(cp_id))) if cp_id == id => {
                            match chk_fut.await {
                                Ok(cp) => {
                                    MapLaneSyncState::from_checkpoint(cp)
                                }
                                Err(err) => {
                                    break Some((Err(err), (events, MapLaneSyncState::Complete)));
                                }
                            }
                        }
                        Either::Left(Some(ev)) => {
                            break Some((Ok(ev), (events, MapLaneSyncState::Checkpointing(chk_fut))));
                        }
                        Either::Left(_) => {
                            //No more events.
                            break None;
                        }
                        Either::Right(Ok(cp)) => {
                            MapLaneSyncState::from_checkpoint(cp)
                        }
                        Either::Right(Err(err)) => {
                            break Some((Err(err), (events, MapLaneSyncState::Complete)));
                        }
                    }
                },
                MapLaneSyncState::Awaiting {
                    mut pending,
                    mut cancellers,
                } => {
                    'inner: loop{
                        let next = select_biased! {
                            maybe_loaded = pending.next() => Either::Right(maybe_loaded),
                            maybe_event = events.next() => Either::Left(maybe_event),
                        };
                        match next {
                            Either::Left(Some(event)) => {
                                match &event {
                                    MapLaneEvent::Update(k, _) | MapLaneEvent::Remove(k) => {
                                        let key_as_value = Form::as_value(k);
                                        if let Some(tx) = cancellers.remove(&key_as_value) {
                                            tx.trigger();
                                        }
                                    },
                                    MapLaneEvent::Clear => {
                                        for (_, tx) in cancellers.drain() {
                                            tx.trigger();
                                        }
                                    },
                                    _ => {}
                                }
                                let new_state = if cancellers.is_empty() {
                                    MapLaneSyncState::Complete
                                } else {
                                    MapLaneSyncState::Awaiting { pending, cancellers }
                                };
                                break 'outer Some((Ok(event), (events, new_state)));
                            },
                            Either::Right(Some(Some((key, value)))) => {
                                cancellers.remove(&key);
                                let typed_key = Form::try_convert(key).expect("Boom!");
                                let event = MapLaneEvent::Update(typed_key, value);
                                break 'outer Some((Ok(event), (events, MapLaneSyncState::Awaiting { pending, cancellers })));
                            },
                            Either::Left(None) | Either::Right(None) => {
                                break 'outer None;
                            },
                            _ => {},
                        }
                    }
                },
                MapLaneSyncState::Complete => {
                    break None;
                },
            }
        }
    })
}
