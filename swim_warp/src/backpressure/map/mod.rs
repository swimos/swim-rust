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

use crate::model::map::MapUpdate;
use either::Either;
use futures::future::join;
use futures::stream::FuturesUnordered;
use futures::{select_biased, StreamExt};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::Arc;
use swim_common::form::ValidatedForm;
use swim_common::sink::item::ItemSender;
use tokio::sync::mpsc;
use utilities::lru_cache::LruCache;
use utilities::sync::{circular_buffer, trigger};

enum SpecialAction {
    Clear,
    Take(usize),
    Drop(usize),
}

impl<K, V> From<SpecialAction> for MapUpdate<K, V>
where
    K: ValidatedForm,
    V: ValidatedForm,
{
    fn from(action: SpecialAction) -> Self {
        match action {
            SpecialAction::Clear => MapUpdate::Clear,
            SpecialAction::Take(n) => MapUpdate::Take(n),
            SpecialAction::Drop(n) => MapUpdate::Drop(n),
        }
    }
}

type BridgeBufferReceiver<V> = circular_buffer::Receiver<Option<Arc<V>>>;

enum Action<K, V> {
    Register {
        key: K,
        values: BridgeBufferReceiver<V>,
    },
    Special {
        kind: SpecialAction,
        on_handled: trigger::Sender,
    },
    Evict {
        key: K,
        on_handled: trigger::Sender,
    },
}

//TODO Remove ValidatedForm constraint.
pub async fn release_pressure<K, V, E, Snk>(
    rx: mpsc::Receiver<MapUpdate<K, V>>,
    sink: Snk,
    yield_after: NonZeroUsize,
    bridge_buffer: NonZeroUsize,
    cache_size: NonZeroUsize,
    buffer_size: NonZeroUsize,
) -> Result<(), E>
where
    K: Hash + Eq + Clone + ValidatedForm + Send + Sync,
    V: Send + ValidatedForm + Sync,
    Snk: ItemSender<MapUpdate<K, V>, E>,
{
    let (bridge_tx, bridge_rx) = mpsc::channel(bridge_buffer.get());
    let feed = feed_buffers(rx, bridge_tx, yield_after, cache_size, buffer_size);
    let consume = consume_buffers(bridge_rx, sink, yield_after);
    join(feed, consume).await.1
}

const INTERNAL_ERROR: &str = "Internal channel dropped.";

async fn feed_buffers<K, V>(
    mut rx: mpsc::Receiver<MapUpdate<K, V>>,
    mut bridge_tx: mpsc::Sender<Action<K, V>>,
    yield_after: NonZeroUsize,
    cache_size: NonZeroUsize,
    buffer_size: NonZeroUsize,
) where
    K: Hash + Eq + Clone + ValidatedForm + Send + Sync,
    V: Send + Sync + ValidatedForm,
{
    let mut senders = LruCache::new(cache_size);
    let mut iteration_count: usize = 0;
    let yield_mod = yield_after.get();

    while let Some(update) = rx.recv().await {
        match update {
            MapUpdate::Update(k, v) => {
                transmit(&mut senders, &mut bridge_tx, buffer_size, k, Some(v)).await;
            }
            MapUpdate::Remove(k) => {
                transmit(&mut senders, &mut bridge_tx, buffer_size, k, None).await;
            }
            MapUpdate::Clear => {
                senders.clear();
                transmit_special(&mut bridge_tx, SpecialAction::Clear).await;
            }
            MapUpdate::Take(n) => {
                senders.clear();
                transmit_special(&mut bridge_tx, SpecialAction::Take(n)).await;
            }
            MapUpdate::Drop(n) => {
                senders.clear();
                transmit_special(&mut bridge_tx, SpecialAction::Drop(n)).await;
            }
        }

        iteration_count = iteration_count.wrapping_add(1);
        if iteration_count % yield_mod == 0 {
            tokio::task::yield_now().await;
        }
    }
}

type TransmitEvent<K, V> = (K, Option<(Option<Arc<V>>, BridgeBufferReceiver<V>)>);

async fn consume_buffers<K, V, E, Snk>(
    bridge_rx: mpsc::Receiver<Action<K, V>>,
    mut sink: Snk,
    yield_after: NonZeroUsize,
) -> Result<(), E>
where
    K: Hash + Eq + Clone + ValidatedForm + Send + Sync,
    V: ValidatedForm + Send + Sync,
    Snk: ItemSender<MapUpdate<K, V>, E>,
{
    let mut iteration_count: usize = 0;
    let yield_mod = yield_after.get();

    let mut active_keys = HashSet::new();
    let mut queued_buffers: HashMap<K, VecDeque<BridgeBufferReceiver<V>>> = HashMap::new();

    let mut buffers = FuturesUnordered::new();

    let mut bridge_rx = bridge_rx.fuse();
    let mut stopped = false;
    loop {
        let event: Either<Action<K, V>, TransmitEvent<K, V>> = if buffers.is_empty() {
            if stopped {
                break;
            } else {
                if let Some(action) = bridge_rx.next().await {
                    Either::Left(action)
                } else {
                    break;
                }
            }
        } else {
            select_biased! {
                maybe_action = bridge_rx.next() => {
                    if let Some(action) = maybe_action {
                        Either::Left(action)
                    } else {
                        stopped = true;
                        continue;
                    }
                }
                maybe_event = buffers.next() => {
                    Either::Right(maybe_event.expect(INTERNAL_ERROR))
                }
            }
        };

        match event {
            Either::Left(Action::Register { key, values }) => {
                if active_keys.contains(&key) {
                    match queued_buffers.entry(key) {
                        Entry::Occupied(mut entry) => entry.get_mut().push_back(values),
                        Entry::Vacant(entry) => {
                            let mut queue = VecDeque::new();
                            queue.push_back(values);
                            entry.insert(queue);
                        }
                    }
                } else {
                    active_keys.insert(key.clone());
                    buffers.push(take_event(key, values));
                }
            }
            Either::Left(Action::Special { kind, on_handled }) => {
                if matches!(kind, SpecialAction::Clear) {
                    buffers = FuturesUnordered::new();
                } else {
                    drain(
                        &mut buffers,
                        &mut queued_buffers,
                        &mut active_keys,
                        &mut sink,
                        take_event,
                        None,
                    )
                    .await?;
                }
                sink.send_item(kind.into()).await?;
                on_handled.trigger();
            }
            Either::Left(Action::Evict { key, on_handled }) => {
                drain(
                    &mut buffers,
                    &mut queued_buffers,
                    &mut active_keys,
                    &mut sink,
                    take_event,
                    Some(key),
                )
                .await?;
                on_handled.trigger();
            }
            Either::Right((key, Some((value, remainder)))) => {
                dispatch_event(key.clone(), value, &mut sink).await?;
                buffers.push(take_event(key, remainder));
            }
            Either::Right((key, _)) => {
                if let Some(values) = take_queued(&mut queued_buffers, &key) {
                    buffers.push(take_event(key, values));
                } else {
                    active_keys.remove(&key);
                }
            }
        }

        iteration_count = iteration_count.wrapping_add(1);
        if iteration_count % yield_mod == 0 {
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}

async fn dispatch_event<K, V, E, Snk>(
    key: K,
    value: Option<Arc<V>>,
    sink: &mut Snk,
) -> Result<(), E>
where
    K: ValidatedForm,
    V: ValidatedForm,
    Snk: ItemSender<MapUpdate<K, V>, E>,
{
    let update = if let Some(v) = value {
        MapUpdate::Update(key, v)
    } else {
        MapUpdate::Remove(key)
    };
    sink.send_item(update).await
}

async fn drain<K, V, E, Snk, F>(
    buffers: &mut FuturesUnordered<F>,
    queued_buffers: &mut HashMap<K, VecDeque<BridgeBufferReceiver<V>>>,
    active_keys: &mut HashSet<K>,
    sink: &mut Snk,
    take: impl Fn(K, BridgeBufferReceiver<V>) -> F,
    stop_on: Option<K>,
) -> Result<(), E>
where
    K: Hash + Eq + Clone + ValidatedForm + Send + Sync,
    V: ValidatedForm + Send + Sync,
    Snk: ItemSender<MapUpdate<K, V>, E>,
    F: Future<Output = TransmitEvent<K, V>>,
{
    while let Some(event) = buffers.next().await {
        match event {
            (key, Some((value, remainder))) => {
                dispatch_event(key.clone(), value, sink).await?;
                buffers.push(take(key, remainder));
            }
            (key, _) => {
                if let Some(values) = take_queued(queued_buffers, &key) {
                    buffers.push(take(key, values));
                } else {
                    active_keys.remove(&key);
                    if matches!(&stop_on, Some(k) if k == &key) {
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

fn take_queued<K: Hash + Eq + Clone, B>(
    queued_buffers: &mut HashMap<K, VecDeque<B>>,
    key: &K,
) -> Option<B> {
    if queued_buffers.contains_key(key) {
        if let Entry::Occupied(mut entry) = queued_buffers.entry(key.clone()) {
            if entry.get().len() < 1 {
                entry.remove().pop_front()
            } else {
                entry.get_mut().pop_front()
            }
        } else {
            None
        }
    } else {
        None
    }
}

async fn take_event<K, V>(key: K, mut rx: BridgeBufferReceiver<V>) -> TransmitEvent<K, V>
where
    V: Send + Sync,
{
    if let Some(value) = rx.next().await {
        (key, Some((value, rx)))
    } else {
        (key, None)
    }
}

type InternalSender<V> = circular_buffer::Sender<Option<Arc<V>>>;

async fn transmit<K, V>(
    senders: &mut LruCache<K, InternalSender<V>>,
    bridge_tx: &mut mpsc::Sender<Action<K, V>>,
    buffer_size: NonZeroUsize,
    key: K,
    value: Option<Arc<V>>,
) where
    K: Hash + Eq + Clone + ValidatedForm + Send + Sync,
    V: ValidatedForm + Send + Sync,
{
    if let Some(tx) = senders.get_mut(&key) {
        tx.try_send(value).ok().expect(INTERNAL_ERROR);
    } else {
        let (mut tx, rx) = circular_buffer::channel(buffer_size);
        tx.try_send(value).ok().expect(INTERNAL_ERROR);
        if let Some((evicted, _)) = senders.insert(key.clone(), tx) {
            let (sync_tx, sync_rx) = trigger::trigger();
            bridge_tx
                .send(Action::Evict {
                    key: evicted,
                    on_handled: sync_tx,
                })
                .await
                .ok()
                .expect(INTERNAL_ERROR);
            sync_rx.await.expect(INTERNAL_ERROR);
        }
        bridge_tx
            .send(Action::Register { key, values: rx })
            .await
            .ok()
            .expect(INTERNAL_ERROR);
    }
}

async fn transmit_special<K, V>(tx: &mut mpsc::Sender<Action<K, V>>, action: SpecialAction)
where
    K: Hash + Eq + Clone + ValidatedForm + Send + Sync,
    V: ValidatedForm + Send + Sync,
{
    let (sync_tx, sync_rx) = trigger::trigger();
    tx.try_send(Action::Special {
        kind: action,
        on_handled: sync_tx,
    })
    .ok()
    .expect(INTERNAL_ERROR);
    sync_rx.await.expect(INTERNAL_ERROR);
}
