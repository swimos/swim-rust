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
use futures::{select_biased, Stream, StreamExt};
use pin_utils::pin_mut;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::Arc;
use swim_common::sink::item::ItemSender;
use tokio::sync::mpsc;
use utilities::lru_cache::LruCache;
use utilities::sync::{circular_buffer, trigger};

/// Trait for types that may consist of either or map lane update or messages of a different type.
/// The purpose of this is to allow streams of updates and streams of warp messages (including
/// linked, synced messages etc, to be treated uniformly.
pub trait MapUpdateMessage<K, V>: Sized {
    /// Discriminate between map updates and other messages.
    fn discriminate(self) -> Either<MapUpdate<K, V>, Self>;

    /// Repack a map update into the overall message type.
    fn repack(update: MapUpdate<K, V>) -> Self;
}

impl<K, V> MapUpdateMessage<K, V> for MapUpdate<K, V> {
    fn discriminate(self) -> Either<MapUpdate<K, V>, Self> {
        Either::Left(self)
    }

    fn repack(update: MapUpdate<K, V>) -> Self {
        update
    }
}

/// Map updates that do not have an associated key.
enum SpecialAction {
    Clear,
    Take(usize),
    Drop(usize),
}

impl<K, V> From<SpecialAction> for MapUpdate<K, V> {
    fn from(action: SpecialAction) -> Self {
        match action {
            SpecialAction::Clear => MapUpdate::Clear,
            SpecialAction::Take(n) => MapUpdate::Take(n),
            SpecialAction::Drop(n) => MapUpdate::Drop(n),
        }
    }
}

type BridgeBufferReceiver<V> = circular_buffer::Receiver<Option<Arc<V>>>;

/// A message type that is used by the input task to communicate with the output task.
enum Action<M, K, V> {
    /// Register a new channel for a key.
    Register {
        key: K,
        values: BridgeBufferReceiver<V>,
    },
    /// Perform a special action, providing a callback for when it has completed.
    Special {
        kind: SpecialAction,
        on_handled: trigger::Sender,
    },
    /// Evict a key from the output task providing a callback for when the channel for that key
    /// has been removed and flushed.
    Evict { key: K, on_handled: trigger::Sender },
    /// Flush all pending buffers and emit a message.
    Flush { message: M },
}

/// Consume a stream of keyed messages with one task that pushes them into a circular buffer (for
/// each key). A second task then consumes the buffers and writes the messages to a sink. If the
/// second tasks does not keep up with the first, for a give key, some messages will be discarded.
/// Up to a fixed maximum number of keys are kept active at any one time. Messages that are not map
/// updates will cause the intermediate buffers to flush before they are emitted.
///
/// #Arguments
///
/// * `rx` - The stream of incoming messages.
/// * `sink` - Sink to which the output task writes.
/// * `yield_after` - The input and output tasks will yield to the runtime after this many
/// iterations of their event loops.
/// * `bridge_buffer` - Buffer size for the communication channel between the input and output
/// tasks.
/// * `cache_size` - The maximum number of active keys. If this number would be exceeded, the least
/// recently used key is evicted and flushed before another channel can be opened (effectively
/// stalling the stream until the flush completes).
/// * `buffer_size` - Size of the circular buffer used for each key.
pub async fn release_pressure<M, K, V, E, Snk>(
    rx: impl Stream<Item = M>,
    sink: Snk,
    yield_after: NonZeroUsize,
    bridge_buffer: NonZeroUsize,
    cache_size: NonZeroUsize,
    buffer_size: NonZeroUsize,
) -> Result<(), E>
where
    K: Hash + Eq + Clone + Send + Sync,
    V: Send + Sync,
    M: MapUpdateMessage<K, V>,
    Snk: ItemSender<M, E>,
{
    let (bridge_tx, bridge_rx) = mpsc::channel(bridge_buffer.get());
    let feed = feed_buffers(rx, bridge_tx, yield_after, cache_size, buffer_size);
    let consume = consume_buffers(bridge_rx, sink, yield_after);
    join(feed, consume).await.1
}

const INTERNAL_ERROR: &str = "Internal channel dropped.";

async fn feed_buffers<M, K, V>(
    rx: impl Stream<Item = M>,
    mut bridge_tx: mpsc::Sender<Action<M, K, V>>,
    yield_after: NonZeroUsize,
    cache_size: NonZeroUsize,
    buffer_size: NonZeroUsize,
) where
    K: Hash + Eq + Clone + Send + Sync,
    V: Send + Sync,
    M: MapUpdateMessage<K, V>,
{
    let mut senders = LruCache::new(cache_size);
    let mut iteration_count: usize = 0;
    let yield_mod = yield_after.get();

    pin_mut!(rx);

    while let Some(update) = rx.next().await {
        match update.discriminate() {
            Either::Left(update) => match update {
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
            },
            Either::Right(message) => {
                senders.clear();
                transmit_flush(&mut bridge_tx, message).await;
            }
        }

        iteration_count = iteration_count.wrapping_add(1);
        if iteration_count % yield_mod == 0 {
            tokio::task::yield_now().await;
        }
    }
}

type TransmitEvent<K, V> = (K, Option<(Option<Arc<V>>, BridgeBufferReceiver<V>)>);

async fn consume_buffers<M, K, V, E, Snk>(
    bridge_rx: mpsc::Receiver<Action<M, K, V>>,
    mut sink: Snk,
    yield_after: NonZeroUsize,
) -> Result<(), E>
where
    K: Hash + Eq + Clone + Send + Sync,
    V: Send + Sync,
    M: MapUpdateMessage<K, V>,
    Snk: ItemSender<M, E>,
{
    let mut iteration_count: usize = 0;
    let yield_mod = yield_after.get();

    let mut active_keys = HashSet::new();
    let mut queued_buffers: HashMap<K, VecDeque<BridgeBufferReceiver<V>>> = HashMap::new();

    let mut buffers = FuturesUnordered::new();

    let mut bridge_rx = bridge_rx.fuse();
    let mut stopped = false;
    loop {
        let event: Either<Action<M, K, V>, TransmitEvent<K, V>> = if buffers.is_empty() {
            if stopped {
                break;
            } else if let Some(action) = bridge_rx.next().await {
                Either::Left(action)
            } else {
                break;
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
                sink.send_item(MapUpdateMessage::repack(kind.into()))
                    .await?;
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
            Either::Left(Action::Flush { message }) => {
                drain(
                    &mut buffers,
                    &mut queued_buffers,
                    &mut active_keys,
                    &mut sink,
                    take_event,
                    None,
                )
                .await?;
                sink.send_item(message).await?;
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

async fn dispatch_event<M, K, V, E, Snk>(
    key: K,
    value: Option<Arc<V>>,
    sink: &mut Snk,
) -> Result<(), E>
where
    M: MapUpdateMessage<K, V>,
    Snk: ItemSender<M, E>,
{
    let update = if let Some(v) = value {
        MapUpdate::Update(key, v)
    } else {
        MapUpdate::Remove(key)
    };
    sink.send_item(MapUpdateMessage::repack(update)).await
}

async fn drain<M, K, V, E, Snk, F>(
    buffers: &mut FuturesUnordered<F>,
    queued_buffers: &mut HashMap<K, VecDeque<BridgeBufferReceiver<V>>>,
    active_keys: &mut HashSet<K>,
    sink: &mut Snk,
    take: impl Fn(K, BridgeBufferReceiver<V>) -> F,
    stop_on: Option<K>,
) -> Result<(), E>
where
    K: Hash + Eq + Clone + Send + Sync,
    V: Send + Sync,
    M: MapUpdateMessage<K, V>,
    Snk: ItemSender<M, E>,
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
            if entry.get().is_empty() {
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

async fn transmit<M, K, V>(
    senders: &mut LruCache<K, InternalSender<V>>,
    bridge_tx: &mut mpsc::Sender<Action<M, K, V>>,
    buffer_size: NonZeroUsize,
    key: K,
    value: Option<Arc<V>>,
) where
    K: Hash + Eq + Clone + Send + Sync,
    V: Send + Sync,
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

async fn transmit_special<M, K, V>(tx: &mut mpsc::Sender<Action<M, K, V>>, action: SpecialAction)
where
    K: Hash + Eq + Clone + Send + Sync,
    V: Send + Sync,
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

async fn transmit_flush<M, K, V>(tx: &mut mpsc::Sender<Action<M, K, V>>, message: M)
where
    K: Hash + Eq + Clone + Send + Sync,
    V: Send + Sync,
{
    tx.try_send(Action::Flush { message })
        .ok()
        .expect(INTERNAL_ERROR);
}
