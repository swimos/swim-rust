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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::num::NonZeroUsize;

use either::Either;
use futures::select_biased;
use futures::stream::FuturesUnordered;
use futures::Future;
use futures::StreamExt;
use tokio::sync::mpsc;

use swim_common::sink::item::ItemSender;
use tokio_stream::wrappers::ReceiverStream;
use utilities::lru_cache::LruCache;
use utilities::sync::{circular_buffer, trigger};

type InternalSender<V> = circular_buffer::Sender<V>;

type BridgeBufferReceiver<V> = circular_buffer::Receiver<V>;
type TransmitEvent<K, V> = (K, Option<(V, BridgeBufferReceiver<V>)>);

const INTERNAL_ERROR: &str = "Internal channel dropped.";

pub async fn transmit<M, K, V, S>(
    senders: &mut LruCache<K, InternalSender<V>>,
    bridge_tx: &mut mpsc::Sender<Action<M, K, V, S>>,
    buffer_size: NonZeroUsize,
    key: K,
    value: V,
) where
    K: Hash + Eq + Clone + Send + Sync,
    V: Send + Sync,
{
    if let Some(tx) = senders.get_mut(&key) {
        tx.try_send(value).ok().expect(INTERNAL_ERROR);
    } else {
        let (mut tx, rx) = circular_buffer::channel(buffer_size);
        tx.try_send(value).ok().expect(INTERNAL_ERROR);
        if let Some((evicted, sender)) = senders.insert(key.clone(), tx) {
            let (sync_tx, sync_rx) = trigger::trigger();

            // The sender needs to be dropped to stop the consume task from locking up waiting for
            // more messages when flushing the buffers.
            drop(sender);

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

pub async fn drain<M, K, V, E, Snk, F>(
    buffers: &mut FuturesUnordered<F>,
    queued_buffers: &mut HashMap<K, VecDeque<BridgeBufferReceiver<V>>>,
    active_keys: &mut HashSet<K>,
    sink: &mut Snk,
    take_fn: &mut impl Fn(K, BridgeBufferReceiver<V>) -> F,
    map_fn: &mut impl Fn(K, V) -> M,
    stop_on: Option<K>,
) -> Result<(), E>
where
    K: Hash + Eq + Clone + Send + Sync,
    V: Send + Sync,
    Snk: ItemSender<M, E>,
    F: Future<Output = TransmitEvent<K, V>>,
{
    while let Some(event) = buffers.next().await {
        match event {
            (key, Some((value, remainder))) => {
                dispatch(map_fn(key.clone(), value), sink).await?;
                buffers.push(take_fn(key, remainder));
            }
            (key, _) => {
                if let Some(values) = take_queued(queued_buffers, &key) {
                    buffers.push(take_fn(key, values));
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

pub fn take_queued<K: Hash + Eq + Clone, B>(
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

pub async fn dispatch<M, E, Snk>(value: M, sink: &mut Snk) -> Result<(), E>
where
    Snk: ItemSender<M, E>,
{
    sink.send_item(value).await
}

pub fn register<F, K, V>(
    key: K,
    values: BridgeBufferReceiver<V>,
    buffers: &mut FuturesUnordered<F>,
    queued_buffers: &mut HashMap<K, VecDeque<BridgeBufferReceiver<V>>>,
    active_keys: &mut HashSet<K>,
    take: &mut impl Fn(K, BridgeBufferReceiver<V>) -> F,
) where
    K: Hash + Eq + Clone,
    F: Future<Output = TransmitEvent<K, V>>,
{
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
        buffers.push(take(key, values));
    }
}

pub fn on_key<K, V, F>(
    key: K,
    active_keys: &mut HashSet<K>,
    queued_buffers: &mut HashMap<K, VecDeque<BridgeBufferReceiver<V>>>,
    buffers: &mut FuturesUnordered<F>,
    take: &mut impl Fn(K, BridgeBufferReceiver<V>) -> F,
) where
    K: Hash + Eq + Clone,
    F: Future<Output = TransmitEvent<K, V>>,
{
    if let Some(values) = take_queued(queued_buffers, &key) {
        buffers.push(take(key, values));
    } else {
        active_keys.remove(&key);
    }
}

/// A message type that is used by the input task to communicate with the output task.
///
/// Type parameters:
/// `M`: message.
/// `K`: key.
/// `V`: value.
/// `S`: special action.
pub enum Action<M, K, V, S> {
    /// Register a new channel for a key.
    Register {
        key: K,
        values: BridgeBufferReceiver<V>,
    },
    /// Perform a special action, providing a callback for when it has completed.
    Special {
        kind: S,
        on_handled: trigger::Sender,
    },
    /// Evict a key from the output task providing a callback for when the channel for that key
    /// has been removed and flushed.
    Evict { key: K, on_handled: trigger::Sender },
    /// Flush all pending buffers and emit a message.
    Flush { message: M },
}

/// What action to take based on the result of an `Action::Special` request.
pub enum SpecialActionResult {
    Clear,
    Drain,
    Noop,
}

pub async fn consume_buffers<M, K, V, E, S, F, Snk>(
    bridge_rx: mpsc::Receiver<Action<M, K, V, S>>,
    mut sink: Snk,
    yield_after: NonZeroUsize,
    on_special: impl Fn(S) -> (SpecialActionResult, Option<M>),
    mut take_fn: impl Fn(K, BridgeBufferReceiver<V>) -> F,
    mut map_fn: impl Fn(K, V) -> M,
) -> Result<(), E>
where
    K: Hash + Eq + Clone + Send + Sync,
    V: Send + Sync,
    Snk: ItemSender<M, E>,
    F: Future<Output = TransmitEvent<K, V>>,
{
    let mut iteration_count: usize = 0;
    let yield_mod = yield_after.get();

    let mut active_keys = HashSet::new();
    let mut queued_buffers: HashMap<K, VecDeque<BridgeBufferReceiver<V>>> = HashMap::new();

    let mut buffers = FuturesUnordered::new();
    let mut bridge_rx = ReceiverStream::new(bridge_rx).fuse();

    let mut stopped = false;

    loop {
        let event = if buffers.is_empty() {
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
                register(
                    key,
                    values,
                    &mut buffers,
                    &mut queued_buffers,
                    &mut active_keys,
                    &mut take_fn,
                );
            }
            Either::Left(Action::Special { kind, on_handled }) => {
                match on_special(kind) {
                    (SpecialActionResult::Clear, Some(message)) => {
                        buffers = FuturesUnordered::new();
                        sink.send_item(message).await?;
                    }
                    (SpecialActionResult::Drain, Some(message)) => {
                        drain(
                            &mut buffers,
                            &mut queued_buffers,
                            &mut active_keys,
                            &mut sink,
                            &mut take_fn,
                            &mut map_fn,
                            None,
                        )
                        .await?;
                        sink.send_item(message).await?;
                    }
                    _ => {}
                }

                on_handled.trigger();
            }
            Either::Left(Action::Evict { key, on_handled }) => {
                drain(
                    &mut buffers,
                    &mut queued_buffers,
                    &mut active_keys,
                    &mut sink,
                    &mut take_fn,
                    &mut map_fn,
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
                    &mut take_fn,
                    &mut map_fn,
                    None,
                )
                .await?;

                sink.send_item(message).await?;
            }
            Either::Right((key, Some((value, remainder)))) => {
                dispatch(map_fn(key.clone(), value), &mut sink).await?;
                buffers.push(take_fn(key, remainder));
            }
            Either::Right((key, _)) => {
                on_key(
                    key,
                    &mut active_keys,
                    &mut queued_buffers,
                    &mut buffers,
                    &mut take_fn,
                );
            }
        }

        iteration_count = iteration_count.wrapping_add(1);
        if iteration_count % yield_mod == 0 {
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}
