// Copyright 2015-2021 SWIM.AI inc.
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

use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::Arc;

use either::Either;
use futures::future::join;
use futures::{Stream, StreamExt};
use pin_utils::pin_mut;
use tokio::sync::mpsc;

use swim_common::sink::item::ItemSender;
use swim_lrucache::LruCache;
use swim_trigger as trigger;
use utilities::sync::circular_buffer;

use crate::backpressure::keyed::common::{consume_buffers, transmit, Action, SpecialActionResult};
use crate::model::map::MapUpdate;

#[cfg(test)]
mod tests;

type BridgeBufferReceiver<V> = circular_buffer::Receiver<Option<Arc<V>>>;
type MapTransmitEvent<K, V> = (K, Option<(Option<Arc<V>>, BridgeBufferReceiver<V>)>);
type MapAction<M, K, V> = Action<M, K, Option<Arc<V>>, SpecialAction>;

const INTERNAL_ERROR: &str = "Internal channel dropped.";

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
#[derive(Copy, Clone)]
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
    let consume = consume_buffers(
        bridge_rx,
        sink,
        yield_after,
        |action| (action.into(), Some(M::repack(action.into()))),
        take_fn,
        map_fn,
    );
    join(feed, consume).await.1
}

impl From<SpecialAction> for SpecialActionResult {
    fn from(action: SpecialAction) -> Self {
        match action {
            SpecialAction::Clear => SpecialActionResult::Clear,
            _ => SpecialActionResult::Drain,
        }
    }
}

async fn feed_buffers<M, K, V>(
    rx: impl Stream<Item = M>,
    mut bridge_tx: mpsc::Sender<MapAction<M, K, V>>,
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
fn map_fn<M, K, V>(key: K, value: Option<Arc<V>>) -> M
where
    M: MapUpdateMessage<K, V>,
{
    let update = if let Some(v) = value {
        MapUpdate::Update(key, v)
    } else {
        MapUpdate::Remove(key)
    };

    MapUpdateMessage::repack(update)
}

async fn take_fn<K, V>(key: K, mut rx: BridgeBufferReceiver<V>) -> MapTransmitEvent<K, V>
where
    V: Send + Sync,
{
    if let Some(value) = rx.next().await {
        (key, Some((value, rx)))
    } else {
        (key, None)
    }
}

async fn transmit_special<M, K, V>(tx: &mut mpsc::Sender<MapAction<M, K, V>>, action: SpecialAction)
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

async fn transmit_flush<M, K, V>(tx: &mut mpsc::Sender<MapAction<M, K, V>>, message: M)
where
    K: Hash + Eq + Clone + Send + Sync,
    V: Send + Sync,
{
    tx.try_send(Action::Flush { message })
        .ok()
        .expect(INTERNAL_ERROR);
}
