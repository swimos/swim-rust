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

use crate::backpressure::common::{consume_buffers, transmit, Action, SpecialActionResult};
use futures::future::join;
use futures::{Stream, StreamExt};
use pin_utils::pin_mut;
use std::hash::Hash;
use std::num::NonZeroUsize;
use swim_common::sink::item::ItemSender;
use tokio::sync::mpsc;
use utilities::lru_cache::LruCache;
use utilities::sync::circular_buffer;

type BridgeBufferReceiver<V> = circular_buffer::Receiver<V>;
type KeyedTransmitEvent<K, V> = (K, Option<(V, BridgeBufferReceiver<V>)>);
type KeyedAction<K, V> = Action<V, K, V, ()>;

/// A trait signifying that the implementor can be addressed by a key.
pub trait Keyed {
    /// The type of the key that will be returned.
    type Key: Hash + Eq + Clone;

    /// Return a key representing this instance.
    fn key(&self) -> Self::Key;
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
pub async fn release_pressure<K, V, E, Snk>(
    rx: impl Stream<Item = V>,
    sink: Snk,
    yield_after: NonZeroUsize,
    bridge_buffer: NonZeroUsize,
    cache_size: NonZeroUsize,
    buffer_size: NonZeroUsize,
) -> Result<(), E>
where
    K: Hash + Eq + Clone + Send + Sync,
    V: Send + Sync + Keyed<Key = K>,
    Snk: ItemSender<V, E>,
{
    let (bridge_tx, bridge_rx) = mpsc::channel(bridge_buffer.get());
    let feed = feed_buffers(rx, bridge_tx, yield_after, cache_size, buffer_size);

    let consume = consume_buffers(
        bridge_rx,
        sink,
        yield_after,
        |_| (SpecialActionResult::Noop, None),
        take_event,
        map,
    );

    join(feed, consume).await.1
}

async fn feed_buffers<K, V>(
    rx: impl Stream<Item = V>,
    mut bridge_tx: mpsc::Sender<KeyedAction<K, V>>,
    yield_after: NonZeroUsize,
    cache_size: NonZeroUsize,
    buffer_size: NonZeroUsize,
) where
    K: Hash + Eq + Clone + Send + Sync,
    V: Send + Sync + Keyed + Keyed<Key = K>,
{
    let mut senders = LruCache::new(cache_size);
    let mut iteration_count: usize = 0;
    let yield_mod = yield_after.get();

    pin_mut!(rx);

    while let Some(update) = rx.next().await {
        transmit(
            &mut senders,
            &mut bridge_tx,
            buffer_size,
            update.key(),
            update,
        )
        .await;

        iteration_count = iteration_count.wrapping_add(1);
        if iteration_count % yield_mod == 0 {
            tokio::task::yield_now().await;
        }
    }
}

fn map<K, V>(_: K, value: V) -> V {
    value
}

async fn take_event<K, V>(key: K, mut rx: BridgeBufferReceiver<V>) -> KeyedTransmitEvent<K, V>
where
    V: Send + Sync,
{
    if let Some(value) = rx.next().await {
        (key, Some((value, rx)))
    } else {
        (key, None)
    }
}
