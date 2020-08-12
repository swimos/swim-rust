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

use crate::downlink::model::map::UntypedMapModification;
use crate::downlink::watch_adapter::{EpochReceiver, EpochSender};
use either::Either;
use futures::stream::SelectAll;
use futures::{select_biased, Stream};
use futures::{FutureExt, StreamExt};
use std::num::NonZeroUsize;
use std::sync::Arc;
use swim_common::model::Value;
use swim_common::routing::RoutingError;
use swim_common::sink::item::{ItemSender, ItemSink, MpscSend};
use swim_runtime::task::{spawn, TaskHandle};
use tokio::sync::{mpsc, oneshot};
use utilities::lru_cache::LruCache;

/// Stream adapter that removes per-key back-pressure from modifications over a map downlink. If
/// the produces pushes in changes, sequentially, to the same key the consumer will only observe
/// those changes as fast as it can read (an intermediate updates will be lost). However, if
/// the keys that are modified change constantly each update will be observed by the consumer and
/// back-pressure will propagate through. This is to ensure that the transmitted state for each key
/// will ultimately be guaranteed to correspond to the last received update.
///
/// For the same reason, compound operations (like clear, take, etc) will wait for all previous
/// operations to have send before being sent and will stall the internal tasks until they complete.
pub struct KeyedWatch {
    sender: mpsc::Sender<UntypedMapModification<Arc<Value>>>,
    _consume_task: TaskHandle<()>,
    _produce_task: TaskHandle<()>,
}

impl<'a> ItemSink<'a, UntypedMapModification<Arc<Value>>> for KeyedWatch {
    type Error = RoutingError;
    type SendFuture = MpscSend<'a, UntypedMapModification<Arc<Value>>, RoutingError>;

    fn send_item(&'a mut self, value: UntypedMapModification<Arc<Value>>) -> Self::SendFuture {
        MpscSend::new(&mut self.sender, value)
    }
}

impl KeyedWatch {
    /// Create a new keyed watcher.
    /// # Arguments
    ///
    /// * `sink` - Sink to which the changes will be pushed.
    /// * `input_buffer_size` - The size of the MPSC buffer for input records.
    /// * `bridge_buffer_size` - The size of the MPSC buffer connecting the producer and consumer tasks.
    /// * `max_active_keys` - The maximum number of keys kept active internally at any time.
    pub async fn new<Snk>(
        sink: Snk,
        input_buffer_size: NonZeroUsize,
        bridge_buffer_size: NonZeroUsize,
        max_active_keys: NonZeroUsize,
        yield_after: NonZeroUsize,
    ) -> KeyedWatch
    where
        Snk: ItemSender<UntypedMapModification<Arc<Value>>, RoutingError> + Send + 'static,
    {
        let (tx, rx) = mpsc::channel(input_buffer_size.get());
        let (bridge_tx, bridge_rx) = mpsc::channel(bridge_buffer_size.get());
        let consumer = ConsumerTask::new(rx, bridge_tx, max_active_keys, yield_after);
        let producer = ProducerTask::new(bridge_rx, sink, yield_after);

        KeyedWatch {
            sender: tx,
            _consume_task: spawn(consumer.run()),
            _produce_task: spawn(producer.run()),
        }
    }
}

type Mod = UntypedMapModification<Arc<Value>>;

#[derive(Clone, Debug)]
struct KeyedAction(Value, Mod);

#[derive(Clone, Debug)]
enum SpecialAction {
    Take(usize),
    Skip(usize),
    Clear,
}

#[derive(Debug)]
enum BridgeMessage {
    Register(EpochReceiver<Mod>),
    Special(SpecialAction, oneshot::Sender<()>),
    Flush(oneshot::Sender<()>),
}

#[derive(Debug)]
pub struct ConsumerTask {
    input: mpsc::Receiver<Mod>,
    bridge: mpsc::Sender<BridgeMessage>,
    yield_after: NonZeroUsize,
    senders: LruCache<Value, EpochSender<Mod>>,
}

impl ConsumerTask {
    fn new(
        input: mpsc::Receiver<Mod>,
        bridge: mpsc::Sender<BridgeMessage>,
        max_active_keys: NonZeroUsize,
        yield_after: NonZeroUsize,
    ) -> Self {
        ConsumerTask {
            input,
            bridge,
            yield_after,
            senders: LruCache::new(max_active_keys),
        }
    }

    async fn run(mut self) {
        let yield_mod = self.yield_after.get();
        let mut iteration_count: usize = 0;
        while let Some(action) = self.input.recv().await {
            match classify(action) {
                Either::Left(keyed) => {
                    if !self.handle_keyed(keyed).await {
                        // The producer task was dropped.
                        break;
                    }
                }
                Either::Right(special) => {
                    if !self.handle_special(special).await {
                        // The producer task was dropped.
                        break;
                    }
                }
            }
            iteration_count += 1;
            if iteration_count % yield_mod == 0 {
                tokio::task::yield_now().await;
            }
        }
    }

    async fn handle_keyed(&mut self, keyed: KeyedAction) -> bool {
        let ConsumerTask {
            senders, bridge, ..
        } = self;
        let KeyedAction(key, action) = keyed;
        match senders.get_mut(&key) {
            Some(sender) => sender.broadcast(action).is_ok(),
            _ => {
                let (tx, rx) = super::channel(action);

                if let Some((_, _evicted)) = senders.insert(key, tx) {
                    //Evicted sender must not be dropped until the flush completes.

                    let (tx_flush, rx_flush) = oneshot::channel();
                    if bridge
                        .send_item(BridgeMessage::Flush(tx_flush))
                        .await
                        .is_err()
                    {
                        return false;
                    }
                    //Wait for the flush to complete
                    if rx_flush.await.is_err() {
                        //The produce task was dropped.
                        return false;
                    }
                }

                let msg = BridgeMessage::Register(rx);
                bridge.send(msg).await.is_ok()
            }
        }
    }

    async fn handle_special(&mut self, special: SpecialAction) -> bool {
        let (tx, rx) = oneshot::channel();
        if self
            .bridge
            .send(BridgeMessage::Special(special, tx))
            .await
            .is_err()
        {
            return false;
        }
        //Stop processing until the special action has been processed to maintain
        //temporal consistency.
        rx.await.is_ok()
    }
}

fn classify(action: Mod) -> Either<KeyedAction, SpecialAction> {
    match action {
        UntypedMapModification::Insert(key, value) => Either::Left(KeyedAction(
            key.clone(),
            UntypedMapModification::Insert(key, value),
        )),
        UntypedMapModification::Remove(key) => Either::Left(KeyedAction(
            key.clone(),
            UntypedMapModification::Remove(key),
        )),
        UntypedMapModification::Take(n) => Either::Right(SpecialAction::Take(n)),
        UntypedMapModification::Skip(n) => Either::Right(SpecialAction::Skip(n)),
        UntypedMapModification::Clear => Either::Right(SpecialAction::Clear),
    }
}

struct ProducerTask<Snk> {
    bridge: mpsc::Receiver<BridgeMessage>,
    sink: Snk,
    yield_after: NonZeroUsize,
}

impl<Snk> ProducerTask<Snk> {
    fn new(bridge: mpsc::Receiver<BridgeMessage>, sink: Snk, yield_after: NonZeroUsize) -> Self {
        ProducerTask {
            bridge,
            sink,
            yield_after,
        }
    }
}

impl<Snk> ProducerTask<Snk>
where
    Snk: ItemSender<Mod, RoutingError>,
{
    async fn run(self) {
        let ProducerTask {
            mut sink,
            bridge,
            yield_after,
        } = self;

        let yield_mod = yield_after.get();
        let mut iteration_count: usize = 0;

        let mut key_streams = SelectAll::new();

        let mut bridge_fused = bridge.fuse();

        loop {
            let maybe_event: Option<Either<BridgeMessage, Mod>> = if key_streams.is_empty() {
                bridge_fused.next().await.map(Either::Left)
            } else {
                select_biased! {
                    message = bridge_fused.next() => message.map(Either::Left),
                    output = key_streams.next() => output.map(Either::Right),
                }
            };

            if let Some(event) = maybe_event {
                match event {
                    Either::Left(BridgeMessage::Register(receiver)) => {
                        key_streams.push(receiver);
                    }
                    Either::Left(BridgeMessage::Special(action, cb)) => {
                        if !producer_handle_special(&mut sink, action, cb, &mut key_streams).await {
                            // The consumer task or router was dropped.
                            break;
                        }
                    }
                    Either::Left(BridgeMessage::Flush(cb)) => {
                        if !flush_key_streams(&mut sink, &mut key_streams).await
                            || cb.send(()).is_err()
                        {
                            // The consumer task or router was dropped.
                            break;
                        }
                    }
                    Either::Right(modification) => {
                        if let Err(RoutingError::RouterDropped) = sink.send_item(modification).await
                        {
                            //Router was dropped.
                            break;
                        }
                    }
                }
            } else {
                break;
            }
            iteration_count += 1;
            if iteration_count % yield_mod == 0 {
                tokio::task::yield_now().await;
            }
        }
    }
}

async fn flush_key_streams<Str, Snk>(sink: &mut Snk, key_streams: &mut Str) -> bool
where
    Str: Stream<Item = Mod> + Unpin + Sync + 'static,
    Snk: ItemSender<Mod, RoutingError>,
{
    while let Some(Some(modification)) = key_streams.next().now_or_never() {
        if let Err(RoutingError::RouterDropped) = sink.send_item(modification).await {
            //Router was dropped.
            return false;
        }
    }
    true
}

async fn producer_handle_special<Str, Snk>(
    sink: &mut Snk,
    action: SpecialAction,
    cb: oneshot::Sender<()>,
    key_streams: &mut Str,
) -> bool
where
    Str: Stream<Item = Mod> + Unpin + Sync + 'static,
    Snk: ItemSender<Mod, RoutingError>,
{
    //Drain all of the keyed streams of immediately available values to
    //maintain ordering of events.
    if !flush_key_streams(sink, key_streams).await {
        return false;
    }
    //Dispatch the special event.
    let special = match action {
        SpecialAction::Take(n) => UntypedMapModification::Take(n),
        SpecialAction::Skip(n) => UntypedMapModification::Skip(n),
        SpecialAction::Clear => UntypedMapModification::Clear,
    };
    if let Err(RoutingError::RouterDropped) = sink.send_item(special).await {
        //Router was dropped.
        return false;
    }
    //Inform the consumer task that we are done.
    cb.send(()).is_ok()
}
