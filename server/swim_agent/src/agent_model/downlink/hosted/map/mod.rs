// Copyright 2015-2021 Swim Inc.
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

use std::{
    cell::RefCell,
    collections::{BTreeSet, HashMap},
};

use futures::{
    future::{select, BoxFuture, Either},
    pin_mut,
    stream::unfold,
    FutureExt, SinkExt, Stream, StreamExt,
};
use std::hash::Hash;
use swim_api::{
    error::FrameIoError,
    protocol::{
        downlink::{DownlinkNotification, MapNotificationDecoder},
        map::{MapMessage, MapOperation, MapOperationEncoder},
    },
};
use swim_form::structural::{read::recognizer::RecognizerReadable, write::StructuralWritable};
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::{
    agent_model::downlink::{handlers::DownlinkChannel, DlState, MapDownlinkConfig},
    downlink_lifecycle::map::MapDownlinkLifecycle,
    event_handler::{BoxEventHandler, EventHandlerExt, Sequentially},
    event_queue::EventQueue,
};

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct MapDlState<K, V> {
    map: HashMap<K, V>,
    order: Option<BTreeSet<K>>,
}

impl<K, V> Default for MapDlState<K, V> {
    fn default() -> Self {
        Self {
            map: Default::default(),
            order: Default::default(),
        }
    }
}

pub trait MapDlStateOps<K, V, Context> {
    fn clear(&self, context: &Context) -> HashMap<K, V>;

    fn with<G, T>(&self, context: &Context, f: G) -> T
    where
        G: FnOnce(&mut MapDlState<K, V>) -> T;

    fn update<'a, LC>(
        &self,
        context: &Context,
        key: K,
        value: V,
        lifecycle: Option<&'a LC>,
    ) -> Option<BoxEventHandler<'a, Context>>
    where
        K: Eq + Hash + Clone + Ord,
        LC: MapDownlinkLifecycle<K, V, Context>,
    {
        self.with(context, move |MapDlState { map, order }| {
            let old = map.insert(key.clone(), value);
            match order {
                Some(ord) if old.is_some() => {
                    ord.insert(key.clone());
                }
                _ => {}
            }
            let new_value = &map[&key];
            lifecycle.map(|lifecycle| lifecycle.on_update(key, &*map, old, new_value).boxed())
        })
    }

    fn remove<'a, LC>(
        &self,
        context: &Context,
        key: K,
        lifecycle: Option<&'a LC>,
    ) -> Option<BoxEventHandler<'a, Context>>
    where
        K: Eq + Hash + Ord,
        LC: MapDownlinkLifecycle<K, V, Context>,
    {
        self.with(context, move |MapDlState { map, order }| {
            map.remove(&key).and_then(move |old| {
                if let Some(ord) = order {
                    ord.remove(&key);
                }
                lifecycle.map(|lifecycle| lifecycle.on_remove(key, &*map, old).boxed())
            })
        })
    }

    fn drop<'a, LC>(
        &self,
        context: &Context,
        n: usize,
        lifecycle: Option<&'a LC>,
    ) -> Option<BoxEventHandler<'a, Context>>
    where
        K: Eq + Hash + Ord + Clone,
        LC: MapDownlinkLifecycle<K, V, Context>,
    {
        self.with(context, move |MapDlState { map, order }| {
            if n >= map.len() {
                *order = None;
                let old = std::mem::take(map);
                lifecycle.map(move |lifecycle| lifecycle.on_clear(old).boxed())
            } else {
                let ord = order.get_or_insert_with(|| map.keys().cloned().collect());

                let to_remove: Vec<_> = ord.iter().take(n).cloned().collect();
                if let Some(lifecycle) = lifecycle {
                    let expected = n.min(map.len());
                    let mut removed = Vec::with_capacity(expected);

                    for k in to_remove {
                        ord.remove(&k);
                        if let Some(v) = map.remove(&k) {
                            removed.push(lifecycle.on_remove(k, map, v));
                        }
                    }
                    if removed.is_empty() {
                        None
                    } else {
                        Some(Sequentially::new(removed).boxed())
                    }
                } else {
                    for k in to_remove {
                        ord.remove(&k);
                        map.remove(&k);
                    }
                    None
                }
            }
        })
    }

    fn take<'a, LC>(
        &self,
        context: &Context,
        n: usize,
        lifecycle: Option<&'a LC>,
    ) -> Option<BoxEventHandler<'a, Context>>
    where
        K: Eq + Hash + Ord + Clone,
        LC: MapDownlinkLifecycle<K, V, Context>,
    {
        self.with(context, move |MapDlState { map, order }| {
            let to_drop = map.len().saturating_sub(n);
            if to_drop > 0 {
                let ord = order.get_or_insert_with(|| map.keys().cloned().collect());

                let to_remove: Vec<_> = ord.iter().rev().take(to_drop).cloned().collect();
                if let Some(lifecycle) = lifecycle {
                    let mut removed = Vec::with_capacity(to_drop);

                    for k in to_remove.into_iter().rev() {
                        ord.remove(&k);
                        if let Some(v) = map.remove(&k) {
                            removed.push(lifecycle.on_remove(k, map, v));
                        }
                    }
                    if removed.is_empty() {
                        None
                    } else {
                        Some(Sequentially::new(removed).boxed())
                    }
                } else {
                    for k in to_remove {
                        ord.remove(&k);
                        map.remove(&k);
                    }
                    None
                }
            } else {
                None
            }
        })
    }
}

impl<K, V, Context> MapDlStateOps<K, V, Context> for RefCell<MapDlState<K, V>> {
    fn clear(&self, _context: &Context) -> HashMap<K, V> {
        self.replace(MapDlState::default()).map
    }

    fn with<F, T>(&self, _context: &Context, f: F) -> T
    where
        F: FnOnce(&mut MapDlState<K, V>) -> T,
    {
        f(&mut *self.borrow_mut())
    }
}

impl<K, V, Context, F> MapDlStateOps<K, V, Context> for F
where
    F: for<'a> Fn(&'a Context) -> &'a RefCell<MapDlState<K, V>>,
{
    fn clear(&self, context: &Context) -> HashMap<K, V> {
        self(context).replace(MapDlState::default()).map
    }
    fn with<G, T>(&self, context: &Context, f: G) -> T
    where
        G: FnOnce(&mut MapDlState<K, V>) -> T,
    {
        f(&mut *self(context).borrow_mut())
    }
}

pub struct HostedMapDownlinkChannel<K: RecognizerReadable, V: RecognizerReadable, LC, State> {
    receiver: Option<FramedRead<ByteReader, MapNotificationDecoder<K, V>>>,
    state: State,
    next: Option<DownlinkNotification<MapMessage<K, V>>>,
    lifecycle: LC,
    config: MapDownlinkConfig,
    dl_state: DlState,
}

impl<K: RecognizerReadable, V: RecognizerReadable, LC, State>
    HostedMapDownlinkChannel<K, V, LC, State>
{
    pub fn new(
        receiver: ByteReader,
        lifecycle: LC,
        state: State,
        config: MapDownlinkConfig,
    ) -> Self {
        HostedMapDownlinkChannel {
            receiver: Some(FramedRead::new(receiver, Default::default())),
            state,
            next: None,
            lifecycle,
            config,
            dl_state: DlState::Unlinked,
        }
    }
}

impl<K, V, LC, State, Context> DownlinkChannel<Context>
    for HostedMapDownlinkChannel<K, V, LC, State>
where
    State: MapDlStateOps<K, V, Context>,
    K: Hash + Eq + Ord + Clone + RecognizerReadable + Send + 'static,
    V: RecognizerReadable + Send + 'static,
    K::Rec: Send,
    V::Rec: Send,
    LC: MapDownlinkLifecycle<K, V, Context> + 'static,
{
    fn await_ready(&mut self) -> BoxFuture<'_, Option<Result<(), FrameIoError>>> {
        let HostedMapDownlinkChannel { receiver, next, .. } = self;
        async move {
            if let Some(rx) = receiver {
                match rx.next().await {
                    Some(Ok(notification)) => {
                        *next = Some(notification);
                        Some(Ok(()))
                    }
                    Some(Err(e)) => {
                        *receiver = None;
                        Some(Err(e))
                    }
                    _ => {
                        *receiver = None;
                        None
                    }
                }
            } else {
                None
            }
        }
        .boxed()
    }

    fn next_event(&mut self, context: &Context) -> Option<BoxEventHandler<'_, Context>> {
        let HostedMapDownlinkChannel {
            receiver,
            state,
            next,
            lifecycle,
            dl_state,
            config:
                MapDownlinkConfig {
                    events_when_not_synced,
                    terminate_on_unlinked,
                    ..
                },
            ..
        } = self;
        if let Some(notification) = next.take() {
            match notification {
                DownlinkNotification::Linked => {
                    if *dl_state == DlState::Unlinked {
                        *dl_state = DlState::Linked;
                    }
                    Some(lifecycle.on_linked().boxed())
                }
                DownlinkNotification::Synced => {
                    *dl_state = DlState::Synced;
                    Some(state.with(context, |map| lifecycle.on_synced(&map.map).boxed()))
                }
                DownlinkNotification::Event { body } => {
                    let maybe_lifecycle = if *dl_state == DlState::Synced || *events_when_not_synced
                    {
                        Some(&*lifecycle)
                    } else {
                        None
                    };

                    match body {
                        MapMessage::Update { key, value } => {
                            state.update(context, key, value, maybe_lifecycle)
                        }
                        MapMessage::Remove { key } => state.remove(context, key, maybe_lifecycle),
                        MapMessage::Clear => {
                            let old_map = state.clear(context);
                            maybe_lifecycle.map(|lifecycle| lifecycle.on_clear(old_map).boxed())
                        }
                        MapMessage::Take(n) => state.take(
                            context,
                            n.try_into()
                                .expect("number to take does not fit into usize"),
                            maybe_lifecycle,
                        ),
                        MapMessage::Drop(n) => state.drop(
                            context,
                            n.try_into()
                                .expect("number to drop does not fit into usize"),
                            maybe_lifecycle,
                        ),
                    }
                }
                DownlinkNotification::Unlinked => {
                    *dl_state = DlState::Unlinked;
                    if *terminate_on_unlinked {
                        *receiver = None;
                    }
                    state.clear(context);
                    Some(lifecycle.on_unlinked().boxed())
                }
            }
        } else {
            None
        }
    }
}

struct WriteStreamState<K, V> {
    rx: mpsc::Receiver<MapOperation<K, V>>,
    write: Option<FramedWrite<ByteWriter, MapOperationEncoder>>,
    queue: EventQueue<K, V>,
}

impl<K, V> WriteStreamState<K, V> {
    fn new(writer: ByteWriter, rx: mpsc::Receiver<MapOperation<K, V>>) -> Self {
        WriteStreamState {
            rx,
            write: Some(FramedWrite::new(writer, Default::default())),
            queue: Default::default(),
        }
    }
}

pub fn map_dl_write_stream<K, V>(
    writer: ByteWriter,
    rx: mpsc::Receiver<MapOperation<K, V>>,
) -> impl Stream<Item = Result<(), std::io::Error>> + Send + 'static
where
    K: Clone + Eq + Hash + StructuralWritable + Send + Sync + 'static,
    V: StructuralWritable + Send + Sync + 'static,
{
    let state = WriteStreamState::<K, V>::new(writer, rx);

    unfold(state, |mut state| async move {
        let WriteStreamState { rx, write, queue } = &mut state;
        if let Some(writer) = write {
            let first = if let Some(op) = queue.pop() {
                op
            } else if let Some(op) = rx.recv().await {
                op
            } else {
                *write = None;
                return None;
            };
            let write_fut = writer.send(first);
            pin_mut!(write_fut);
            let result = loop {
                let recv = rx.recv();
                pin_mut!(recv);
                match select(write_fut.as_mut(), recv).await {
                    Either::Left((Ok(_), _)) => break Some(Ok(())),
                    Either::Left((Err(e), _)) => {
                        *write = None;
                        break Some(Err(e));
                    }
                    Either::Right((Some(op), _)) => {
                        queue.push(op);
                    }
                    _ => {
                        *write = None;
                        break None;
                    }
                }
            };
            result.map(move |r| (r, state))
        } else {
            None
        }
    })
}
