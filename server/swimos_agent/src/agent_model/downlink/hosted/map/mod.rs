// Copyright 2015-2023 Swim Inc.
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
    pin::{pin, Pin},
    sync::{atomic::AtomicU8, Arc},
    task::{Context, Poll},
};

use futures::{
    future::{BoxFuture, Either, OptionFuture},
    ready, FutureExt, Sink, SinkExt, Stream, StreamExt,
};
use pin_project::pin_project;
use std::hash::Hash;
use swimos_agent_protocol::{
    encoding::{downlink::MapNotificationDecoder, map::MapOperationEncoder},
    DownlinkNotification, MapMessage, MapOperation,
};
use swimos_api::{
    address::Address,
    agent::DownlinkKind,
    error::{AgentRuntimeError, FrameIoError},
};
use swimos_form::{read::RecognizerReadable, write::StructuralWritable, Form};
use swimos_model::Text;
use swimos_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    trigger,
};
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, error, info, trace};

use crate::{
    agent_model::downlink::handlers::{
        BoxDownlinkChannel, DownlinkChannel, DownlinkChannelError, DownlinkChannelEvent,
    },
    config::MapDownlinkConfig,
    downlink_lifecycle::map::MapDownlinkLifecycle,
    event_handler::{BoxEventHandler, HandlerActionExt, Sequentially},
    event_queue::EventQueue,
};

use super::{DlState, DlStateObserver, DlStateTracker, OutputWriter, RestartableOutput};

#[cfg(test)]
mod tests;

/// Internal state of a map downlink. For most purposes this uses the hashmap (for constant time
/// accesses). To support the (infrequently used) take and drop operations, it will generate a
/// separate ordered set of the keys which will then be kept up to date with the map.
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

/// Operations that need to be supported by the state store of a map downlink. The intention
/// of this trait is to abstract over a self contained store a store contained within the field
/// of an agent. In both cases, the store itself will a [`RefCell`] containing a [`MapDlState`].
pub trait MapDlStateOps<K, V, Context>: Send {
    fn clear(&self, context: &Context) -> HashMap<K, V>;

    // Perform an operation in a context with access to the state.
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

                //Decompose the take into a sequence of removals.
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

                //Decompose the drop into a sequence of removals.
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

impl<K, V, Context> MapDlStateOps<K, V, Context> for RefCell<MapDlState<K, V>>
where
    K: Send,
    V: Send,
{
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
    F: for<'a> Fn(&'a Context) -> &'a RefCell<MapDlState<K, V>> + Send,
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

pub struct HostedMapDownlinkFactory<K, V, LC, State> {
    address: Address<Text>,
    state: State,
    lifecycle: LC,
    config: MapDownlinkConfig,
    dl_state: Arc<AtomicU8>,
    stop_rx: trigger::Receiver,
    op_rx: mpsc::UnboundedReceiver<MapOperation<K, V>>,
}

impl<K, V, LC, State> HostedMapDownlinkFactory<K, V, LC, State>
where
    K: Hash + Eq + Ord + Clone + Form + Send + 'static,
    V: Form + Send + 'static,
    K::Rec: Send,
    V::Rec: Send,
{
    pub fn new(
        address: Address<Text>,
        lifecycle: LC,
        state: State,
        config: MapDownlinkConfig,
        stop_rx: trigger::Receiver,
        op_rx: mpsc::UnboundedReceiver<MapOperation<K, V>>,
    ) -> Self {
        HostedMapDownlinkFactory {
            address,
            state,
            lifecycle,
            config,
            dl_state: Default::default(),
            stop_rx,
            op_rx,
        }
    }

    pub fn create<Context>(
        self,
        context: &Context,
        sender: ByteWriter,
        receiver: ByteReader,
    ) -> BoxDownlinkChannel<Context>
    where
        State: MapDlStateOps<K, V, Context> + 'static,
        LC: MapDownlinkLifecycle<K, V, Context> + 'static,
    {
        let HostedMapDownlinkFactory {
            address,
            state,
            lifecycle,
            config,
            dl_state,
            stop_rx,
            op_rx,
        } = self;
        let mut chan = HostedMapDownlink {
            address,
            receiver: None,
            write_stream: Writes::Inactive(op_rx),
            state,
            next: None,
            lifecycle,
            config,
            dl_state: DlStateTracker::new(dl_state),
            stop_rx: Some(stop_rx),
        };
        chan.connect(context, sender, receiver);
        Box::new(chan)
    }

    pub fn dl_state(&self) -> &Arc<AtomicU8> {
        &self.dl_state
    }
}

type Writes<K, V> = OutputWriter<MapWriteStream<K, V>>;

/// An implementation of [`DownlinkChannel`] to allow a map downlink to be driven by an agent
/// task.
pub struct HostedMapDownlink<K: RecognizerReadable, V: RecognizerReadable, LC, State> {
    address: Address<Text>,
    receiver: Option<FramedRead<ByteReader, MapNotificationDecoder<K, V>>>,
    write_stream: Writes<K, V>,
    state: State,
    next: Option<Result<DownlinkNotification<MapMessage<K, V>>, FrameIoError>>,
    lifecycle: LC,
    config: MapDownlinkConfig,
    dl_state: DlStateTracker,
    stop_rx: Option<trigger::Receiver>,
}

impl<K: StructuralWritable, V: StructuralWritable> MapWriteStream<K, V> {
    pub async fn close(&mut self) -> Result<(), std::io::Error> {
        SinkExt::<MapOperation<K, V>>::close(&mut self.write).await
    }
}

impl<K, V, LC, State> HostedMapDownlink<K, V, LC, State>
where
    K: Hash + Eq + Ord + Clone + Form + Send + 'static,
    V: Form + Send + 'static,
    K::Rec: Send,
    V::Rec: Send,
{
    async fn select_next(&mut self) -> Option<Result<DownlinkChannelEvent, DownlinkChannelError>> {
        let HostedMapDownlink {
            address,
            receiver,
            next,
            stop_rx,
            write_stream,
            dl_state,
            ..
        } = self;
        let select_next = pin!(async {
            tokio::select! {
                maybe_result = OptionFuture::from(receiver.as_mut().map(|rx| rx.next())) => {
                    match maybe_result {
                        Some(r@Some(Ok(_))) => {
                            *next = r;
                            Some(Ok(DownlinkChannelEvent::HandlerReady))
                        }
                        Some(Some(Err(error))) => {
                            error!(address = %address,  error = %error, "Downlink input channel failed.");
                            *next = Some(Err(error));
                            *receiver = None;
                            Some(Err(DownlinkChannelError::ReadFailed))
                        }
                        Some(None) => {
                            info!(address = %address, "Downlink terminated normally.");
                            *receiver = None;
                            if dl_state.get().is_linked() {
                                *next = Some(Ok(DownlinkNotification::Unlinked));
                                Some(Ok(DownlinkChannelEvent::HandlerReady))
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                },
                maybe_result = OptionFuture::from(write_stream.as_mut().map(|str| str.next())), if write_stream.is_active() => {
                    match maybe_result.flatten() {
                        Some(Ok(_)) => Some(Ok(DownlinkChannelEvent::WriteCompleted)),
                        Some(Err(e)) => {
                            write_stream.make_inactive();
                            Some(Err(DownlinkChannelError::WriteFailed(e)))
                        },
                        _ => {
                            *write_stream = Writes::Stopped;
                            Some(Ok(DownlinkChannelEvent::WriteStreamTerminated))
                        }
                    }
                }
            }
        });
        let result = if let Some(stop_signal) = stop_rx.as_mut() {
            match futures::future::select(stop_signal, select_next).await {
                Either::Left((triggered_result, select_next)) => {
                    *stop_rx = None;
                    if triggered_result.is_ok() {
                        *receiver = None;
                        if dl_state.get().is_linked() {
                            *next = Some(Ok(DownlinkNotification::Unlinked));
                            Some(Ok(DownlinkChannelEvent::HandlerReady))
                        } else {
                            None
                        }
                    } else {
                        select_next.await
                    }
                }
                Either::Right((result, _)) => result,
            }
        } else {
            select_next.await
        };
        if receiver.is_none() {
            if let Writes::Active(w) = write_stream {
                if let Err(error) = w.close().await {
                    error!(error= %error, "Closing write stream failed.");
                }
                write_stream.make_inactive();
            }
        }
        result
    }
}

impl<K, V, LC, Context, State> DownlinkChannel<Context> for HostedMapDownlink<K, V, LC, State>
where
    State: MapDlStateOps<K, V, Context>,
    K: Hash + Eq + Ord + Clone + Form + Send + 'static,
    V: Form + Send + 'static,
    K::Rec: Send,
    V::Rec: Send,
    LC: MapDownlinkLifecycle<K, V, Context> + 'static,
{
    fn kind(&self) -> DownlinkKind {
        DownlinkKind::Map
    }

    fn address(&self) -> &Address<Text> {
        &self.address
    }

    fn await_ready(
        &mut self,
    ) -> BoxFuture<'_, Option<Result<DownlinkChannelEvent, DownlinkChannelError>>> {
        self.select_next().boxed()
    }

    fn next_event(&mut self, context: &Context) -> Option<BoxEventHandler<'_, Context>> {
        let HostedMapDownlink {
            address,
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
                Ok(DownlinkNotification::Linked) => {
                    debug!(address = %address, "Downlink linked.");
                    if dl_state.get() == DlState::Unlinked {
                        dl_state.set(DlState::Linked);
                    }
                    Some(lifecycle.on_linked().boxed())
                }
                Ok(DownlinkNotification::Synced) => {
                    debug!(address = %address, "Downlink synced.");
                    dl_state.set(DlState::Synced);
                    Some(state.with(context, |map| lifecycle.on_synced(&map.map).boxed()))
                }
                Ok(DownlinkNotification::Event { body }) => {
                    let maybe_lifecycle =
                        if dl_state.get() == DlState::Synced || *events_when_not_synced {
                            Some(&*lifecycle)
                        } else {
                            None
                        };
                    trace!(address = %address, "Event received for downlink.");

                    match body {
                        MapMessage::Update { key, value } => {
                            trace!("Updating an entry.");
                            state.update(context, key, value, maybe_lifecycle)
                        }
                        MapMessage::Remove { key } => {
                            trace!("Removing an entry.");
                            state.remove(context, key, maybe_lifecycle)
                        }
                        MapMessage::Clear => {
                            trace!("Clearing the map.");
                            let old_map = state.clear(context);
                            maybe_lifecycle.map(|lifecycle| lifecycle.on_clear(old_map).boxed())
                        }
                        MapMessage::Take(n) => {
                            trace!("Retaining the first {} items.", n);
                            state.take(
                                context,
                                n.try_into()
                                    .expect("number to take does not fit into usize"),
                                maybe_lifecycle,
                            )
                        }
                        MapMessage::Drop(n) => {
                            trace!("Dropping the first {} items.", n);
                            state.drop(
                                context,
                                n.try_into()
                                    .expect("number to drop does not fit into usize"),
                                maybe_lifecycle,
                            )
                        }
                    }
                }
                Ok(DownlinkNotification::Unlinked) => {
                    debug!(address = %address, "Downlink unlinked.");
                    if *terminate_on_unlinked {
                        *receiver = None;
                        dl_state.set(DlState::Stopped);
                    } else {
                        dl_state.set(DlState::Unlinked);
                    }
                    state.clear(context);
                    Some(lifecycle.on_unlinked().boxed())
                }
                Err(_) => {
                    debug!(address = %address, "Downlink failed.");
                    if *terminate_on_unlinked {
                        *receiver = None;
                        dl_state.set(DlState::Stopped);
                    } else {
                        dl_state.set(DlState::Unlinked);
                    }
                    state.clear(context);
                    Some(lifecycle.on_failed().boxed())
                }
            }
        } else {
            None
        }
    }

    fn connect(&mut self, context: &Context, output: ByteWriter, input: ByteReader) {
        let HostedMapDownlink {
            receiver,
            write_stream,
            state,
            next,
            dl_state,
            ..
        } = self;
        *receiver = Some(FramedRead::new(input, Default::default()));
        write_stream.restart(output);
        state.clear(context);
        *next = None;
        dl_state.set(DlState::Unlinked);
    }

    fn can_restart(&self) -> bool {
        !self.config.terminate_on_unlinked && self.stop_rx.is_some()
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<(), std::io::Error>> {
        async move {
            let HostedMapDownlink { write_stream, .. } = self;
            if let Some(w) = write_stream.as_mut() {
                w.flush().await
            } else {
                Ok(())
            }
        }
        .boxed()
    }
}

/// A handle which can be used to modify the state of a map lane through a downlink.
#[derive(Debug)]
pub struct MapDownlinkHandle<K, V> {
    address: Address<Text>,
    sender: mpsc::UnboundedSender<MapOperation<K, V>>,
    stop_tx: Option<trigger::Sender>,
    observer: DlStateObserver,
}

impl<K, V> MapDownlinkHandle<K, V> {
    pub fn new(
        address: Address<Text>,
        sender: mpsc::UnboundedSender<MapOperation<K, V>>,
        stop_tx: trigger::Sender,
        state: &Arc<AtomicU8>,
    ) -> Self {
        MapDownlinkHandle {
            address,
            sender,
            stop_tx: Some(stop_tx),
            observer: DlStateObserver::new(state),
        }
    }

    /// Instruct the downlink to stop.
    pub fn stop(&mut self) {
        trace!(address = %self.address, "Stopping a map downlink.");
        if let Some(tx) = self.stop_tx.take() {
            tx.trigger();
        }
    }

    /// True if the downlink has stopped (regardless of whether it stopped cleanly or failed.)
    pub fn is_stopped(&self) -> bool {
        self.observer.get() == DlState::Stopped
    }

    /// True if the downlink is running and linked.
    pub fn is_linked(&self) -> bool {
        matches!(self.observer.get(), DlState::Linked | DlState::Synced)
    }
}

impl<K, V> MapDownlinkHandle<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    pub fn update(&self, key: K, value: V) -> Result<(), AgentRuntimeError> {
        trace!(address = %self.address, "Updating an entry on a map downlink.");
        self.sender.send(MapOperation::Update { key, value })?;
        Ok(())
    }

    pub fn remove(&self, key: K) -> Result<(), AgentRuntimeError> {
        trace!(address = %self.address, "Removing an entry on a map downlink.");
        self.sender.send(MapOperation::Remove { key })?;
        Ok(())
    }

    pub fn clear(&self) -> Result<(), AgentRuntimeError> {
        trace!(address = %self.address, "Clearing a map downlink.");
        self.sender.send(MapOperation::Clear)?;
        Ok(())
    }
}

#[derive(Default)]
enum MapWriteStreamState {
    #[default]
    Active,
    Stopping,
    Stopped,
}

type ReconWriter = FramedWrite<ByteWriter, MapOperationEncoder>;

#[pin_project]
pub struct MapWriteStream<K, V, S = ReconWriter> {
    #[pin]
    write: S,
    #[pin]
    op_rx: mpsc::UnboundedReceiver<MapOperation<K, V>>,
    queue: EventQueue<K, V>,
    state: MapWriteStreamState,
}

impl<K, V> MapWriteStream<K, V> {
    pub fn new(writer: ByteWriter, op_rx: mpsc::UnboundedReceiver<MapOperation<K, V>>) -> Self {
        Self::with_sink(FramedWrite::new(writer, Default::default()), op_rx)
    }
}

impl<K, V, S> MapWriteStream<K, V, S> {
    pub fn with_sink(sink: S, op_rx: mpsc::UnboundedReceiver<MapOperation<K, V>>) -> Self {
        MapWriteStream {
            write: sink,
            op_rx,
            queue: Default::default(),
            state: Default::default(),
        }
    }
}

impl<K, V, S> MapWriteStream<K, V, S>
where
    S: Sink<MapOperation<K, V>, Error = std::io::Error> + Unpin,
{
    async fn flush(&mut self) -> Result<(), std::io::Error> {
        SinkExt::<MapOperation<K, V>>::flush(&mut self.write).await
    }
}

impl<K, V, S> Stream for MapWriteStream<K, V, S>
where
    K: Clone + Eq + Hash + StructuralWritable + Send + 'static,
    V: StructuralWritable + Send + 'static,
    S: Sink<MapOperation<K, V>, Error = std::io::Error>,
{
    type Item = Result<(), std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut projected = self.project();
        loop {
            match projected.state {
                MapWriteStreamState::Active => {
                    let received = match projected.op_rx.as_mut().poll_recv(cx) {
                        Poll::Ready(Some(op)) => {
                            projected.queue.push(op);
                            true
                        }
                        Poll::Ready(None) => {
                            *projected.state = MapWriteStreamState::Stopping;
                            continue;
                        }
                        Poll::Pending => {
                            if projected.queue.is_empty() {
                                let result = ready!(Sink::<MapOperation<K, V>>::poll_flush(
                                    projected.write.as_mut(),
                                    cx
                                ));
                                break if result.is_err() {
                                    *projected.state = MapWriteStreamState::Stopped;
                                    Poll::Ready(Some(result))
                                } else {
                                    Poll::Pending
                                };
                            }
                            false
                        }
                    };
                    break match Sink::<MapOperation<K, V>>::poll_ready(projected.write.as_mut(), cx)
                    {
                        Poll::Ready(Ok(_)) => {
                            let op = projected.queue.pop().expect("Queue should be non-empty.");
                            let result = projected.write.start_send(op);
                            if result.is_err() {
                                *projected.state = MapWriteStreamState::Stopped;
                            }
                            Poll::Ready(Some(result))
                        }
                        Poll::Ready(Err(e)) => {
                            *projected.state = MapWriteStreamState::Stopped;
                            Poll::Ready(Some(Err(e)))
                        }
                        Poll::Pending if received => {
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        }
                        _ => Poll::Pending,
                    };
                }
                MapWriteStreamState::Stopping => {
                    if projected.queue.is_empty() {
                        let result = ready!(Sink::<MapOperation<K, V>>::poll_close(
                            projected.write.as_mut(),
                            cx
                        ));
                        *projected.state = MapWriteStreamState::Stopped;
                        if let Err(e) = result {
                            break Poll::Ready(Some(Err(e)));
                        }
                    } else {
                        break match ready!(Sink::<MapOperation<K, V>>::poll_ready(
                            projected.write.as_mut(),
                            cx
                        )) {
                            Ok(_) => {
                                let op = projected.queue.pop().expect("Queue should be non-empty.");
                                let result = projected.write.start_send(op);
                                if result.is_err() {
                                    *projected.state = MapWriteStreamState::Stopped;
                                }
                                Poll::Ready(Some(result))
                            }
                            Err(e) => {
                                *projected.state = MapWriteStreamState::Stopped;
                                Poll::Ready(Some(Err(e)))
                            }
                        };
                    }
                }
                MapWriteStreamState::Stopped => break Poll::Ready(None),
            }
        }
    }
}

impl<K, V> RestartableOutput for MapWriteStream<K, V> {
    type Source = mpsc::UnboundedReceiver<MapOperation<K, V>>;

    fn make_inactive(self) -> Self::Source {
        self.op_rx
    }

    fn restart(writer: ByteWriter, source: Self::Source) -> Self {
        MapWriteStream::new(writer, source)
    }
}
