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

use bytes::BytesMut;
use futures::{
    future::{BoxFuture, Either, OptionFuture},
    stream::unfold,
    FutureExt, SinkExt, Stream, StreamExt,
};
use std::{
    cell::RefCell,
    fmt::Write,
    pin::pin,
    sync::{atomic::AtomicU8, Arc},
};
use swim_api::{
    downlink::DownlinkKind,
    error::{DownlinkFailureReason, DownlinkRuntimeError, FrameIoError},
    protocol::{
        downlink::{DownlinkNotification, ValueNotificationDecoder},
        WithLengthBytesCodec,
    },
};
use swim_form::{
    structural::{read::recognizer::RecognizerReadable, write::StructuralWritable},
    Form,
};
use swim_model::{address::Address, Text};
use swim_recon::printer::print_recon_compact;
use swim_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    sync::circular_buffer,
    trigger,
};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, error, info, trace};

use crate::{
    agent_model::downlink::handlers::{
        BoxDownlinkChannel2, DownlinkChannel, DownlinkChannel2, DownlinkChannelError,
        DownlinkChannelEvent, DownlinkFailed,
    },
    config::SimpleDownlinkConfig,
    downlink_lifecycle::value::ValueDownlinkLifecycle,
    event_handler::{BoxEventHandler, HandlerActionExt},
};

use super::{DlState, DlStateObserver, DlStateTracker};

#[cfg(test)]
mod tests;

/// Operations that need to be supported by the state store of a value downlink. The intention
/// of this trait is to abstract over a self contained store a store contained within the field
/// of an agent. In both cases, the store itself will a [`RefCell`] containing an optional value.
trait ValueDlState<T, Context>: Send {
    fn take_current(&self, context: &Context) -> Option<T>;

    fn replace(&self, context: &Context, value: T);

    // Perform an operation in a context with access to the state.
    fn with<R, Op: FnOnce(Option<&T>) -> R>(&self, context: &Context, op: Op) -> R;

    fn clear(&self, context: &Context);
}

impl<T: Send, Context> ValueDlState<T, Context> for RefCell<Option<T>> {
    fn take_current(&self, _context: &Context) -> Option<T> {
        self.replace(None)
    }

    fn replace(&self, _context: &Context, value: T) {
        self.replace(Some(value));
    }

    fn with<R, Op: FnOnce(Option<&T>) -> R>(&self, _context: &Context, op: Op) -> R {
        op(self.borrow().as_ref())
    }

    fn clear(&self, _context: &Context) {
        self.replace(None);
    }
}

/// An implementation of [`DownlinkChannel`] to allow a value downlink to be driven by an agent
/// task.
pub struct HostedValueDownlinkChannel<T: RecognizerReadable, LC, State> {
    address: Address<Text>,
    receiver: Option<FramedRead<ByteReader, ValueNotificationDecoder<T>>>,
    state: State,
    next: Option<Result<DownlinkNotification<T>, FrameIoError>>,
    lifecycle: LC,
    config: SimpleDownlinkConfig,
    dl_state: DlStateTracker,
    stop_rx: Option<trigger::Receiver>,
}

pub struct HostedValueDownlinkFactory<T, LC, State> {
    address: Address<Text>,
    state: State,
    lifecycle: LC,
    config: SimpleDownlinkConfig,
    dl_state: Arc<AtomicU8>,
    stop_rx: trigger::Receiver,
    watch_rx: circular_buffer::Receiver<T>,
}

impl<T, LC, State> HostedValueDownlinkFactory<T, LC, State>
where
    T: Form + Send + 'static,
    T::Rec: Send,
{
    pub fn new(
        address: Address<Text>,
        lifecycle: LC,
        state: State,
        config: SimpleDownlinkConfig,
        stop_rx: trigger::Receiver,
        watch_rx: circular_buffer::Receiver<T>,
    ) -> Self {
        HostedValueDownlinkFactory {
            address,
            state,
            lifecycle,
            config,
            dl_state: Default::default(),
            stop_rx,
            watch_rx,
        }
    }

    fn create<Context>(self, io: (ByteWriter, ByteReader)) -> BoxDownlinkChannel2<Context>
    where
        State: ValueDlState<T, Context> + Send + 'static,
        LC: ValueDownlinkLifecycle<T, Context> + 'static,
    {
        let HostedValueDownlinkFactory {
            address,
            state,
            lifecycle,
            config,
            dl_state,
            stop_rx,
            watch_rx,
        } = self;
        let (sender, receiver) = io;
        let write_stream = value_dl_write_stream(sender, watch_rx).boxed();
        let chan = HostedValueDownlink2 {
            address,
            receiver: Some(FramedRead::new(receiver, Default::default())),
            write_stream: Some(write_stream),
            state,
            next: None,
            lifecycle,
            config,
            dl_state: DlStateTracker::new(dl_state),
            stop_rx: Some(stop_rx),
        };
        Box::new(chan)
    }
}

pub struct HostedValueDownlink2<T: RecognizerReadable, LC, State, Writes> {
    address: Address<Text>,
    receiver: Option<FramedRead<ByteReader, ValueNotificationDecoder<T>>>,
    write_stream: Option<Writes>,
    state: State,
    next: Option<Result<DownlinkNotification<T>, FrameIoError>>,
    lifecycle: LC,
    config: SimpleDownlinkConfig,
    dl_state: DlStateTracker,
    stop_rx: Option<trigger::Receiver>,
}

impl<T, LC, State, Writes> HostedValueDownlink2<T, LC, State, Writes>
where
    T: RecognizerReadable + Send + 'static,
    T::Rec: Send,
    Writes: Stream<Item = Result<(), std::io::Error>> + Send + Unpin + 'static,
{
    async fn select_next(&mut self) -> Option<Result<DownlinkChannelEvent, DownlinkChannelError>> {
        let HostedValueDownlink2 {
            address,
            receiver,
            next,
            stop_rx,
            write_stream,
            ..
        } = self;
        let select_next = pin!(async {
            tokio::select! {
                maybe_result = OptionFuture::from(receiver.as_mut().map(|rx| rx.next())), if receiver.is_some() => {
                    match maybe_result.flatten() {
                        r@Some(Ok(_)) => {
                            *next = r;
                            Some(Ok(DownlinkChannelEvent::HandlerReady))
                        }
                        r@Some(Err(_)) => {
                            *next = r;
                            *receiver = None;
                            error!(address = %address, "Downlink input channel failed.");
                            Some(Err(DownlinkChannelError::ReadFailed))
                        }
                        _ => {
                            *receiver = None;
                            None
                        }
                    }
                },
                maybe_result = OptionFuture::from(write_stream.as_mut().map(|str| str.next())), if write_stream.is_some() => {
                    match maybe_result.flatten() {
                        Some(Ok(_)) => Some(Ok(DownlinkChannelEvent::WriteCompleted)),
                        Some(Err(e)) => Some(Err(DownlinkChannelError::WriteFailed(e))),
                        _ => {
                            *write_stream = None;
                            Some(Ok(DownlinkChannelEvent::WriteStreamTerminated))
                        }
                    }
                }
                else => {
                    info!(address = %address, "Downlink terminated normally.");
                    None
                },
            }
        });
        if let Some(stop_signal) = stop_rx.as_mut() {
            match futures::future::select(stop_signal, select_next).await {
                Either::Left((triggered_result, select_next)) => {
                    *stop_rx = None;
                    if triggered_result.is_ok() {
                        *receiver = None;
                        *write_stream = None;
                        *next = Some(Ok(DownlinkNotification::Unlinked));
                        Some(Ok(DownlinkChannelEvent::HandlerReady))
                    } else {
                        select_next.await
                    }
                }
                Either::Right((result, _)) => result,
            }
        } else {
            select_next.await
        }
    }
}

impl<T, LC, Context, State, Writes> DownlinkChannel2<Context>
    for HostedValueDownlink2<T, LC, State, Writes>
where
    State: ValueDlState<T, Context>,
    T: RecognizerReadable + Send + 'static,
    T::Rec: Send,
    LC: ValueDownlinkLifecycle<T, Context> + 'static,
    Writes: Stream<Item = Result<(), std::io::Error>> + Send + Unpin + 'static,
{
    fn kind(&self) -> DownlinkKind {
        DownlinkKind::Value
    }

    fn await_ready(
        &mut self,
    ) -> BoxFuture<'_, Option<Result<DownlinkChannelEvent, DownlinkChannelError>>> {
        self.select_next().boxed()
    }

    fn next_event(&mut self, context: &Context) -> Option<BoxEventHandler<'_, Context>> {
        let HostedValueDownlink2 {
            address,
            receiver,
            state,
            next,
            lifecycle,
            dl_state,
            config:
                SimpleDownlinkConfig {
                    events_when_not_synced,
                    terminate_on_unlinked,
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
                Ok(DownlinkNotification::Synced) => state.with(context, |maybe_value| {
                    debug!(address = %address, "Downlink synced.");
                    dl_state.set(DlState::Synced);
                    maybe_value.map(|value| lifecycle.on_synced(value).boxed())
                }),
                Ok(DownlinkNotification::Event { body }) => {
                    trace!(address = %address, "Event received for downlink.");
                    let prev = state.take_current(context);
                    let handler = if dl_state.get() == DlState::Synced || *events_when_not_synced {
                        let handler = lifecycle
                            .on_event(&body)
                            .followed_by(lifecycle.on_set(prev, &body))
                            .boxed();
                        Some(handler)
                    } else {
                        None
                    };
                    state.replace(context, body);
                    handler
                }
                Ok(DownlinkNotification::Unlinked) => {
                    debug!(address = %address, "Downlink unlinked.");
                    state.clear(context);
                    if *terminate_on_unlinked {
                        *receiver = None;
                        dl_state.set(DlState::Stopped);
                    } else {
                        dl_state.set(DlState::Unlinked);
                    }
                    Some(lifecycle.on_unlinked().boxed())
                }
                Err(_) => {
                    debug!(address = %address, "Downlink failed.");
                    state.clear(context);
                    if *terminate_on_unlinked {
                        *receiver = None;
                        dl_state.set(DlState::Stopped);
                    } else {
                        dl_state.set(DlState::Unlinked);
                    }
                    Some(lifecycle.on_failed().boxed())
                }
            }
        } else {
            None
        }
    }
}

impl<T: RecognizerReadable, LC, State> HostedValueDownlinkChannel<T, LC, State> {
    pub fn new(
        address: Address<Text>,
        receiver: ByteReader,
        lifecycle: LC,
        state: State,
        config: SimpleDownlinkConfig,
        stop_rx: trigger::Receiver,
        dl_state: Arc<AtomicU8>,
    ) -> Self {
        HostedValueDownlinkChannel {
            address,
            receiver: Some(FramedRead::new(receiver, Default::default())),
            state,
            next: None,
            lifecycle,
            config,
            dl_state: DlStateTracker::new(dl_state),
            stop_rx: Some(stop_rx),
        }
    }
}

impl<T, LC, Context, State> DownlinkChannel<Context> for HostedValueDownlinkChannel<T, LC, State>
where
    State: ValueDlState<T, Context>,
    T: RecognizerReadable + Send + 'static,
    T::Rec: Send,
    LC: ValueDownlinkLifecycle<T, Context> + 'static,
{
    fn await_ready(&mut self) -> BoxFuture<'_, Option<Result<(), DownlinkFailed>>> {
        let HostedValueDownlinkChannel {
            address,
            receiver,
            next,
            stop_rx,
            ..
        } = self;
        async move {
            let result = if let Some(rx) = receiver {
                if let Some(stop_signal) = stop_rx.as_mut() {
                    tokio::select! {
                        biased;
                        triggered_result = stop_signal => {
                            *stop_rx = None;
                            if triggered_result.is_ok() {
                                *receiver = None;
                                Some(Ok(DownlinkNotification::Unlinked))
                            } else {
                                rx.next().await
                            }
                        }
                        result = rx.next() => result,
                    }
                } else {
                    rx.next().await
                }
            } else {
                return None;
            };
            match result {
                Some(Ok(notification)) => {
                    *next = Some(Ok(notification));
                    Some(Ok(()))
                }
                Some(Err(e)) => {
                    error!(address = %address, "Downlink input channel failed.");
                    *receiver = None;
                    *next = Some(Err(e));
                    Some(Err(DownlinkFailed))
                }
                _ => {
                    info!(address = %address, "Downlink terminated normally.");
                    *receiver = None;
                    None
                }
            }
        }
        .boxed()
    }

    fn next_event(&mut self, context: &Context) -> Option<BoxEventHandler<'_, Context>> {
        let HostedValueDownlinkChannel {
            address,
            receiver,
            state,
            next,
            lifecycle,
            dl_state,
            config:
                SimpleDownlinkConfig {
                    events_when_not_synced,
                    terminate_on_unlinked,
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
                Ok(DownlinkNotification::Synced) => state.with(context, |maybe_value| {
                    debug!(address = %address, "Downlink synced.");
                    dl_state.set(DlState::Synced);
                    maybe_value.map(|value| lifecycle.on_synced(value).boxed())
                }),
                Ok(DownlinkNotification::Event { body }) => {
                    trace!(address = %address, "Event received for downlink.");
                    let prev = state.take_current(context);
                    let handler = if dl_state.get() == DlState::Synced || *events_when_not_synced {
                        let handler = lifecycle
                            .on_event(&body)
                            .followed_by(lifecycle.on_set(prev, &body))
                            .boxed();
                        Some(handler)
                    } else {
                        None
                    };
                    state.replace(context, body);
                    handler
                }
                Ok(DownlinkNotification::Unlinked) => {
                    debug!(address = %address, "Downlink unlinked.");
                    state.clear(context);
                    if *terminate_on_unlinked {
                        *receiver = None;
                        dl_state.set(DlState::Stopped);
                    } else {
                        dl_state.set(DlState::Unlinked);
                    }
                    Some(lifecycle.on_unlinked().boxed())
                }
                Err(_) => {
                    debug!(address = %address, "Downlink failed.");
                    state.clear(context);
                    if *terminate_on_unlinked {
                        *receiver = None;
                        dl_state.set(DlState::Stopped);
                    } else {
                        dl_state.set(DlState::Unlinked);
                    }
                    Some(lifecycle.on_failed().boxed())
                }
            }
        } else {
            None
        }
    }

    fn kind(&self) -> DownlinkKind {
        DownlinkKind::Value
    }
}

/// A handle which can be used to set the value of a lane through a value downlink or stop the
/// downlink.
#[derive(Debug)]
pub struct ValueDownlinkHandle<T> {
    address: Address<Text>,
    inner: circular_buffer::Sender<T>,
    stop_tx: Option<trigger::Sender>,
    observer: DlStateObserver,
}

impl<T> ValueDownlinkHandle<T> {
    pub fn new(
        address: Address<Text>,
        inner: circular_buffer::Sender<T>,
        stop_tx: trigger::Sender,
        state: &Arc<AtomicU8>,
    ) -> Self {
        ValueDownlinkHandle {
            address,
            inner,
            stop_tx: Some(stop_tx),
            observer: DlStateObserver::new(state),
        }
    }
}

impl<T> ValueDownlinkHandle<T> {
    /// Instruct the downlink to stop.
    pub fn stop(&mut self) {
        trace!(address = %self.address, "Stopping a value downlink.");
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

impl<T> ValueDownlinkHandle<T>
where
    T: Send + Sync,
{
    pub fn set(&mut self, value: T) -> Result<(), DownlinkRuntimeError> {
        trace!(address = %self.address, "Attempting to set a value into a downlink.");
        if self.inner.try_send(value).is_err() {
            info!(address = %self.address, "Downlink writer failed.");
            Err(DownlinkRuntimeError::DownlinkConnectionFailed(
                DownlinkFailureReason::DownlinkStopped,
            ))
        } else {
            Ok(())
        }
    }
}

/// Task to write the values sent by a [`ValueDownlinkHandle`] to an outgoing channel.
pub fn value_dl_write_stream<T>(
    writer: ByteWriter,
    watch_rx: circular_buffer::Receiver<T>,
) -> impl Stream<Item = Result<(), std::io::Error>> + Send + 'static
where
    T: StructuralWritable + Send + 'static,
{
    let buffer = BytesMut::new();

    let writer = FramedWrite::new(writer, WithLengthBytesCodec::default());

    unfold((writer, watch_rx, buffer), |state| async move {
        let (mut writer, mut rx, mut buffer) = state;

        if let Ok(value) = rx.recv().await {
            buffer.clear();
            write!(&mut buffer, "{}", print_recon_compact(&value))
                .expect("Writing to Recon should be infallible.");
            trace!(content = ?buffer.as_ref(), "Writing a value to a value downlink.");
            let result = writer.send(buffer.as_ref()).await;
            Some((result, (writer, rx, buffer)))
        } else {
            None
        }
    })
}
