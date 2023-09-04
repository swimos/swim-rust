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

use futures::{
    future::{BoxFuture, OptionFuture},
    ready, FutureExt, Sink, SinkExt, Stream, StreamExt,
};
use pin_project::pin_project;
use std::{
    cell::RefCell,
    pin::{pin, Pin},
    sync::{atomic::AtomicU8, Arc},
    task::{Context, Poll},
};
use swim_api::protocol::WithLenReconEncoder;
use swim_api::{
    downlink::DownlinkKind,
    error::{DownlinkFailureReason, DownlinkRuntimeError, FrameIoError},
    protocol::downlink::{DownlinkNotification, ValueNotificationDecoder},
};
use swim_form::{
    structural::{read::recognizer::RecognizerReadable, write::StructuralWritable},
    Form,
};
use swim_model::{address::Address, Text};
use swim_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    sync::circular_buffer,
    trigger,
};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, error, info, trace};

use crate::{
    agent_model::downlink::handlers::{
        BoxDownlinkChannel, DownlinkChannel, DownlinkChannelError, DownlinkChannelEvent,
    },
    config::SimpleDownlinkConfig,
    downlink_lifecycle::value::ValueDownlinkLifecycle,
    event_handler::{BoxEventHandler, HandlerActionExt},
};

use super::{DlState, DlStateObserver, DlStateTracker, OutputWriter, RestartableOutput};

#[cfg(test)]
mod tests;

/// Operations that need to be supported by the state store of a value downlink. The intention
/// of this trait is to abstract over a self contained store a store contained within the field
/// of an agent. In both cases, the store itself will a [`RefCell`] containing an optional value.
pub trait ValueDlState<T>: Send {
    fn take_current(&self) -> Option<T>;

    fn replace(&self, value: T);

    // Perform an operation in a context with access to the state.
    fn with<R, Op: FnOnce(Option<&T>) -> R>(&self, op: Op) -> R;

    fn clear(&self);
}

impl<T: Send> ValueDlState<T> for RefCell<Option<T>> {
    fn take_current(&self) -> Option<T> {
        self.replace(None)
    }

    fn replace(&self, value: T) {
        self.replace(Some(value));
    }

    fn with<R, Op: FnOnce(Option<&T>) -> R>(&self, op: Op) -> R {
        op(self.borrow().as_ref())
    }

    fn clear(&self) {
        self.replace(None);
    }
}

pub struct HostedValueDownlinkFactory<T: RecognizerReadable, LC, State> {
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

    pub fn create<Context>(
        self,
        context: &Context,
        sender: ByteWriter,
        receiver: ByteReader,
    ) -> BoxDownlinkChannel<Context>
    where
        State: ValueDlState<T> + Send + 'static,
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
        let mut chan = HostedValueDownlink {
            address,
            receiver: None,
            write_stream: Writes::Inactive(watch_rx),
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

type Writes<T> = OutputWriter<ValueWriteStream<T>>;

pub struct HostedValueDownlink<T: RecognizerReadable, LC, State> {
    address: Address<Text>,
    receiver: Option<FramedRead<ByteReader, ValueNotificationDecoder<T>>>,
    write_stream: Writes<T>,
    state: State,
    next: Option<Result<DownlinkNotification<T>, FrameIoError>>,
    lifecycle: LC,
    config: SimpleDownlinkConfig,
    dl_state: DlStateTracker,
    stop_rx: Option<trigger::Receiver>,
}

impl<T, LC, State> HostedValueDownlink<T, LC, State>
where
    T: Form + Send + 'static,
    T::Rec: Send,
{
    async fn select_next(&mut self) -> Option<Result<DownlinkChannelEvent, DownlinkChannelError>> {
        let HostedValueDownlink {
            address,
            receiver,
            next,
            stop_rx,
            write_stream,
            dl_state,
            ..
        } = self;
        let mut select_next = pin!(async {
            tokio::select! {
                maybe_result = OptionFuture::from(receiver.as_mut().map(|rx| rx.next())) => {
                    match maybe_result {
                        Some(r@Some(Ok(_))) => {
                            *next = r;
                            Some(Ok(DownlinkChannelEvent::HandlerReady))
                        }
                        Some(r@Some(Err(_))) => {
                            *next = r;
                            *receiver = None;
                            error!(address = %address, "Downlink input channel failed.");
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
                        _ => {
                            None
                        }
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
            tokio::select! {
                biased;
                triggered_result = stop_signal => {
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
                result = &mut select_next => {
                    result
                }
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

impl<T, LC, Context, State> DownlinkChannel<Context> for HostedValueDownlink<T, LC, State>
where
    State: ValueDlState<T>,
    T: Form + Send + 'static,
    T::Rec: Send,
    LC: ValueDownlinkLifecycle<T, Context> + 'static,
{
    fn kind(&self) -> DownlinkKind {
        DownlinkKind::Value
    }

    fn address(&self) -> &Address<Text> {
        &self.address
    }

    fn await_ready(
        &mut self,
    ) -> BoxFuture<'_, Option<Result<DownlinkChannelEvent, DownlinkChannelError>>> {
        self.select_next().boxed()
    }

    fn next_event(&mut self, _context: &Context) -> Option<BoxEventHandler<'_, Context>> {
        let HostedValueDownlink {
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
                Ok(DownlinkNotification::Synced) => state.with(|maybe_value| {
                    debug!(address = %address, "Downlink synced.");
                    dl_state.set(DlState::Synced);
                    maybe_value.map(|value| lifecycle.on_synced(value).boxed())
                }),
                Ok(DownlinkNotification::Event { body }) => {
                    trace!(address = %address, "Event received for downlink.");
                    let prev = state.take_current();
                    let handler = if dl_state.get() == DlState::Synced || *events_when_not_synced {
                        let handler = lifecycle
                            .on_event(&body)
                            .followed_by(lifecycle.on_set(prev, &body))
                            .boxed();
                        Some(handler)
                    } else {
                        None
                    };
                    state.replace(body);
                    handler
                }
                Ok(DownlinkNotification::Unlinked) => {
                    debug!(address = %address, "Downlink unlinked.");
                    state.clear();
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
                    state.clear();
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

    fn connect(&mut self, _context: &Context, output: ByteWriter, input: ByteReader) {
        let HostedValueDownlink {
            receiver,
            write_stream,
            state,
            next,
            dl_state,
            ..
        } = self;
        *receiver = Some(FramedRead::new(input, Default::default()));
        write_stream.restart(output);
        state.clear();
        *next = None;
        dl_state.set(DlState::Unlinked);
    }

    fn can_restart(&self) -> bool {
        !self.config.terminate_on_unlinked && self.stop_rx.is_some()
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<(), std::io::Error>> {
        async move {
            let HostedValueDownlink { write_stream, .. } = self;
            if let Some(w) = write_stream.as_mut() {
                w.flush().await
            } else {
                Ok(())
            }
        }
        .boxed()
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

enum ValueWriteStreamState<T> {
    Active(Option<T>),
    Stopping(Option<T>),
    Stopped,
}

impl<T> std::fmt::Debug for ValueWriteStreamState<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active(_) => f.debug_tuple("Active").finish(),
            Self::Stopping(_) => f.debug_tuple("Stopping").finish(),
            Self::Stopped => write!(f, "Stopped"),
        }
    }
}

impl<T> Default for ValueWriteStreamState<T> {
    fn default() -> Self {
        ValueWriteStreamState::Active(None)
    }
}

type ReconWriter = FramedWrite<ByteWriter, WithLenReconEncoder>;

#[pin_project]
pub struct ValueWriteStream<T, S = ReconWriter> {
    #[pin]
    write: S,
    #[pin]
    watch_rx: circular_buffer::Receiver<T>,
    state: ValueWriteStreamState<T>,
}

impl<T, S> std::fmt::Debug for ValueWriteStream<T, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueWriteStream")
            .field("write", &"..")
            .field("watch_rx", &"..")
            .field("state", &self.state)
            .finish()
    }
}

impl<T, S> ValueWriteStream<T, S> {
    fn with_sink(write: S, watch_rx: circular_buffer::Receiver<T>) -> Self {
        ValueWriteStream {
            write,
            watch_rx,
            state: Default::default(),
        }
    }
}

impl<T> ValueWriteStream<T> {
    pub fn new(writer: ByteWriter, watch_rx: circular_buffer::Receiver<T>) -> Self {
        Self::with_sink(FramedWrite::new(writer, Default::default()), watch_rx)
    }

    pub fn into_watch_rx(self) -> circular_buffer::Receiver<T> {
        self.watch_rx
    }
}

impl<T: StructuralWritable> ValueWriteStream<T> {
    pub async fn close(&mut self) -> Result<(), std::io::Error> {
        SinkExt::<T>::close(&mut self.write).await
    }
}

impl<T, S> ValueWriteStream<T, S>
where
    S: Sink<T, Error = std::io::Error> + Unpin,
{
    async fn flush(&mut self) -> Result<(), std::io::Error> {
        SinkExt::<T>::flush(&mut self.write).await
    }
}

impl<T, S> Stream for ValueWriteStream<T, S>
where
    T: StructuralWritable + Send + 'static,
    S: Sink<T, Error = std::io::Error>,
{
    type Item = Result<(), std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut projected = self.project();
        loop {
            match projected.state {
                ValueWriteStreamState::Active(pending) => {
                    let mut received = false;
                    match projected.watch_rx.as_mut().poll_next(cx) {
                        Poll::Ready(Some(value)) => {
                            received = true;
                            *pending = Some(value);
                        }
                        Poll::Ready(_) => {
                            *projected.state = ValueWriteStreamState::Stopping(pending.take());
                            continue;
                        }
                        Poll::Pending => {}
                    }
                    if let Some(value) = pending.take() {
                        match Sink::<T>::poll_ready(projected.write.as_mut(), cx) {
                            Poll::Ready(Ok(_)) => {
                                let result = projected.write.as_mut().start_send(value);
                                if result.is_err() {
                                    *projected.state = ValueWriteStreamState::Stopped;
                                }
                                break Poll::Ready(Some(result));
                            }
                            Poll::Ready(Err(e)) => {
                                *projected.state = ValueWriteStreamState::Stopped;
                                break Poll::Ready(Some(Err(e)));
                            }
                            Poll::Pending => {
                                if received {
                                    cx.waker().wake_by_ref();
                                }
                                *projected.state = ValueWriteStreamState::Active(Some(value));
                                break Poll::Pending;
                            }
                        }
                    } else {
                        let result = ready!(Sink::<T>::poll_flush(projected.write.as_mut(), cx));
                        break if let Err(e) = result {
                            *projected.state = ValueWriteStreamState::Stopped;
                            Poll::Ready(Some(Err(e)))
                        } else {
                            Poll::Pending
                        };
                    }
                }
                ValueWriteStreamState::Stopping(pending) => {
                    if let Some(value) = pending.take() {
                        match Sink::<T>::poll_ready(projected.write.as_mut(), cx) {
                            Poll::Ready(Ok(_)) => {
                                let result = projected.write.as_mut().start_send(value);
                                if result.is_err() {
                                    *projected.state = ValueWriteStreamState::Stopped;
                                }
                                break Poll::Ready(Some(result));
                            }
                            Poll::Ready(Err(e)) => {
                                *projected.state = ValueWriteStreamState::Stopped;
                                break Poll::Ready(Some(Err(e)));
                            }
                            Poll::Pending => {
                                *projected.state = ValueWriteStreamState::Stopping(Some(value));
                                break Poll::Pending;
                            }
                        }
                    } else {
                        let result = ready!(Sink::<T>::poll_close(projected.write.as_mut(), cx));
                        *projected.state = ValueWriteStreamState::Stopped;
                        result?;
                    }
                }
                ValueWriteStreamState::Stopped => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl<T> RestartableOutput for ValueWriteStream<T> {
    type Source = circular_buffer::Receiver<T>;

    fn make_inactive(self) -> Self::Source {
        self.into_watch_rx()
    }

    fn restart(writer: ByteWriter, source: Self::Source) -> Self {
        ValueWriteStream::new(writer, source)
    }
}
