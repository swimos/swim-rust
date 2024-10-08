// Copyright 2015-2024 Swim Inc.
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
    marker::PhantomData,
    sync::{atomic::AtomicU8, Arc},
};

use futures::{
    future::{ready, BoxFuture},
    FutureExt, StreamExt,
};
use swimos_agent_protocol::encoding::downlink::ValueNotificationDecoder;
use swimos_agent_protocol::DownlinkNotification;
use swimos_api::{address::Address, agent::DownlinkKind, error::FrameIoError};
use swimos_form::read::{RecognizerReadable, StructuralReadable};
use swimos_model::Text;
use swimos_utilities::{
    byte_channel::{ByteReader, ByteWriter},
    trigger,
};
use tokio_util::codec::FramedRead;
use tracing::{debug, error, info, trace};

use crate::{
    agent_model::downlink::{
        BoxDownlinkChannel, DownlinkChannel, DownlinkChannelError, DownlinkChannelEvent,
        DownlinkChannelFactory,
    },
    config::SimpleDownlinkConfig,
    downlink_lifecycle::EventDownlinkLifecycle,
    event_handler::{HandlerActionExt, LocalBoxEventHandler},
};

use super::{DlState, DlStateObserver, DlStateTracker};

#[cfg(test)]
mod tests;

pub struct EventDownlinkFactory<T: RecognizerReadable, LC> {
    address: Address<Text>,
    lifecycle: LC,
    config: SimpleDownlinkConfig,
    dl_state: Arc<AtomicU8>,
    stop_rx: trigger::Receiver,
    map_events: bool,
    _type: PhantomData<fn() -> T>,
}

impl<T, LC> EventDownlinkFactory<T, LC>
where
    T: StructuralReadable + Send + 'static,
    T::Rec: Send,
{
    pub fn new(
        address: Address<Text>,
        lifecycle: LC,
        config: SimpleDownlinkConfig,
        stop_rx: trigger::Receiver,
        map_events: bool,
    ) -> Self {
        EventDownlinkFactory {
            address,
            lifecycle,
            config,
            dl_state: Default::default(),
            stop_rx,
            map_events,
            _type: PhantomData,
        }
    }

    pub fn dl_state(&self) -> &Arc<AtomicU8> {
        &self.dl_state
    }
}

impl<T, LC, Context> DownlinkChannelFactory<Context> for EventDownlinkFactory<T, LC>
where
    T: StructuralReadable + Send + 'static,
    T::Rec: Send,
    LC: EventDownlinkLifecycle<T, Context> + 'static,
{
    fn create(
        self,
        _context: &Context,
        _: ByteWriter,
        receiver: ByteReader,
    ) -> BoxDownlinkChannel<Context> {
        let EventDownlinkFactory {
            address,
            lifecycle,
            config,
            dl_state,
            stop_rx,
            map_events,
            ..
        } = self;
        let chan = HostedEventDownlink {
            address,
            receiver: Some(FramedRead::new(receiver, Default::default())),
            next: None,
            lifecycle,
            config,
            dl_state: DlStateTracker::new(dl_state),
            stop_rx: Some(stop_rx),
            write_terminated: false,
            map_events,
        };
        Box::new(chan)
    }

    fn create_box(
        self: Box<Self>,
        context: &Context,
        tx: ByteWriter,
        rx: ByteReader,
    ) -> BoxDownlinkChannel<Context> {
        (*self).create(context, tx, rx)
    }

    fn kind(&self) -> DownlinkKind {
        if self.map_events {
            DownlinkKind::MapEvent
        } else {
            DownlinkKind::Event
        }
    }
}

/// An implementation of [`DownlinkChannel`] to allow an event downlink to be driven by an agent
/// task.
pub struct HostedEventDownlink<T: RecognizerReadable, LC> {
    address: Address<Text>,
    receiver: Option<FramedRead<ByteReader, ValueNotificationDecoder<T>>>,
    next: Option<Result<DownlinkNotification<T>, FrameIoError>>,
    lifecycle: LC,
    config: SimpleDownlinkConfig,
    dl_state: DlStateTracker,
    stop_rx: Option<trigger::Receiver>,
    write_terminated: bool,
    map_events: bool,
}

impl<T, LC> HostedEventDownlink<T, LC>
where
    T: RecognizerReadable + Send + 'static,
    T::Rec: Send,
{
    async fn select_next(&mut self) -> Option<Result<DownlinkChannelEvent, DownlinkChannelError>> {
        let HostedEventDownlink {
            address,
            receiver,
            next,
            stop_rx,
            write_terminated,
            dl_state,
            ..
        } = self;
        // Most downlinks can send values to their remote lanes. Event downlink are an exception to this.
        // As the interface for downlinks includes and write half by default we indicate that we will never
        // send any values but flagging the output as terminated immediately.
        if !*write_terminated {
            *write_terminated = true;
            return Some(Ok(DownlinkChannelEvent::WriteStreamTerminated));
        }
        if let Some(rx) = receiver {
            if let Some(stop_signal) = stop_rx.as_mut() {
                tokio::select! {
                    biased;
                    triggered_result = stop_signal => {
                        *stop_rx = None;
                        if triggered_result.is_ok() {
                            info!(address = %address, "Downlink stopped by trigger.");
                            *receiver = None;
                            if dl_state.get().is_linked() {
                                *next = Some(Ok(DownlinkNotification::Unlinked));
                                Some(Ok(DownlinkChannelEvent::HandlerReady))
                            } else {
                                None
                            }
                        } else {
                            handle_read(rx.next().await, address, next, receiver, dl_state)
                        }
                    }
                    result = rx.next() => handle_read(result, address, next, receiver, dl_state),
                }
            } else {
                handle_read(rx.next().await, address, next, receiver, dl_state)
            }
        } else {
            info!(address = %address, "Downlink terminated normally.");
            None
        }
    }
}

fn handle_read<T: RecognizerReadable>(
    maybe_result: Option<Result<DownlinkNotification<T>, FrameIoError>>,
    address: &Address<Text>,
    next: &mut Option<Result<DownlinkNotification<T>, FrameIoError>>,
    receiver: &mut Option<FramedRead<ByteReader, ValueNotificationDecoder<T>>>,
    dl_state: &DlStateTracker,
) -> Option<Result<DownlinkChannelEvent, DownlinkChannelError>> {
    match maybe_result {
        r @ Some(Ok(_)) => {
            *next = r;
            Some(Ok(DownlinkChannelEvent::HandlerReady))
        }
        Some(Err(error)) => {
            error!(address = %address, error = %error, "Downlink input channel failed.");
            *next = Some(Err(error));
            *receiver = None;
            Some(Err(DownlinkChannelError::ReadFailed))
        }
        _ => {
            trace!("Downlink receiver closed.");
            *receiver = None;
            if dl_state.get().is_linked() {
                *next = Some(Ok(DownlinkNotification::Unlinked));
                Some(Ok(DownlinkChannelEvent::HandlerReady))
            } else {
                None
            }
        }
    }
}

impl<T, LC, Context> DownlinkChannel<Context> for HostedEventDownlink<T, LC>
where
    T: RecognizerReadable + Send + 'static,
    T::Rec: Send,
    LC: EventDownlinkLifecycle<T, Context> + 'static,
{
    fn kind(&self) -> DownlinkKind {
        if self.map_events {
            DownlinkKind::MapEvent
        } else {
            DownlinkKind::Event
        }
    }

    fn await_ready(
        &mut self,
    ) -> BoxFuture<'_, Option<Result<DownlinkChannelEvent, DownlinkChannelError>>> {
        self.select_next().boxed()
    }

    fn next_event(&mut self, _context: &Context) -> Option<LocalBoxEventHandler<'_, Context>> {
        let HostedEventDownlink {
            address,
            receiver,
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
                    Some(lifecycle.on_linked().boxed_local())
                }
                Ok(DownlinkNotification::Synced) => {
                    debug!(address = %address, "Downlink synced.");
                    dl_state.set(DlState::Synced);
                    Some(lifecycle.on_synced(&()).boxed_local())
                }
                Ok(DownlinkNotification::Event { body }) => {
                    trace!(address = %address, "Event received for downlink.");
                    let handler = if dl_state.get() == DlState::Synced || *events_when_not_synced {
                        let handler = lifecycle.on_event(body).boxed_local();
                        Some(handler)
                    } else {
                        None
                    };
                    handler
                }
                Ok(DownlinkNotification::Unlinked) => {
                    debug!(address = %address, "Downlink unlinked.");
                    if *terminate_on_unlinked {
                        *receiver = None;
                        dl_state.set(DlState::Stopped);
                    } else {
                        dl_state.set(DlState::Unlinked);
                    }
                    Some(lifecycle.on_unlinked().boxed_local())
                }
                Err(_) => {
                    debug!(address = %address, "Downlink failed.");
                    if *terminate_on_unlinked {
                        *receiver = None;
                        dl_state.set(DlState::Stopped);
                    } else {
                        dl_state.set(DlState::Unlinked);
                    }
                    Some(lifecycle.on_failed().boxed_local())
                }
            }
        } else {
            None
        }
    }

    fn connect(&mut self, _context: &Context, _output: ByteWriter, input: ByteReader) {
        let HostedEventDownlink {
            receiver,
            next,
            dl_state,
            write_terminated,
            ..
        } = self;
        *next = None;
        dl_state.set(DlState::Unlinked);
        *write_terminated = false;
        *receiver = Some(FramedRead::new(input, Default::default()));
    }

    fn can_restart(&self) -> bool {
        !self.config.terminate_on_unlinked && self.stop_rx.is_some()
    }

    fn address(&self) -> &Address<Text> {
        &self.address
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<(), std::io::Error>> {
        ready(Ok(())).boxed()
    }
}

/// A handle which can be used to stop an event downlink.
#[derive(Debug)]
pub struct EventDownlinkHandle {
    address: Address<Text>,
    stop_tx: Option<trigger::Sender>,
    observer: DlStateObserver,
}

impl EventDownlinkHandle {
    pub fn new(address: Address<Text>, stop_tx: trigger::Sender, state: &Arc<AtomicU8>) -> Self {
        EventDownlinkHandle {
            address,
            stop_tx: Some(stop_tx),
            observer: DlStateObserver::new(state),
        }
    }

    /// Instruct the downlink to stop.
    pub fn stop(&mut self) {
        trace!(address = %self.address, "Stopping an event downlink.");
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

    /// Consumes this handle and returns the stop handle, if it exists.
    pub fn into_stop_rx(self) -> Option<trigger::Sender> {
        self.stop_tx
    }
}
