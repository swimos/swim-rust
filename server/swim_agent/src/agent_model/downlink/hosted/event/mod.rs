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
    marker::PhantomData,
    sync::{atomic::AtomicU8, Arc},
};

use futures::{future::BoxFuture, FutureExt, StreamExt};
use swim_api::{
    downlink::DownlinkKind,
    error::FrameIoError,
    protocol::downlink::{DownlinkNotification, ValueNotificationDecoder},
};
use swim_form::structural::read::{recognizer::RecognizerReadable, StructuralReadable};
use swim_model::{address::Address, Text};
use swim_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    trigger,
};
use tokio_util::codec::FramedRead;
use tracing::{debug, error, info, trace};

use crate::{
    agent_model::downlink::handlers::{
        BoxDownlinkChannel, DownlinkChannel, DownlinkChannelError, DownlinkChannelEvent,
    },
    config::SimpleDownlinkConfig,
    downlink_lifecycle::event::EventDownlinkLifecycle,
    event_handler::{BoxEventHandler, HandlerActionExt},
};

use super::{DlState, DlStateObserver, DlStateTracker};

#[cfg(test)]
mod tests;

pub struct HostedEventDownlinkFactory<T, LC> {
    address: Address<Text>,
    lifecycle: LC,
    config: SimpleDownlinkConfig,
    dl_state: Arc<AtomicU8>,
    stop_rx: trigger::Receiver,
    _type: PhantomData<fn() -> T>,
}

impl<T, LC> HostedEventDownlinkFactory<T, LC>
where
    T: StructuralReadable + Send + 'static,
    T::Rec: Send,
{
    pub fn new(
        address: Address<Text>,
        lifecycle: LC,
        config: SimpleDownlinkConfig,
        stop_rx: trigger::Receiver,
    ) -> Self {
        HostedEventDownlinkFactory {
            address,
            lifecycle,
            config,
            dl_state: Default::default(),
            stop_rx,
            _type: PhantomData,
        }
    }

    pub fn create<Context>(self, receiver: ByteReader) -> BoxDownlinkChannel<Context>
    where
        LC: EventDownlinkLifecycle<T, Context> + 'static,
    {
        let HostedEventDownlinkFactory {
            address,
            lifecycle,
            config,
            dl_state,
            stop_rx,
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
        };
        Box::new(chan)
    }

    pub fn dl_state(&self) -> &Arc<AtomicU8> {
        &self.dl_state
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
        r @ Some(Err(_)) => {
            *next = r;
            *receiver = None;
            error!(address = %address, "Downlink input channel failed.");
            Some(Err(DownlinkChannelError::ReadFailed))
        }
        _ => {
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
        DownlinkKind::Event
    }

    fn await_ready(
        &mut self,
    ) -> BoxFuture<'_, Option<Result<DownlinkChannelEvent, DownlinkChannelError>>> {
        self.select_next().boxed()
    }

    fn next_event(&mut self, _context: &Context) -> Option<BoxEventHandler<'_, Context>> {
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
                    Some(lifecycle.on_linked().boxed())
                }
                Ok(DownlinkNotification::Synced) => {
                    debug!(address = %address, "Downlink synced.");
                    dl_state.set(DlState::Synced);
                    Some(lifecycle.on_synced(&()).boxed())
                }
                Ok(DownlinkNotification::Event { body }) => {
                    trace!(address = %address, "Event received for downlink.");
                    let handler = if dl_state.get() == DlState::Synced || *events_when_not_synced {
                        let handler = lifecycle.on_event(body).boxed();
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
                    Some(lifecycle.on_failed().boxed())
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
}
