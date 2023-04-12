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

use std::sync::{atomic::AtomicU8, Arc};

use futures::{future::BoxFuture, FutureExt, StreamExt};
use swim_api::{
    downlink::DownlinkKind,
    error::FrameIoError,
    protocol::downlink::{DownlinkNotification, ValueNotificationDecoder},
};
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_model::{address::Address, Text};
use swim_utilities::{io::byte_channel::ByteReader, trigger};
use tokio_util::codec::FramedRead;
use tracing::{debug, error, info, trace};

use crate::{
    agent_model::downlink::handlers::{DownlinkChannel, DownlinkFailed},
    config::SimpleDownlinkConfig,
    downlink_lifecycle::event::EventDownlinkLifecycle,
    event_handler::{BoxEventHandler, HandlerActionExt},
};

use super::{DlState, DlStateObserver, DlStateTracker};

#[cfg(test)]
mod tests;

/// An implementation of [`DownlinkChannel`] to allow an event downlink to be driven by an agent
/// task.
pub struct HostedEventDownlinkChannel<T: RecognizerReadable, LC> {
    address: Address<Text>,
    receiver: Option<FramedRead<ByteReader, ValueNotificationDecoder<T>>>,
    next: Option<Result<DownlinkNotification<T>, FrameIoError>>,
    lifecycle: LC,
    config: SimpleDownlinkConfig,
    dl_state: DlStateTracker,
    stop_rx: Option<trigger::Receiver>,
}

impl<T: RecognizerReadable, LC> HostedEventDownlinkChannel<T, LC> {
    pub fn new(
        address: Address<Text>,
        receiver: ByteReader,
        lifecycle: LC,
        config: SimpleDownlinkConfig,
        stop_rx: trigger::Receiver,
        state: Arc<AtomicU8>,
    ) -> Self {
        HostedEventDownlinkChannel {
            address,
            receiver: Some(FramedRead::new(receiver, Default::default())),
            next: None,
            lifecycle,
            config,
            dl_state: DlStateTracker::new(state),
            stop_rx: Some(stop_rx),
        }
    }
}

impl<T, LC, Context> DownlinkChannel<Context> for HostedEventDownlinkChannel<T, LC>
where
    T: RecognizerReadable + Send + 'static,
    T::Rec: Send,
    LC: EventDownlinkLifecycle<T, Context> + 'static,
{
    fn await_ready(&mut self) -> BoxFuture<'_, Option<Result<(), DownlinkFailed>>> {
        let HostedEventDownlinkChannel {
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
                                None
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

    fn next_event(&mut self, _context: &Context) -> Option<BoxEventHandler<'_, Context>> {
        let HostedEventDownlinkChannel {
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

    fn kind(&self) -> DownlinkKind {
        DownlinkKind::Event
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

    pub fn stop(&mut self) {
        trace!(address = %self.address, "Stopping an event downlink.");
        if let Some(tx) = self.stop_tx.take() {
            tx.trigger();
        }
    }

    pub fn is_stopped(&self) -> bool {
        self.observer.get() == DlState::Stopped
    }

    pub fn is_linked(&self) -> bool {
        matches!(self.observer.get(), DlState::Linked | DlState::Synced)
    }
}
