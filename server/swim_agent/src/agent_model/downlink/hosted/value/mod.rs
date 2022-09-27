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

use std::cell::RefCell;

use bytes::BytesMut;
use futures::{future::BoxFuture, stream::unfold, FutureExt, SinkExt, Stream, StreamExt};
use std::fmt::Write;
use swim_api::{
    error::{DownlinkFailureReason, DownlinkRuntimeError, FrameIoError},
    protocol::{
        downlink::{DownlinkNotification, ValueNotificationDecoder},
        WithLengthBytesCodec,
    },
};
use swim_form::structural::{read::recognizer::RecognizerReadable, write::StructuralWritable};
use swim_model::{address::Address, Text};
use swim_recon::printer::print_recon_compact;
use swim_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    sync::circular_buffer,
};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, error, info, trace};

use crate::{
    agent_model::downlink::handlers::DownlinkChannel,
    config::ValueDownlinkConfig,
    downlink_lifecycle::value::ValueDownlinkLifecycle,
    event_handler::{BoxEventHandler, EventHandlerExt, HandlerActionExt},
};

use super::DlState;

#[cfg(test)]
mod tests;

/// Operations that need to be supported by the state store of a value downlink. The intention
/// of this trait is to abstract over a self contained store a store contained within the field
/// of an agent. In both cases, the store itself will a [`RefCell`] containing an optional value.
trait ValueDlState<T, Context> {
    fn take_current(&self, context: &Context) -> Option<T>;

    fn replace(&self, context: &Context, value: T);

    // Perform an operation in a context with access to the state.
    fn with<R, Op: FnOnce(Option<&T>) -> R>(&self, context: &Context, op: Op) -> R;

    fn clear(&self, context: &Context);
}

impl<T, Context> ValueDlState<T, Context> for RefCell<Option<T>> {
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

impl<T, Context, F> ValueDlState<T, Context> for F
where
    F: for<'a> Fn(&'a Context) -> &'a RefCell<Option<T>>,
{
    fn take_current(&self, context: &Context) -> Option<T> {
        self(context).replace(None)
    }

    fn replace(&self, context: &Context, value: T) {
        self(context).replace(Some(value));
    }

    fn with<R, Op: FnOnce(Option<&T>) -> R>(&self, context: &Context, op: Op) -> R {
        op(self(context).borrow().as_ref())
    }

    fn clear(&self, context: &Context) {
        self(context).replace(None);
    }
}

/// An implementation of [`DownlinkChannel`] to allow a value downlink to be driven by an agent
/// task.
pub struct HostedValueDownlinkChannel<T: RecognizerReadable, LC, State> {
    address: Address<Text>,
    receiver: Option<FramedRead<ByteReader, ValueNotificationDecoder<T>>>,
    state: State,
    next: Option<DownlinkNotification<T>>,
    lifecycle: LC,
    config: ValueDownlinkConfig,
    dl_state: DlState,
}

impl<T: RecognizerReadable, LC, State> HostedValueDownlinkChannel<T, LC, State> {
    pub fn new(
        address: Address<Text>,
        receiver: ByteReader,
        lifecycle: LC,
        state: State,
        config: ValueDownlinkConfig,
    ) -> Self {
        HostedValueDownlinkChannel {
            address,
            receiver: Some(FramedRead::new(receiver, Default::default())),
            state,
            next: None,
            lifecycle,
            config,
            dl_state: DlState::Unlinked,
        }
    }
}

impl<T, LC, State, Context> DownlinkChannel<Context> for HostedValueDownlinkChannel<T, LC, State>
where
    State: ValueDlState<T, Context>,
    T: RecognizerReadable + Send + 'static,
    T::Rec: Send,
    LC: ValueDownlinkLifecycle<T, Context> + 'static,
{
    fn await_ready(&mut self) -> BoxFuture<'_, Option<Result<(), FrameIoError>>> {
        let HostedValueDownlinkChannel {
            address,
            receiver,
            next,
            ..
        } = self;
        async move {
            if let Some(rx) = receiver {
                match rx.next().await {
                    Some(Ok(notification)) => {
                        *next = Some(notification);
                        Some(Ok(()))
                    }
                    Some(Err(e)) => {
                        error!(address = %address, "Downlink input channel failed.");
                        *receiver = None;
                        Some(Err(e))
                    }
                    _ => {
                        info!(address = %address, "Downlink terminated normally.");
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
        let HostedValueDownlinkChannel {
            address,
            state,
            receiver,
            next,
            lifecycle,
            dl_state,
            config:
                ValueDownlinkConfig {
                    events_when_not_synced,
                    terminate_on_unlinked,
                },
            ..
        } = self;
        if let Some(notification) = next.take() {
            match notification {
                DownlinkNotification::Linked => {
                    debug!(address = %address, "Downlink linked.");
                    if *dl_state == DlState::Unlinked {
                        *dl_state = DlState::Linked;
                    }
                    Some(lifecycle.on_linked().boxed())
                }
                DownlinkNotification::Synced => state.with(context, |maybe_value| {
                    debug!(address = %address, "Downlink synced.");
                    *dl_state = DlState::Synced;
                    maybe_value.map(|value| lifecycle.on_synced(value).boxed())
                }),
                DownlinkNotification::Event { body } => {
                    trace!(address = %address, "Event received for downlink.");
                    let prev = state.take_current(context);
                    let handler = if *dl_state == DlState::Synced || *events_when_not_synced {
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
                DownlinkNotification::Unlinked => {
                    debug!(address = %address, "Downlink unlinked.");
                    state.clear(context);
                    if *terminate_on_unlinked {
                        *receiver = None;
                    }
                    Some(lifecycle.on_unlinked().boxed())
                }
            }
        } else {
            None
        }
    }
}

/// A handle which can be used to set the value of a lane through a value downlink.
pub struct ValueDownlinkHandle<T> {
    address: Address<Text>,
    inner: circular_buffer::Sender<T>,
}

impl<T> ValueDownlinkHandle<T> {
    pub fn new(address: Address<Text>, inner: circular_buffer::Sender<T>) -> Self {
        ValueDownlinkHandle { address, inner }
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
    T: StructuralWritable + Send + Sync + 'static,
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
