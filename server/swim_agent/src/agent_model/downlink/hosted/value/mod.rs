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
    error::{AgentRuntimeError, DownlinkFailureReason, FrameIoError},
    protocol::{
        downlink::{DownlinkNotification, ValueNotificationDecoder},
        WithLengthBytesCodec,
    },
};
use swim_form::structural::{read::recognizer::RecognizerReadable, write::StructuralWritable};
use swim_recon::printer::print_recon_compact;
use swim_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    sync::circular_buffer,
};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::{
    agent_model::downlink::{handlers::DownlinkChannel, DlState, ValueDownlinkConfig},
    downlink_lifecycle::value::ValueDownlinkLifecycle,
    event_handler::{BoxEventHandler, EventHandlerExt, HandlerActionExt},
};

#[cfg(test)]
mod tests;

pub trait ValueDlState<T, Context> {
    fn take_current(&self, context: &Context) -> Option<T>;

    fn replace(&self, context: &Context, value: T);

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

pub struct HostedValueDownlinkChannel<T: RecognizerReadable, LC, State> {
    receiver: Option<FramedRead<ByteReader, ValueNotificationDecoder<T>>>,
    state: State,
    next: Option<DownlinkNotification<T>>,
    lifecycle: LC,
    config: ValueDownlinkConfig,
    dl_state: DlState,
}

impl<T: RecognizerReadable, LC, State> HostedValueDownlinkChannel<T, LC, State> {
    pub fn new(
        receiver: ByteReader,
        lifecycle: LC,
        state: State,
        config: ValueDownlinkConfig,
    ) -> Self {
        HostedValueDownlinkChannel {
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
        let HostedValueDownlinkChannel { receiver, next, .. } = self;
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
        let HostedValueDownlinkChannel {
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
                    if *dl_state == DlState::Unlinked {
                        *dl_state = DlState::Linked;
                    }
                    Some(lifecycle.on_linked().boxed())
                }
                DownlinkNotification::Synced => state.with(context, |maybe_value| {
                    *dl_state = DlState::Synced;
                    maybe_value.map(|value| lifecycle.on_synced(value).boxed())
                }),
                DownlinkNotification::Event { body } => {
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

pub struct ValueDownlinkHandle<T> {
    inner: circular_buffer::Sender<T>,
}

impl<T> ValueDownlinkHandle<T> {
    pub fn new(inner: circular_buffer::Sender<T>) -> Self {
        ValueDownlinkHandle { inner }
    }
}

impl<T> ValueDownlinkHandle<T>
where
    T: Send + Sync,
{
    pub fn set(&mut self, value: T) -> Result<(), AgentRuntimeError> {
        if self.inner.try_send(value).is_err() {
            Err(AgentRuntimeError::DownlinkConnectionFailed(
                DownlinkFailureReason::DownlinkStopped,
            ))
        } else {
            Ok(())
        }
    }
}

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
            let result = writer.send(buffer.as_ref()).await;
            Some((result, (writer, rx, buffer)))
        } else {
            None
        }
    })
}
