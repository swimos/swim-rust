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
use swim_api::protocol::{
    downlink::{DownlinkNotification, ValueNotificationDecoder},
    WithLengthBytesCodec,
};
use swim_form::structural::{read::recognizer::RecognizerReadable, write::StructuralWritable};
use swim_recon::printer::print_recon_compact;
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};
use tokio::sync::watch;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::{
    agent_model::downlink::handlers::DownlinkChannel,
    downlink_lifecycle::value::ValueDownlinkLifecycle,
    event_handler::{BoxEventHandler, EventHandlerExt, HandlerActionExt},
};

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
    receiver: FramedRead<ByteReader, ValueNotificationDecoder<T>>,
    state: State,
    next: Option<DownlinkNotification<T>>,
    lifecycle: LC,
}

impl<T: RecognizerReadable, LC, State> HostedValueDownlinkChannel<T, LC, State> {
    pub fn new(receiver: ByteReader, lifecycle: LC, state: State) -> Self {
        HostedValueDownlinkChannel {
            receiver: FramedRead::new(receiver, Default::default()),
            state,
            next: None,
            lifecycle,
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
    fn await_ready(&mut self) -> BoxFuture<'_, bool> {
        let HostedValueDownlinkChannel { receiver, next, .. } = self;
        async move {
            if let Some(Ok(notification)) = receiver.next().await {
                *next = Some(notification);
                true
            } else {
                false
            }
        }
        .boxed()
    }

    fn next_event(&mut self, context: &Context) -> Option<BoxEventHandler<'_, Context>> {
        let HostedValueDownlinkChannel {
            state,
            next,
            lifecycle,
            ..
        } = self;
        if let Some(notification) = next.take() {
            match notification {
                DownlinkNotification::Linked => Some(lifecycle.on_linked().boxed()),
                DownlinkNotification::Synced => state.with(context, |maybe_value| {
                    maybe_value.map(|value| lifecycle.on_synced(value).boxed())
                }),
                DownlinkNotification::Event { body } => {
                    let prev = state.take_current(context);
                    let handler = lifecycle
                        .on_event(&body)
                        .followed_by(lifecycle.on_set(prev, &body))
                        .boxed();
                    state.replace(context, body);
                    Some(handler)
                }
                DownlinkNotification::Unlinked => {
                    state.clear(context);
                    Some(lifecycle.on_unlinked().boxed())
                }
            }
        } else {
            None
        }
    }
}

pub fn value_dl_write_stream<T>(
    writer: FramedWrite<ByteWriter, WithLengthBytesCodec>,
    rx: watch::Receiver<Option<T>>,
) -> impl Stream<Item = Result<(), std::io::Error>> + Send + 'static
where
    T: StructuralWritable + Send + Sync + 'static,
{
    let buffer = BytesMut::new();

    unfold((writer, rx, buffer), |state| async move {
        let (mut writer, mut rx, mut buffer) = state;
        if rx.changed().await.is_err() {
            None
        } else {
            loop {
                buffer.clear();
                {
                    //This block is required for the Send check on this async future to pass.
                    let body_ref = rx.borrow();
                    if let Some(value) = &*body_ref {
                        write!(&mut buffer, "{}", print_recon_compact(value))
                            .expect("Writing to Recon should be infallible.");
                    } else {
                        continue;
                    }
                }
                let result = writer.send(buffer.as_ref()).await;
                break Some((result, (writer, rx, buffer)));
            }
        }
    })
}
