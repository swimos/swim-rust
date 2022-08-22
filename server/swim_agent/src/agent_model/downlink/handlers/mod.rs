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

use futures::{future::BoxFuture, FutureExt};
use tokio::sync::mpsc;

use crate::{
    downlink_lifecycle::value::ValueDownlinkLifecycle,
    event_handler::{BoxEventHandler, EventHandlerExt, HandlerActionExt},
};

use super::DownlinkMessage;

pub trait DownlinkChannel<Context> {
    fn await_ready(&mut self) -> BoxFuture<'_, bool>;

    fn next_event(&mut self) -> Option<BoxEventHandler<'_, Context>>;
}

pub type BoxDownlinkChannel<Context> = Box<dyn DownlinkChannel<Context> + Send>;

pub trait DownlinkChannelExt<Context>: DownlinkChannel<Context> {
    fn boxed(self) -> BoxDownlinkChannel<Context>
    where
        Self: Sized + Send + 'static,
    {
        Box::new(self)
    }
}

impl<Context, C> DownlinkChannelExt<Context> for C where C: DownlinkChannel<Context> {}

pub struct ValueDownlinkEndpoint<T, LC> {
    receiver: mpsc::Receiver<DownlinkMessage<T>>,
    lifecycle: LC,
    current: Option<T>,
    event: Option<DownlinkMessage<T>>,
}

impl<T, LC> ValueDownlinkEndpoint<T, LC> {
    pub fn new(receiver: mpsc::Receiver<DownlinkMessage<T>>, lifecycle: LC) -> Self {
        ValueDownlinkEndpoint {
            receiver,
            lifecycle,
            current: None,
            event: None,
        }
    }
}

impl<T, LC, Context> DownlinkChannel<Context> for ValueDownlinkEndpoint<T, LC>
where
    T: Send,
    LC: ValueDownlinkLifecycle<T, Context>,
{
    fn await_ready(&mut self) -> BoxFuture<'_, bool> {
        async move {
            let ValueDownlinkEndpoint {
                receiver, event, ..
            } = self;
            if let Some(message) = receiver.recv().await {
                *event = Some(message);
                true
            } else {
                false
            }
        }
        .boxed()
    }

    fn next_event(&mut self) -> Option<BoxEventHandler<'_, Context>> {
        let ValueDownlinkEndpoint {
            lifecycle,
            current,
            event,
            ..
        } = self;
        event.take().and_then(move |message| match message {
            DownlinkMessage::Linked => Some(lifecycle.on_linked().boxed()),
            DownlinkMessage::Synced => {
                if let Some(value) = current {
                    Some(lifecycle.on_synced(value).boxed())
                } else {
                    None
                }
            }
            DownlinkMessage::Event(new_value) => {
                let prev = current.take();
                let next_value = current.insert(new_value);
                Some(
                    lifecycle
                        .on_event(next_value)
                        .followed_by(lifecycle.on_set(prev, next_value))
                        .boxed(),
                )
            }
            DownlinkMessage::Unlinked => Some(lifecycle.on_linked().boxed()),
        })
    }
}
