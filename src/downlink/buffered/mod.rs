// Copyright 2015-2020 SWIM.AI inc.
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

use crate::downlink::{raw, Downlink, DownlinkError};
use crate::sink::item::{ItemSink, MpscSend};
use crate::topic::{BroadcastReceiver, BroadcastTopic, SubscriptionError, Topic};
use futures::future::Ready;
use tokio::sync::broadcast;
use tokio::sync::mpsc;

/// A downlink where subscribers consume via a shared queue that will start dropping (the oldest)
/// records if any fall behind.
pub struct BufferedDownlink<Act, Upd: Clone> {
    input: raw::Sender<mpsc::Sender<Act>>,
    topic: BroadcastTopic<Upd>,
}

impl<Act, Upd> BufferedDownlink<Act, Upd>
where
    Upd: Clone + Send + Sync + 'static,
{
    pub fn assemble<F>(
        raw_factory: F,
        buffer_size: usize,
    ) -> (BufferedDownlink<Act, Upd>, BroadcastReceiver<Upd>)
    where
        F: FnOnce(broadcast::Sender<Upd>) -> raw::Sender<mpsc::Sender<Act>>,
    {
        let (topic, sender, first_receiver) = BroadcastTopic::new(buffer_size);
        let raw_sender = raw_factory(sender);
        (
            BufferedDownlink {
                input: raw_sender,
                topic,
            },
            first_receiver,
        )
    }
}

impl<Act, Upd> Topic<Upd> for BufferedDownlink<Act, Upd>
where
    Upd: Clone + Send + Sync + 'static,
{
    type Receiver = BroadcastReceiver<Upd>;
    type Fut = Ready<Result<BroadcastReceiver<Upd>, SubscriptionError>>;

    fn subscribe(&mut self) -> Self::Fut {
        self.topic.subscribe()
    }
}

impl<'a, Act, Upd> ItemSink<'a, Act> for BufferedDownlink<Act, Upd>
where
    Act: Unpin + Send + 'static,
    Upd: Clone + Send + Sync + 'static,
{
    type Error = DownlinkError;
    type SendFuture = MpscSend<'a, Act, DownlinkError>;

    fn send_item(&'a mut self, value: Act) -> Self::SendFuture {
        self.input.send_item(value)
    }
}

impl<Act, Upd> Downlink<Act, Upd> for BufferedDownlink<Act, Upd>
where
    Act: Unpin + Send + 'static,
    Upd: Clone + Send + Sync + 'static,
{
    type DlTopic = BroadcastTopic<Upd>;
    type DlSink = raw::Sender<mpsc::Sender<Act>>;

    fn split(self) -> (Self::DlTopic, Self::DlSink) {
        let BufferedDownlink { input, topic } = self;
        (topic, input)
    }
}
