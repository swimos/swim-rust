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
use crate::topic::{SubscriptionError, Topic, WatchTopic, WatchTopicReceiver};
use futures::future::Ready;
use tokio::sync::mpsc;
use tokio::sync::watch;

pub struct DroppingDownlink<Act, Upd: Clone> {
    input: raw::Sender<mpsc::Sender<Act>>,
    topic: WatchTopic<Upd>,
}

impl<Act, Upd> DroppingDownlink<Act, Upd>
where
    Upd: Clone + Send + Sync + 'static,
{
    pub fn from_raw(
        raw: raw::RawDownlink<mpsc::Sender<Act>, watch::Receiver<Upd>>,
    ) -> (DroppingDownlink<Act, Upd>, WatchTopicReceiver<Upd>) {
        let raw::RawDownlink {
            receiver: raw::Receiver { event_stream },
            sender,
        } = raw;
        let (topic, first_receiver) = WatchTopic::new(event_stream);
        (
            DroppingDownlink {
                input: sender,
                topic,
            },
            first_receiver,
        )
    }
}

impl<Act, Upd> Topic<Upd> for DroppingDownlink<Act, Upd>
where
    Upd: Clone + Send + Sync + 'static,
{
    type Receiver = WatchTopicReceiver<Upd>;
    type Fut = Ready<Result<WatchTopicReceiver<Upd>, SubscriptionError>>;

    fn subscribe(&mut self) -> Self::Fut {
        self.topic.subscribe()
    }
}

impl<'a, Act, Upd> ItemSink<'a, Act> for DroppingDownlink<Act, Upd>
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

impl<Act, Upd> Downlink<Act, Upd> for DroppingDownlink<Act, Upd>
where
    Act: Unpin + Send + 'static,
    Upd: Clone + Send + Sync + 'static,
{
    type DlTopic = WatchTopic<Upd>;
    type DlSink = raw::Sender<mpsc::Sender<Act>>;

    fn split(self) -> (Self::DlTopic, Self::DlSink) {
        let DroppingDownlink { input, topic } = self;
        (topic, input)
    }
}
