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

use super::raw;
use crate::downlink::{Downlink, DownlinkError};
use crate::sink::item::{ItemSink, MpscSend};
use common::topic::{MpscTopic, SendFuture, Topic};
use tokio::sync::mpsc;

/// A downlink where subscribers consume via independent queues that will block if any one falls
/// behind.
pub struct QueueDownlink<Act, Upd: Clone> {
    input: raw::Sender<mpsc::Sender<Act>>,
    topic: MpscTopic<Upd>,
}

impl<Act, Upd> QueueDownlink<Act, Upd>
where
    Upd: Clone + Send + Sync + 'static,
{
    pub fn from_raw(
        raw: raw::RawDownlink<mpsc::Sender<Act>, mpsc::Receiver<Upd>>,
        buffer_size: usize,
    ) -> (QueueDownlink<Act, Upd>, mpsc::Receiver<Upd>) {
        let raw::RawDownlink {
            receiver: raw::Receiver { event_stream },
            sender,
        } = raw;
        let (topic, first_receiver) = MpscTopic::new(event_stream, buffer_size);
        (
            QueueDownlink {
                input: sender,
                topic,
            },
            first_receiver,
        )
    }
}

impl<Act, Upd> Topic<Upd> for QueueDownlink<Act, Upd>
where
    Upd: Clone + Send + Sync + 'static,
{
    type Receiver = mpsc::Receiver<Upd>;
    type Fut = SendFuture<Upd>;

    fn subscribe(&mut self) -> Self::Fut {
        self.topic.subscribe()
    }
}

impl<'a, Act, Upd> ItemSink<'a, Act> for QueueDownlink<Act, Upd>
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

impl<Act, Upd> Downlink<Act, Upd> for QueueDownlink<Act, Upd>
where
    Act: Unpin + Send + 'static,
    Upd: Clone + Send + Sync + 'static,
{
    type DlTopic = MpscTopic<Upd>;
    type DlSink = raw::Sender<mpsc::Sender<Act>>;

    fn split(self) -> (Self::DlTopic, Self::DlSink) {
        let QueueDownlink { input, topic } = self;
        (topic, input)
    }
}
