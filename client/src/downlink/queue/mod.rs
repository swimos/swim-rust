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
use crate::downlink::any::AnyDownlink;
use crate::downlink::{Command, Downlink, DownlinkError, Event, Message, Model, StateMachine};
use crate::sink::item;
use crate::sink::item::{ItemSink, MpscSend};
use common::topic::{MpscTopic, MpscTopicReceiver, SendRequest, Sequenced, Topic};
use futures::{Stream, StreamExt};
use tokio::sync::{mpsc, oneshot, watch};

/// A downlink where subscribers consume via independent queues that will block if any one falls
/// behind.
#[derive(Debug)]
pub struct QueueDownlink<Act, Upd> {
    input: raw::Sender<mpsc::Sender<Act>>,
    topic: MpscTopic<Event<Upd>>,
}

impl<Act, Upd> Clone for QueueDownlink<Act, Upd> {
    fn clone(&self) -> Self {
        QueueDownlink {
            input: self.input.clone(),
            topic: self.topic.clone(),
        }
    }
}

impl<Act, Upd> QueueDownlink<Act, Upd> {
    pub fn any_dl(self) -> AnyDownlink<Act, Upd> {
        AnyDownlink::Queue(self)
    }
}

impl<Act, Upd> QueueDownlink<Act, Upd>
where
    Upd: Clone + Send + Sync + 'static,
{
    pub fn from_raw(
        raw: raw::RawDownlink<mpsc::Sender<Act>, mpsc::Receiver<Event<Upd>>>,
        buffer_size: usize,
    ) -> (QueueDownlink<Act, Upd>, QueueReceiver<Upd>) {
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

impl<Act, Upd> Topic<Event<Upd>> for QueueDownlink<Act, Upd>
where
    Upd: Clone + Send + Sync + 'static,
{
    type Receiver = QueueReceiver<Upd>;
    type Fut = Sequenced<SendRequest<Event<Upd>>, oneshot::Receiver<MpscTopicReceiver<Event<Upd>>>>;

    fn subscribe(&mut self) -> Self::Fut {
        self.topic.subscribe()
    }
}

impl<'a, Act, Upd> ItemSink<'a, Act> for QueueDownlink<Act, Upd>
where
    Act: Send + 'static,
{
    type Error = DownlinkError;
    type SendFuture = MpscSend<'a, Act, DownlinkError>;

    fn send_item(&'a mut self, value: Act) -> Self::SendFuture {
        self.input.send_item(value)
    }
}

impl<Act, Upd> Downlink<Act, Event<Upd>> for QueueDownlink<Act, Upd>
where
    Act: Send + 'static,
    Upd: Clone + Send + Sync + 'static,
{
    type DlTopic = MpscTopic<Event<Upd>>;
    type DlSink = raw::Sender<mpsc::Sender<Act>>;

    fn split(self) -> (Self::DlTopic, Self::DlSink) {
        let QueueDownlink { input, topic } = self;
        (topic, input)
    }
}

pub type QueueReceiver<T> = MpscTopicReceiver<Event<T>>;

pub(in crate::downlink) fn make_downlink<Err, M, A, State, Updates, Commands>(
    init: State,
    update_stream: Updates,
    cmd_sink: Commands,
    buffer_size: usize,
    queue_size: usize,
) -> (QueueDownlink<A, State::Ev>, QueueReceiver<State::Ev>)
where
    M: Send + 'static,
    A: Send + 'static,
    State: StateMachine<M, A> + Send + 'static,
    State::Ev: Clone + Send + Sync + 'static,
    State::Cmd: Send + 'static,
    Err: Into<DownlinkError> + Send + 'static,
    Updates: Stream<Item = Message<M>> + Send + 'static,
    Commands: for<'b> ItemSink<'b, Command<State::Cmd>, Error = Err> + Send + 'static,
{
    let model = Model::new(init);
    let (act_tx, act_rx) = mpsc::channel::<A>(buffer_size);
    let (event_tx, event_rx) = mpsc::channel::<Event<State::Ev>>(buffer_size);
    let (stop_tx, stop_rx) = watch::channel::<Option<()>>(None);
    let (closed_tx, closed_rx) = watch::channel(None);

    let event_sink = item::for_mpsc_sender::<_, DownlinkError>(event_tx);

    // The task that maintains the internal state of the lane.
    let lane_task = raw::make_downlink_task(
        model,
        raw::combine_inputs(update_stream, stop_rx),
        act_rx.fuse(),
        cmd_sink,
        event_sink,
        closed_tx
    );

    let join_handle = tokio::task::spawn(lane_task);

    let dl_task = raw::DownlinkTask::new(join_handle,
                                         stop_tx, closed_rx);

    let raw_dl = raw::RawDownlink::new(
        act_tx, event_rx, dl_task);

    QueueDownlink::from_raw(raw_dl, queue_size)
}
