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

use crate::downlink::any::AnyDownlink;
use crate::downlink::raw::DownlinkTask;
use crate::downlink::{raw, Command, Downlink, DownlinkError, Event, Message, StateMachine};
use common::sink::item;
use common::sink::item::{ItemSender, ItemSink, MpscSend};
use common::topic::{Topic, TopicError, WatchTopic, WatchTopicReceiver};
use futures::future::Ready;
use futures::{Stream, StreamExt};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

/// A downlink where subscribers observe the latest output record whenever the poll the receiver
/// stream.
#[derive(Debug)]
pub struct DroppingDownlink<Act, Upd> {
    input: raw::Sender<mpsc::Sender<Act>>,
    topic: WatchTopic<Event<Upd>>,
}

impl<Act, Upd> Clone for DroppingDownlink<Act, Upd> {
    fn clone(&self) -> Self {
        DroppingDownlink {
            input: self.input.clone(),
            topic: self.topic.clone(),
        }
    }
}

impl<Act, Upd> DroppingDownlink<Act, Upd> {
    pub fn any_dl(self) -> AnyDownlink<Act, Upd> {
        AnyDownlink::Dropping(self)
    }

    pub fn same_downlink(&self, other: &Self) -> bool {
        self.input.same_sender(&other.input)
    }
}

pub type DroppingReceiver<T> = WatchTopicReceiver<Event<T>>;

impl<Act, Upd> DroppingDownlink<Act, Upd>
where
    Upd: Clone + Send + Sync + 'static,
{
    pub fn from_raw(
        raw: raw::RawDownlink<mpsc::Sender<Act>, watch::Receiver<Option<Event<Upd>>>>,
    ) -> (DroppingDownlink<Act, Upd>, DroppingReceiver<Upd>) {
        let raw::RawDownlink {
            receiver,
            sender,
            task,
        } = raw;
        let (topic, first_receiver) = WatchTopic::new(receiver);
        (
            DroppingDownlink {
                input: raw::Sender::new(sender, Arc::new(task)),
                topic,
            },
            first_receiver,
        )
    }
}

impl<Act, Upd> Topic<Event<Upd>> for DroppingDownlink<Act, Upd>
where
    Upd: Clone + Send + Sync + 'static,
{
    type Receiver = DroppingReceiver<Upd>;
    type Fut = Ready<Result<DroppingReceiver<Upd>, TopicError>>;

    fn subscribe(&mut self) -> Self::Fut {
        self.topic.subscribe()
    }
}

impl<'a, Act, Upd> ItemSink<'a, Act> for DroppingDownlink<Act, Upd>
where
    Act: Send + 'static,
{
    type Error = DownlinkError;
    type SendFuture = MpscSend<'a, Act, DownlinkError>;

    fn send_item(&'a mut self, value: Act) -> Self::SendFuture {
        self.input.send_item(value)
    }
}

impl<Act, Upd> Downlink<Act, Event<Upd>> for DroppingDownlink<Act, Upd>
where
    Act: Send + 'static,
    Upd: Clone + Send + Sync + 'static,
{
    type DlTopic = WatchTopic<Event<Upd>>;
    type DlSink = raw::Sender<mpsc::Sender<Act>>;

    fn split(self) -> (Self::DlTopic, Self::DlSink) {
        let DroppingDownlink { input, topic } = self;
        (topic, input)
    }
}

pub(in crate::downlink) fn make_downlink<M, A, State, Updates, Commands>(
    init: State,
    update_stream: Updates,
    cmd_sink: Commands,
    buffer_size: usize,
) -> (DroppingDownlink<A, State::Ev>, DroppingReceiver<State::Ev>)
where
    M: Send + 'static,
    A: Send + 'static,
    State: StateMachine<M, A> + Send + 'static,
    State::Ev: Clone + Send + Sync + 'static,
    State::Cmd: Send + 'static,
    Updates: Stream<Item = Message<M>> + Send + 'static,
    Commands: ItemSender<Command<State::Cmd>, DownlinkError> + Send + 'static,
{
    let (act_tx, act_rx) = mpsc::channel::<A>(buffer_size);
    let (event_tx, event_rx) = watch::channel::<Option<Event<State::Ev>>>(None);

    let event_sink = item::for_watch_sender::<_, DownlinkError>(event_tx);

    let (stopped_tx, stopped_rx) = watch::channel(None);

    // The task that maintains the internal state of the lane.
    // The task that maintains the internal state of the lane.
    let task = DownlinkTask::new(init, cmd_sink, event_sink, stopped_tx);

    let lane_task = task.run(raw::make_operation_stream(update_stream), act_rx.fuse());

    let join_handle = tokio::task::spawn(lane_task);

    let dl_task = raw::DownlinkTaskHandle::new(join_handle, stopped_rx);

    let raw_dl = raw::RawDownlink::new(act_tx, event_rx, dl_task);

    DroppingDownlink::from_raw(raw_dl)
}
