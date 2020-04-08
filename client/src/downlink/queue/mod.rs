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
use crate::downlink::raw::{DownlinkTask, DownlinkTaskHandle};
use crate::downlink::topic::{DownlinkReceiver, DownlinkTopic, MakeReceiver};
use crate::downlink::{
    Command, Downlink, DownlinkError, DroppedError, Event, Message, StateMachine,
};
use crate::router::RoutingError;
use common::request::request_future::SendAndAwait;
use common::request::Request;
use common::sink::item::{self, ItemSender, ItemSink, MpscSend};
use common::topic::{MpscTopic, MpscTopicReceiver, Topic, TopicError};
use futures::future::ErrInto;
use futures::{Stream, StreamExt};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use utilities::future::TransformedFuture;

/// A downlink where subscribers consume via independent queues that will block if any one falls
/// behind.
#[derive(Debug)]
pub struct QueueDownlink<Act, Upd> {
    input: mpsc::Sender<Act>,
    topic: MpscTopic<Event<Upd>>,
    task: Arc<DownlinkTaskHandle>,
}

impl<Act, Upd> Clone for QueueDownlink<Act, Upd> {
    fn clone(&self) -> Self {
        QueueDownlink {
            input: self.input.clone(),
            topic: self.topic.clone(),
            task: self.task.clone(),
        }
    }
}

impl<Act, Upd> QueueDownlink<Act, Upd> {
    pub fn any_dl(self) -> AnyDownlink<Act, Upd> {
        AnyDownlink::Queue(self)
    }

    pub fn same_downlink(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.task, &other.task)
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
            receiver,
            sender,
            task,
        } = raw;
        let task_ptr = Arc::new(task);
        let (topic, first) = MpscTopic::new(receiver, buffer_size);
        let first_receiver = DownlinkReceiver::new(first, task_ptr.clone());
        (
            QueueDownlink {
                input: sender,
                topic,
                task: task_ptr,
            },
            first_receiver,
        )
    }
}

type EventReceiver<Upd> = MpscTopicReceiver<Event<Upd>>;

type OpenReceiver<Upd> =
    ErrInto<SendAndAwait<Request<EventReceiver<Upd>>, EventReceiver<Upd>>, TopicError>;

impl<Act, Upd> Topic<Event<Upd>> for QueueDownlink<Act, Upd>
where
    Upd: Clone + Send + Sync + 'static,
{
    type Receiver = QueueReceiver<Upd>;
    type Fut = TransformedFuture<OpenReceiver<Upd>, MakeReceiver>;

    fn subscribe(&mut self) -> Self::Fut {
        let attach = MakeReceiver::new(self.task.clone());
        TransformedFuture::new(self.topic.subscribe(), attach)
    }
}

impl<'a, Act, Upd> ItemSink<'a, Act> for QueueDownlink<Act, Upd>
where
    Act: Send + 'static,
{
    type Error = DownlinkError;
    type SendFuture = MpscSend<'a, Act, DownlinkError>;

    fn send_item(&'a mut self, value: Act) -> Self::SendFuture {
        MpscSend::new(&mut self.input, value)
    }
}

impl<Act, Upd> Downlink<Act, Event<Upd>> for QueueDownlink<Act, Upd>
where
    Act: Send + 'static,
    Upd: Clone + Send + Sync + 'static,
{
    type DlTopic = DownlinkTopic<MpscTopic<Event<Upd>>>;
    type DlSink = raw::Sender<mpsc::Sender<Act>>;

    fn split(self) -> (Self::DlTopic, Self::DlSink) {
        let QueueDownlink { input, topic, task } = self;
        let sender = raw::Sender::new(input, task.clone());
        let dl_topic = DownlinkTopic::new(topic, task);
        (dl_topic, sender)
    }
}

pub type QueueTopicReceiver<T> = MpscTopicReceiver<Event<T>>;
pub type QueueReceiver<T> = DownlinkReceiver<MpscTopicReceiver<Event<T>>>;

pub(in crate::downlink) fn make_downlink<M, A, State, Updates, Commands>(
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
    Updates: Stream<Item = Message<M>> + Send + 'static,
    Commands: ItemSender<Command<State::Cmd>, RoutingError> + Send + 'static,
{
    let (act_tx, act_rx) = mpsc::channel::<A>(buffer_size);
    let (event_tx, event_rx) = mpsc::channel::<Event<State::Ev>>(buffer_size);

    let event_sink = item::for_mpsc_sender::<_, DroppedError>(event_tx);

    let (stopped_tx, stopped_rx) = watch::channel(None);

    // The task that maintains the internal state of the lane.
    // The task that maintains the internal state of the lane.
    let task = DownlinkTask::new(init, cmd_sink, event_sink, stopped_tx);

    let lane_task = task.run(raw::make_operation_stream(update_stream), act_rx.fuse());

    let join_handle = tokio::task::spawn(lane_task);

    let dl_task = raw::DownlinkTaskHandle::new(join_handle, stopped_rx);

    let raw_dl = raw::RawDownlink::new(act_tx, event_rx, dl_task);

    QueueDownlink::from_raw(raw_dl, queue_size)
}
