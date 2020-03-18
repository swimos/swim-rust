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
use crate::downlink::{raw, Command, Downlink, DownlinkError, Event, Message, Model, StateMachine};
use crate::sink::item;
use crate::sink::item::{ItemSink, MpscSend};
use common::topic::{BroadcastReceiver, BroadcastTopic, Topic, TopicError};
use futures::future::Ready;
use futures::Stream;
use futures_util::stream::StreamExt;
use tokio::sync::{broadcast, mpsc, watch};

/// A downlink where subscribers consume via a shared queue that will start dropping (the oldest)
/// records if any fall behind.
#[derive(Debug)]
pub struct BufferedDownlink<Act, Upd> {
    input: raw::Sender<mpsc::Sender<Act>>,
    topic: BroadcastTopic<Event<Upd>>,
}

impl<Act, Upd> Clone for BufferedDownlink<Act, Upd> {
    fn clone(&self) -> Self {
        BufferedDownlink {
            input: self.input.clone(),
            topic: self.topic.clone(),
        }
    }
}

impl<Act, Upd> BufferedDownlink<Act, Upd> {
    pub fn any_dl(self) -> AnyDownlink<Act, Upd> {
        AnyDownlink::Buffered(self)
    }
}

pub type BufferedReceiver<T> = BroadcastReceiver<Event<T>>;

impl<Act, Upd> BufferedDownlink<Act, Upd>
where
    Upd: Clone + Send + Sync + 'static,
{
    pub fn assemble<F>(
        raw_factory: F,
        buffer_size: usize,
    ) -> (BufferedDownlink<Act, Upd>, BufferedReceiver<Upd>)
    where
        F: FnOnce(broadcast::Sender<Event<Upd>>) -> raw::Sender<mpsc::Sender<Act>>,
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

impl<Act, Upd> Topic<Event<Upd>> for BufferedDownlink<Act, Upd>
where
    Upd: Clone + Send + Sync + 'static,
{
    type Receiver = BufferedReceiver<Upd>;
    type Fut = Ready<Result<BufferedReceiver<Upd>, TopicError>>;

    fn subscribe(&mut self) -> Self::Fut {
        self.topic.subscribe()
    }
}

impl<'a, Act, Upd> ItemSink<'a, Act> for BufferedDownlink<Act, Upd>
where
    Act: Send + 'static,
{
    type Error = DownlinkError;
    type SendFuture = MpscSend<'a, Act, DownlinkError>;

    fn send_item(&'a mut self, value: Act) -> Self::SendFuture {
        self.input.send_item(value)
    }
}

impl<Act, Upd> Downlink<Act, Event<Upd>> for BufferedDownlink<Act, Upd>
where
    Act: Send + 'static,
    Upd: Clone + Send + Sync + 'static,
{
    type DlTopic = BroadcastTopic<Event<Upd>>;
    type DlSink = raw::Sender<mpsc::Sender<Act>>;

    fn split(self) -> (Self::DlTopic, Self::DlSink) {
        let BufferedDownlink { input, topic } = self;
        (topic, input)
    }
}

pub(in crate::downlink) fn make_downlink<Err, M, A, State, Updates, Commands>(
    init: State,
    update_stream: Updates,
    cmd_sink: Commands,
    buffer_size: usize,
    queue_size: usize,
) -> (BufferedDownlink<A, State::Ev>, BufferedReceiver<State::Ev>)
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
    let fac = move |event_tx: broadcast::Sender<Event<State::Ev>>| {
        let model = Model::new(init);
        let (act_tx, act_rx) = mpsc::channel::<A>(buffer_size);

        let (stop_tx, stop_rx) = watch::channel::<Option<()>>(None);
        let (closed_tx, closed_rx) = watch::channel(None);

        let event_sink = item::for_broadcast_sender::<_, DownlinkError>(event_tx);

        // The task that maintains the internal state of the lane.
        let lane_task = raw::make_downlink_task(
            model,
            raw::combine_inputs(update_stream, stop_rx),
            act_rx.fuse(),
            cmd_sink,
            event_sink,
            closed_tx,
        );

        let join_handle = tokio::task::spawn(lane_task);

        let dl_task = raw::DownlinkTask::new(join_handle, stop_tx, closed_rx);

        raw::Sender::new(act_tx, dl_task)
    };

    BufferedDownlink::assemble(fac, queue_size)
}
