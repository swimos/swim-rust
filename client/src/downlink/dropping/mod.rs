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

use crate::configuration::downlink::DownlinkParams;
use crate::downlink::any::AnyDownlink;
use crate::downlink::raw::{DownlinkTask, DownlinkTaskHandle};
use crate::downlink::topic::{DownlinkReceiver, DownlinkTopic, MakeReceiver};
use crate::downlink::{
    raw, Command, Downlink, DownlinkError, DownlinkInternals, DroppedError, Event, Message,
    StateMachine, StoppedFuture,
};
use crate::router::RoutingError;
use common::sink::item::{self, ItemSender, ItemSink, MpscSend};
use common::topic::{Topic, TopicError, WatchTopic, WatchTopicReceiver};
use futures::future::Ready;
use futures::{Stream, StreamExt};
use std::fmt::{Debug, Formatter};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Weak};
use tokio::sync::{mpsc, watch};
use utilities::future::{SwimFutureExt, TransformedFuture};
use utilities::rt::task::spawn;

/// A downlink where subscribers observe the latest output record whenever the poll the receiver
/// stream.
#[derive(Debug)]
pub struct DroppingDownlink<Act, Upd> {
    input: mpsc::Sender<Act>,
    topic: WatchTopic<Event<Upd>>,
    internal: Arc<Internal<Act, Upd>>,
}

struct Internal<Act, Upd> {
    input: mpsc::Sender<Act>,
    topic: WatchTopic<Event<Upd>>,
    task: DownlinkTaskHandle,
}

/// A weak handle on a dropping downlink. Holding this will not keep the downlink running nor prevent
/// its sender and topic from being dropped.
#[derive(Debug)]
pub struct WeakDroppingDownlink<Act, Upd>(Weak<Internal<Act, Upd>>);

impl<Act, Upd> WeakDroppingDownlink<Act, Upd> {
    /// Attempt to upgrade this weak handle to a strong one.
    pub fn upgrade(&self) -> Option<DroppingDownlink<Act, Upd>> {
        self.0.upgrade().map(|internal| DroppingDownlink {
            input: internal.input.clone(),
            topic: internal.topic.clone(),
            internal,
        })
    }
}

impl<Act, Upd> DownlinkInternals for Internal<Act, Upd>
where
    Act: Send,
    Upd: Send + Sync,
{
    fn task_handle(&self) -> &DownlinkTaskHandle {
        &self.task
    }
}

impl<Act, Upd> Debug for Internal<Act, Upd> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "<Dropping Downlink Internals>")
    }
}

impl<Act, Upd> Clone for DroppingDownlink<Act, Upd> {
    fn clone(&self) -> Self {
        DroppingDownlink {
            input: self.input.clone(),
            topic: self.topic.clone(),
            internal: self.internal.clone(),
        }
    }
}

impl<Act, Upd> DroppingDownlink<Act, Upd> {
    pub fn any_dl(self) -> AnyDownlink<Act, Upd> {
        AnyDownlink::Dropping(self)
    }

    pub fn same_downlink(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.internal, &other.internal)
    }

    /// Downgrade this handle to a weak handle.
    pub fn downgrade(&self) -> WeakDroppingDownlink<Act, Upd> {
        WeakDroppingDownlink(Arc::downgrade(&self.internal))
    }

    /// Determine if the downlink is still running.
    pub fn is_running(&self) -> bool {
        !self.internal.task.is_complete()
    }

    /// Get a future that will complete when the downlink stops running.
    pub fn await_stopped(&self) -> StoppedFuture {
        self.internal.task.await_stopped()
    }
}

pub type DroppingTopicReceiver<T> = WatchTopicReceiver<Event<T>>;
pub type DroppingReceiver<T> = DownlinkReceiver<WatchTopicReceiver<Event<T>>>;

impl<Act, Upd> DroppingDownlink<Act, Upd>
where
    Act: Send + 'static,
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
        let (topic, first) = WatchTopic::new(receiver);
        let internal = Internal {
            input: sender.clone(),
            topic: topic.clone(),
            task,
        };
        let internals_ptr = Arc::new(internal);
        let first_receiver = DownlinkReceiver::new(first, internals_ptr.clone());
        (
            DroppingDownlink {
                input: sender,
                topic,
                internal: internals_ptr,
            },
            first_receiver,
        )
    }
}

impl<Act, Upd> Topic<Event<Upd>> for DroppingDownlink<Act, Upd>
where
    Act: Send + 'static,
    Upd: Clone + Send + Sync + 'static,
{
    type Receiver = DroppingReceiver<Upd>;
    type Fut =
        TransformedFuture<Ready<Result<DroppingTopicReceiver<Upd>, TopicError>>, MakeReceiver>;

    fn subscribe(&mut self) -> Self::Fut {
        self.topic
            .subscribe()
            .transform(MakeReceiver::new(self.internal.clone()))
    }
}

impl<'a, Act, Upd> ItemSink<'a, Act> for DroppingDownlink<Act, Upd>
where
    Act: Send + 'static,
{
    type Error = DownlinkError;
    type SendFuture = MpscSend<'a, Act, DownlinkError>;

    fn send_item(&'a mut self, value: Act) -> Self::SendFuture {
        MpscSend::new(&mut self.input, value)
    }
}

impl<Act, Upd> Downlink<Act, Event<Upd>> for DroppingDownlink<Act, Upd>
where
    Act: Send + 'static,
    Upd: Clone + Send + Sync + 'static,
{
    type DlTopic = DownlinkTopic<WatchTopic<Event<Upd>>>;
    type DlSink = raw::Sender<mpsc::Sender<Act>>;

    fn split(self) -> (Self::DlTopic, Self::DlSink) {
        let DroppingDownlink {
            input,
            topic,
            internal,
        } = self;
        let sender = raw::Sender::new(input, internal.clone());
        let dl_topic = DownlinkTopic::new(topic, internal);
        (dl_topic, sender)
    }
}

pub(in crate::downlink) fn make_downlink<M, A, State, Machine, Updates, Commands>(
    machine: Machine,
    update_stream: Updates,
    cmd_sink: Commands,
    config: &DownlinkParams,
) -> (
    DroppingDownlink<A, Machine::Ev>,
    DroppingReceiver<Machine::Ev>,
)
where
    M: Send + 'static,
    A: Send + 'static,
    State: Send + 'static,
    Machine: StateMachine<State, M, A> + Send + 'static,
    Machine::Ev: Clone + Send + Sync + 'static,
    Machine::Cmd: Send + 'static,
    Updates: Stream<Item = Result<Message<M>, RoutingError>> + Send + 'static,
    Commands: ItemSender<Command<Machine::Cmd>, RoutingError> + Send + 'static,
{
    let (act_tx, act_rx) = mpsc::channel::<A>(config.buffer_size.get());
    let (event_tx, event_rx) = watch::channel::<Option<Event<Machine::Ev>>>(None);

    let event_sink = item::for_watch_sender::<_, DroppedError>(event_tx);

    let (stopped_tx, stopped_rx) = watch::channel(None);

    let completed = Arc::new(AtomicBool::new(false));

    // The task that maintains the internal state of the lane.
    let task = DownlinkTask::new(
        cmd_sink,
        event_sink,
        completed.clone(),
        stopped_tx,
        config.on_invalid,
    );

    let lane_task = task.run(
        raw::make_operation_stream(update_stream),
        act_rx.fuse(),
        machine,
        config.yield_after,
    );

    let join_handle = spawn(lane_task);

    let dl_task = raw::DownlinkTaskHandle::new(join_handle, stopped_rx, completed);

    let raw_dl = raw::RawDownlink::new(act_tx, event_rx, dl_task);

    DroppingDownlink::from_raw(raw_dl)
}
