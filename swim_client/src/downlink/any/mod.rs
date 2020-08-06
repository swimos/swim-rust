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

use crate::downlink::buffered::{
    BufferedDownlink, BufferedReceiver, BufferedTopicReceiver, WeakBufferedDownlink,
};
use crate::downlink::dropping::{
    DroppingDownlink, DroppingReceiver, DroppingTopicReceiver, WeakDroppingDownlink,
};
use crate::downlink::queue::{QueueDownlink, QueueReceiver, QueueTopicReceiver, WeakQueueDownlink};
use crate::downlink::topic::{DownlinkTopic, MakeReceiver};
use crate::downlink::{raw, StoppedFuture};
use crate::downlink::{Downlink, DownlinkError, Event};
use futures::future::{ErrInto, Ready};
use futures::task::{Context, Poll};
use futures::{Future, Stream};
use futures_util::StreamExt;
use pin_project::pin_project;
use std::fmt::{Display, Formatter};
use swim_common::request::request_future::{RequestFuture, Sequenced};
use swim_common::request::Request;
use swim_common::sink::item::{ItemSink, MpscSend};
use swim_common::topic::{BroadcastTopic, MpscTopic, Topic, TopicError, WatchTopic};
use tokio::macros::support::Pin;
use tokio::sync::{mpsc, oneshot};
use utilities::future::TransformedFuture;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicKind {
    Queue,
    Dropping,
    Buffered,
}

impl Display for TopicKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TopicKind::Queue => write!(f, "Queue"),
            TopicKind::Dropping => write!(f, "Dropping"),
            TopicKind::Buffered => write!(f, "Buffered"),
        }
    }
}

/// Wrapper around any one of queueing, dropping and buffered downlink implementations. This
/// itself implements the Downlink trait (although using it like this will be slightly less
/// efficient than using the wrapped downlink directly).
#[derive(Debug)]
pub enum AnyDownlink<Act, Upd> {
    Queue(QueueDownlink<Act, Upd>),
    Dropping(DroppingDownlink<Act, Upd>),
    Buffered(BufferedDownlink<Act, Upd>),
}

/// A weak handle on a downlink. Holding this will not keep the downlink running nor prevent
/// its sender and topic from being dropped.
#[derive(Debug)]
pub enum AnyWeakDownlink<Act, Upd> {
    Queue(WeakQueueDownlink<Act, Upd>),
    Dropping(WeakDroppingDownlink<Act, Upd>),
    Buffered(WeakBufferedDownlink<Act, Upd>),
}

impl<Act, Upd> AnyWeakDownlink<Act, Upd> {
    /// Attempt to upgrade this weak handle to a strong one.
    pub fn upgrade(&self) -> Option<AnyDownlink<Act, Upd>> {
        match self {
            AnyWeakDownlink::Queue(qdl) => qdl.upgrade().map(AnyDownlink::Queue),
            AnyWeakDownlink::Dropping(ddl) => ddl.upgrade().map(AnyDownlink::Dropping),
            AnyWeakDownlink::Buffered(bdl) => bdl.upgrade().map(AnyDownlink::Buffered),
        }
    }
}

impl<Act, Upd> AnyDownlink<Act, Upd> {
    pub fn kind(&self) -> TopicKind {
        match self {
            AnyDownlink::Queue(_) => TopicKind::Queue,
            AnyDownlink::Dropping(_) => TopicKind::Dropping,
            AnyDownlink::Buffered(_) => TopicKind::Buffered,
        }
    }

    pub fn same_downlink(&self, other: &Self) -> bool {
        match (self, other) {
            (AnyDownlink::Queue(ql), AnyDownlink::Queue(qr)) => ql.same_downlink(qr),
            (AnyDownlink::Dropping(dl), AnyDownlink::Dropping(dr)) => dl.same_downlink(dr),
            (AnyDownlink::Buffered(bl), AnyDownlink::Buffered(br)) => bl.same_downlink(br),
            _ => false,
        }
    }

    /// Downgrade this handle to a weak handle.
    pub fn downgrade(&self) -> AnyWeakDownlink<Act, Upd> {
        match self {
            AnyDownlink::Queue(qdl) => AnyWeakDownlink::Queue(qdl.downgrade()),
            AnyDownlink::Dropping(ddl) => AnyWeakDownlink::Dropping(ddl.downgrade()),
            AnyDownlink::Buffered(bdl) => AnyWeakDownlink::Buffered(bdl.downgrade()),
        }
    }

    /// Determine if the downlink is still running.
    pub fn is_running(&self) -> bool {
        match self {
            AnyDownlink::Queue(qdl) => qdl.is_running(),
            AnyDownlink::Dropping(ddl) => ddl.is_running(),
            AnyDownlink::Buffered(bdl) => bdl.is_running(),
        }
    }

    /// Get a future that will complete when the downlink stops running.
    pub fn await_stopped(&self) -> StoppedFuture {
        match self {
            AnyDownlink::Queue(qdl) => qdl.await_stopped(),
            AnyDownlink::Dropping(ddl) => ddl.await_stopped(),
            AnyDownlink::Buffered(bdl) => bdl.await_stopped(),
        }
    }
}

impl<Act, Upd> Clone for AnyDownlink<Act, Upd> {
    fn clone(&self) -> Self {
        match self {
            AnyDownlink::Queue(dl) => AnyDownlink::Queue((*dl).clone()),
            AnyDownlink::Dropping(dl) => AnyDownlink::Dropping((*dl).clone()),
            AnyDownlink::Buffered(dl) => AnyDownlink::Buffered((*dl).clone()),
        }
    }
}

#[derive(Debug)]
pub struct AnyEventReceiver<Upd: Clone + Send>(AnyReceiver<Upd>);

impl<Upd: Clone + Send> AnyEventReceiver<Upd> {
    pub fn new(recv: AnyReceiver<Upd>) -> Self {
        AnyEventReceiver(recv)
    }

    pub async fn recv(&mut self) -> Option<Upd> {
        self.0.next().await.map(|event| event.get_inner())
    }
}

#[pin_project(project = AnyReceiverProj)]
#[derive(Debug)]
pub enum AnyReceiver<Upd> {
    Queue(#[pin] QueueReceiver<Upd>),
    Dropping(#[pin] DroppingReceiver<Upd>),
    Buffered(#[pin] BufferedReceiver<Upd>),
}

impl<Upd: Clone + Send> Stream for AnyReceiver<Upd> {
    type Item = Event<Upd>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projected = self.project();
        match projected {
            AnyReceiverProj::Queue(rec) => rec.poll_next(cx),
            AnyReceiverProj::Dropping(rec) => rec.poll_next(cx),
            AnyReceiverProj::Buffered(rec) => rec.poll_next(cx),
        }
    }
}

pub type QueueSubFuture<Upd> = TransformedFuture<
    ErrInto<
        Sequenced<
            RequestFuture<Request<QueueTopicReceiver<Upd>>>,
            oneshot::Receiver<QueueTopicReceiver<Upd>>,
        >,
        TopicError,
    >,
    MakeReceiver,
>;
pub type DroppingSubFuture<Upd> =
    TransformedFuture<Ready<Result<DroppingTopicReceiver<Upd>, TopicError>>, MakeReceiver>;
pub type BufferedSubFuture<Upd> =
    TransformedFuture<Ready<Result<BufferedTopicReceiver<Upd>, TopicError>>, MakeReceiver>;

#[pin_project(project = AnySubFutureProj)]
pub enum AnySubFuture<Upd: Send + 'static> {
    Queue(#[pin] QueueSubFuture<Upd>),
    Dropping(#[pin] DroppingSubFuture<Upd>),
    Buffered(#[pin] BufferedSubFuture<Upd>),
}

impl<Upd: Clone + Send> Future for AnySubFuture<Upd> {
    type Output = Result<AnyReceiver<Upd>, TopicError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            AnySubFutureProj::Queue(fut) => fut.poll(cx).map(|r| r.map(AnyReceiver::Queue)),
            AnySubFutureProj::Dropping(fut) => fut.poll(cx).map(|r| r.map(AnyReceiver::Dropping)),
            AnySubFutureProj::Buffered(fut) => fut.poll(cx).map(|r| r.map(AnyReceiver::Buffered)),
        }
    }
}

impl<Act, Upd> Topic<Event<Upd>> for AnyDownlink<Act, Upd>
where
    Act: Send + 'static,
    Upd: Clone + Send + Sync + 'static,
{
    type Receiver = AnyReceiver<Upd>;
    type Fut = AnySubFuture<Upd>;

    fn subscribe(&mut self) -> Self::Fut {
        match self {
            AnyDownlink::Queue(qdl) => AnySubFuture::Queue(qdl.subscribe()),
            AnyDownlink::Dropping(ddl) => AnySubFuture::Dropping(ddl.subscribe()),
            AnyDownlink::Buffered(bdl) => AnySubFuture::Buffered(bdl.subscribe()),
        }
    }
}

impl<'a, Act, Upd> ItemSink<'a, Act> for AnyDownlink<Act, Upd>
where
    Act: Send + 'static,
{
    type Error = DownlinkError;
    type SendFuture = MpscSend<'a, Act, DownlinkError>;

    fn send_item(&'a mut self, value: Act) -> Self::SendFuture {
        match self {
            AnyDownlink::Queue(qdl) => qdl.send_item(value),
            AnyDownlink::Dropping(ddl) => ddl.send_item(value),
            AnyDownlink::Buffered(bdl) => bdl.send_item(value),
        }
    }
}

pub enum AnyDownlinkTopic<Upd> {
    Queue(DownlinkTopic<MpscTopic<Event<Upd>>>),
    Dropping(DownlinkTopic<WatchTopic<Event<Upd>>>),
    Buffered(DownlinkTopic<BroadcastTopic<Event<Upd>>>),
}

impl<Upd> Topic<Event<Upd>> for AnyDownlinkTopic<Upd>
where
    Upd: Clone + Send + Sync + 'static,
{
    type Receiver = AnyReceiver<Upd>;
    type Fut = AnySubFuture<Upd>;

    fn subscribe(&mut self) -> Self::Fut {
        match self {
            AnyDownlinkTopic::Queue(qdl) => AnySubFuture::Queue(qdl.subscribe()),
            AnyDownlinkTopic::Dropping(ddl) => AnySubFuture::Dropping(ddl.subscribe()),
            AnyDownlinkTopic::Buffered(bdl) => AnySubFuture::Buffered(bdl.subscribe()),
        }
    }
}
impl<Act, Upd> Downlink<Act, Event<Upd>> for AnyDownlink<Act, Upd>
where
    Upd: Clone + Send + Sync + 'static,
    Act: Send + 'static,
{
    type DlTopic = AnyDownlinkTopic<Upd>;
    type DlSink = raw::Sender<mpsc::Sender<Act>>;

    fn split(self) -> (Self::DlTopic, Self::DlSink) {
        match self {
            AnyDownlink::Queue(qdl) => {
                let (topic, sink) = qdl.split();
                (AnyDownlinkTopic::Queue(topic), sink)
            }
            AnyDownlink::Dropping(ddl) => {
                let (topic, sink) = ddl.split();
                (AnyDownlinkTopic::Dropping(topic), sink)
            }
            AnyDownlink::Buffered(bdl) => {
                let (topic, sink) = bdl.split();
                (AnyDownlinkTopic::Buffered(topic), sink)
            }
        }
    }
}
