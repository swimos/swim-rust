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

use futures::future::Ready;
use futures::task::{Context, Poll};
use futures::{Future, Stream};
use tokio::macros::support::Pin;

use common::topic::{
    BroadcastTopic, MpscTopic, SendRequest, Sequenced, SubscriptionError, Topic, WatchTopic,
};
use pin_project::pin_project;

use crate::downlink::buffered::{BufferedDownlink, BufferedReceiver};
use crate::downlink::dropping::{DroppingDownlink, DroppingReceiver};
use crate::downlink::queue::{QueueDownlink, QueueReceiver};
use crate::downlink::raw;
use crate::downlink::{Downlink, DownlinkError, Event};
use crate::sink::item::{ItemSink, MpscSend};
use tokio::sync::{mpsc, oneshot};

/// Wrapper around any one of queueing, dropping and buffered downlink implementations. This
/// itself implements the Downlink trait (although using it like this will be slightly less
/// efficient than using the wrapped downlink directly).
pub enum AnyDownlink<Act, Upd> {
    Queue(QueueDownlink<Act, Upd>),
    Dropping(DroppingDownlink<Act, Upd>),
    Buffered(BufferedDownlink<Act, Upd>),
}

#[pin_project]
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
            __AnyReceiverProjection::Queue(rec) => rec.poll_next(cx),
            __AnyReceiverProjection::Dropping(rec) => rec.poll_next(cx),
            __AnyReceiverProjection::Buffered(rec) => rec.poll_next(cx),
        }
    }
}

pub type QueueSubFuture<Upd> =
    Sequenced<SendRequest<Event<Upd>>, oneshot::Receiver<mpsc::Receiver<Event<Upd>>>>;
pub type DroppingSubFuture<Upd> = Ready<Result<DroppingReceiver<Upd>, SubscriptionError>>;
pub type BufferedSubFuture<Upd> = Ready<Result<BufferedReceiver<Upd>, SubscriptionError>>;

#[pin_project]
pub enum AnySubFuture<Upd> {
    Queue(#[pin] QueueSubFuture<Upd>),
    Dropping(#[pin] DroppingSubFuture<Upd>),
    Buffered(#[pin] BufferedSubFuture<Upd>),
}

impl<Upd: Clone + Send> Future for AnySubFuture<Upd> {
    type Output = Result<AnyReceiver<Upd>, SubscriptionError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let projected = self.project();
        match projected {
            __AnySubFutureProjection::Queue(fut) => fut.poll(cx).map_ok(AnyReceiver::Queue),
            __AnySubFutureProjection::Dropping(fut) => fut.poll(cx).map_ok(AnyReceiver::Dropping),
            __AnySubFutureProjection::Buffered(fut) => fut.poll(cx).map_ok(AnyReceiver::Buffered),
        }
    }
}

impl<Act, Upd> Topic<Event<Upd>> for AnyDownlink<Act, Upd>
where
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
    Act: Unpin + Send + 'static,
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
    Queue(MpscTopic<Event<Upd>>),
    Dropping(WatchTopic<Event<Upd>>),
    Buffered(BroadcastTopic<Event<Upd>>),
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
    Act: Unpin + Send + 'static,
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
