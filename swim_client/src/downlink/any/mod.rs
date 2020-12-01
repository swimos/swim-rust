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

use crate::downlink::buffered::{BufferedDownlink, BufferedReceiver, WeakBufferedDownlink};
use crate::downlink::dropping::{DroppingDownlink, DroppingReceiver, WeakDroppingDownlink};
use crate::downlink::queue::{QueueDownlink, QueueReceiver, WeakQueueDownlink};
use crate::downlink::raw;
use crate::downlink::topic::DownlinkTopic;
use crate::downlink::{Downlink, DownlinkError, Event};
use futures::future::BoxFuture;
use futures::task::{Context, Poll};
use futures::{FutureExt, Stream, StreamExt};
use pin_project::pin_project;
use std::fmt::{Display, Formatter};
use swim_common::topic::{BroadcastTopic, MpscTopic, Topic, TopicError, WatchTopic};
use tokio::macros::support::Pin;
use utilities::sync::promise;

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
    pub fn await_stopped(&self) -> promise::Receiver<Result<(), DownlinkError>> {
        match self {
            AnyDownlink::Queue(qdl) => qdl.await_stopped(),
            AnyDownlink::Dropping(ddl) => ddl.await_stopped(),
            AnyDownlink::Buffered(bdl) => bdl.await_stopped(),
        }
    }

    pub async fn send_item(&mut self, value: Act) -> Result<(), DownlinkError> {
        match self {
            AnyDownlink::Queue(qdl) => qdl.send_item(value).await,
            AnyDownlink::Dropping(ddl) => ddl.send_item(value).await,
            AnyDownlink::Buffered(bdl) => bdl.send_item(value).await,
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

impl<Act, Upd> Topic<Event<Upd>> for AnyDownlink<Act, Upd>
where
    Act: Send + 'static,
    Upd: Clone + Send + Sync + 'static,
{
    type Receiver = AnyReceiver<Upd>;

    fn subscribe(&mut self) -> BoxFuture<Result<Self::Receiver, TopicError>> {
        async move {
            let rec = match self {
                AnyDownlink::Queue(qdl) => AnyReceiver::Queue(qdl.subscribe().await?),
                AnyDownlink::Dropping(ddl) => AnyReceiver::Dropping(ddl.subscribe().await?),
                AnyDownlink::Buffered(bdl) => AnyReceiver::Buffered(bdl.subscribe().await?),
            };
            Ok(rec)
        }
        .boxed()
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

    fn subscribe(&mut self) -> BoxFuture<Result<Self::Receiver, TopicError>> {
        async move {
            let rec = match self {
                AnyDownlinkTopic::Queue(qdl) => AnyReceiver::Queue(qdl.subscribe().await?),
                AnyDownlinkTopic::Dropping(ddl) => AnyReceiver::Dropping(ddl.subscribe().await?),
                AnyDownlinkTopic::Buffered(bdl) => AnyReceiver::Buffered(bdl.subscribe().await?),
            };
            Ok(rec)
        }
        .boxed()
    }
}
impl<Act, Upd> Downlink<Act, Event<Upd>> for AnyDownlink<Act, Upd>
where
    Upd: Clone + Send + Sync + 'static,
    Act: Send + 'static,
{
    type DlTopic = AnyDownlinkTopic<Upd>;
    type DlSink = raw::Sender<Act>;

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
