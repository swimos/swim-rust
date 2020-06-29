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

use crate::downlink::DownlinkInternals;
use common::topic::{Topic, TopicError};
use futures::task::{Context, Poll};
use futures::Stream;
use pin_project::pin_project;
use std::pin::Pin;
use std::sync::Arc;
use utilities::future::{TransformOnce, TransformedFuture};

/// A wrapper around a [`Topic`] containing a pointer to an associated downlink task.
#[derive(Debug)]
pub struct DownlinkTopic<Inner> {
    inner: Inner,
    task: Arc<dyn DownlinkInternals>,
}

/// A wrapper around a topic receiver containing a pointer to an associated downlink task.
#[pin_project]
#[derive(Debug)]
pub struct DownlinkReceiver<Inner> {
    #[pin]
    inner: Inner,
    _task: Arc<dyn DownlinkInternals>,
}

impl<Inner> DownlinkTopic<Inner> {
    pub(in crate::downlink) fn new<T>(inner_topic: Inner, task: Arc<dyn DownlinkInternals>) -> Self
    where
        Inner: Topic<T>,
    {
        DownlinkTopic {
            inner: inner_topic,
            task,
        }
    }
}

impl<Inner> DownlinkReceiver<Inner> {
    pub(in crate::downlink) fn new(inner_receiver: Inner, task: Arc<dyn DownlinkInternals>) -> Self
    where
        Inner: Stream,
    {
        DownlinkReceiver {
            inner: inner_receiver,
            _task: task,
        }
    }
}

impl<Inner> Stream for DownlinkReceiver<Inner>
where
    Inner: Stream,
{
    type Item = Inner::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projected = self.project();
        projected.inner.poll_next(cx)
    }
}

pub struct MakeReceiver(Arc<dyn DownlinkInternals>);

impl MakeReceiver {
    pub(in crate::downlink) fn new(task: Arc<dyn DownlinkInternals>) -> Self {
        MakeReceiver(task)
    }
}

impl<Inner: Stream> TransformOnce<Result<Inner, TopicError>> for MakeReceiver {
    type Out = Result<DownlinkReceiver<Inner>, TopicError>;

    fn transform(self, result: Result<Inner, TopicError>) -> Self::Out {
        let MakeReceiver(task) = self;
        result.map(|inner| DownlinkReceiver::new(inner, task))
    }
}

impl<T, Inner> Topic<T> for DownlinkTopic<Inner>
where
    Inner: Topic<T>,
{
    type Receiver = DownlinkReceiver<Inner::Receiver>;
    type Fut = TransformedFuture<Inner::Fut, MakeReceiver>;

    fn subscribe(&mut self) -> Self::Fut {
        let DownlinkTopic { inner, task } = self;
        let attach = MakeReceiver::new(task.clone());
        TransformedFuture::new(inner.subscribe(), attach)
    }
}
