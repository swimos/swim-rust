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

use crate::agent::AttachError;
use futures::Stream;
use std::any::Any;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;
use stm::var::observer::ObsSender;
use swim_common::topic::{BroadcastTopic, MpscTopic, Topic, TransformedTopic};
use tokio::sync::{mpsc, oneshot};
use utilities::future::TransformMut;

pub mod action;
pub mod map;
pub mod supply;
pub mod value;

pub trait DeferredLaneView<T>: Send + Sync {
    type View: Topic<T> + Send + Sync + 'static;

    fn attach(self) -> Result<Self::View, AttachError>;

    fn transform<Trans>(self, transform: Trans) -> TransformedDeferredLaneView<T, Self, Trans>
    where
        Self: Sized,
        Trans: TransformMut<T> + Clone + Send + 'static,
        Trans::Out: Stream + Send + 'static,
    {
        TransformedDeferredLaneView::new(self, transform)
    }
}

pub struct DeferredMpscView<T> {
    injector: oneshot::Sender<ObsSender<T>>,
    buffer_size: NonZeroUsize,
    yield_after: NonZeroUsize,
}

impl<T> DeferredMpscView<T> {
    pub fn new(
        injector: oneshot::Sender<ObsSender<T>>,
        buffer_size: NonZeroUsize,
        yield_after: NonZeroUsize,
    ) -> Self {
        DeferredMpscView {
            injector,
            buffer_size,
            yield_after,
        }
    }
}

pub struct DeferredBroadcastView<T> {
    injector: oneshot::Sender<ObsSender<T>>,
    buffer_size: NonZeroUsize,
}

impl<T> DeferredBroadcastView<T> {
    pub fn new(injector: oneshot::Sender<ObsSender<T>>, buffer_size: NonZeroUsize) -> Self {
        DeferredBroadcastView {
            injector,
            buffer_size,
        }
    }
}

impl<T> DeferredLaneView<Arc<T>> for DeferredMpscView<T>
where
    T: Any + Send + Sync,
{
    type View = MpscTopic<Arc<T>>;

    fn attach(self) -> Result<Self::View, AttachError> {
        let DeferredMpscView {
            injector,
            buffer_size,
            yield_after,
        } = self;
        let (tx, rx) = mpsc::channel::<Arc<T>>(buffer_size.get());
        if injector.send(tx.into()).is_err() {
            Err(AttachError::LaneStoppedReporting)
        } else {
            let (topic, _) = MpscTopic::new(rx, buffer_size, yield_after);
            Ok(topic)
        }
    }
}

impl<T> DeferredLaneView<Arc<T>> for DeferredBroadcastView<T>
where
    T: Any + Send + Sync,
{
    type View = BroadcastTopic<Arc<T>>;

    fn attach(self) -> Result<Self::View, AttachError> {
        let DeferredBroadcastView {
            injector,
            buffer_size,
        } = self;
        let (topic, tx, _) = BroadcastTopic::new(buffer_size.get());
        let tx = tx.try_into_inner().unwrap(); //TODO Make this better.
        if injector.send(tx.into()).is_err() {
            Err(AttachError::LaneStoppedReporting)
        } else {
            Ok(topic)
        }
    }
}

pub struct TransformedDeferredLaneView<T, View: DeferredLaneView<T>, Trans> {
    _type: PhantomData<fn(T) -> T>,
    inner: View,
    transform: Trans,
}

impl<T, View, Trans> TransformedDeferredLaneView<T, View, Trans>
where
    View: DeferredLaneView<T>,
    Trans: TransformMut<T> + Clone + Send + 'static,
    Trans::Out: Stream + Send + 'static,
{
    pub fn new(inner: View, transform: Trans) -> Self {
        TransformedDeferredLaneView {
            _type: PhantomData,
            inner,
            transform,
        }
    }
}

impl<T, View, Trans> DeferredLaneView<<<Trans as TransformMut<T>>::Out as Stream>::Item>
    for TransformedDeferredLaneView<T, View, Trans>
where
    T: 'static,
    View: DeferredLaneView<T>,
    Trans: TransformMut<T> + Clone + Send + Sync + 'static,
    Trans::Out: Stream + Send + 'static,
{
    type View = TransformedTopic<T, View::View, Trans>;

    fn attach(self) -> Result<Self::View, AttachError> {
        let TransformedDeferredLaneView {
            inner, transform, ..
        } = self;
        inner.attach().map(|top| top.transform(transform))
    }
}
