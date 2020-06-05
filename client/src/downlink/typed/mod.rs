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

pub mod action;
pub mod any;
pub mod event;
pub mod topic;

use crate::downlink::any::{AnyDownlink, TopicKind};
use crate::downlink::model::map::{MapAction, ViewWithEvent};
use crate::downlink::model::value::{Action, SharedValue};
use crate::downlink::typed::action::{MapActions, ValueActions};
use crate::downlink::typed::any::map::AnyMapDownlink;
use crate::downlink::typed::any::value::AnyValueDownlink;
use crate::downlink::typed::event::TypedViewWithEvent;
use crate::downlink::typed::topic::{
    ApplyForm, ApplyFormsMap, TryTransformTopic, WrapUntilFailure,
};
use crate::downlink::{Downlink, Event, StoppedFuture};
use common::model::Value;
use common::sink::item::ItemSink;
use common::topic::Topic;
use form::Form;
use std::marker::PhantomData;
use utilities::future::{SwimFutureExt, TransformedFuture, UntilFailure};

/// A wrapper around a value downlink, applying a [`Form`] to the values.
#[derive(Debug)]
pub struct ValueDownlink<Inner, T> {
    inner: Inner,
    _value_type: PhantomData<fn(T) -> T>,
}

impl<Inner: Clone, T> Clone for ValueDownlink<Inner, T> {
    fn clone(&self) -> Self {
        ValueDownlink {
            inner: self.inner.clone(),
            _value_type: PhantomData,
        }
    }
}

impl<T> ValueDownlink<AnyDownlink<Action, SharedValue>, T>
where
    T: Form + Send + 'static,
{
    /// Unwrap an [`AnyDownlink`] and then reapply the type transformation to it.
    pub fn into_specific(self) -> AnyValueDownlink<T> {
        match self.inner {
            AnyDownlink::Queue(qdl) => AnyValueDownlink::Queue(ValueDownlink::new(qdl)),
            AnyDownlink::Dropping(ddl) => AnyValueDownlink::Dropping(ValueDownlink::new(ddl)),
            AnyDownlink::Buffered(bdl) => AnyValueDownlink::Buffered(ValueDownlink::new(bdl)),
        }
    }

    pub fn kind(&self) -> TopicKind {
        self.inner.kind()
    }

    /// Determine if the downlink is still running.
    pub fn is_running(&self) -> bool {
        self.inner.is_running()
    }

    pub fn same_downlink(&self, other: &Self) -> bool {
        self.inner.same_downlink(&other.inner)
    }

    /// Get a future that will complete when the downlink stops running.
    pub fn await_stopped(&self) -> StoppedFuture {
        self.inner.await_stopped()
    }
}

impl<Inner, T> ValueDownlink<Inner, T>
where
    Inner: Downlink<Action, Event<SharedValue>>,
    T: Form,
{
    pub fn new(inner: Inner) -> Self {
        ValueDownlink {
            inner,
            _value_type: PhantomData,
        }
    }
}

type MapValueType<K, V> = PhantomData<fn(K, V) -> (K, V)>;

/// A wrapper around a map downlink, applying [`Form`]s to the keys and values.
#[derive(Debug)]
pub struct MapDownlink<Inner, K, V> {
    inner: Inner,
    _value_type: MapValueType<K, V>,
}

impl<Inner: Clone, K, V> Clone for MapDownlink<Inner, K, V> {
    fn clone(&self) -> Self {
        MapDownlink {
            inner: self.inner.clone(),
            _value_type: PhantomData,
        }
    }
}

impl<K, V> MapDownlink<AnyDownlink<MapAction, ViewWithEvent>, K, V>
where
    K: Form + Send + 'static,
    V: Form + Send + 'static,
{
    /// Unwrap an [`AnyDownlink`] and then reapply the type transformation to it.
    pub fn into_specific(self) -> AnyMapDownlink<K, V> {
        match self.inner {
            AnyDownlink::Queue(qdl) => AnyMapDownlink::Queue(MapDownlink::new(qdl)),
            AnyDownlink::Dropping(ddl) => AnyMapDownlink::Dropping(MapDownlink::new(ddl)),
            AnyDownlink::Buffered(bdl) => AnyMapDownlink::Buffered(MapDownlink::new(bdl)),
        }
    }

    pub fn kind(&self) -> TopicKind {
        self.inner.kind()
    }

    /// Determine if the downlink is still running.
    pub fn is_running(&self) -> bool {
        self.inner.is_running()
    }

    pub fn same_downlink(&self, other: &Self) -> bool {
        self.inner.same_downlink(&other.inner)
    }

    /// Get a future that will complete when the downlink stops running.
    pub fn await_stopped(&self) -> StoppedFuture {
        self.inner.await_stopped()
    }
}

impl<Inner, K, V> MapDownlink<Inner, K, V>
where
    Inner: Downlink<MapAction, Event<ViewWithEvent>>,
    K: Form,
    V: Form,
{
    pub fn new(inner: Inner) -> Self {
        MapDownlink {
            inner,
            _value_type: PhantomData,
        }
    }
}

impl<Inner, T> Topic<Event<T>> for ValueDownlink<Inner, T>
where
    Inner: Topic<Event<SharedValue>>,
    T: Form + Send + 'static,
{
    type Receiver = UntilFailure<Inner::Receiver, ApplyForm<T>>;
    type Fut = TransformedFuture<Inner::Fut, WrapUntilFailure<ApplyForm<T>>>;

    fn subscribe(&mut self) -> Self::Fut {
        self.inner
            .subscribe()
            .transform(WrapUntilFailure::new(ApplyForm::new()))
    }
}

impl<Inner, K, V> Topic<Event<TypedViewWithEvent<K, V>>> for MapDownlink<Inner, K, V>
where
    Inner: Topic<Event<ViewWithEvent>>,
    K: Form + Send + 'static,
    V: Form + Send + 'static,
{
    type Receiver = UntilFailure<Inner::Receiver, ApplyFormsMap<K, V>>;
    type Fut = TransformedFuture<Inner::Fut, WrapUntilFailure<ApplyFormsMap<K, V>>>;

    fn subscribe(&mut self) -> Self::Fut {
        self.inner
            .subscribe()
            .transform(WrapUntilFailure::new(ApplyFormsMap::new()))
    }
}

impl<'a, Inner, T> ItemSink<'a, Action> for ValueDownlink<Inner, T>
where
    Inner: ItemSink<'a, Action>,
    T: Form + Send + 'static,
{
    type Error = Inner::Error;
    type SendFuture = Inner::SendFuture;

    fn send_item(&'a mut self, value: Action) -> Self::SendFuture {
        self.inner.send_item(value)
    }
}

impl<'a, Inner, K, V> ItemSink<'a, MapAction> for MapDownlink<Inner, K, V>
where
    Inner: ItemSink<'a, MapAction>,
    K: Form + Send + 'static,
    V: Form + Send + 'static,
{
    type Error = Inner::Error;
    type SendFuture = Inner::SendFuture;

    fn send_item(&'a mut self, value: MapAction) -> Self::SendFuture {
        self.inner.send_item(value)
    }
}

impl<Inner, T> Downlink<Action, Event<T>> for ValueDownlink<Inner, T>
where
    Inner: Downlink<Action, Event<SharedValue>>,
    T: Form + Send + 'static,
{
    type DlTopic = TryTransformTopic<SharedValue, Inner::DlTopic, ApplyForm<T>>;
    type DlSink = ValueActions<Inner::DlSink, T>;

    fn split(self) -> (Self::DlTopic, Self::DlSink) {
        let (inner_topic, inner_sink) = self.inner.split();
        let topic = TryTransformTopic::new(inner_topic, ApplyForm::new());
        let sink = ValueActions::new(inner_sink);
        (topic, sink)
    }
}

impl<Inner, K, V> Downlink<MapAction, Event<TypedViewWithEvent<K, V>>> for MapDownlink<Inner, K, V>
where
    Inner: Downlink<MapAction, Event<ViewWithEvent>>,
    K: Form + Send + 'static,
    V: Form + Send + 'static,
{
    type DlTopic = TryTransformTopic<ViewWithEvent, Inner::DlTopic, ApplyFormsMap<K, V>>;
    type DlSink = MapActions<Inner::DlSink, K, V>;

    fn split(self) -> (Self::DlTopic, Self::DlSink) {
        let (inner_topic, inner_sink) = self.inner.split();
        let topic = TryTransformTopic::new(inner_topic, ApplyFormsMap::new());
        let sink = MapActions::new(inner_sink);
        (topic, sink)
    }
}

/// A wrapper around a command value downlink, applying a [`Form`] to the values.
#[derive(Debug)]
pub struct CommandDownlink<Inner, T> {
    inner: Inner,
    _value_type: PhantomData<fn(T)>,
}

impl<Inner, T> CommandDownlink<Inner, T>
where
    // Inner: Downlink<CommandValue, ()>,
    T: Form,
{
    pub fn new(inner: Inner) -> Self {
        CommandDownlink {
            inner,
            _value_type: PhantomData,
        }
    }
}

impl<'a, Inner, T> ItemSink<'a, Value> for CommandDownlink<Inner, T>
where
    Inner: ItemSink<'a, Value>,
    T: Form + Send + 'static,
{
    type Error = Inner::Error;
    type SendFuture = Inner::SendFuture;

    fn send_item(&'a mut self, value: Value) -> Self::SendFuture {
        self.inner.send_item(value)
    }
}
