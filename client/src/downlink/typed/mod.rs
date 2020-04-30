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
pub mod event;
pub mod topic;

use crate::downlink::model::map::{MapAction, ViewWithEvent};
use crate::downlink::model::value::{Action, SharedValue};
use crate::downlink::typed::action::{MapActions, ValueActions};
use crate::downlink::typed::event::TypedViewWithEvent;
use crate::downlink::typed::topic::{ApplyForm, ApplyFormMap, TryTransformTopic, WrapUntilFailure};
use crate::downlink::{Downlink, Event};
use common::sink::item::ItemSink;
use common::topic::Topic;
use form::Form;
use std::marker::PhantomData;
use utilities::future::{SwimFutureExt, TransformedFuture, UntilFailure};

/// A wrapper around a value downlink, applying a [`Form`] to the values.
pub struct ValueDownlink<Inner, T> {
    inner: Inner,
    _value_type: PhantomData<T>,
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

/// A wrapper around a map downlink, applying [`Form`]s to the keys and values.
pub struct MapDownlink<Inner, K, V> {
    inner: Inner,
    _value_type: PhantomData<(K, V)>,
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
    type Receiver = UntilFailure<Inner::Receiver, ApplyFormMap<K, V>>;
    type Fut = TransformedFuture<Inner::Fut, WrapUntilFailure<ApplyFormMap<K, V>>>;

    fn subscribe(&mut self) -> Self::Fut {
        self.inner
            .subscribe()
            .transform(WrapUntilFailure::new(ApplyFormMap::new()))
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
    type DlTopic = TryTransformTopic<ViewWithEvent, Inner::DlTopic, ApplyFormMap<K, V>>;
    type DlSink = MapActions<Inner::DlSink, K, V>;

    fn split(self) -> (Self::DlTopic, Self::DlSink) {
        let (inner_topic, inner_sink) = self.inner.split();
        let topic = TryTransformTopic::new(inner_topic, ApplyFormMap::new());
        let sink = MapActions::new(inner_sink);
        (topic, sink)
    }
}
