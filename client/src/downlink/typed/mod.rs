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

use crate::downlink::model::value::{SharedValue, Action};
use crate::downlink::{Downlink, Event};
use form::Form;
use std::marker::PhantomData;
use futures::Stream;
use deserialize::FormDeserializeErr;
use utilities::future::{Transform, UntilFailure, TransformedFuture, SwimFutureExt};
use crate::downlink::model::map::{ViewWithEvent, MapAction};
use common::topic::{Topic, TopicError};
use common::sink::item::ItemSink;
use crate::downlink::typed::action::{ValueActions, MapActions};
use std::convert::TryInto;
use crate::downlink::typed::event::TypedViewWithEvent;

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
        self.inner.subscribe().transform(WrapUntilFailure(ApplyForm::new()))
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
        self.inner.subscribe().transform(WrapUntilFailure(ApplyFormMap::new()))
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

pub struct ApplyForm<T>(PhantomData<T>);

impl<T> Clone for ApplyForm<T> {
    fn clone(&self) -> Self {
        ApplyForm(self.0)
    }
}

impl<T> Copy for ApplyForm<T> {}

pub struct ApplyFormMap<K, V>(PhantomData<(K, V)>);

impl<K, V> Clone for ApplyFormMap<K, V> {
    fn clone(&self) -> Self {
        ApplyFormMap(self.0)
    }
}

impl<K, V> Copy for ApplyFormMap<K, V> {}

impl<T: Form> ApplyForm<T> {
    fn new() -> Self {
        ApplyForm(PhantomData)
    }
}

impl<K: Form, V: Form> ApplyFormMap<K, V> {
    pub fn new() -> Self {
        ApplyFormMap(PhantomData)
    }
}

impl<T: Form> Transform<Event<SharedValue>> for ApplyForm<T> {
    type Out = Result<Event<T>, FormDeserializeErr>;

    fn transform(&self, value: Event<SharedValue>) -> Self::Out {
        let Event(value, local) = value;
        <T as Form>::try_from_value(value.as_ref())
            .map(|t| Event(t, local))
    }
}

impl<K: Form, V: Form> Transform<Event<ViewWithEvent>> for ApplyFormMap<K, V> {
    type Out = Result<Event<TypedViewWithEvent<K, V>>, FormDeserializeErr>;

    fn transform(&self, input: Event<ViewWithEvent>) -> Self::Out {
        let Event(value, local) = input;
        value.try_into().map(|v| Event(v, local))
    }
}

pub struct TryTransformTopic<In, Top, Trans> {
    topic: Top,
    transform: Trans,
    _input_type: PhantomData<In>,
}

impl<In, Top, Trans> TryTransformTopic<In, Top, Trans> {

    fn new(topic: Top,
           transform: Trans) -> Self {
        TryTransformTopic {
            topic,
            transform,
            _input_type: PhantomData
        }
    }

}


pub struct WrapUntilFailure<Trans>(Trans);

impl<Str, Trans> Transform<Result<Str, TopicError>> for WrapUntilFailure<Trans>
where
    Str: Stream,
    Trans: Transform<Str::Item> + Clone,
{
    type Out = Result<UntilFailure<Str, Trans>, TopicError>;

    fn transform(&self, result: Result<Str, TopicError>) -> Self::Out {
        result.map(|input| UntilFailure::new(input, self.0.clone()))
    }
}

impl<In, Out, Err, Top, Trans> Topic<Event<Out>> for TryTransformTopic<In, Top, Trans>
where
    In: Clone,
    Top: Topic<Event<In>>,
    Trans: Transform<Event<In>, Out = Result<Event<Out>, Err>> + Clone + Send + 'static,
{
    type Receiver = UntilFailure<Top::Receiver, Trans>;
    type Fut = TransformedFuture<Top::Fut, WrapUntilFailure<Trans>>;

    fn subscribe(&mut self) -> Self::Fut {
        self.topic.subscribe().transform(WrapUntilFailure(self.transform.clone()))
    }
}
