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

#[cfg(test)]
mod tests;

use crate::downlink::model::map::ViewWithEvent;
use crate::downlink::model::value::SharedValue;
use crate::downlink::typed::event::TypedViewWithEvent;
use crate::downlink::Event;
use futures::Stream;
use std::convert::TryInto;
use std::marker::PhantomData;
use swim_common::topic::{Topic, TopicError};
use swim_form::Form;
use swim_form::FormDeserializeErr;
use utilities::future::{SwimFutureExt, Transform, TransformedFuture, UntilFailure};

/// A transformation that attempts to apply a form to an [`Event<Value>`].
#[derive(Debug)]
pub struct ApplyForm<T>(PhantomData<T>);

impl<T: Form> Default for ApplyForm<T> {
    fn default() -> Self {
        ApplyForm::new()
    }
}

impl<T> Clone for ApplyForm<T> {
    fn clone(&self) -> Self {
        ApplyForm(self.0)
    }
}

impl<T> Copy for ApplyForm<T> {}

/// A transformation that attempts to apply forms to a [`Event<ViewWithEvent>`].
#[derive(Debug)]
pub struct ApplyFormsMap<K, V>(PhantomData<(K, V)>);

impl<K: Form, V: Form> Default for ApplyFormsMap<K, V> {
    fn default() -> Self {
        ApplyFormsMap::new()
    }
}

impl<K, V> Clone for ApplyFormsMap<K, V> {
    fn clone(&self) -> Self {
        ApplyFormsMap(self.0)
    }
}

impl<K, V> Copy for ApplyFormsMap<K, V> {}

impl<T: Form> ApplyForm<T> {
    pub(super) fn new() -> Self {
        ApplyForm(PhantomData)
    }
}

impl<K: Form, V: Form> ApplyFormsMap<K, V> {
    pub(super) fn new() -> Self {
        ApplyFormsMap(PhantomData)
    }
}

impl<T: Form> Transform<Event<SharedValue>> for ApplyForm<T> {
    type Out = Result<Event<T>, FormDeserializeErr>;

    fn transform(&self, value: Event<SharedValue>) -> Self::Out {
        value.try_transform(|val| T::try_from_value(val.as_ref()))
    }
}

impl<K: Form, V: Form> Transform<Event<ViewWithEvent>> for ApplyFormsMap<K, V> {
    type Out = Result<Event<TypedViewWithEvent<K, V>>, FormDeserializeErr>;

    fn transform(&self, input: Event<ViewWithEvent>) -> Self::Out {
        input.try_transform(|val| val.try_into())
    }
}

/// A wrapper around a topic of events on a downlink that will attempt to apply [`Form`]s to each
/// event until the transformation fails at which point the topic streams will terminate.
pub struct TryTransformTopic<In, Top, Trans> {
    topic: Top,
    transform: Trans,
    _input_type: PhantomData<In>,
}

impl<In, Top, Trans> TryTransformTopic<In, Top, Trans> {
    pub(super) fn new(topic: Top, transform: Trans) -> Self {
        TryTransformTopic {
            topic,
            transform,
            _input_type: PhantomData,
        }
    }
}

/// Transformation that wraps a [`Stream`] with [`UntilFailure`].
pub struct WrapUntilFailure<Trans>(Trans);

impl<Trans> WrapUntilFailure<Trans> {
    pub(super) fn new(transform: Trans) -> Self {
        WrapUntilFailure(transform)
    }
}

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
        self.topic
            .subscribe()
            .transform(WrapUntilFailure::new(self.transform.clone()))
    }
}
