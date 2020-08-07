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

use crate::downlink::any::{AnyDownlink, AnyEventReceiver, TopicKind};
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
use common::form::{Form, ValidatedForm};
use common::model::schema::StandardSchema;
use common::model::Value;
use common::sink::item::ItemSink;
use common::topic::Topic;
use std::cmp::Ordering;
use std::error::Error;
use std::fmt::{Display, Formatter};
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

impl<T, Inner> ValueDownlink<Inner, T>
where
    Inner: Downlink<Action, Event<SharedValue>> + Clone,
    T: ValidatedForm + Send + 'static,
{
    /// Create a read-only view for a value downlink that converts all received values to a new type.
    /// The type of the view must have an equal or greater schema than the original downlink.
    pub async fn read_only_view<ViewType: ValidatedForm>(
        &mut self,
    ) -> Result<TryTransformTopic<SharedValue, Inner::DlTopic, ApplyForm<ViewType>>, ValueViewError>
    {
        let schema_cmp = ViewType::schema().partial_cmp(&T::schema());

        if schema_cmp.is_some() && schema_cmp != Some(Ordering::Less) {
            let (topic, _) = self.inner.clone().split();
            let topic = TryTransformTopic::new(topic, ApplyForm::<ViewType>::new());
            Ok(topic)
        } else {
            Err(ValueViewError {
                existing: T::schema(),
                requested: ViewType::schema(),
                mode: ViewMode::ReadOnly,
            })
        }
    }

    /// Create a write-only sender for a value downlink that converts all sent values to a new type.
    /// The type of the sender must have an equal or lesser schema than the original downlink.
    pub async fn write_only_sender<ViewType: ValidatedForm>(
        &mut self,
    ) -> Result<ValueActions<Inner::DlSink, ViewType>, ValueViewError> {
        let schema_cmp = ViewType::schema().partial_cmp(&T::schema());

        if schema_cmp.is_some() && schema_cmp != Some(Ordering::Greater) {
            let (_, sink) = self.inner.clone().split();
            let sink = ValueActions::new(sink);
            Ok(sink)
        } else {
            Err(ValueViewError {
                existing: T::schema(),
                requested: ViewType::schema(),
                mode: ViewMode::WriteOnly,
            })
        }
    }
}

#[derive(Debug, Clone)]
pub enum ViewMode {
    ReadOnly,
    WriteOnly,
}

impl Display for ViewMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ViewMode::ReadOnly => write!(f, "read-only"),
            ViewMode::WriteOnly => write!(f, "write-only"),
        }
    }
}

/// Error type returned when creating a view
/// for a value downlink with incompatible type.
#[derive(Debug, Clone)]
pub struct ValueViewError {
    // A validation schema for the type of the original value downlink.
    existing: StandardSchema,
    // A validation schema for the type of the requested view.
    requested: StandardSchema,
    // The mode of the view.
    mode: ViewMode,
}

/// Error types returned when creating a view
/// for a map downlink with incompatible type.
#[derive(Debug, Clone)]
pub enum MapViewError {
    // Error returned when the key schemas are incompatible
    SchemaKeyError {
        // A validation schema for the key type of the original map downlink.
        existing: StandardSchema,
        // A validation schema for the key type of the requested view.
        requested: StandardSchema,
        // The mode of the view.
        mode: ViewMode,
    },
    // Error returned when the value schemas are incompatible
    SchemaValueError {
        // A validation schema for the value type of the original map downlink.
        existing: StandardSchema,
        // A validation schema for the value type of the requested view.
        requested: StandardSchema,
        // The mode of the view.
        mode: ViewMode,
    },
}

impl Display for ValueViewError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "A {} value downlink with schema {} was requested but the original value downlink is running with schema {}.", self.mode, self.requested, self.existing)
    }
}

impl Display for MapViewError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MapViewError::SchemaKeyError {
                existing,
                requested,
                mode
            } => write!(f, "A {} map downlink with key schema {} was requested but the original map downlink is running with key schema {}.", mode, requested, existing),
            MapViewError::SchemaValueError {
                existing,
                requested,
                mode,
            } => write!(f, "A {} map downlink with value schema {} was requested but the original map downlink is running with value schema {}.", mode, requested, existing),
        }
    }
}

impl Error for ValueViewError {}
impl Error for MapViewError {}

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

impl<Inner, K, V> MapDownlink<Inner, K, V>
where
    Inner: Downlink<MapAction, Event<ViewWithEvent>> + Clone,
    K: ValidatedForm + Send + 'static,
    V: ValidatedForm + Send + 'static,
{
    /// Create a read-only view for a map downlink that converts all received keys and values to new types.
    /// The types of the view must have an equal or greater schemas than the original downlink.
    pub async fn read_only_view<ViewKeyType: ValidatedForm, ViewValueType: ValidatedForm>(
        &mut self,
    ) -> Result<
        TryTransformTopic<ViewWithEvent, Inner::DlTopic, ApplyFormsMap<ViewKeyType, ViewValueType>>,
        MapViewError,
    > {
        let key_schema_cmp = ViewKeyType::schema().partial_cmp(&K::schema());
        let value_schema_cmp = ViewValueType::schema().partial_cmp(&V::schema());

        if key_schema_cmp.is_some() && key_schema_cmp != Some(Ordering::Less) {
            if value_schema_cmp.is_some() && value_schema_cmp != Some(Ordering::Less) {
                let (topic, _) = self.inner.clone().split();
                let topic = TryTransformTopic::new(
                    topic,
                    ApplyFormsMap::<ViewKeyType, ViewValueType>::new(),
                );
                Ok(topic)
            } else {
                Err(MapViewError::SchemaValueError {
                    existing: V::schema(),
                    requested: ViewValueType::schema(),
                    mode: ViewMode::ReadOnly,
                })
            }
        } else {
            Err(MapViewError::SchemaKeyError {
                existing: K::schema(),
                requested: ViewKeyType::schema(),
                mode: ViewMode::ReadOnly,
            })
        }
    }

    /// Create a write-only sender for a map downlink that converts all sent keys and values to a new type.
    /// The types of the sender must have an equal or lesser schemas than the original downlink.
    pub async fn write_only_sender<ViewKeyType: ValidatedForm, ViewValueType: ValidatedForm>(
        &mut self,
    ) -> Result<MapActions<Inner::DlSink, ViewKeyType, ViewValueType>, MapViewError> {
        let key_schema_cmp = ViewKeyType::schema().partial_cmp(&K::schema());
        let value_schema_cmp = ViewValueType::schema().partial_cmp(&V::schema());

        if key_schema_cmp.is_some() && key_schema_cmp != Some(Ordering::Greater) {
            if value_schema_cmp.is_some() && value_schema_cmp != Some(Ordering::Greater) {
                let (_, sink) = self.inner.clone().split();
                let sink = MapActions::new(sink);
                Ok(sink)
            } else {
                Err(MapViewError::SchemaValueError {
                    existing: V::schema(),
                    requested: ViewValueType::schema(),
                    mode: ViewMode::WriteOnly,
                })
            }
        } else {
            Err(MapViewError::SchemaKeyError {
                existing: K::schema(),
                requested: ViewKeyType::schema(),
                mode: ViewMode::WriteOnly,
            })
        }
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

/// A wrapper around a command downlink, applying a [`Form`] to the values.
#[derive(Debug)]
pub struct CommandDownlink<Inner, T> {
    inner: Inner,
    _value_type: PhantomData<fn(T)>,
}

impl<Inner, T> CommandDownlink<Inner, T>
where
    T: Form,
{
    pub fn new(inner: Inner) -> Self {
        CommandDownlink {
            inner,
            _value_type: PhantomData,
        }
    }
}

impl<'a, Inner, T> ItemSink<'a, T> for CommandDownlink<Inner, T>
where
    Inner: ItemSink<'a, Value>,
    T: Form + Send + 'static,
{
    type Error = Inner::Error;
    type SendFuture = Inner::SendFuture;

    fn send_item(&'a mut self, item: T) -> Self::SendFuture {
        self.inner.send_item(item.into_value())
    }
}

/// A wrapper around an event downlink, applying a [`Form`] to the values.
#[derive(Debug)]
pub struct EventDownlink<T> {
    inner: AnyEventReceiver<Value>,
    _value_type: PhantomData<fn(T)>,
}

impl<T> EventDownlink<T>
where
    T: Form,
{
    pub fn new(inner: AnyEventReceiver<Value>) -> Self {
        EventDownlink {
            inner,
            _value_type: PhantomData,
        }
    }
}

impl<T> EventDownlink<T>
where
    T: Form,
{
    pub async fn recv(&mut self) -> Option<T> {
        let value = self.inner.recv().await?;
        T::try_from_value(&value).ok()
    }
}

#[derive(Eq, PartialEq, Clone, Copy, Debug, Hash)]
pub enum SchemaViolations {
    Ignore,
    Report,
}

impl Default for SchemaViolations {
    fn default() -> Self {
        SchemaViolations::Report
    }
}
