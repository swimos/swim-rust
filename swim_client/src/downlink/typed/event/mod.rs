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

use crate::downlink::model::map::{MapEvent, ValMap, ViewWithEvent};
use crate::downlink::typed::{UntypedEventDownlink, ViewMode};
use crate::downlink::{Downlink, DownlinkError, Event};
use futures::stream::unfold;
use futures::Stream;
use im::OrdMap;
use std::any::type_name;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use swim_common::form::{Form, FormErr, ValidatedForm};
use swim_common::model::schema::StandardSchema;
use swim_common::model::Value;
use utilities::sync::{promise, topic};

#[cfg(test)]
mod tests;

/// Event representing a change of the state of a map downlink with type information applied using
/// a [`Form`] for the keys and values.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TypedViewWithEvent<K, V> {
    pub view: TypedMapView<K, V>,
    pub event: MapEvent<K>,
}

/// Typed view of a [`ValMap`] with type information applied using a [`Form`] for the keys and
/// values.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TypedMapView<K, V> {
    inner: ValMap,
    _entry_type: PhantomData<(K, V)>,
}

impl<K, V> TypedMapView<K, V> {
    pub fn new(inner: ValMap) -> Self {
        TypedMapView {
            inner,
            _entry_type: PhantomData,
        }
    }
}

impl<K: Form, V: Form> TypedMapView<K, V> {
    /// Get the value associated with a key.
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner
            .get(&key.as_value())
            .and_then(|value| V::try_from_value(value.as_ref()).ok())
    }

    /// The size of the underlying map.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the underlying map is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// An iterator over the entries of the map.
    pub fn iter(&self) -> impl Iterator<Item = (K, V)> + '_ {
        self.inner.iter().filter_map(|(key, value)| {
            match (K::try_from_value(key), V::try_from_value(value.as_ref())) {
                (Ok(k), Ok(v)) => Some((k, v)),
                _ => None,
            }
        })
    }

    /// An iterator over the keys of the map.
    pub fn keys(&self) -> impl Iterator<Item = K> + '_ {
        self.inner
            .keys()
            .filter_map(|key| K::try_from_value(key).ok())
    }
}

impl<K: Form + Hash + Eq, V: Form> TypedMapView<K, V> {
    /// Create a [`HashMap`] containing the typed values.
    pub fn as_hash_map(&self) -> HashMap<K, V> {
        let mut map = HashMap::new();
        for (key, value) in self.iter() {
            map.insert(key, value);
        }
        map
    }
}

impl<K: Form + Ord, V: Form> TypedMapView<K, V> {
    /// Create a [`BTreeMap`] containing the typed values.
    pub fn as_btree_map(&self) -> BTreeMap<K, V> {
        let mut map = BTreeMap::new();
        for (key, value) in self.iter() {
            map.insert(key, value);
        }
        map
    }
}

impl<K: Form + Ord + Clone, V: Form + Clone> TypedMapView<K, V> {
    /// Create an [`OrdMap`] containing the typed values.
    pub fn as_ord_map(&self) -> OrdMap<K, V> {
        let mut map = OrdMap::new();
        for (key, value) in self.iter() {
            map.insert(key, value);
        }
        map
    }
}

impl<K: Form, V: Form> TryFrom<ViewWithEvent> for TypedViewWithEvent<K, V> {
    type Error = FormErr;

    fn try_from(view: ViewWithEvent) -> Result<Self, Self::Error> {
        let ViewWithEvent { view, event } = view;

        let typed_event = type_event(event);
        typed_event.map(|ev| TypedViewWithEvent {
            view: TypedMapView::new(view),
            event: ev,
        })
    }
}

fn type_event<K: Form>(event: MapEvent<Value>) -> Result<MapEvent<K>, FormErr> {
    match event {
        MapEvent::Initial => Ok(MapEvent::Initial),
        MapEvent::Update(k) => K::try_convert(k).map(MapEvent::Update),
        MapEvent::Remove(k) => K::try_convert(k).map(MapEvent::Remove),
        MapEvent::Take(n) => Ok(MapEvent::Take(n)),
        MapEvent::Skip(n) => Ok(MapEvent::Skip(n)),
        MapEvent::Clear => Ok(MapEvent::Clear),
    }
}

pub struct TypedEventDownlink<T> {
    inner: Arc<UntypedEventDownlink>,
    _type: PhantomData<fn() -> T>,
}

impl<T> TypedEventDownlink<T> {
    pub(crate) fn new(inner: Arc<UntypedEventDownlink>) -> Self {
        TypedEventDownlink {
            inner,
            _type: PhantomData,
        }
    }
}

impl<T> Debug for TypedEventDownlink<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedEventDownlink")
            .field("inner", &self.inner)
            .field("type", &type_name::<T>())
            .finish()
    }
}

impl<T> Clone for TypedEventDownlink<T> {
    fn clone(&self) -> Self {
        TypedEventDownlink {
            inner: self.inner.clone(),
            _type: PhantomData,
        }
    }
}

impl<T> Downlink for TypedEventDownlink<T> {
    fn is_stopped(&self) -> bool {
        self.inner.is_stopped()
    }

    fn await_stopped(&self) -> promise::Receiver<Result<(), DownlinkError>> {
        self.inner.await_stopped()
    }

    fn same_downlink(left: &Self, right: &Self) -> bool {
        Arc::ptr_eq(&left.inner, &right.inner)
    }
}

impl<T: Form> TypedEventDownlink<T> {
    pub fn subscriber(&self) -> EventDownlinkSubscriber<T> {
        EventDownlinkSubscriber::new(self.inner.subscriber())
    }

    pub fn subscribe(&self) -> Option<EventDownlinkReceiver<T>> {
        self.inner.subscribe().map(EventDownlinkReceiver::new)
    }
}

pub struct EventDownlinkReceiver<T> {
    inner: topic::Receiver<Event<Value>>,
    _type: PhantomData<fn() -> T>,
}

impl<T> Debug for EventDownlinkReceiver<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventDownlinkReceiver")
            .field("inner", &self.inner)
            .field("type", &type_name::<T>())
            .finish()
    }
}

impl<T> Clone for EventDownlinkReceiver<T> {
    fn clone(&self) -> Self {
        EventDownlinkReceiver::new(self.inner.clone())
    }
}

pub struct EventDownlinkSubscriber<T> {
    inner: topic::Subscriber<Event<Value>>,
    _type: PhantomData<fn() -> T>,
}

impl<T> Debug for EventDownlinkSubscriber<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventDownlinkSubscriber")
            .field("inner", &self.inner)
            .field("type", &type_name::<T>())
            .finish()
    }
}

impl<T> Clone for EventDownlinkSubscriber<T> {
    fn clone(&self) -> Self {
        EventDownlinkSubscriber::new(self.inner.clone())
    }
}

impl<T> EventDownlinkReceiver<T> {
    fn new(inner: topic::Receiver<Event<Value>>) -> Self {
        EventDownlinkReceiver {
            inner,
            _type: PhantomData,
        }
    }
}

impl<T> EventDownlinkSubscriber<T> {
    fn new(inner: topic::Subscriber<Event<Value>>) -> Self {
        EventDownlinkSubscriber {
            inner,
            _type: PhantomData,
        }
    }
}

/// Error type returned when creating a view
/// for a event downlink with incompatible type.
#[derive(Debug, Clone)]
pub struct EventViewError {
    // A validation schema for the type of the original value downlink.
    existing: StandardSchema,
    // A validation schema for the type of the requested view.
    requested: StandardSchema,
    // The mode of the view.
    mode: ViewMode,
}

impl<T: ValidatedForm> EventDownlinkSubscriber<T> {
    /// Create a read-only view for a value downlink that converts all received values to a new type.
    /// The type of the view must have an equal or greater schema than the original downlink.
    pub async fn covariant_cast<U: ValidatedForm>(
        &self,
    ) -> Result<EventDownlinkSubscriber<U>, EventViewError> {
        let schema_cmp = U::schema().partial_cmp(&T::schema());

        if schema_cmp.is_some() && schema_cmp != Some(Ordering::Less) {
            Ok(EventDownlinkSubscriber::new(self.inner.clone()))
        } else {
            Err(EventViewError {
                existing: T::schema(),
                requested: U::schema(),
                mode: ViewMode::ReadOnly,
            })
        }
    }
}

impl<T: Form + 'static> EventDownlinkReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        let value = self.inner.recv().await;
        value.map(|g| Form::try_from_value(&*(&*g.get_inner_ref())).expect("Inconsistent Form"))
    }

    pub fn into_stream(self) -> impl Stream<Item = T> + Send + 'static {
        unfold(
            self,
            |mut rx| async move { rx.recv().await.map(|v| (v, rx)) },
        )
    }
}

impl<T: Form> EventDownlinkSubscriber<T> {
    pub async fn subscribe(&mut self) -> Result<EventDownlinkReceiver<T>, topic::SubscribeError> {
        self.inner.subscribe().map(EventDownlinkReceiver::new)
    }
}
