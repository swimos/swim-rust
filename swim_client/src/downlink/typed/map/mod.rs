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

pub mod events;
#[cfg(test)]
mod tests;

use crate::downlink::model::map::{MapAction, MapEvent, ValMap, ViewWithEvent};
use crate::downlink::typed::{UntypedMapDownlink, ViewMode};
use crate::downlink::{Downlink, DownlinkError, Event};
use events::{TypedMapView, TypedViewWithEvent};
use futures::stream::unfold;
use futures::Stream;
use std::any::type_name;
use std::cmp::Ordering;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;
use swim_common::form::{Form, FormErr, ValidatedForm};
use swim_common::model::schema::StandardSchema;
use swim_common::model::Value;
use swim_common::request::Request;
use tokio::sync::{mpsc, oneshot};
use utilities::sync::{promise, topic};

type MapDlVariance<K, V> = fn(K, V) -> (K, V);

/// A downlink to a remote map lane containing values that are compatible with the
/// [`ValidatedForm`] implementations for `K` and `V`.
pub struct TypedMapDownlink<K, V> {
    inner: Arc<UntypedMapDownlink>,
    _type: PhantomData<MapDlVariance<K, V>>,
}

impl<K, V> TypedMapDownlink<K, V> {
    pub(crate) fn new(inner: Arc<UntypedMapDownlink>) -> Self {
        TypedMapDownlink {
            inner,
            _type: PhantomData,
        }
    }
}

impl<K, V> Debug for TypedMapDownlink<K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedValueDownlink")
            .field("inner", &self.inner)
            .field("key_type", &type_name::<K>())
            .field("value_type", &type_name::<V>())
            .finish()
    }
}

impl<K, V> Clone for TypedMapDownlink<K, V> {
    fn clone(&self) -> Self {
        TypedMapDownlink {
            inner: self.inner.clone(),
            _type: PhantomData,
        }
    }
}

impl<K, V> Downlink for TypedMapDownlink<K, V> {
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

impl<K: Form, V: Form> TypedMapDownlink<K, V> {
    /// Create a subscriber that can be used to produce receivers that observe changes to the state
    /// of the downlink.
    pub fn subscriber(&self) -> MapDownlinkSubscriber<K, V> {
        MapDownlinkSubscriber::new(self.inner.subscriber())
    }

    /// Create a new receiver, attached to the downlink, to observe changes to the state.
    pub fn subscribe(&self) -> Option<MapDownlinkReceiver<K, V>> {
        self.inner.subscribe().map(MapDownlinkReceiver::new)
    }

    /// Create a sender that can be used to interact with the map in the downlink.
    pub fn sender(&self) -> MapDownlinkSender<K, V> {
        MapDownlinkSender::new(self.inner.sender().clone())
    }
}

impl<K: ValidatedForm + 'static, V: ValidatedForm + 'static> TypedMapDownlink<K, V> {
    /// Get the value associated with a specific key.
    pub async fn get(&self, key: K) -> Result<Option<V>, DownlinkError> {
        actions::<K, V>(self.inner.sender()).get(key).await
    }

    /// Update an entry into the map returning any existing value associated with the key.
    pub async fn update(&self, key: K, value: V) -> Result<Option<V>, DownlinkError> {
        actions::<K, V>(self.inner.sender())
            .update(key, value)
            .await
    }

    /// Insert an entry into the map without waiting for the operation to complete.
    pub async fn update_and_forget(&self, key: K, value: V) -> Result<(), DownlinkError> {
        actions::<K, V>(self.inner.sender())
            .update_and_forget(key, value)
            .await
    }

    /// Modify the value associated with a key, returning the values associated withe the key before
    /// and after the operation.
    pub async fn modify<F>(
        &self,
        key: K,
        update_fn: F,
    ) -> Result<(Option<V>, Option<V>), DownlinkError>
    where
        F: FnOnce(Option<V>) -> Option<V> + Send + 'static,
    {
        actions::<K, V>(self.inner.sender())
            .modify(key, update_fn)
            .await
    }

    /// Modify the value associated with a key without waiting for the operation to complete.
    pub async fn modify_and_forget<F>(&self, key: K, update_fn: F) -> Result<(), DownlinkError>
    where
        F: FnOnce(Option<V>) -> Option<V> + Send + 'static,
    {
        actions::<K, V>(self.inner.sender())
            .modify_and_forget(key, update_fn)
            .await
    }

    /// Remove any value associated with a key, returning it.
    pub async fn remove(&self, key: K) -> Result<Option<V>, DownlinkError> {
        actions::<K, V>(self.inner.sender()).remove(key).await
    }

    /// Remove any value associated with a key without waiting for the operation to complete.
    pub async fn remove_and_forget(&self, key: K) -> Result<(), DownlinkError> {
        actions::<K, V>(self.inner.sender())
            .remove_and_forget(key)
            .await
    }

    /// Clear the contents of the map.
    pub async fn clear(&self) -> Result<(), DownlinkError> {
        actions::<K, V>(self.inner.sender()).clear().await
    }

    /// Remove all elements of the map and return its previous contents. This is equivalent to
    /// `clear` aside from returning the previous contents of the map to the caller.
    pub async fn remove_all(&self) -> Result<TypedMapView<K, V>, DownlinkError> {
        actions::<K, V>(self.inner.sender()).remove_all().await
    }

    /// Clear the contents of the map without waiting for the operation to complete.
    pub async fn clear_and_forget(&self) -> Result<(), DownlinkError> {
        actions::<K, V>(self.inner.sender())
            .clear_and_forget()
            .await
    }

    /// Retain only the first `n` elements of the map.
    pub async fn take(&self, n: usize) -> Result<(), DownlinkError> {
        actions::<K, V>(self.inner.sender()).take(n).await
    }

    /// Retain only the first `n` elements of the map, returning the state of the map before and
    /// after the operation.
    pub async fn take_and_get(
        &self,
        n: usize,
    ) -> Result<(TypedMapView<K, V>, TypedMapView<K, V>), DownlinkError> {
        actions::<K, V>(self.inner.sender()).take_and_get(n).await
    }

    /// Retain only the first `n` elements of the map without waiting for the operation to complete.
    pub async fn take_and_forget(&self, n: usize) -> Result<(), DownlinkError> {
        actions::<K, V>(self.inner.sender())
            .take_and_forget(n)
            .await
    }

    /// Skip the first `n` elements of the map.
    pub async fn skip(&self, n: usize) -> Result<(), DownlinkError> {
        actions::<K, V>(self.inner.sender()).skip(n).await
    }

    /// Skip the first `n` elements of the map returning the state of the map before and after the
    /// operation.
    pub async fn skip_and_get(
        &self,
        n: usize,
    ) -> Result<(TypedMapView<K, V>, TypedMapView<K, V>), DownlinkError> {
        actions::<K, V>(self.inner.sender()).skip_and_get(n).await
    }

    /// Skip the first `n` elements of the map without waiting for the operation to complete.
    pub async fn skip_and_forget(&self, n: usize) -> Result<(), DownlinkError> {
        actions::<K, V>(self.inner.sender())
            .skip_and_forget(n)
            .await
    }

    /// Get the current state of the map.
    pub async fn view(&self) -> Result<TypedMapView<K, V>, DownlinkError> {
        actions::<K, V>(self.inner.sender()).view().await
    }
}

/// A handle to interact with the map in a value downlink.
pub struct MapDownlinkSender<K, V> {
    inner: mpsc::Sender<MapAction>,
    _type: PhantomData<fn(K, V)>,
}

pub struct MapDownlinkContraView<K, V> {
    inner: mpsc::Sender<MapAction>,
    _type: PhantomData<fn(K, V)>,
}

pub struct MapDownlinkView<K, V> {
    inner: mpsc::Sender<MapAction>,
    _type: PhantomData<fn(K, V)>,
}

impl<K, V> Debug for MapDownlinkSender<K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueDownlinkSender")
            .field("inner", &self.inner)
            .field("key_type", &type_name::<K>())
            .field("value_type", &type_name::<V>())
            .finish()
    }
}

impl<K, V> Debug for MapDownlinkView<K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueDownlinkView")
            .field("inner", &self.inner)
            .field("key_type", &type_name::<K>())
            .field("value_type", &type_name::<V>())
            .finish()
    }
}

impl<K, V> Debug for MapDownlinkContraView<K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueDownlinkContraView")
            .field("inner", &self.inner)
            .field("key_type", &type_name::<K>())
            .field("value_type", &type_name::<V>())
            .finish()
    }
}

impl<K, V> Clone for MapDownlinkSender<K, V> {
    fn clone(&self) -> Self {
        MapDownlinkSender::new(self.inner.clone())
    }
}

impl<K, V> Clone for MapDownlinkView<K, V> {
    fn clone(&self) -> Self {
        MapDownlinkView::new(self.inner.clone())
    }
}

impl<K, V> Clone for MapDownlinkContraView<K, V> {
    fn clone(&self) -> Self {
        MapDownlinkContraView::new(self.inner.clone())
    }
}

impl<K, V> MapDownlinkSender<K, V> {
    fn new(inner: mpsc::Sender<MapAction>) -> Self {
        MapDownlinkSender {
            inner,
            _type: PhantomData,
        }
    }
}

impl<K, V> MapDownlinkView<K, V> {
    fn new(inner: mpsc::Sender<MapAction>) -> Self {
        MapDownlinkView {
            inner,
            _type: PhantomData,
        }
    }
}

impl<K, V> MapDownlinkContraView<K, V> {
    fn new(inner: mpsc::Sender<MapAction>) -> Self {
        MapDownlinkContraView {
            inner,
            _type: PhantomData,
        }
    }
}

fn actions<K, V>(sender: &mpsc::Sender<MapAction>) -> MapActions<K, V> {
    MapActions::new(sender)
}

impl<K: ValidatedForm, V: ValidatedForm> MapDownlinkSender<K, V> {
    /// Create a sender for more refined key and value types (the [`ValidatedForm`] implementations
    /// for `K2` and `V2` will always produce [`Value`]s that are acceptable to the
    /// [`ValidatedForm`] implementations for `K` and `V`) to the downlink.
    pub fn contravariant_view<K2, V2>(&self) -> Result<MapDownlinkContraView<K2, V2>, MapViewError>
    where
        K2: ValidatedForm,
        V2: ValidatedForm,
    {
        let key_schema_good = K2::schema()
            .partial_cmp(&K::schema())
            .map(|c| c != Ordering::Greater)
            .unwrap_or(false);
        let value_schema_good = V2::schema()
            .partial_cmp(&V::schema())
            .map(|c| c != Ordering::Greater)
            .unwrap_or(false);

        if key_schema_good && value_schema_good {
            Ok(MapDownlinkContraView::new(self.inner.clone()))
        } else {
            let incompatibility = if key_schema_good {
                Incompatibility::Value
            } else if value_schema_good {
                Incompatibility::Key
            } else {
                Incompatibility::Both
            };
            Err(MapViewError {
                mode: ViewMode::WriteOnly,
                existing_key: K::schema(),
                existing_value: V::schema(),
                requested_key: K2::schema(),
                requested_value: V2::schema(),
                incompatibility,
            })
        }
    }

    /// Create a sender for more general key and value types (the [`ValidatedForm`] implementations
    /// for `K` and `V` will always produce [`Value`]s that are acceptable to the
    /// [`ValidatedForm`] implementations for `K2` and `Vv`) to the downlink.
    pub fn covariant_view<K2, V2>(&self) -> Result<MapDownlinkView<K2, V2>, MapViewError>
    where
        K2: ValidatedForm,
        V2: ValidatedForm,
    {
        let key_schema_good = K2::schema()
            .partial_cmp(&K::schema())
            .map(|c| c != Ordering::Less)
            .unwrap_or(false);
        let value_schema_good = V2::schema()
            .partial_cmp(&V::schema())
            .map(|c| c != Ordering::Less)
            .unwrap_or(false);

        if key_schema_good && value_schema_good {
            Ok(MapDownlinkView::new(self.inner.clone()))
        } else {
            let incompatibility = if key_schema_good {
                Incompatibility::Value
            } else if value_schema_good {
                Incompatibility::Key
            } else {
                Incompatibility::Both
            };
            Err(MapViewError {
                mode: ViewMode::ReadOnly,
                existing_key: K::schema(),
                existing_value: V::schema(),
                requested_key: K2::schema(),
                requested_value: V2::schema(),
                incompatibility,
            })
        }
    }
}

impl<K, V> MapDownlinkSender<K, V>
where
    K: ValidatedForm + 'static,
    V: ValidatedForm + 'static,
{
    /// Get the value associated with a specific key.
    pub async fn get(&self, key: K) -> Result<Option<V>, DownlinkError> {
        actions::<K, V>(&self.inner).get(key).await
    }

    /// Update an entry into the map returning any existing value associated with the key.
    pub async fn update(&self, key: K, value: V) -> Result<Option<V>, DownlinkError> {
        actions::<K, V>(&self.inner).update(key, value).await
    }

    /// Insert an entry into the map without waiting for the operation to complete.
    pub async fn update_and_forget(&self, key: K, value: V) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner)
            .update_and_forget(key, value)
            .await
    }

    /// Modify the value associated with a key, returning the values associated withe the key before
    /// and after the operation.
    pub async fn modify<F>(
        &self,
        key: K,
        update_fn: F,
    ) -> Result<(Option<V>, Option<V>), DownlinkError>
    where
        F: FnOnce(Option<V>) -> Option<V> + Send + 'static,
    {
        actions::<K, V>(&self.inner).modify(key, update_fn).await
    }

    /// Modify the value associated with a key without waiting for the operation to complete.
    pub async fn modify_and_forget<F>(&self, key: K, update_fn: F) -> Result<(), DownlinkError>
    where
        F: FnOnce(Option<V>) -> Option<V> + Send + 'static,
    {
        actions::<K, V>(&self.inner)
            .modify_and_forget(key, update_fn)
            .await
    }

    /// Remove any value associated with a key, returning it.
    pub async fn remove(&self, key: K) -> Result<Option<V>, DownlinkError> {
        actions::<K, V>(&self.inner).remove(key).await
    }

    /// Remove any value associated with a key without waiting for the operation to complete.
    pub async fn remove_and_forget(&self, key: K) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).remove_and_forget(key).await
    }

    /// Clear the contents of the map.
    pub async fn clear(&self) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).clear().await
    }

    /// Remove all elements of the map and return its previous contents. This is equivalent to
    /// `clear` aside from returning the previous contents of the map to the caller.
    pub async fn remove_all(&self) -> Result<TypedMapView<K, V>, DownlinkError> {
        actions::<K, V>(&self.inner).remove_all().await
    }

    /// Clear the contents of the map without waiting for the operation to complete.
    pub async fn clear_and_forget(&self) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).clear_and_forget().await
    }

    /// Retain only the first `n` elements of the map.
    pub async fn take(&self, n: usize) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).take(n).await
    }

    /// Retain only the first `n` elements of the map, returning the state of the map before and
    /// after the operation.
    pub async fn take_and_get(
        &self,
        n: usize,
    ) -> Result<(TypedMapView<K, V>, TypedMapView<K, V>), DownlinkError> {
        actions::<K, V>(&self.inner).take_and_get(n).await
    }

    /// Retain only the first `n` elements of the map without waiting for the operation to complete.
    pub async fn take_and_forget(&self, n: usize) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).take_and_forget(n).await
    }

    /// Skip the first `n` elements of the map.
    pub async fn skip(&self, n: usize) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).skip(n).await
    }

    /// Skip the first `n` elements of the map returning the state of the map before and after the
    /// operation.
    pub async fn skip_and_get(
        &self,
        n: usize,
    ) -> Result<(TypedMapView<K, V>, TypedMapView<K, V>), DownlinkError> {
        actions::<K, V>(&self.inner).skip_and_get(n).await
    }

    /// Skip the first `n` elements of the map without waiting for the operation to complete.
    pub async fn skip_and_forget(&self, n: usize) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).skip_and_forget(n).await
    }

    /// Get the current state of the map.
    pub async fn view(&self) -> Result<TypedMapView<K, V>, DownlinkError> {
        actions::<K, V>(&self.inner).view().await
    }
}

impl<K, V> MapDownlinkView<K, V>
where
    K: ValidatedForm + 'static,
    V: ValidatedForm + 'static,
{
    /// Get the value associated with a specific key.
    pub async fn get(&self, key: K) -> Result<Option<V>, DownlinkError> {
        actions::<K, V>(&self.inner).get(key).await
    }

    /// Remove any value associated with a key, returning it.
    pub async fn remove(&self, key: K) -> Result<Option<V>, DownlinkError> {
        actions::<K, V>(&self.inner).remove(key).await
    }

    /// Remove any value associated with a key without waiting for the operation to complete.
    pub async fn remove_and_forget(&self, key: K) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).remove_and_forget(key).await
    }

    /// Clear the contents of the map.
    pub async fn clear(&self) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).clear().await
    }

    /// Clear the contents of the map without waiting for the operation to complete.
    pub async fn clear_and_forget(&self) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).clear_and_forget().await
    }

    /// Retain only the first `n` elements of the map.
    pub async fn take(&self, n: usize) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).take(n).await
    }

    /// Retain only the first `n` elements of the map without waiting for the operation to complete.
    pub async fn take_and_forget(&self, n: usize) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).take_and_forget(n).await
    }

    /// Skip the first `n` elements of the map.
    pub async fn skip(&self, n: usize) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).skip(n).await
    }

    /// Skip the first `n` elements of the map without waiting for the operation to complete.
    pub async fn skip_and_forget(&self, n: usize) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).skip_and_forget(n).await
    }

    /// Get the current state of the map.
    pub async fn view(&self) -> Result<TypedMapView<K, V>, DownlinkError> {
        actions::<K, V>(&self.inner).view().await
    }
}

impl<K, V> MapDownlinkContraView<K, V>
where
    K: ValidatedForm + 'static,
    V: ValidatedForm + 'static,
{
    /// Update an entry into the map returning any existing value associated with the key.
    pub async fn update(&self, key: K, value: V) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner)
            .update_discard(key, value)
            .await
    }

    /// Insert an entry into the map without waiting for the operation to complete.
    pub async fn update_and_forget(&self, key: K, value: V) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner)
            .update_and_forget(key, value)
            .await
    }

    /// Remove any value associated with a key, returning it.
    pub async fn remove(&self, key: K) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).remove_discard(key).await
    }

    /// Remove any value associated with a key without waiting for the operation to complete.
    pub async fn remove_and_forget(&self, key: K) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).remove_and_forget(key).await
    }

    /// Clear the contents of the map.
    pub async fn clear(&self) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).clear().await
    }

    /// Clear the contents of the map without waiting for the operation to complete.
    pub async fn clear_and_forget(&self) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).clear_and_forget().await
    }

    /// Retain only the first `n` elements of the map.
    pub async fn take(&self, n: usize) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).take(n).await
    }

    /// Retain only the first `n` elements of the map without waiting for the operation to complete.
    pub async fn take_and_forget(&self, n: usize) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).take_and_forget(n).await
    }

    /// Skip the first `n` elements of the map.
    pub async fn skip(&self, n: usize) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).skip(n).await
    }

    /// Skip the first `n` elements of the map without waiting for the operation to complete.
    pub async fn skip_and_forget(&self, n: usize) -> Result<(), DownlinkError> {
        actions::<K, V>(&self.inner).skip_and_forget(n).await
    }
}

/// A receiver that observes the state changes of the downlink. Note that a receiver must consume
/// the state changes or the downlink will block.
pub struct MapDownlinkReceiver<K, V> {
    inner: topic::Receiver<Event<ViewWithEvent>>,
    _type: PhantomData<fn() -> (K, V)>,
}

impl<K, V> Debug for MapDownlinkReceiver<K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueDownlinkReceiver")
            .field("inner", &self.inner)
            .field("key_type", &type_name::<K>())
            .field("value_type", &type_name::<V>())
            .finish()
    }
}

impl<K, V> Clone for MapDownlinkReceiver<K, V> {
    fn clone(&self) -> Self {
        MapDownlinkReceiver::new(self.inner.clone())
    }
}

/// Map downlink handle that can produce receivers that will observe the state changes of the
/// downlink.
pub struct MapDownlinkSubscriber<K, V> {
    inner: topic::Subscriber<Event<ViewWithEvent>>,
    _type: PhantomData<fn() -> (K, V)>,
}

impl<K, V> Debug for MapDownlinkSubscriber<K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueDownlinkSubscriber")
            .field("inner", &self.inner)
            .field("key_type", &type_name::<K>())
            .field("value_type", &type_name::<V>())
            .finish()
    }
}

impl<K, V> Clone for MapDownlinkSubscriber<K, V> {
    fn clone(&self) -> Self {
        MapDownlinkSubscriber::new(self.inner.clone())
    }
}

impl<K, V> MapDownlinkReceiver<K, V> {
    pub(crate) fn new(inner: topic::Receiver<Event<ViewWithEvent>>) -> Self {
        MapDownlinkReceiver {
            inner,
            _type: PhantomData,
        }
    }
}

impl<K, V> MapDownlinkSubscriber<K, V> {
    fn new(inner: topic::Subscriber<Event<ViewWithEvent>>) -> Self {
        MapDownlinkSubscriber {
            inner,
            _type: PhantomData,
        }
    }
}

impl<K: Form + 'static, V: Form + 'static> MapDownlinkReceiver<K, V> {
    /// Observe the next state change from the downlink.
    pub async fn recv(&mut self) -> Option<Event<TypedViewWithEvent<K, V>>> {
        let value = self.inner.recv().await;
        value.map(|g| transform_event(&*g))
    }

    /// Convert this receiver in a [`Stream`] of state changes.
    pub fn into_stream(
        self,
    ) -> impl Stream<Item = Event<TypedViewWithEvent<K, V>>> + Send + 'static {
        unfold(
            self,
            |mut rx| async move { rx.recv().await.map(|v| (v, rx)) },
        )
    }
}

fn transform_event<K: Form, V: Form>(
    event: &Event<ViewWithEvent>,
) -> Event<TypedViewWithEvent<K, V>> {
    match event {
        Event::Local(v) => Event::Local(transform_view(v)),
        Event::Remote(v) => Event::Remote(transform_view(v)),
    }
}

fn transform_view<K: Form, V: Form>(view: &ViewWithEvent) -> TypedViewWithEvent<K, V> {
    let ViewWithEvent { view, event } = view;
    let typed_map_view = TypedMapView::new(view.clone());
    let typed_event = type_event_ref(event).expect("Inconsistent Form");
    TypedViewWithEvent {
        view: typed_map_view,
        event: typed_event,
    }
}

impl<K: Form, V: Form> MapDownlinkSubscriber<K, V> {
    pub fn subscribe(&self) -> Result<MapDownlinkReceiver<K, V>, topic::SubscribeError> {
        self.inner.subscribe().map(MapDownlinkReceiver::new)
    }
}

pub fn type_event_ref<K: Form>(event: &MapEvent<Value>) -> Result<MapEvent<K>, FormErr> {
    match event {
        MapEvent::Initial => Ok(MapEvent::Initial),
        MapEvent::Update(k) => K::try_from_value(k).map(MapEvent::Update),
        MapEvent::Remove(k) => K::try_from_value(k).map(MapEvent::Remove),
        MapEvent::Take(n) => Ok(MapEvent::Take(*n)),
        MapEvent::Skip(n) => Ok(MapEvent::Skip(*n)),
        MapEvent::Clear => Ok(MapEvent::Clear),
    }
}

/// Wraps a sender of updates to a value downlink, providing typed, asynchronous operations
/// that can be performed on in.
struct MapActions<'a, K, V> {
    sender: &'a mpsc::Sender<MapAction>,
    _entry_type: PhantomData<MapDlVariance<K, V>>,
}

impl<'a, K, V> MapActions<'a, K, V> {
    pub fn new(sender: &'a mpsc::Sender<MapAction>) -> Self {
        MapActions {
            sender,
            _entry_type: PhantomData,
        }
    }
}

impl<'a, K, V> MapActions<'a, K, V>
where
    K: ValidatedForm + 'static,
    V: ValidatedForm + 'static,
{
    /// Get the value associated with a specific key.
    pub async fn get(&self, key: K) -> Result<Option<V>, DownlinkError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::new(tx);
        self.sender
            .send(MapAction::get(key.into_value(), req))
            .await?;
        super::await_optional(rx).await
    }

    /// Update an entry into the map returning any existing value associated with the key.
    pub async fn update(&self, key: K, value: V) -> Result<Option<V>, DownlinkError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::new(tx);
        self.sender
            .send(MapAction::update_and_await(
                key.into_value(),
                value.into_value(),
                req,
            ))
            .await?;
        super::await_optional(rx).await
    }

    /// Update an entry into the map.
    pub async fn update_discard(&self, key: K, value: V) -> Result<(), DownlinkError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::new(tx);
        self.sender
            .send(MapAction::update_and_await(
                key.into_value(),
                value.into_value(),
                req,
            ))
            .await?;
        super::await_discard(rx).await
    }

    /// Insert an entry into the map without waiting for the operation to complete.
    pub async fn update_and_forget(&self, key: K, value: V) -> Result<(), DownlinkError> {
        Ok(self
            .sender
            .send(MapAction::update(key.into_value(), value.into_value()))
            .await?)
    }

    /// Modify the value associated with a key, returning the values associated withe the key before
    /// and after the operation.
    pub async fn modify<F>(
        &self,
        key: K,
        update_fn: F,
    ) -> Result<(Option<V>, Option<V>), DownlinkError>
    where
        F: FnOnce(Option<V>) -> Option<V> + Send + 'static,
    {
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        let req1 = Request::new(tx1);
        let req2 = Request::new(tx2);
        self.sender
            .send(MapAction::try_modify_and_await(
                key.into_value(),
                super::wrap_option_update_fn(update_fn),
                req1,
                req2,
            ))
            .await?;
        let before = super::await_fallible_optional(rx1).await?;
        let after = super::await_fallible_optional(rx2).await?;
        Ok((before, after))
    }

    /// Modify the value associated with a key without waiting for the operation to complete.
    pub async fn modify_and_forget<F>(&self, key: K, update_fn: F) -> Result<(), DownlinkError>
    where
        F: FnOnce(Option<V>) -> Option<V> + Send + 'static,
    {
        self.sender
            .send(MapAction::try_modify(
                key.into_value(),
                super::wrap_option_update_fn(update_fn),
            ))
            .await?;
        Ok(())
    }

    /// Remove any value associated with a key, returning it.
    pub async fn remove(&self, key: K) -> Result<Option<V>, DownlinkError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::new(tx);
        self.sender
            .send(MapAction::remove_and_await(key.into_value(), req))
            .await?;
        super::await_optional(rx).await
    }

    /// Remove any value associated with a key.
    pub async fn remove_discard(&self, key: K) -> Result<(), DownlinkError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::new(tx);
        self.sender
            .send(MapAction::remove_and_await(key.into_value(), req))
            .await?;
        super::await_discard(rx).await
    }

    /// Remove any value associated with a key without waiting for the operation to complete.
    pub async fn remove_and_forget(&self, key: K) -> Result<(), DownlinkError> {
        Ok(self
            .sender
            .send(MapAction::remove(key.into_value()))
            .await?)
    }

    async fn clear_internal(&self) -> Result<ValMap, DownlinkError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::new(tx);
        self.sender.send(MapAction::clear_and_await(req)).await?;
        rx.await.map_err(|_| DownlinkError::DroppedChannel)?
    }

    /// Clear the contents of the map.
    pub async fn clear(&self) -> Result<(), DownlinkError> {
        self.clear_internal().await.map(|_| ())
    }

    /// Remove all elements of the map and return its previous contents. This is equivalent to
    /// `clear` aside from returning the previous contents of the map to the caller.
    pub async fn remove_all(&self) -> Result<TypedMapView<K, V>, DownlinkError> {
        self.clear_internal().await.map(TypedMapView::new)
    }

    /// Clear the contents of the map without waiting for the operation to complete.
    pub async fn clear_and_forget(&self) -> Result<(), DownlinkError> {
        Ok(self.sender.send(MapAction::clear()).await?)
    }

    async fn take_internal(&self, n: usize) -> Result<(ValMap, ValMap), DownlinkError> {
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        let req1 = Request::new(tx1);
        let req2 = Request::new(tx2);
        self.sender
            .send(MapAction::take_and_await(n, req1, req2))
            .await?;
        let before = rx1.await.map_err(|_| DownlinkError::DroppedChannel)??;
        let after = rx2.await.map_err(|_| DownlinkError::DroppedChannel)??;
        Ok((before, after))
    }

    /// Retain only the first `n` elements of the map.
    pub async fn take(&self, n: usize) -> Result<(), DownlinkError> {
        self.take_internal(n).await.map(|_| ())
    }

    /// Retain only the first `n` elements of the map, returning the state of the map before and
    /// after the operation.
    pub async fn take_and_get(
        &self,
        n: usize,
    ) -> Result<(TypedMapView<K, V>, TypedMapView<K, V>), DownlinkError> {
        self.take_internal(n)
            .await
            .map(|(before, after)| (TypedMapView::new(before), TypedMapView::new(after)))
    }

    /// Retain only the first `n` elements of the map without waiting for the operation to complete.
    pub async fn take_and_forget(&self, n: usize) -> Result<(), DownlinkError> {
        Ok(self.sender.send(MapAction::take(n)).await?)
    }

    async fn skip_internal(&self, n: usize) -> Result<(ValMap, ValMap), DownlinkError> {
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        let req1 = Request::new(tx1);
        let req2 = Request::new(tx2);
        self.sender
            .send(MapAction::skip_and_await(n, req1, req2))
            .await?;
        let before = rx1.await.map_err(|_| DownlinkError::DroppedChannel)??;
        let after = rx2.await.map_err(|_| DownlinkError::DroppedChannel)??;
        Ok((before, after))
    }

    /// Skip the first `n` elements of the map.
    pub async fn skip(&self, n: usize) -> Result<(), DownlinkError> {
        self.skip_internal(n).await.map(|_| ())
    }

    /// Skip the first `n` elements of the map returning the state of the map before and after the
    /// operation.
    pub async fn skip_and_get(
        &self,
        n: usize,
    ) -> Result<(TypedMapView<K, V>, TypedMapView<K, V>), DownlinkError> {
        self.skip_internal(n)
            .await
            .map(|(before, after)| (TypedMapView::new(before), TypedMapView::new(after)))
    }

    /// Skip the first `n` elements of the map without waiting for the operation to complete.
    pub async fn skip_and_forget(&self, n: usize) -> Result<(), DownlinkError> {
        Ok(self.sender.send(MapAction::skip(n)).await?)
    }

    /// Get the current state of the map.
    pub async fn view(&self) -> Result<TypedMapView<K, V>, DownlinkError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::new(tx);
        self.sender.send(MapAction::get_map(req)).await?;
        let view = rx.await.map_err(|_| DownlinkError::DroppedChannel)??;
        Ok(TypedMapView::new(view))
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Incompatibility {
    Key,
    Value,
    Both,
}

/// Error types returned when creating a view
/// for a map downlink with incompatible type.
#[derive(Debug, Clone)]
pub struct MapViewError {
    mode: ViewMode,
    existing_key: StandardSchema,
    existing_value: StandardSchema,
    requested_key: StandardSchema,
    requested_value: StandardSchema,
    incompatibility: Incompatibility,
}

impl Display for MapViewError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let MapViewError {
            mode,
            existing_key,
            existing_value,
            requested_key,
            requested_value,
            incompatibility,
        } = self;
        let reason = match incompatibility {
            Incompatibility::Key => "The key",
            Incompatibility::Value => "The value",
            Incompatibility::Both => "Both",
        };
        write!(f, "A {} view of a map downlink (key schema {} and value schema {})) was requested with key schema {} and value schema {}. {} schemas are incompatible.", mode, existing_key, existing_value, requested_key, requested_value, reason)
    }
}

impl Error for MapViewError {}

impl<K: ValidatedForm, V: ValidatedForm> MapDownlinkSubscriber<K, V> {
    /// Create a read-only view for a value downlink that converts all received values to a new type.
    /// The type of the view must have an equal or greater schema than the original downlink.
    pub fn covariant_cast<K2, V2>(self) -> Result<MapDownlinkSubscriber<K2, V2>, MapViewError>
    where
        K2: ValidatedForm,
        V2: ValidatedForm,
    {
        let key_schema_good = K2::schema()
            .partial_cmp(&K::schema())
            .map(|c| c != Ordering::Less)
            .unwrap_or(false);
        let value_schema_good = V2::schema()
            .partial_cmp(&V::schema())
            .map(|c| c != Ordering::Less)
            .unwrap_or(false);

        if key_schema_good && value_schema_good {
            Ok(MapDownlinkSubscriber::new(self.inner))
        } else {
            let incompatibility = if key_schema_good {
                Incompatibility::Value
            } else if value_schema_good {
                Incompatibility::Key
            } else {
                Incompatibility::Both
            };
            Err(MapViewError {
                mode: ViewMode::ReadOnly,
                existing_key: K::schema(),
                existing_value: V::schema(),
                requested_key: K2::schema(),
                requested_value: V2::schema(),
                incompatibility,
            })
        }
    }
}
