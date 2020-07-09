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

use crate::downlink::model::map::{MapAction, ValMap};
use crate::downlink::model::value::{Action, SharedValue, UpdateResult};
use crate::downlink::typed::event::TypedMapView;
use crate::downlink::{DownlinkError, UpdateFailure};
use common::model::Value;
use common::request::Request;
use common::sink::item::{ItemSender, ItemSink};
use std::marker::PhantomData;
use swim_form_old::{Form, ValidatedForm};
use tokio::sync::oneshot;

/// Wraps a sender of updates to a value downlink, providing typed, asynchronous operations
/// that can be performed on in.
pub struct ValueActions<Sender, T> {
    sender: Sender,
    _value_type: PhantomData<T>,
}

impl<Sender, T> ValueActions<Sender, T> {
    pub fn new(sender: Sender) -> Self {
        ValueActions {
            sender,
            _value_type: PhantomData,
        }
    }
}

impl<Sender, T> ValueActions<Sender, T>
where
    T: ValidatedForm + 'static,
    Sender: ItemSender<Action, DownlinkError>,
{
    /// Get the current value of the downlink.
    pub async fn get(&mut self) -> Result<T, DownlinkError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::new(tx);
        self.sender.send_item(Action::get(req)).await?;
        await_value(rx).await
    }

    /// Set the value of the downlink to a new value, waiting for the operation to complete.
    pub async fn set(&mut self, value: T) -> Result<(), DownlinkError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::new(tx);
        self.sender
            .send_item(Action::set_and_await(value.into_value(), req))
            .await?;
        rx.await.map_err(|_| DownlinkError::DroppedChannel)?
    }

    /// Set the value of the downlink without waiting for the operation to complete.
    pub async fn set_and_forget(&mut self, value: T) -> Result<(), DownlinkError> {
        self.sender.send_item(Action::set(value.into_value())).await
    }

    /// Update the value of the downlink, returning the previous value.
    pub async fn update<F>(&mut self, update_fn: F) -> Result<T, DownlinkError>
    where
        F: FnOnce(T) -> T + Send + 'static,
    {
        let wrapped = wrap_update_fn(update_fn);
        let (tx, rx) = oneshot::channel();
        let req = Request::new(tx);
        self.sender
            .send_item(Action::try_update_and_await(wrapped, req))
            .await?;
        await_fallible(rx).await
    }

    /// Update the value of the downlink without waiting for the operation to complete.
    pub async fn update_and_forget<F>(&mut self, update_fn: F) -> Result<(), DownlinkError>
    where
        F: FnOnce(T) -> T + Send + 'static,
    {
        let wrapped = wrap_update_fn::<T, F>(update_fn);
        self.sender.send_item(Action::try_update(wrapped)).await
    }
}

impl<'a, Sender, T> ItemSink<'a, Action> for ValueActions<Sender, T>
where
    Sender: ItemSink<'a, Action>,
{
    type Error = Sender::Error;
    type SendFuture = Sender::SendFuture;

    fn send_item(&'a mut self, value: Action) -> Self::SendFuture {
        self.sender.send_item(value)
    }
}

/// Wraps a sender up updates to a map downlink providing typed, asynchronous operations
/// that can be performed on in.
pub struct MapActions<Sender, K, V> {
    sender: Sender,
    _entry_type: PhantomData<(K, V)>,
}

impl<Sender, K, V> MapActions<Sender, K, V> {
    pub fn new(sender: Sender) -> Self {
        MapActions {
            sender,
            _entry_type: PhantomData,
        }
    }
}

impl<Sender, K, V> MapActions<Sender, K, V>
where
    K: ValidatedForm + 'static,
    V: ValidatedForm + 'static,
    Sender: ItemSender<MapAction, DownlinkError>,
{
    /// Get the value associated with a specific key.
    pub async fn get(&mut self, key: K) -> Result<Option<V>, DownlinkError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::new(tx);
        self.sender
            .send_item(MapAction::get(key.into_value(), req))
            .await?;
        await_optional(rx).await
    }

    /// Insert an entry into the map returning any existing value associated with the key.
    pub async fn insert(&mut self, key: K, value: V) -> Result<Option<V>, DownlinkError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::new(tx);
        self.sender
            .send_item(MapAction::insert_and_await(
                key.into_value(),
                value.into_value(),
                req,
            ))
            .await?;
        await_optional(rx).await
    }

    /// Insert an entry into the map without waiting for the operation to complete.
    pub async fn insert_and_forget(&mut self, key: K, value: V) -> Result<(), DownlinkError> {
        self.sender
            .send_item(MapAction::insert(key.into_value(), value.into_value()))
            .await
    }

    /// Update the value associated with a key, returning the values associated withe the key before
    /// and after the operation.
    pub async fn update<F>(
        &mut self,
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
            .send_item(MapAction::try_update_and_await(
                key.into_value(),
                wrap_option_update_fn(update_fn),
                req1,
                req2,
            ))
            .await?;
        let before = await_fallible_optional(rx1).await?;
        let after = await_fallible_optional(rx2).await?;
        Ok((before, after))
    }

    /// Update the value associated with a key without waiting for the operation to complete.
    pub async fn update_and_forget<F>(&mut self, key: K, update_fn: F) -> Result<(), DownlinkError>
    where
        F: FnOnce(Option<V>) -> Option<V> + Send + 'static,
    {
        self.sender
            .send_item(MapAction::try_update(
                key.into_value(),
                wrap_option_update_fn(update_fn),
            ))
            .await?;
        Ok(())
    }

    /// Remove any value associated with a key, returning it.
    pub async fn remove(&mut self, key: K) -> Result<Option<V>, DownlinkError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::new(tx);
        self.sender
            .send_item(MapAction::remove_and_await(key.into_value(), req))
            .await?;
        await_optional(rx).await
    }

    /// Remove any value associated with a key without waiting for the operation to complete.
    pub async fn remove_and_forget(&mut self, key: K) -> Result<(), DownlinkError> {
        self.sender
            .send_item(MapAction::remove(key.into_value()))
            .await
    }

    async fn clear_internal(&mut self) -> Result<ValMap, DownlinkError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::new(tx);
        self.sender
            .send_item(MapAction::clear_and_await(req))
            .await?;
        rx.await.map_err(|_| DownlinkError::DroppedChannel)?
    }

    /// Clear the contents of the map.
    pub async fn clear(&mut self) -> Result<(), DownlinkError> {
        self.clear_internal().await.map(|_| ())
    }

    /// Remove all elements of the map and return its previous contents. This is equivalent to
    /// [`clear`] aside from returning the previous contents of the map to the caller.
    pub async fn remove_all(&mut self) -> Result<TypedMapView<K, V>, DownlinkError> {
        self.clear_internal().await.map(TypedMapView::new)
    }

    /// Clear the contents of the map without waiting for the operation to complete.
    pub async fn clear_and_forget(&mut self) -> Result<(), DownlinkError> {
        self.sender.send_item(MapAction::clear()).await
    }

    async fn take_internal(&mut self, n: usize) -> Result<(ValMap, ValMap), DownlinkError> {
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        let req1 = Request::new(tx1);
        let req2 = Request::new(tx2);
        self.sender
            .send_item(MapAction::take_and_await(n, req1, req2))
            .await?;
        let before = rx1.await.map_err(|_| DownlinkError::DroppedChannel)??;
        let after = rx2.await.map_err(|_| DownlinkError::DroppedChannel)??;
        Ok((before, after))
    }

    /// Retain only the first `n` elements of the map.
    pub async fn take(&mut self, n: usize) -> Result<(), DownlinkError> {
        self.take_internal(n).await.map(|_| ())
    }

    /// Retain only the first `n` elements of the map, returning the state of the map before and
    /// after the operation.
    pub async fn take_and_get(
        &mut self,
        n: usize,
    ) -> Result<(TypedMapView<K, V>, TypedMapView<K, V>), DownlinkError> {
        self.take_internal(n)
            .await
            .map(|(before, after)| (TypedMapView::new(before), TypedMapView::new(after)))
    }

    /// Retain only the first `n` elements of the map without waiting for the operation to complete.
    pub async fn take_and_forget(&mut self, n: usize) -> Result<(), DownlinkError> {
        self.sender.send_item(MapAction::take(n)).await
    }

    async fn skip_internal(&mut self, n: usize) -> Result<(ValMap, ValMap), DownlinkError> {
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        let req1 = Request::new(tx1);
        let req2 = Request::new(tx2);
        self.sender
            .send_item(MapAction::skip_and_await(n, req1, req2))
            .await?;
        let before = rx1.await.map_err(|_| DownlinkError::DroppedChannel)??;
        let after = rx2.await.map_err(|_| DownlinkError::DroppedChannel)??;
        Ok((before, after))
    }

    /// Skip the first `n` elements of the map.
    pub async fn skip(&mut self, n: usize) -> Result<(), DownlinkError> {
        self.skip_internal(n).await.map(|_| ())
    }

    /// Skip the first `n` elements of the map returning the state of the map before and after the
    /// operation.
    pub async fn skip_and_get(
        &mut self,
        n: usize,
    ) -> Result<(TypedMapView<K, V>, TypedMapView<K, V>), DownlinkError> {
        self.skip_internal(n)
            .await
            .map(|(before, after)| (TypedMapView::new(before), TypedMapView::new(after)))
    }

    /// Skip the first `n` elements of the map without waiting for the operation to complete.
    pub async fn skip_and_forget(&mut self, n: usize) -> Result<(), DownlinkError> {
        self.sender.send_item(MapAction::skip(n)).await
    }

    /// Get the current state of the map.
    pub async fn view(&mut self) -> Result<TypedMapView<K, V>, DownlinkError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::new(tx);
        self.sender.send_item(MapAction::get_map(req)).await?;
        let view = rx.await.map_err(|_| DownlinkError::DroppedChannel)??;
        Ok(TypedMapView::new(view))
    }
}

impl<'a, Sender, K, V> ItemSink<'a, MapAction> for MapActions<Sender, K, V>
where
    Sender: ItemSink<'a, MapAction>,
{
    type Error = Sender::Error;
    type SendFuture = Sender::SendFuture;

    fn send_item(&'a mut self, value: MapAction) -> Self::SendFuture {
        self.sender.send_item(value)
    }
}

fn wrap_update_fn<T, F>(update_fn: F) -> impl FnOnce(&Value) -> UpdateResult<Value>
where
    T: Form,
    F: FnOnce(T) -> T,
{
    move |value: &Value| match Form::try_from_value(value) {
        Ok(t) => Ok(update_fn(t).into_value()),
        Err(e) => Err(UpdateFailure(e.to_string())),
    }
}

fn wrap_option_update_fn<T, F>(
    update_fn: F,
) -> impl FnOnce(&Option<&Value>) -> UpdateResult<Option<Value>>
where
    T: Form,
    F: FnOnce(Option<T>) -> Option<T>,
{
    move |maybe_value| match maybe_value.as_ref() {
        Some(value) => match T::try_from_value(value) {
            Ok(t) => Ok(update_fn(Some(t)).map(Form::into_value)),
            Err(e) => Err(UpdateFailure(e.to_string())),
        },
        _ => Ok(update_fn(None).map(Form::into_value)),
    }
}

async fn await_value<T: ValidatedForm>(
    rx: oneshot::Receiver<Result<SharedValue, DownlinkError>>,
) -> Result<T, DownlinkError> {
    let value = rx.await.map_err(|_| DownlinkError::DroppedChannel)??;
    Form::try_from_value(value.as_ref()).map_err(|_| {
        let schema = T::schema();
        DownlinkError::SchemaViolation((*value).clone(), schema)
    })
}

async fn await_fallible<T: ValidatedForm>(
    rx: oneshot::Receiver<Result<UpdateResult<SharedValue>, DownlinkError>>,
) -> Result<T, DownlinkError> {
    let value = rx
        .await
        .map_err(|_| DownlinkError::DroppedChannel)??
        .map_err(|_| DownlinkError::InvalidAction)?;
    Form::try_from_value(value.as_ref()).map_err(|_| {
        let schema = T::schema();
        DownlinkError::SchemaViolation((*value).clone(), schema)
    })
}

async fn await_optional<T: ValidatedForm>(
    rx: oneshot::Receiver<Result<Option<SharedValue>, DownlinkError>>,
) -> Result<Option<T>, DownlinkError> {
    let maybe_value = rx.await.map_err(|_| DownlinkError::DroppedChannel)??;
    match maybe_value {
        Some(value) => Form::try_from_value(value.as_ref())
            .map_err(|_| {
                let schema = <T as ValidatedForm>::schema();
                DownlinkError::SchemaViolation((*value).clone(), schema)
            })
            .map(Some),
        _ => Ok(None),
    }
}

async fn await_fallible_optional<T: ValidatedForm>(
    rx: oneshot::Receiver<Result<UpdateResult<Option<SharedValue>>, DownlinkError>>,
) -> Result<Option<T>, DownlinkError> {
    let maybe_value = rx
        .await
        .map_err(|_| DownlinkError::DroppedChannel)??
        .map_err(|_| DownlinkError::InvalidAction)?;
    match maybe_value {
        Some(value) => Form::try_from_value(value.as_ref())
            .map_err(|_| {
                let schema = <T as ValidatedForm>::schema();
                DownlinkError::SchemaViolation((*value).clone(), schema)
            })
            .map(Some),
        _ => Ok(None),
    }
}
