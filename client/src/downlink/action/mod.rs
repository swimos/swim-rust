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

use crate::downlink::model::map::{MapAction, TypedMapView, ValMap};
use crate::downlink::model::value::{Action, SharedValue};
use crate::downlink::DownlinkError;
use common::model::Value;
use common::request::Request;
use common::sink::item::{ItemSender, ItemSink};
use form::{Form, ValidatedForm};
use std::marker::PhantomData;
use tokio::sync::oneshot;

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
    pub async fn get(&mut self) -> Result<T, DownlinkError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::new(tx);
        self.sender.send_item(Action::get(req)).await?;
        await_value(rx).await
    }

    pub async fn set(&mut self, value: T) -> Result<(), DownlinkError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::new(tx);
        self.sender
            .send_item(Action::set_and_await(value.into_value(), req))
            .await?;
        rx.await.map_err(|_| DownlinkError::DroppedChannel)
    }

    pub async fn set_and_forget(&mut self, value: T) -> Result<(), DownlinkError> {
        self.sender.send_item(Action::set(value.into_value())).await
    }

    pub async fn update<F>(&mut self, update_fn: F) -> Result<T, DownlinkError>
    where
        F: FnOnce(T) -> T + Send + 'static,
    {
        let wrapped = wrap_update_fn(update_fn);
        let (tx, rx) = oneshot::channel();
        let req = Request::new(tx);
        self.sender
            .send_item(Action::update_and_await(wrapped, req))
            .await?;
        await_value(rx).await
    }

    pub async fn update_and_forget<F>(&mut self, update_fn: F) -> Result<(), DownlinkError>
    where
        F: FnOnce(T) -> T + Send + 'static,
    {
        let wrapped = wrap_update_fn::<T, F>(update_fn);
        self.sender.send_item(Action::update(wrapped)).await
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
    pub async fn get(&mut self, key: K) -> Result<Option<V>, DownlinkError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::new(tx);
        self.sender
            .send_item(MapAction::get(key.into_value(), req))
            .await?;
        await_optional(rx).await
    }

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

    pub async fn update<F>(&mut self, key: K, update_fn: F) -> Result<(Option<V>, Option<V>), DownlinkError>
    where
        F: FnOnce(Option<V>) -> Option<V> + Send + 'static,
    {
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        let req1 = Request::new(tx1);
        let req2 = Request::new(tx2);
        self.sender
            .send_item(MapAction::update_and_await(
                key.into_value(),
                wrap_option_update_fn(update_fn),
                req1, req2
            ))
            .await?;
        let before = await_optional(rx1).await?;
        let after = await_optional(rx2).await?;
        Ok((before, after))
    }

    pub async fn insert_and_forget(&mut self, key: K, value: V) -> Result<(), DownlinkError> {
        self.sender
            .send_item(MapAction::insert(key.into_value(), value.into_value()))
            .await
    }

    pub async fn remove(&mut self, key: K) -> Result<Option<V>, DownlinkError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::new(tx);
        self.sender
            .send_item(MapAction::remove_and_await(key.into_value(), req))
            .await?;
        await_optional(rx).await
    }

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
        rx.await
            .map_err(|_| DownlinkError::DroppedChannel)
    }

    pub async fn clear(&mut self) -> Result<(), DownlinkError> {
       self.clear_internal().await.map(|_| ())
    }

    pub async fn clear_and_get(&mut self) -> Result<TypedMapView<K, V>, DownlinkError> {
        self.clear_internal().await.map(TypedMapView::new)
    }

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
        let before = rx1.await.map_err(|_| DownlinkError::DroppedChannel)?;
        let after = rx2.await.map_err(|_| DownlinkError::DroppedChannel)?;
        Ok((before, after))
    }

    pub async fn take(&mut self, n: usize) -> Result<(), DownlinkError> {
        self.take_internal(n).await.map(|_| ())
    }

    pub async fn take_and_get(&mut self, n: usize) -> Result<(TypedMapView<K, V>, TypedMapView<K, V>), DownlinkError> {
        self.take_internal(n).await.map(|(before, after)| {
            (TypedMapView::new(before), TypedMapView::new(after))
        })
    }

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
        let before = rx1.await.map_err(|_| DownlinkError::DroppedChannel)?;
        let after = rx2.await.map_err(|_| DownlinkError::DroppedChannel)?;
        Ok((before, after))
    }

    pub async fn skip(&mut self, n: usize) -> Result<(), DownlinkError> {
        self.skip_internal(n).await.map(|_| ())
    }

    pub async fn skip_and_get(&mut self, n: usize) -> Result<(TypedMapView<K, V>, TypedMapView<K, V>), DownlinkError> {
        self.skip_internal(n).await.map(|(before, after)| {
            (TypedMapView::new(before), TypedMapView::new(after))
        })
    }

    pub async fn skip_and_forget(&mut self, n: usize) -> Result<(), DownlinkError> {
        self.sender.send_item(MapAction::skip(n)).await
    }

    pub async fn view(&mut self) -> Result<TypedMapView<K, V>, DownlinkError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::new(tx);
        self.sender
            .send_item(MapAction::get_map(req))
            .await?;
        rx.await.map_err(|_| DownlinkError::DroppedChannel).map(TypedMapView::new)
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

fn wrap_update_fn<T, F>(update_fn: F) -> impl FnOnce(&Value) -> Value
where
    T: Form,
    F: FnOnce(T) -> T,
{
    move |value: &Value| {
        match Form::try_from_value(value) {
            Ok(t) => update_fn(t).into_value(),
            Err(_) => Value::Extant, //TODO Make the update function fallible so this can be avoided.
        }
    }
}

fn wrap_option_update_fn<T, F>(update_fn: F) -> impl FnOnce(&Option<&Value>) -> Option<Value>
    where
        T: Form,
        F: FnOnce(Option<T>) -> Option<T>,
{
    move |maybe_value| {
        match maybe_value.as_ref() {
            Some(value) => {
                match <T as Form>::try_from_value(value) {
                    Ok(t) => update_fn(Some(t)),
                    Err(_) => None, //TODO Make the update function fallible so this can be avoided.
                }
            }
            _ => update_fn(None)
        }.map(Form::into_value)
    }
}

async fn await_value<T: ValidatedForm>(
    rx: oneshot::Receiver<SharedValue>,
) -> Result<T, DownlinkError> {
    let value = rx.await.map_err(|_| DownlinkError::DroppedChannel)?;
    Form::try_from_value(value.as_ref()).map_err(|_| {
        let schema = <T as ValidatedForm>::schema();
        DownlinkError::SchemaViolation((*value).clone(), schema)
    })
}

async fn await_optional<T: ValidatedForm>(
    rx: oneshot::Receiver<Option<SharedValue>>,
) -> Result<Option<T>, DownlinkError> {
    let maybe_value = rx.await.map_err(|_| DownlinkError::DroppedChannel)?;
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
