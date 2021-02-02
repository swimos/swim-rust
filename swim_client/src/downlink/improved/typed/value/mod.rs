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

use crate::downlink::model::value::{SharedValue, Action};
use std::marker::PhantomData;
use swim_common::form::{Form, ValidatedForm};
use utilities::sync::{promise, topic};
use crate::downlink::{DownlinkError, Event, DownlinkRequest, Downlink};
use swim_common::model::Value;
use tokio::sync::{oneshot, mpsc};
use futures::Stream;
use futures::stream::unfold;
use swim_common::model::schema::StandardSchema;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter, Display};
use std::any::type_name;
use crate::downlink::improved::typed::{UntypedValueDownlink, ViewMode};
use std::sync::Arc;
use std::error::Error;

pub struct TypedValueDownlink<T> {
    inner: Arc<UntypedValueDownlink>,
    _type: PhantomData<fn(T) -> T>,
}

impl<T> TypedValueDownlink<T> {

    pub(crate) fn new(inner: Arc<UntypedValueDownlink>) -> Self {
        TypedValueDownlink {
            inner,
            _type: PhantomData,
        }
    }

}

impl<T> Debug for TypedValueDownlink<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedValueDownlink")
            .field("inner", &self.inner)
            .field("type", &type_name::<T>())
            .finish()
    }
}

impl<T> Clone for TypedValueDownlink<T> {
    fn clone(&self) -> Self {
        TypedValueDownlink {
            inner: self.inner.clone(),
            _type: PhantomData
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

impl Display for ValueViewError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "A {} value downlink with schema {} was requested but the original value downlink is running with schema {}.", self.mode, self.requested, self.existing)
    }
}

impl Error for ValueViewError {}

impl<T> Downlink for TypedValueDownlink<T> {
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

impl<T: Form> TypedValueDownlink<T> {

    pub fn subscriber(&self) -> ValueDownlinkSubscriber<T> {
        ValueDownlinkSubscriber::new(self.inner.subscriber())
    }

    pub fn subscribe(&self) -> Option<ValueDownlinkReceiver<T>> {
        self.inner.subscribe().map(ValueDownlinkReceiver::new)
    }

    pub fn sender(&self) -> ValueDownlinkSender<T> {
        ValueDownlinkSender::new(self.inner.sender().clone())
    }
}

pub struct ValueDownlinkSender<T> {
    inner: mpsc::Sender<Action>,
    _type: PhantomData<fn(T)>,
}

impl<T> Debug for ValueDownlinkSender<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueDownlinkSender")
            .field("inner", &self.inner)
            .field("type", &type_name::<T>())
            .finish()
    }
}

impl<T> Clone for ValueDownlinkSender<T> {
    fn clone(&self) -> Self {
        ValueDownlinkSender::new(self.inner.clone())
    }
}

impl<T> ValueDownlinkSender<T> {

    fn new(inner: mpsc::Sender<Action>) -> Self {
        ValueDownlinkSender {
            inner,
            _type: PhantomData,
        }
    }

}

impl<T: ValidatedForm + 'static> ValueDownlinkSender<T> {
    pub async fn get(&self) -> Result<T, DownlinkError> {
        ValueActions::new(&self.inner).get().await
    }

    pub async fn set(&self, value: T) -> Result<(), DownlinkError> {
        ValueActions::new(&self.inner).set(value).await
    }

    pub async fn set_and_forget(&self, value: T) -> Result<(), DownlinkError> {
        ValueActions::new(&self.inner).set_and_forget(value).await
    }

    pub async fn update<F>(&self, update_fn: F) -> Result<T, DownlinkError>
        where
            F: FnOnce(T) -> T + Send + 'static
    {
        ValueActions::new(&self.inner).update(update_fn).await
    }

    pub async fn update_and_forget<F>(&self, update_fn: F) -> Result<(), DownlinkError>
        where
            F: FnOnce(T) -> T + Send + 'static
    {
        ValueActions::new(&self.inner).update_and_forget(update_fn).await
    }
}


impl<T: ValidatedForm + 'static> TypedValueDownlink<T> {

    pub async fn get(&self) -> Result<T, DownlinkError> {
        ValueActions::new(self.inner.sender()).get().await
    }

    pub async fn set(&self, value: T) -> Result<(), DownlinkError> {
        ValueActions::new(self.inner.sender()).set(value).await
    }

    pub async fn set_and_forget(&self, value: T) -> Result<(), DownlinkError> {
        ValueActions::new(self.inner.sender()).set_and_forget(value).await
    }

    pub async fn update<F>(&self, update_fn: F) -> Result<T, DownlinkError>
        where
            F: FnOnce(T) -> T + Send + 'static
    {
        ValueActions::new(self.inner.sender()).update(update_fn).await
    }

    pub async fn update_and_forget<F>(&self, update_fn: F) -> Result<(), DownlinkError>
        where
            F: FnOnce(T) -> T + Send + 'static
    {
        ValueActions::new(self.inner.sender()).update_and_forget(update_fn).await
    }

}


/// Wraps a sender up updates to a value downlink providing typed, asynchronous operations
/// that can be performed on it.
struct ValueActions<'a, T> {
    sender: &'a mpsc::Sender<Action>,
    _entry_type: PhantomData<fn(T) -> T>,
}

impl<'a, T> ValueActions<'a, T> {

    fn new(sender: &'a mpsc::Sender<Action>) -> Self {
        ValueActions {
            sender,
            _entry_type: PhantomData
        }
    }

}

impl<'a, T> ValueActions<'a, T>
    where
        T: ValidatedForm + 'static,
{


    async fn get(&self) -> Result<T, DownlinkError> {
        let (tx, rx) = oneshot::channel();
        self.sender.send(Action::Get(DownlinkRequest::new(tx))).await?;
        super::await_value(rx).await
    }

    async fn set(&self, value: T) -> Result<(), DownlinkError> {
        let (tx, rx) = oneshot::channel();
        self.sender.send(Action::Set(value.into_value(), Some(DownlinkRequest::new(tx)))).await?;
        rx.await.map_err(|_| DownlinkError::DroppedChannel)?
    }

    async fn set_and_forget(&self, value: T) -> Result<(), DownlinkError> {
        Ok(self.sender.send(Action::Set(value.into_value(), None)).await?)
    }

    async fn update<F>(&self, update_fn: F) -> Result<T, DownlinkError>
        where
            F: FnOnce(T) -> T + Send + 'static
    {
        let wrapped = super::wrap_update_fn(update_fn);
        let (tx, rx) = oneshot::channel();
        let req = DownlinkRequest::new(tx);
        self.sender.send(Action::try_update_and_await(wrapped, req))
            .await?;
        super::await_fallible(rx).await
    }

    async fn update_and_forget<F>(&self, update_fn: F) -> Result<(), DownlinkError>
        where
            F: FnOnce(T) -> T + Send + 'static
    {
        let wrapped = super::wrap_update_fn::<T, F>(update_fn);
        Ok(self.sender
            .send(Action::try_update(wrapped))
            .await?)
    }

}


pub struct ValueDownlinkReceiver<T> {
    inner: topic::Receiver<Event<SharedValue>>,
    _type: PhantomData<fn() -> T>,
}

impl<T> Debug for ValueDownlinkReceiver<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueDownlinkReceiver")
            .field("inner", &self.inner)
            .field("type", &type_name::<T>())
            .finish()
    }
}

impl<T> Clone for ValueDownlinkReceiver<T> {
    fn clone(&self) -> Self {
        ValueDownlinkReceiver::new(self.inner.clone())
    }
}

pub struct ValueDownlinkSubscriber<T> {
    inner: topic::Subscriber<Event<SharedValue>>,
    _type: PhantomData<fn() -> T>,
}

impl<T> Debug for ValueDownlinkSubscriber<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueDownlinkSubscriber")
            .field("inner", &self.inner)
            .field("type", &type_name::<T>())
            .finish()
    }
}

impl<T> Clone for ValueDownlinkSubscriber<T> {
    fn clone(&self) -> Self {
        ValueDownlinkSubscriber::new(self.inner.clone())
    }
}

impl<T> ValueDownlinkReceiver<T> {

    pub(crate) fn new(inner: topic::Receiver<Event<SharedValue>>) -> Self {
        ValueDownlinkReceiver {
            inner,
            _type: PhantomData,
        }
    }

}

impl<T> ValueDownlinkSubscriber<T> {

    fn new(inner: topic::Subscriber<Event<SharedValue>>) -> Self {
        ValueDownlinkSubscriber {
            inner,
            _type: PhantomData,
        }
    }

}

impl<T: ValidatedForm> ValueDownlinkSubscriber<T> {

    /// Create a read-only view for a value downlink that converts all received values to a new type.
    /// The type of the view must have an equal or greater schema than the original downlink.
    pub async fn covariant_cast<U: ValidatedForm>(
        &self,
    ) -> Result<ValueDownlinkSubscriber<U>, ValueViewError>
    {
        let schema_cmp = U::schema().partial_cmp(&T::schema());

        if schema_cmp.is_some() && schema_cmp != Some(Ordering::Less) {
            Ok(ValueDownlinkSubscriber::new(self.inner.clone()))
        } else {
            Err(ValueViewError {
                existing: T::schema(),
                requested: U::schema(),
                mode: ViewMode::ReadOnly,
            })
        }
    }

}

impl<T: Form + 'static> ValueDownlinkReceiver<T> {

    pub async fn recv(&mut self) -> Option<Event<T>> {
        let value = self.inner.recv().await;
        value.map(|g| {
            transform_event(&*g, |v| Form::try_from_value(v).expect("Inconsistent Form"))
        })
    }

    pub fn into_stream(self) -> impl Stream<Item = Event<T>> + Send + 'static {
        unfold(self, |mut rx| async move {
            rx.recv().await.map(|v| (v, rx))
        })
    }

}

impl<T: Form> ValueDownlinkSubscriber<T> {

    pub async fn subscribe(&mut self) -> Result<ValueDownlinkReceiver<T>, topic::SubscribeError> {
        self.inner.subscribe().map(ValueDownlinkReceiver::new)
    }

}

fn transform_event<T>(event: &Event<SharedValue>, f: impl FnOnce(&Value) -> T) -> Event<T> {
    match event {
        Event::Local(v) => Event::Local(f(&**v)),
        Event::Remote(v) => Event::Remote(f(&**v)),
    }
}
