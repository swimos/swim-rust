// Copyright 2015-2021 SWIM.AI inc.
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

use crate::downlink::model::value::{Action, SharedValue};
use crate::downlink::typed::{UntypedValueDownlink, ViewMode};
use crate::downlink::{Downlink, DownlinkError, DownlinkRequest, Event};
use futures::stream::unfold;
use futures::Stream;
use std::any::type_name;
use std::cmp::Ordering;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;
use swim_form::Form;
use swim_schema::ValueSchema;
use swim_schema::schema::StandardSchema;
use swim_model::Value;
use swim_utilities::sync::topic;
use swim_utilities::trigger::promise;
use tokio::sync::{mpsc, oneshot};

/// A downlink to a remote value lane containing values that are compatible with the
/// [`ValueSchema`] implementation for `T`.
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
            _type: PhantomData,
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
        write!(f, "A {} view of a value downlink with schema {} was requested but the original value downlink is running with schema {}.", self.mode, self.requested, self.existing)
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
    /// Create a subscriber that can be used to produce receivers that observe changes to the state
    /// of the downlink.
    pub fn subscriber(&self) -> ValueDownlinkSubscriber<T> {
        ValueDownlinkSubscriber::new(self.inner.subscriber())
    }

    /// Create a new receiver, attached to the downlink, to observe changes to the state.
    pub fn subscribe(&self) -> Option<ValueDownlinkReceiver<T>> {
        self.inner.subscribe().map(ValueDownlinkReceiver::new)
    }

    /// Create a sender that can be used to interact with the map in the downlink.
    pub fn sender(&self) -> ValueDownlinkSender<T> {
        ValueDownlinkSender::new(self.inner.sender().clone())
    }
}

/// A handle to interact with the value of a value downlink.
pub struct ValueDownlinkSender<T> {
    inner: mpsc::Sender<Action>,
    _type: PhantomData<fn(T)>,
}

pub struct ValueDownlinkContraView<T> {
    inner: mpsc::Sender<Action>,
    _type: PhantomData<fn(T)>,
}

pub struct ValueDownlinkView<T> {
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

impl<T> Debug for ValueDownlinkContraView<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueDownlinkContraView")
            .field("inner", &self.inner)
            .field("type", &type_name::<T>())
            .finish()
    }
}

impl<T> Debug for ValueDownlinkView<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueDownlinkView")
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

impl<T> Clone for ValueDownlinkContraView<T> {
    fn clone(&self) -> Self {
        ValueDownlinkContraView::new(self.inner.clone())
    }
}

impl<T> Clone for ValueDownlinkView<T> {
    fn clone(&self) -> Self {
        ValueDownlinkView::new(self.inner.clone())
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

impl<T> ValueDownlinkContraView<T> {
    fn new(inner: mpsc::Sender<Action>) -> Self {
        ValueDownlinkContraView {
            inner,
            _type: PhantomData,
        }
    }
}

impl<T> ValueDownlinkView<T> {
    fn new(inner: mpsc::Sender<Action>) -> Self {
        ValueDownlinkView {
            inner,
            _type: PhantomData,
        }
    }
}

impl<T: ValueSchema> ValueDownlinkSender<T> {
    /// Create a sender for a more refined type (the [`ValueSchema`] implementation for `U`
    /// will always produce a [`Value`] that is acceptable to the [`ValueSchema`] implementation
    /// for `T`) to the downlink.
    pub fn contravariant_view<U>(&self) -> Result<ValueDownlinkContraView<U>, ValueViewError>
    where
        U: ValueSchema,
    {
        let schema_cmp = U::schema().partial_cmp(&T::schema());

        if schema_cmp.is_some() && schema_cmp != Some(Ordering::Greater) {
            Ok(ValueDownlinkContraView::new(self.inner.clone()))
        } else {
            Err(ValueViewError {
                existing: T::schema(),
                requested: U::schema(),
                mode: ViewMode::WriteOnly,
            })
        }
    }

    /// Create a read-only view for a value downlink sender that converts all  values to a new type.
    /// The type of the view must have an equal or greater schema than the original downlink.
    pub fn covariant_view<U>(&self) -> Result<ValueDownlinkView<U>, ValueViewError>
    where
        U: ValueSchema,
    {
        let schema_cmp = U::schema().partial_cmp(&T::schema());

        if schema_cmp.is_some() && schema_cmp != Some(Ordering::Less) {
            Ok(ValueDownlinkView::new(self.inner.clone()))
        } else {
            Err(ValueViewError {
                existing: T::schema(),
                requested: U::schema(),
                mode: ViewMode::ReadOnly,
            })
        }
    }
}

impl<T: Form + ValueSchema + 'static> ValueDownlinkSender<T> {
    /// Get the current value of the downlink.
    pub async fn get(&self) -> Result<T, DownlinkError> {
        ValueActions::new(&self.inner).get().await
    }

    /// Set the value of the downlink, waiting until the set has completed.
    pub async fn set(&self, value: T) -> Result<(), DownlinkError> {
        ValueActions::new(&self.inner).set(value).await
    }

    /// Set the value of the downlink, returning immediately.
    pub async fn set_and_forget(&self, value: T) -> Result<(), DownlinkError> {
        ValueActions::new(&self.inner).set_and_forget(value).await
    }

    /// Update the value of the downlink, waiting until the changes has completed.
    pub async fn update<F>(&self, update_fn: F) -> Result<T, DownlinkError>
    where
        F: FnOnce(T) -> T + Send + 'static,
    {
        ValueActions::new(&self.inner).update(update_fn).await
    }

    /// Update the value of the downlink, returning immediately.
    pub async fn update_and_forget<F>(&self, update_fn: F) -> Result<(), DownlinkError>
    where
        F: FnOnce(T) -> T + Send + 'static,
    {
        ValueActions::new(&self.inner)
            .update_and_forget(update_fn)
            .await
    }
}

impl<T: Form + ValueSchema + 'static> ValueDownlinkContraView<T> {
    /// Set the value of the downlink, waiting until the set has completed.
    pub async fn set(&self, value: T) -> Result<(), DownlinkError> {
        ValueActions::new(&self.inner).set(value).await
    }

    /// Set the value of the downlink, returning immediately.
    pub async fn set_and_forget(&self, value: T) -> Result<(), DownlinkError> {
        ValueActions::new(&self.inner).set_and_forget(value).await
    }
}

impl<T: Form + ValueSchema + 'static> ValueDownlinkView<T> {
    /// Get the current value of the downlink.
    pub async fn get(&self) -> Result<T, DownlinkError> {
        ValueActions::new(&self.inner).get().await
    }
}

impl<T: Form + ValueSchema + 'static> TypedValueDownlink<T> {
    /// Get the current value of the downlink.
    pub async fn get(&self) -> Result<T, DownlinkError> {
        ValueActions::new(self.inner.sender()).get().await
    }

    /// Set the value of the downlink, waiting until the set has completed.
    pub async fn set(&self, value: T) -> Result<(), DownlinkError> {
        ValueActions::new(self.inner.sender()).set(value).await
    }

    /// Set the value of the downlink, returning immediately.
    pub async fn set_and_forget(&self, value: T) -> Result<(), DownlinkError> {
        ValueActions::new(self.inner.sender())
            .set_and_forget(value)
            .await
    }

    /// Update the value of the downlink, waiting until the changes has completed.
    pub async fn update<F>(&self, update_fn: F) -> Result<T, DownlinkError>
    where
        F: FnOnce(T) -> T + Send + 'static,
    {
        ValueActions::new(self.inner.sender())
            .update(update_fn)
            .await
    }

    /// Update the value of the downlink, returning immediately.
    pub async fn update_and_forget<F>(&self, update_fn: F) -> Result<(), DownlinkError>
    where
        F: FnOnce(T) -> T + Send + 'static,
    {
        ValueActions::new(self.inner.sender())
            .update_and_forget(update_fn)
            .await
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
            _entry_type: PhantomData,
        }
    }
}

impl<'a, T> ValueActions<'a, T>
where
    T: Form + ValueSchema + 'static,
{
    async fn get(&self) -> Result<T, DownlinkError> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Action::Get(DownlinkRequest::new(tx)))
            .await?;
        super::await_value(rx).await
    }

    async fn set(&self, value: T) -> Result<(), DownlinkError> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Action::Set(
                value.into_value(),
                Some(DownlinkRequest::new(tx)),
            ))
            .await?;
        rx.await.map_err(|_| DownlinkError::DroppedChannel)?
    }

    async fn set_and_forget(&self, value: T) -> Result<(), DownlinkError> {
        Ok(self
            .sender
            .send(Action::Set(value.into_value(), None))
            .await?)
    }

    async fn update<F>(&self, update_fn: F) -> Result<T, DownlinkError>
    where
        F: FnOnce(T) -> T + Send + 'static,
    {
        let wrapped = super::wrap_update_fn(update_fn);
        let (tx, rx) = oneshot::channel();
        let req = DownlinkRequest::new(tx);
        self.sender
            .send(Action::try_update_and_await(wrapped, req))
            .await?;
        super::await_fallible(rx).await
    }

    async fn update_and_forget<F>(&self, update_fn: F) -> Result<(), DownlinkError>
    where
        F: FnOnce(T) -> T + Send + 'static,
    {
        let wrapped = super::wrap_update_fn::<T, F>(update_fn);
        Ok(self.sender.send(Action::try_update(wrapped)).await?)
    }
}

/// A receiver that observes the state changes of the downlink. Note that a receiver must consume
/// the state changes or the downlink will block.
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

/// Value downlink handle that can produce receivers that will observe the state changes of the
/// downlink.
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

impl<T: Form + ValueSchema> ValueDownlinkSubscriber<T> {
    /// Create a read-only view for a value downlink subscriberthat converts all received values
    /// to a new type. The type of the view must have an equal or greater schema than the original
    /// downlink.
    pub fn covariant_cast<U>(self) -> Result<ValueDownlinkSubscriber<U>, ValueViewError>
    where
        U: Form + ValueSchema,
    {
        let schema_cmp = U::schema().partial_cmp(&T::schema());

        if schema_cmp.is_some() && schema_cmp != Some(Ordering::Less) {
            Ok(ValueDownlinkSubscriber::new(self.inner))
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
    /// Observe the next state change from the downlink.
    pub async fn recv(&mut self) -> Option<Event<T>> {
        let value = self.inner.recv().await;
        value.map(|g| transform_event(&*g, |v| Form::try_from_value(v).expect("Inconsistent Form")))
    }

    /// Convert this receiver in a [`Stream`] of state changes.
    pub fn into_stream(self) -> impl Stream<Item = Event<T>> + Send + 'static {
        unfold(
            self,
            |mut rx| async move { rx.recv().await.map(|v| (v, rx)) },
        )
    }
}

impl<T: Form> ValueDownlinkSubscriber<T> {
    pub fn subscribe(&self) -> Result<ValueDownlinkReceiver<T>, topic::SubscribeError> {
        self.inner.subscribe().map(ValueDownlinkReceiver::new)
    }
}

fn transform_event<T>(event: &Event<SharedValue>, f: impl FnOnce(&Value) -> T) -> Event<T> {
    match event {
        Event::Local(v) => Event::Local(f(&**v)),
        Event::Remote(v) => Event::Remote(f(&**v)),
    }
}
