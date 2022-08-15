// Copyright 2015-2021 Swim Inc.
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

use crate::downlink::typed::UntypedEventDownlink;
use crate::downlink::{Downlink, DownlinkError, Event};
use futures::stream::unfold;
use futures::Stream;
use std::any::type_name;
use std::cmp::Ordering;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;
use swim_form::Form;
use swim_model::Value;
use swim_schema::schema::StandardSchema;
use swim_schema::ValueSchema;
use swim_utilities::sync::topic;
use swim_utilities::trigger::promise;

#[cfg(test)]
mod tests;

/// A downlink to a remote lane producing events that are compatible with the [`ValueSchema`]
/// implementation for `T`.
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
    /// Create a subscriber that can be used to produce receivers that observe events from the
    /// downlink.
    pub fn subscriber(&self) -> EventDownlinkSubscriber<T> {
        EventDownlinkSubscriber::new(self.inner.subscriber())
    }

    /// Create a new receiver, attached to the downlink, to observe the events.
    pub fn subscribe(&self) -> Option<EventDownlinkReceiver<T>> {
        self.inner.subscribe().map(EventDownlinkReceiver::new)
    }
}

/// A receiver that observes the events from the downlink. Note that a receiver must consume
/// the events or the downlink will block.
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

/// Value downlink handle that can produce receivers that will observe the events from the
/// downlink.
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
}

impl Display for EventViewError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "A Read Only view of an event downlink with schema {} was requested but the original event downlink is running with schema {}.", self.requested, self.existing)
    }
}

impl Error for EventViewError {}

impl<T: ValueSchema> EventDownlinkSubscriber<T> {
    /// Create a read-only view for a value downlink that converts all received values to a new type.
    /// The type of the view must have an equal or greater schema than the original downlink.
    pub fn covariant_cast<U: ValueSchema>(
        self,
    ) -> Result<EventDownlinkSubscriber<U>, EventViewError> {
        let schema_cmp = U::schema().partial_cmp(&T::schema());

        if schema_cmp.is_some() && schema_cmp != Some(Ordering::Less) {
            Ok(EventDownlinkSubscriber::new(self.inner))
        } else {
            Err(EventViewError {
                existing: T::schema(),
                requested: U::schema(),
            })
        }
    }
}

impl<T: Form + 'static> EventDownlinkReceiver<T> {
    /// Observe the next event from the downlink.
    pub async fn recv(&mut self) -> Option<T> {
        let value = self.inner.recv().await;
        value.map(|g| Form::try_from_value(g.get_inner_ref()).expect("Inconsistent Form"))
    }

    /// Convert this receiver in a [`Stream`] of events.
    pub fn into_stream(self) -> impl Stream<Item = T> + Send + 'static {
        unfold(
            self,
            |mut rx| async move { rx.recv().await.map(|v| (v, rx)) },
        )
    }
}

impl<T: Form> EventDownlinkSubscriber<T> {
    pub fn subscribe(&self) -> Result<EventDownlinkReceiver<T>, topic::SubscribeError> {
        self.inner.subscribe().map(EventDownlinkReceiver::new)
    }
}
