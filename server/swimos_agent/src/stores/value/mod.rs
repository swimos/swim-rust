// Copyright 2015-2024 Swim Inc.
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

use std::{
    any::type_name,
    borrow::Borrow,
    cell::{Cell, RefCell},
    fmt::Formatter,
    marker::PhantomData,
};

use bytes::BytesMut;
use static_assertions::assert_impl_all;
use swimos_agent_protocol::{encoding::store::ValueStoreResponseEncoder, StoreResponse};
use swimos_form::write::StructuralWritable;
use tokio_util::codec::Encoder;

use crate::{
    agent_model::{AgentDescription, WriteResult},
    event_handler::{ActionContext, EventHandlerError, HandlerAction, Modification, StepResult},
    item::{AgentItem, MutableValueLikeItem, ValueItem, ValueLikeItem},
    meta::AgentMetadata,
};

use super::StoreItem;

#[cfg(test)]
mod tests;

#[derive(Debug)]
struct Inner<T> {
    content: T,
    previous: Option<T>,
}

/// Adding a [`ValueStore`] to an agent provides additional state that is not exposed as a lane.
/// If persistence is enabled (and the store is not marked as transient) the state of the store
/// will be persisted in the same was as the state of a lane.
#[derive(Debug)]
pub struct ValueStore<T> {
    id: u64,
    inner: RefCell<Inner<T>>,
    dirty: Cell<bool>,
}

assert_impl_all!(ValueStore<()>: Send);

impl<T> ValueStore<T> {
    /// # Arguments
    /// * `id` - The ID of the store. This should be unique in an agent.
    /// * `init` - The initial value of the store.
    pub fn new(id: u64, init: T) -> Self {
        ValueStore {
            id,
            inner: RefCell::new(Inner {
                content: init,
                previous: None,
            }),
            dirty: Cell::new(false),
        }
    }

    /// Read the state of the store.
    pub fn read<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let ValueStore { inner, .. } = self;
        let guard = inner.borrow();
        f(&guard.content)
    }

    /// Read the state of the store, consuming the previous value (used when triggering the `on_set` event
    /// handler for the store).
    pub(crate) fn read_with_prev<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<T>, &T) -> R,
    {
        let ValueStore { inner, .. } = self;
        let mut guard = inner.borrow_mut();
        let Inner { content, previous } = &mut *guard;
        let prev = previous.take();
        f(prev, content)
    }

    /// Update the state of the store.
    pub fn set(&self, value: T) {
        let ValueStore { inner, dirty, .. } = self;
        let mut guard = inner.borrow_mut();
        let Inner { content, previous } = &mut *guard;
        let prev = std::mem::replace(content, value);
        *previous = Some(prev);
        dirty.replace(true);
    }

    pub(crate) fn init(&self, value: T) {
        let ValueStore { inner, .. } = self;
        let mut guard = inner.borrow_mut();
        let Inner { content, .. } = &mut *guard;
        *content = value;
    }

    pub(crate) fn has_data_to_write(&self) -> bool {
        self.dirty.get()
    }

    pub(crate) fn consume<F>(&self, f: F) -> bool
    where
        F: FnOnce(&T),
    {
        if self.dirty.replace(false) {
            self.read(f);
            true
        } else {
            false
        }
    }

    pub(crate) fn replace<F>(&self, f: F)
    where
        F: FnOnce(&T) -> T,
    {
        let ValueStore { inner, dirty, .. } = self;
        let mut guard = inner.borrow_mut();
        let Inner { content, previous } = &mut *guard;
        let new_value = f(content);
        let prev = std::mem::replace(content, new_value);
        *previous = Some(prev);
        dirty.replace(true);
    }

    pub(crate) fn with<F, B, U>(&self, f: F) -> U
    where
        B: ?Sized,
        T: Borrow<B>,
        F: FnOnce(&B) -> U,
    {
        let ValueStore { inner, .. } = self;
        let guard = inner.borrow();
        let Inner { content, .. } = &*guard;
        f(content.borrow())
    }
}

impl<T> AgentItem for ValueStore<T> {
    fn id(&self) -> u64 {
        self.id
    }
}

impl<T> ValueItem<T> for ValueStore<T> {
    fn read_with_prev<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<T>, &T) -> R,
    {
        let ValueStore { inner, .. } = self;
        let mut guard = inner.borrow_mut();
        let Inner { content, previous } = &mut *guard;
        let prev = previous.take();
        f(prev, content)
    }

    fn init(&self, value: T) {
        let ValueStore { inner, .. } = self;
        let mut guard = inner.borrow_mut();
        let Inner { content, .. } = &mut *guard;
        *content = value;
    }
}

const INFALLIBLE_SER: &str = "Serializing to recon should be infallible.";

impl<T: StructuralWritable> StoreItem for ValueStore<T> {
    fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult {
        let ValueStore { inner, dirty, .. } = self;

        let mut encoder = ValueStoreResponseEncoder::default();
        if dirty.get() {
            let guard = inner.borrow();
            let Inner { content, .. } = &*guard;
            let response = StoreResponse::new(content);
            encoder.encode(response, buffer).expect(INFALLIBLE_SER);
            dirty.set(false);
            WriteResult::Done
        } else {
            WriteResult::NoData
        }
    }
}

/// An [event handler](crate::event_handler::EventHandler) that will get the value of a value store.
pub struct ValueStoreGet<C, T> {
    projection: for<'a> fn(&'a C) -> &'a ValueStore<T>,
    done: bool,
}

impl<C, T> ValueStoreGet<C, T> {
    /// # Arguments
    /// * `projection` - Projection from the agent context to the store.
    pub fn new(projection: for<'a> fn(&'a C) -> &'a ValueStore<T>) -> Self {
        ValueStoreGet {
            projection,
            done: false,
        }
    }
}

///  An [event handler](crate::event_handler::EventHandler) that will set the value of a value store.
pub struct ValueStoreSet<C, T> {
    projection: for<'a> fn(&'a C) -> &'a ValueStore<T>,
    value: Option<T>,
}

impl<C, T> ValueStoreSet<C, T> {
    /// # Arguments
    /// * `projection` - Projection from the agent context to the store.
    /// * `value` - The new value for the store.
    pub fn new(projection: for<'a> fn(&'a C) -> &'a ValueStore<T>, value: T) -> Self {
        ValueStoreSet {
            projection,
            value: Some(value),
        }
    }
}

impl<C: AgentDescription, T: Clone> HandlerAction<C> for ValueStoreGet<C, T> {
    type Completion = T;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let ValueStoreGet { projection, done } = self;
        if *done {
            StepResult::after_done()
        } else {
            *done = true;
            let store = projection(context);
            let value = store.read(T::clone);
            StepResult::done(value)
        }
    }

    fn describe(&self, context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let lane = (self.projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("ValueStoreGet")
            .field("id", &lane.id())
            .field("store_name", &name.as_ref().map(|s| s.as_ref()))
            .finish()
    }
}

impl<C: AgentDescription, T> HandlerAction<C> for ValueStoreSet<C, T> {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let ValueStoreSet { projection, value } = self;
        if let Some(value) = value.take() {
            let store = projection(context);
            store.set(value);
            StepResult::Complete {
                modified_item: Some(Modification::of(store.id())),
                result: (),
            }
        } else {
            StepResult::Fail(EventHandlerError::SteppedAfterComplete)
        }
    }

    fn describe(
        &self,
        context: &C,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        let ValueStoreSet { projection, value } = self;
        let lane = (projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("ValueStoreSet")
            .field("id", &lane.id())
            .field("store_name", &name.as_ref().map(|s| s.as_ref()))
            .field("consumed", &value.is_none())
            .finish()
    }
}

/// An [`HandlerAction`] that will produce a value from a reference to the contents of the store.
pub struct ValueStoreWithValue<C, T, F, B: ?Sized> {
    projection: for<'a> fn(&'a C) -> &'a ValueStore<T>,
    f: Option<F>,
    _type: PhantomData<fn(&B)>,
}

impl<C, T, F, B: ?Sized> ValueStoreWithValue<C, T, F, B> {
    /// #Arguments
    /// * `projection` - Projection from the agent context to the store.
    /// * `f` - Closure to apply to the value of the store.
    pub fn new(projection: for<'a> fn(&'a C) -> &'a ValueStore<T>, f: F) -> Self {
        ValueStoreWithValue {
            projection,
            f: Some(f),
            _type: PhantomData,
        }
    }
}

impl<C: AgentDescription, T, F, B, U> HandlerAction<C> for ValueStoreWithValue<C, T, F, B>
where
    T: Borrow<B>,
    B: ?Sized,
    F: FnOnce(&B) -> U,
{
    type Completion = U;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        if let Some(f) = self.f.take() {
            let store = (self.projection)(context);
            StepResult::done(store.with(f))
        } else {
            StepResult::after_done()
        }
    }

    fn describe(
        &self,
        context: &C,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        let lane = (self.projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("ValueStoreWithValue")
            .field("id", &lane.id())
            .field("store_name", &name.as_ref().map(|s| s.as_ref()))
            .field("result_type", &type_name::<U>())
            .finish()
    }
}

impl<T> ValueLikeItem<T> for ValueStore<T>
where
    T: Clone + Send + 'static,
{
    type GetHandler<C> = ValueStoreGet<C, T>
    where
        C: AgentDescription + 'static;

    type WithValueHandler<'a, C, F, B, U> = ValueStoreWithValue<C, T, F, B>
    where
        Self: 'static,
        C: AgentDescription + 'a,
        T: Borrow<B>,
        B: ?Sized + 'static,
        F: FnOnce(&B) -> U + Send + 'a;

    fn get_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
    ) -> Self::GetHandler<C> {
        ValueStoreGet::new(projection)
    }

    fn with_value_handler<'a, Item, C, F, B, U>(
        projection: fn(&C) -> &Self,
        f: F,
    ) -> Self::WithValueHandler<'a, C, F, B, U>
    where
        C: AgentDescription + 'a,
        T: Borrow<B>,
        B: ?Sized + 'static,
        F: FnOnce(&B) -> U + Send + 'a,
    {
        ValueStoreWithValue::new(projection, f)
    }
}

impl<T> MutableValueLikeItem<T> for ValueStore<T>
where
    T: Send + 'static,
{
    type SetHandler<C> = ValueStoreSet<C, T>
    where
        C: AgentDescription + 'static;

    fn set_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
        value: T,
    ) -> Self::SetHandler<C> {
        ValueStoreSet::new(projection, value)
    }
}
