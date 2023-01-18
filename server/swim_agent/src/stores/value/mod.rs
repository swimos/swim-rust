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

use std::cell::{Cell, RefCell};

use bytes::BytesMut;
use static_assertions::assert_impl_all;
use swim_api::protocol::agent::{StoreResponse, ValueStoreResponseEncoder};
use swim_form::structural::write::StructuralWritable;
use tokio_util::codec::Encoder;

use crate::{
    agent_model::WriteResult,
    event_handler::{ActionContext, EventHandlerError, HandlerAction, Modification, StepResult},
    meta::AgentMetadata,
    AgentItem,
};

use super::Store;

#[cfg(test)]
mod tests;

/// Adding a [`ValueStore`] to an agent provides additional state that is not exposed as a lane.
/// If persistence is enabled (and the store is not marked as transient) the state of the store
/// will be persisted in the same was as the state of a lane.
#[derive(Debug)]
pub struct ValueStore<T> {
    id: u64,
    content: RefCell<T>,
    previous: RefCell<Option<T>>,
    dirty: Cell<bool>,
}

assert_impl_all!(ValueStore<()>: Send);

impl<T> ValueStore<T> {
    /// #Arguments
    /// * `id` - The ID of the store. This should be unique in an agent.
    /// * `init` - The initial value of the store.
    pub fn new(id: u64, init: T) -> Self {
        ValueStore {
            id,
            content: RefCell::new(init),
            previous: Default::default(),
            dirty: Cell::new(false),
        }
    }

    /// Read the state of the store.
    pub fn read<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let ValueStore { content, .. } = self;
        let value = content.borrow();
        f(&*value)
    }

    /// Read the state of the store, consuming the previous value (used when triggering the `on_set` event
    /// handler for the store).
    pub(crate) fn read_with_prev<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<T>, &T) -> R,
    {
        let ValueStore {
            content, previous, ..
        } = self;
        let prev = previous.borrow_mut().take();
        let value = content.borrow();
        f(prev, &*value)
    }

    /// Update the state of the store.
    pub fn set(&self, value: T) {
        let ValueStore {
            content,
            previous,
            dirty,
            ..
        } = self;
        let prev = content.replace(value);
        previous.replace(Some(prev));
        dirty.replace(true);
    }

    pub(crate) fn init(&self, value: T) {
        let ValueStore { content, .. } = self;
        content.replace(value);
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
}

impl<T> AgentItem for ValueStore<T> {
    fn id(&self) -> u64 {
        self.id
    }
}

const INFALLIBLE_SER: &str = "Serializing to recon should be infallible.";

impl<T: StructuralWritable> Store for ValueStore<T> {
    fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult {
        let ValueStore { content, dirty, .. } = self;
        let mut encoder = ValueStoreResponseEncoder::default();
        if dirty.get() {
            let value_guard = content.borrow();
            let response = StoreResponse::new(&*value_guard);
            encoder.encode(response, buffer).expect(INFALLIBLE_SER);
            dirty.set(false);
            WriteResult::Done
        } else {
            WriteResult::NoData
        }
    }
}

/// An [`EventHandler`] that will get the value of a value store.
pub struct ValueStoreGet<C, T> {
    projection: for<'a> fn(&'a C) -> &'a ValueStore<T>,
    done: bool,
}

impl<C, T> ValueStoreGet<C, T> {
    /// #Arguments
    /// * `projection` - Projection from the agent context to the store.
    pub fn new(projection: for<'a> fn(&'a C) -> &'a ValueStore<T>) -> Self {
        ValueStoreGet {
            projection,
            done: false,
        }
    }
}

/// An [`EventHandler`] that will set the value of a value store.
pub struct ValueStoreSet<C, T> {
    projection: for<'a> fn(&'a C) -> &'a ValueStore<T>,
    value: Option<T>,
}

impl<C, T> ValueStoreSet<C, T> {
    /// #Arguments
    /// * `projection` - Projection from the agent context to the store.
    /// * `value` - The new value for the store.
    pub fn new(projection: for<'a> fn(&'a C) -> &'a ValueStore<T>, value: T) -> Self {
        ValueStoreSet {
            projection,
            value: Some(value),
        }
    }
}

impl<C, T: Clone> HandlerAction<C> for ValueStoreGet<C, T> {
    type Completion = T;

    fn step(
        &mut self,
        _action_context: ActionContext<C>,
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
}

impl<C, T> HandlerAction<C> for ValueStoreSet<C, T> {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let ValueStoreSet { projection, value } = self;
        if let Some(value) = value.take() {
            let store = projection(context);
            store.set(value);
            StepResult::Complete {
                modified_lane: Some(Modification::of(store.id())),
                result: (),
            }
        } else {
            StepResult::Fail(EventHandlerError::SteppedAfterComplete)
        }
    }
}
