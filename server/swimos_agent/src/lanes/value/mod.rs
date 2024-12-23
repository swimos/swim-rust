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

pub mod lifecycle;

#[cfg(test)]
mod tests;

use std::{
    any::type_name,
    borrow::Borrow,
    cell::RefCell,
    collections::VecDeque,
    fmt::{Debug, Formatter},
    marker::PhantomData,
};

use bytes::BytesMut;
use static_assertions::assert_impl_all;
use swimos_agent_protocol::{encoding::lane::ValueLaneResponseEncoder, LaneResponse};
use swimos_form::{read::RecognizerReadable, write::StructuralWritable};
use tokio_util::codec::Encoder;
use uuid::Uuid;

use crate::{
    agent_model::{AgentDescription, WriteResult},
    event_handler::{
        ActionContext, AndThen, Decode, Described, EventHandlerError, HandlerAction,
        HandlerActionExt, HandlerTrans, Modification, StepResult,
    },
    item::{AgentItem, MutableValueLikeItem, ValueItem, ValueLikeItem},
    meta::AgentMetadata,
    stores::value::ValueStore,
    ReconDecoder,
};

use super::{LaneItem, ProjTransform, Selector, SelectorFn};

/// Model of a value lane. This maintains a state and triggers an event each time this state is updated.
/// Updates may come from external commands or from an action performed by an event handler on the agent.
#[derive(Debug)]
pub struct ValueLane<T> {
    store: ValueStore<T>,
    sync_queue: RefCell<VecDeque<Uuid>>,
}

assert_impl_all!(ValueLane<()>: Send);

impl<T> ValueLane<T> {
    /// # Arguments
    /// * `id` - The ID of the lane. This should be unique in an agent.
    /// * `init` - The initial value of the lane.
    pub fn new(id: u64, init: T) -> Self {
        ValueLane {
            store: ValueStore::new(id, init),
            sync_queue: Default::default(),
        }
    }

    /// Read the state of the lane.
    pub fn read<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        self.store.read(f)
    }

    /// Update the state of the lane.
    pub(crate) fn set(&self, value: T) {
        self.store.set(value)
    }

    pub(crate) fn sync(&self, id: Uuid) {
        let ValueLane { sync_queue, .. } = self;
        sync_queue.borrow_mut().push_back(id);
    }

    /// Replace the contents of the lane.
    pub fn replace<F>(&self, f: F)
    where
        F: FnOnce(&T) -> T,
    {
        self.store.replace(f);
    }

    pub(crate) fn with<F, B, U>(&self, f: F) -> U
    where
        B: ?Sized,
        T: Borrow<B>,
        F: FnOnce(&B) -> U,
    {
        self.store.with(f)
    }
}

impl<T> AgentItem for ValueLane<T> {
    fn id(&self) -> u64 {
        self.store.id()
    }
}

const INFALLIBLE_SER: &str = "Serializing to recon should be infallible.";

impl<T: StructuralWritable> LaneItem for ValueLane<T> {
    fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult {
        let ValueLane {
            store, sync_queue, ..
        } = self;
        let mut encoder = ValueLaneResponseEncoder::default();
        let mut sync = sync_queue.borrow_mut();
        if let Some(id) = sync.pop_front() {
            store.read(|value| {
                let value_response = LaneResponse::sync_event(id, value);
                encoder
                    .encode(value_response, buffer)
                    .expect(INFALLIBLE_SER);
            });
            let synced_response = LaneResponse::<&T>::synced(id);
            encoder
                .encode(synced_response, buffer)
                .expect(INFALLIBLE_SER);
            if store.has_data_to_write() || !sync.is_empty() {
                WriteResult::DataStillAvailable
            } else {
                WriteResult::Done
            }
        } else {
            let try_write_event = |value: &T| {
                let response = LaneResponse::event(value);
                encoder.encode(response, buffer).expect(INFALLIBLE_SER);
            };
            if store.consume(try_write_event) {
                WriteResult::Done
            } else {
                WriteResult::NoData
            }
        }
    }
}

impl<T> ValueItem<T> for ValueLane<T> {
    fn read_with_prev<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<T>, &T) -> R,
    {
        self.store.read_with_prev(f)
    }

    fn init(&self, value: T) {
        self.store.init(value)
    }
}

///  An [event handler](crate::event_handler::EventHandler)`] that will get the value of a value lane.
pub struct ValueLaneGet<C, T> {
    projection: for<'a> fn(&'a C) -> &'a ValueLane<T>,
    done: bool,
}

impl<C, T> ValueLaneGet<C, T> {
    /// # Arguments
    /// * `projection` - Projection from the agent context to the lane.
    pub fn new(projection: for<'a> fn(&'a C) -> &'a ValueLane<T>) -> Self {
        ValueLaneGet {
            projection,
            done: false,
        }
    }
}

///  An [event handler](crate::event_handler::EventHandler)`] that will set the value of a value lane.
pub struct ValueLaneSet<C, T> {
    projection: for<'a> fn(&'a C) -> &'a ValueLane<T>,
    value: Option<T>,
}

///  An [event handler](crate::event_handler::EventHandler)`] that will request a sync from the lane.
pub struct ValueLaneSync<C, T> {
    projection: for<'b> fn(&'b C) -> &'b ValueLane<T>,
    id: Option<Uuid>,
}

impl<C, T> ValueLaneSet<C, T> {
    /// # Arguments
    /// * `projection` - Projection from the agent context to the lane.
    /// * `value` - The new value for the lane.
    pub fn new(projection: for<'a> fn(&'a C) -> &'a ValueLane<T>, value: T) -> Self {
        ValueLaneSet {
            projection,
            value: Some(value),
        }
    }
}

impl<C, T> ValueLaneSync<C, T> {
    /// # Arguments
    /// * `projection` - Projection from the agent context to the lane.
    /// * `id` - The ID of the remote that requested the sync.
    pub fn new(projection: for<'a> fn(&'a C) -> &'a ValueLane<T>, id: Uuid) -> Self {
        ValueLaneSync {
            projection,
            id: Some(id),
        }
    }
}

impl<C: AgentDescription, T: Clone> HandlerAction<C> for ValueLaneGet<C, T> {
    type Completion = T;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let ValueLaneGet { projection, done } = self;
        if *done {
            StepResult::after_done()
        } else {
            *done = true;
            let lane = projection(context);
            let value = lane.read(T::clone);
            StepResult::done(value)
        }
    }

    fn describe(&self, context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let lane = (self.projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("ValueLaneGet")
            .field("id", &lane.id())
            .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
            .finish()
    }
}

impl<C: AgentDescription, T> HandlerAction<C> for ValueLaneSet<C, T> {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let ValueLaneSet { projection, value } = self;
        if let Some(value) = value.take() {
            let lane = projection(context);
            lane.set(value);
            StepResult::Complete {
                modified_item: Some(Modification::of(lane.id())),
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
        let ValueLaneSet { projection, value } = self;
        let lane = (projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("ValueLaneSet")
            .field("id", &lane.id())
            .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
            .field("consumed", &value.is_none())
            .finish()
    }
}

impl<C: AgentDescription, T> HandlerAction<C> for ValueLaneSync<C, T> {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let ValueLaneSync { projection, id } = self;
        if let Some(id) = id.take() {
            let lane = projection(context);
            lane.sync(id);
            StepResult::Complete {
                modified_item: Some(Modification::no_trigger(lane.id())),
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
        let ValueLaneSync { projection, id } = self;
        let lane = (projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("ValueLaneSync")
            .field("id", &lane.id())
            .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
            .field("sync_id", &id)
            .finish()
    }
}

/// An [`HandlerAction`] that will produce a value from a reference to the contents of the lane.
pub struct ValueLaneWithValue<C, T, F, B: ?Sized> {
    projection: for<'a> fn(&'a C) -> &'a ValueLane<T>,
    f: Option<F>,
    _type: PhantomData<fn(&B)>,
}

impl<C, T, F, B: ?Sized> ValueLaneWithValue<C, T, F, B> {
    /// #Arguments
    /// * `projection` - Projection from the agent context to the lane.
    /// * `f` - Closure to apply to the value of the lane.
    pub fn new(projection: for<'a> fn(&'a C) -> &'a ValueLane<T>, f: F) -> Self {
        ValueLaneWithValue {
            projection,
            f: Some(f),
            _type: PhantomData,
        }
    }
}

impl<C, T, F, B, U> HandlerAction<C> for ValueLaneWithValue<C, T, F, B>
where
    C: AgentDescription,
    B: ?Sized,
    T: Borrow<B>,
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
            let lane = (self.projection)(context);
            StepResult::done(lane.with(f))
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
        f.debug_struct("ValueLaneWithValue")
            .field("id", &lane.id())
            .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
            .field("result_type", &type_name::<U>())
            .finish()
    }
}

impl<C, T> HandlerTrans<T> for ProjTransform<C, ValueLane<T>> {
    type Out = ValueLaneSet<C, T>;

    fn transform(self, input: T) -> Self::Out {
        let ProjTransform { projection } = self;
        ValueLaneSet::new(projection, input)
    }
}

pub type DecodeAndSet<'a, C, T> =
    AndThen<Decode<'a, T>, ValueLaneSet<C, T>, ProjTransform<C, ValueLane<T>>>;

/// Create an event handler that will decode an incoming command and set the value into a value lane.
pub fn decode_and_set<'a, C: AgentDescription, T: RecognizerReadable>(
    decoder: &'a mut ReconDecoder<T>,
    buffer: BytesMut,
    projection: fn(&C) -> &ValueLane<T>,
) -> DecodeAndSet<'a, C, T> {
    let decode: Decode<'a, T> = Decode::new(decoder, buffer);
    decode.and_then(ProjTransform::new(projection))
}

impl<T> ValueLikeItem<T> for ValueLane<T>
where
    T: Clone + Send + 'static,
{
    type GetHandler<C> = ValueLaneGet<C, T>
    where
        C: AgentDescription + 'static;

    type WithValueHandler<'a, C, F, B, U> = ValueLaneWithValue<C, T, F, B>
    where
        Self: 'static,
        C: AgentDescription + 'a,
        T: Borrow<B>,
        B: ?Sized + 'static,
        F: FnOnce(&B) -> U + Send + 'a;

    fn get_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
    ) -> Self::GetHandler<C> {
        ValueLaneGet::new(projection)
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
        ValueLaneWithValue::new(projection, f)
    }
}

impl<T> MutableValueLikeItem<T> for ValueLane<T>
where
    T: Send + 'static,
{
    type SetHandler<C> = ValueLaneSet<C, T>
    where
        C: AgentDescription + 'static;

    fn set_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
        value: T,
    ) -> Self::SetHandler<C> {
        ValueLaneSet::new(projection, value)
    }
}

/// An [event handler](crate::event_handler::EventHandler) that attempts to set to a value lane, if
/// that lane exists.
pub struct ValueLaneSelectSet<C, T, F> {
    _type: PhantomData<fn(&C)>,
    projection_value: Option<(F, T)>,
}

impl<C, T: Debug, F: Debug> Debug for ValueLaneSelectSet<C, T, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueLaneSelectSet")
            .field("projection_value", &self.projection_value)
            .finish()
    }
}

impl<C, T, F> ValueLaneSelectSet<C, T, F> {
    /// # Arguments
    /// * `projection` - A projection from the agent type onto an (optional) value lane.
    /// * `value` - The value to set.
    pub fn new(projection: F, value: T) -> Self {
        ValueLaneSelectSet {
            _type: PhantomData,
            projection_value: Some((projection, value)),
        }
    }
}

impl<C, T, F> HandlerAction<C> for ValueLaneSelectSet<C, T, F>
where
    C: AgentDescription,
    F: SelectorFn<C, Target = ValueLane<T>>,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let ValueLaneSelectSet {
            projection_value, ..
        } = self;
        if let Some((projection, value)) = projection_value.take() {
            let selector = projection.selector(context);
            if let Some(lane) = selector.select() {
                lane.set(value);
                StepResult::Complete {
                    modified_item: Some(Modification::of(lane.id())),
                    result: (),
                }
            } else {
                StepResult::Fail(EventHandlerError::LaneNotFound(selector.name().to_string()))
            }
        } else {
            StepResult::Fail(EventHandlerError::SteppedAfterComplete)
        }
    }

    fn describe(
        &self,
        _context: &C,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        let ValueLaneSelectSet {
            projection_value, ..
        } = self;
        if let Some((projection, _)) = projection_value {
            f.debug_struct("ValueLaneSelectSet")
                .field("lane_name", &projection.name())
                .field("value_type", &type_name::<T>())
                .field("consumed", &false)
                .finish()
        } else {
            f.debug_struct("ValueLaneSelectSet")
                .field("value_type", &type_name::<T>())
                .field("consumed", &true)
                .finish()
        }
    }
}

#[derive(Default)]
#[doc(hidden)]
pub enum DecodeAndSelectSet<'a, C, T: RecognizerReadable, F> {
    Decoding(Decode<'a, T>, F),
    Selecting(ValueLaneSelectSet<C, T, F>),
    #[default]
    Done,
}

impl<'a, C, T, F> HandlerAction<C> for DecodeAndSelectSet<'a, C, T, F>
where
    C: AgentDescription,
    T: RecognizerReadable,
    F: SelectorFn<C, Target = ValueLane<T>>,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<C>,
        meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        match std::mem::take(self) {
            DecodeAndSelectSet::Decoding(mut decoding, selector) => {
                match decoding.step(action_context, meta, context) {
                    StepResult::Continue { modified_item } => {
                        *self = DecodeAndSelectSet::Decoding(decoding, selector);
                        StepResult::Continue { modified_item }
                    }
                    StepResult::Fail(err) => StepResult::Fail(err),
                    StepResult::Complete {
                        modified_item,
                        result,
                    } => {
                        *self = DecodeAndSelectSet::Selecting(ValueLaneSelectSet::new(
                            selector, result,
                        ));
                        StepResult::Continue { modified_item }
                    }
                }
            }
            DecodeAndSelectSet::Selecting(mut selector) => {
                let result = selector.step(action_context, meta, context);
                if !result.is_cont() {
                    *self = DecodeAndSelectSet::Done;
                }
                result
            }
            DecodeAndSelectSet::Done => StepResult::after_done(),
        }
    }

    fn describe(
        &self,
        context: &C,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        match self {
            DecodeAndSelectSet::Decoding(decode, proj) => f
                .debug_struct("DecodeAndSelectSet")
                .field("state", &"Decoding")
                .field("decoder", &Described::new(context, decode))
                .field("lane_name", &proj.name())
                .finish(),
            DecodeAndSelectSet::Selecting(selector) => f
                .debug_struct("DecodeAndSelectSet")
                .field("state", &"Selecting")
                .field("selector", &Described::new(context, selector))
                .finish(),
            DecodeAndSelectSet::Done => f
                .debug_tuple("DecodeAndSelectSet")
                .field(&"<<CONSUMED>>")
                .finish(),
        }
    }
}

/// Create an event handler that will decode an incoming command and set the value into a value lane.
pub fn decode_and_select_set<C, T, F>(
    decoder: &mut ReconDecoder<T>,
    buffer: BytesMut,
    projection: F,
) -> DecodeAndSelectSet<'_, C, T, F>
where
    T: RecognizerReadable,
    F: SelectorFn<C, Target = ValueLane<T>>,
{
    let decode: Decode<T> = Decode::new(decoder, buffer);
    DecodeAndSelectSet::Decoding(decode, projection)
}

///  An [event handler](crate::event_handler::EventHandler)`] that will request a sync from the lane.
pub struct ValueLaneSelectSync<C, T, F> {
    _type: PhantomData<fn(&C) -> &T>,
    projection_id: Option<(F, Uuid)>,
}

impl<C, T, F> ValueLaneSelectSync<C, T, F> {
    /// # Arguments
    /// * `projection` - Projection from the agent context to the lane.
    /// * `id` - The ID of the remote that requested the sync.
    pub fn new(projection: F, id: Uuid) -> Self {
        ValueLaneSelectSync {
            _type: PhantomData,
            projection_id: Some((projection, id)),
        }
    }
}

impl<C, T, F> HandlerAction<C> for ValueLaneSelectSync<C, T, F>
where
    C: AgentDescription,
    F: SelectorFn<C, Target = ValueLane<T>>,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let ValueLaneSelectSync { projection_id, .. } = self;
        if let Some((projection, id)) = projection_id.take() {
            let selector = projection.selector(context);
            if let Some(lane) = selector.select() {
                lane.sync(id);
                StepResult::Complete {
                    modified_item: Some(Modification::no_trigger(lane.id())),
                    result: (),
                }
            } else {
                StepResult::Fail(EventHandlerError::LaneNotFound(selector.name().to_string()))
            }
        } else {
            StepResult::Fail(EventHandlerError::SteppedAfterComplete)
        }
    }

    fn describe(
        &self,
        _context: &C,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        let ValueLaneSelectSync { projection_id, .. } = self;
        if let Some((proj, id)) = projection_id {
            f.debug_struct("ValueLaneSelectSync")
                .field("lane_name", &proj.name())
                .field("sync_id", id)
                .finish()
        } else {
            f.debug_tuple("ValueLaneSelectSync")
                .field(&"<<CONSUMED>>")
                .finish()
        }
    }
}
