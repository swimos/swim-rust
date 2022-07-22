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

use std::marker::PhantomData;

use swim_api::handlers::{FnHandler, NoHandler};

use crate::agent_lifecycle::utility::HandlerContext;

use self::{
    on_event::{OnEvent, OnEventShared},
    on_set::{OnSet, OnSetShared},
};

pub mod on_event;
pub mod on_set;

/// Trait for the lifecycle of a value lane.
///
/// #Type Parameters
/// * `T` - The type of the state of the lane.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
pub trait ValueLaneLifecycle<T, Context>: for<'a> ValueLaneHandlers<'a, T, Context> {}

/// Trait for the lifecycle of a value lane where the lifecycle has access to some shared state (shared
/// with all other lifecycles in the agent).
///
/// #Type Parameters
/// * `T` - The type of the state of the lane.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `Shared` - The shared state to which the lifecycle has access.
pub trait ValueLaneLifecycleShared<T, Context, Shared>:
    for<'a> ValueLaneHandlersShared<'a, T, Context, Shared>
{
}

pub trait ValueLaneHandlers<'a, T, Context>:
    OnEvent<'a, T, Context> + OnSet<'a, T, Context>
{
}

impl<'a, T, Context, L> ValueLaneHandlers<'a, T, Context> for L where
    L: OnEvent<'a, T, Context> + OnSet<'a, T, Context>
{
}

pub trait ValueLaneHandlersShared<'a, T, Context, Shared>:
    OnEventShared<'a, T, Context, Shared> + OnSetShared<'a, T, Context, Shared>
{
}

impl<T, Context, L> ValueLaneLifecycle<T, Context> for L where
    L: for<'a> ValueLaneHandlers<'a, T, Context>
{
}

impl<L, T, Context, Shared> ValueLaneLifecycleShared<T, Context, Shared> for L where
    L: for<'a> ValueLaneHandlersShared<'a, T, Context, Shared>
{
}

impl<'a, L, T, Context, Shared> ValueLaneHandlersShared<'a, T, Context, Shared> for L where
    L: OnEventShared<'a, T, Context, Shared> + OnSetShared<'a, T, Context, Shared>
{
}

/// A lifecycle for a value lane with some shared state (shard with other lifecycles in the same agent).
///
/// #Type Parameters
/// * `Context` - The contect for the event handlers (providing access to the agent lanes).
/// * `Shared` - The shared state to which the lifecycle has access.
/// * `FEv` - The `on_event` event handler.
/// * `FSet` - The `on_set` event handler.
pub struct StatefulValueLaneLifecycle<Context, Shared, T, FEv = NoHandler, FSet = NoHandler> {
    _value_type: PhantomData<fn(Context, Shared, T)>,
    on_event: FEv,
    on_set: FSet,
}

impl<Context, Shared, T, FEv: Clone, FSet: Clone> Clone
    for StatefulValueLaneLifecycle<Context, Shared, T, FEv, FSet>
{
    fn clone(&self) -> Self {
        Self {
            _value_type: PhantomData,
            on_event: self.on_event.clone(),
            on_set: self.on_set.clone(),
        }
    }
}

impl<Context, Shared, T> Default for StatefulValueLaneLifecycle<Context, Shared, T> {
    fn default() -> Self {
        Self {
            _value_type: Default::default(),
            on_event: Default::default(),
            on_set: Default::default(),
        }
    }
}

impl<Context, Shared, T, FEv, FSet> StatefulValueLaneLifecycle<Context, Shared, T, FEv, FSet> {
    /// Replace the `on_event` handler with another derived from a closure.
    pub fn on_event<F>(
        self,
        f: F,
    ) -> StatefulValueLaneLifecycle<Context, Shared, T, FnHandler<F>, FSet>
    where
        FnHandler<F>: for<'a> OnEventShared<'a, T, Context, Shared>,
    {
        StatefulValueLaneLifecycle {
            _value_type: PhantomData,
            on_event: FnHandler(f),
            on_set: self.on_set,
        }
    }

    /// Replace the `on_set` handler with another derived from a closure.
    pub fn on_set<F>(
        self,
        f: F,
    ) -> StatefulValueLaneLifecycle<Context, Shared, T, FEv, FnHandler<F>>
    where
        FnHandler<F>: for<'a> OnSetShared<'a, T, Context, Shared>,
    {
        StatefulValueLaneLifecycle {
            _value_type: PhantomData,
            on_event: self.on_event,
            on_set: FnHandler(f),
        }
    }
}

impl<'a, T, FEv, FSet, Context, Shared> OnEventShared<'a, T, Context, Shared>
    for StatefulValueLaneLifecycle<Context, Shared, T, FEv, FSet>
where
    FSet: Send,
    FEv: OnEventShared<'a, T, Context, Shared>,
{
    type OnEventHandler = FEv::OnEventHandler;

    fn on_event(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
    ) -> Self::OnEventHandler {
        self.on_event.on_event(shared, handler_context, value)
    }
}

impl<'a, T, FEv, FSet, Context, Shared> OnSetShared<'a, T, Context, Shared>
    for StatefulValueLaneLifecycle<Context, Shared, T, FEv, FSet>
where
    FEv: Send,
    FSet: OnSetShared<'a, T, Context, Shared>,
{
    type OnSetHandler = FSet::OnSetHandler;

    fn on_set(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        new_value: &T,
        existing: Option<T>,
    ) -> Self::OnSetHandler {
        self.on_set
            .on_set(shared, handler_context, new_value, existing)
    }
}
