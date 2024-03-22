// Copyright 2015-2023 Swim Inc.
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

use std::{borrow::Borrow, marker::PhantomData};

use swimos_api::handlers::{BorrowHandler, NoHandler};

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
pub trait ValueLaneLifecycle<T, Context>: OnEvent<T, Context> + OnSet<T, Context> {}

/// Trait for the lifecycle of a value lane where the lifecycle has access to some shared state (shared
/// with all other lifecycles in the agent).
///
/// #Type Parameters
/// * `T` - The type of the state of the lane.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `Shared` - The shared state to which the lifecycle has access.
pub trait ValueLaneLifecycleShared<T, Context, Shared>:
    OnEventShared<T, Context, Shared> + OnSetShared<T, Context, Shared>
{
}

impl<T, Context, L> ValueLaneLifecycle<T, Context> for L where
    L: OnEvent<T, Context> + OnSet<T, Context>
{
}

impl<L, T, Context, Shared> ValueLaneLifecycleShared<T, Context, Shared> for L where
    L: OnEventShared<T, Context, Shared> + OnSetShared<T, Context, Shared>
{
}

/// A lifecycle for a value lane with some shared state (shard with other lifecycles in the same agent).
///
/// #Type Parameters
/// * `Context` - The context for the event handlers (providing access to the agent lanes).
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
    pub fn on_event<F, B>(
        self,
        f: F,
    ) -> StatefulValueLaneLifecycle<Context, Shared, T, BorrowHandler<F, B>, FSet>
    where
        B: ?Sized,
        T: Borrow<B>,
        BorrowHandler<F, B>: OnEventShared<T, Context, Shared>,
    {
        StatefulValueLaneLifecycle {
            _value_type: PhantomData,
            on_event: BorrowHandler::new(f),
            on_set: self.on_set,
        }
    }

    /// Replace the `on_set` handler with another derived from a closure.
    pub fn on_set<F, B>(
        self,
        f: F,
    ) -> StatefulValueLaneLifecycle<Context, Shared, T, FEv, BorrowHandler<F, B>>
    where
        B: ?Sized,
        T: Borrow<B>,
        BorrowHandler<F, B>: OnSetShared<T, Context, Shared>,
    {
        StatefulValueLaneLifecycle {
            _value_type: PhantomData,
            on_event: self.on_event,
            on_set: BorrowHandler::new(f),
        }
    }
}

impl<T, FEv, FSet, Context, Shared> OnEventShared<T, Context, Shared>
    for StatefulValueLaneLifecycle<Context, Shared, T, FEv, FSet>
where
    FSet: Send,
    FEv: OnEventShared<T, Context, Shared>,
{
    type OnEventHandler<'a> = FEv::OnEventHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_event<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
    ) -> Self::OnEventHandler<'a> {
        self.on_event.on_event(shared, handler_context, value)
    }
}

impl<T, FEv, FSet, Context, Shared> OnSetShared<T, Context, Shared>
    for StatefulValueLaneLifecycle<Context, Shared, T, FEv, FSet>
where
    FEv: Send,
    FSet: OnSetShared<T, Context, Shared>,
{
    type OnSetHandler<'a> = FSet::OnSetHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_set<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        new_value: &T,
        existing: Option<T>,
    ) -> Self::OnSetHandler<'a> {
        self.on_set
            .on_set(shared, handler_context, new_value, existing)
    }
}
