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

use std::{borrow::Borrow, marker::PhantomData};

use swimos_utilities::handlers::{BorrowHandler, FnHandler, NoHandler};

use crate::agent_lifecycle::HandlerContext;

use self::{
    on_clear::{OnClear, OnClearShared},
    on_remove::{OnRemove, OnRemoveShared},
    on_update::{OnUpdate, OnUpdateShared},
};

pub mod on_clear;
pub mod on_remove;
pub mod on_update;

/// Trait for the lifecycle of a map lane.
///
/// # Type Parameters
/// * `K` - The type of the map keys.
/// * `V` - The type of the map values.
/// * `M` - The map type underlying the lane (i.e. [`std::collections::HashMap<K, V>`]).
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
pub trait MapLaneLifecycle<K, V, M, Context>:
    OnUpdate<K, V, M, Context> + OnRemove<K, V, M, Context> + OnClear<M, Context>
{
}

impl<K, V, M, Context, L> MapLaneLifecycle<K, V, M, Context> for L where
    L: OnUpdate<K, V, M, Context> + OnRemove<K, V, M, Context> + OnClear<M, Context>
{
}

pub trait MapLaneLifecycleShared<K, V, M, Context, Shared>:
    OnUpdateShared<K, V, M, Context, Shared>
    + OnRemoveShared<K, V, M, Context, Shared>
    + OnClearShared<M, Context, Shared>
{
}

impl<L, K, V, M, Context, Shared> MapLaneLifecycleShared<K, V, M, Context, Shared> for L where
    L: OnUpdateShared<K, V, M, Context, Shared>
        + OnRemoveShared<K, V, M, Context, Shared>
        + OnClearShared<M, Context, Shared>
{
}

type LifecycleType<Context, Shared, K, V, M> = fn(Context, Shared, K, V, M);

/// A lifecycle for a map lane with some shared state (shard with other lifecycles in the same agent).
///
/// # Type Parameters
/// * `Context` - The context for the event handlers (providing access to the agent lanes).
/// * `Shared` - The shared state to which the lifecycle has access.
/// * `K` - The key type of the map.
/// * `V` - The value type of the map.
/// * `M` - The map type underlying the lane (i.e. [`std::collections::HashMap<K, V>`]).
/// * `FUpd` - The `on_update` event handler.
/// * `FRem` - The `on_remove` event handler.
/// * `FClr` - The `on_clear` event handler.
pub struct StatefulMapLaneLifecycle<
    Context,
    Shared,
    K,
    V,
    M,
    FUpd = NoHandler,
    FRem = NoHandler,
    FClr = NoHandler,
> {
    _value_type: PhantomData<LifecycleType<Context, Shared, K, V, M>>,
    on_update: FUpd,
    on_remove: FRem,
    on_clear: FClr,
}

impl<Context, Shared, K, V, M, FUpd: Clone, FRem: Clone, FClr: Clone> Clone
    for StatefulMapLaneLifecycle<Context, Shared, K, V, M, FUpd, FRem, FClr>
{
    fn clone(&self) -> Self {
        Self {
            _value_type: PhantomData,
            on_update: self.on_update.clone(),
            on_remove: self.on_remove.clone(),
            on_clear: self.on_clear.clone(),
        }
    }
}

impl<Context, Shared, K, V, M> Default for StatefulMapLaneLifecycle<Context, Shared, K, V, M> {
    fn default() -> Self {
        Self {
            _value_type: Default::default(),
            on_update: Default::default(),
            on_remove: Default::default(),
            on_clear: Default::default(),
        }
    }
}

impl<Context, Shared, K, V, M, FUpd, FRem, FClr>
    StatefulMapLaneLifecycle<Context, Shared, K, V, M, FUpd, FRem, FClr>
{
    pub fn on_update<F, B>(
        self,
        f: F,
    ) -> StatefulMapLaneLifecycle<Context, Shared, K, V, M, BorrowHandler<F, B>, FRem, FClr>
    where
        B: ?Sized,
        V: Borrow<B>,
        BorrowHandler<F, B>: for<'a> OnUpdateShared<K, V, M, Context, Shared>,
    {
        StatefulMapLaneLifecycle {
            _value_type: PhantomData,
            on_update: BorrowHandler::new(f),
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    pub fn on_remove<F>(
        self,
        f: F,
    ) -> StatefulMapLaneLifecycle<Context, Shared, K, V, M, FUpd, FnHandler<F>, FClr>
    where
        FnHandler<F>: OnRemoveShared<K, V, M, Context, Shared>,
    {
        StatefulMapLaneLifecycle {
            _value_type: PhantomData,
            on_update: self.on_update,
            on_remove: FnHandler(f),
            on_clear: self.on_clear,
        }
    }

    pub fn on_clear<F>(
        self,
        f: F,
    ) -> StatefulMapLaneLifecycle<Context, Shared, K, V, M, FUpd, FRem, FnHandler<F>>
    where
        FnHandler<F>: OnClearShared<M, Context, Shared>,
    {
        StatefulMapLaneLifecycle {
            _value_type: PhantomData,
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: FnHandler(f),
        }
    }
}

impl<K, V, M, Context, Shared, FUpd, FRem, FClr> OnUpdateShared<K, V, M, Context, Shared>
    for StatefulMapLaneLifecycle<Context, Shared, K, V, M, FUpd, FRem, FClr>
where
    FUpd: OnUpdateShared<K, V, M, Context, Shared>,
    FRem: Send,
    FClr: Send,
{
    type OnUpdateHandler<'a> = FUpd::OnUpdateHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_update<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: &M,
        key: K,
        prev_value: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        self.on_update
            .on_update(shared, handler_context, map, key, prev_value, new_value)
    }
}

impl<K, V, M, Context, Shared, FUpd, FRem, FClr> OnRemoveShared<K, V, M, Context, Shared>
    for StatefulMapLaneLifecycle<Context, Shared, K, V, M, FUpd, FRem, FClr>
where
    FUpd: Send,
    FRem: OnRemoveShared<K, V, M, Context, Shared>,
    FClr: Send,
{
    type OnRemoveHandler<'a> = FRem::OnRemoveHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_remove<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: &M,
        key: K,
        prev_value: V,
    ) -> Self::OnRemoveHandler<'a> {
        self.on_remove
            .on_remove(shared, handler_context, map, key, prev_value)
    }
}

impl<K, V, M, Context, Shared, FUpd, FRem, FClr> OnClearShared<M, Context, Shared>
    for StatefulMapLaneLifecycle<Context, Shared, K, V, M, FUpd, FRem, FClr>
where
    FUpd: Send,
    FRem: Send,
    FClr: OnClearShared<M, Context, Shared>,
{
    type OnClearHandler<'a> = FClr::OnClearHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_clear<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        before: M,
    ) -> Self::OnClearHandler<'a> {
        self.on_clear.on_clear(shared, handler_context, before)
    }
}
