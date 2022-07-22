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
/// #Type Parameters
/// * `K` - The type of the map keys.
/// * `V` - The type of the map values.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
pub trait MapLaneLifecycle<K, V, Context>: for<'a> MapLaneHandlers<'a, K, V, Context> {}

/// Trait for the lifecycle of a map lane where the lifecycle has access to some shared state (shared
/// with all other lifecycles in the agent).
///
/// #Type Parameters
/// * `K` - The type of the map keys.
/// * `V` - The type of the map values.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `Shared` - The shared state to which the lifecycle has access.
pub trait MapLaneHandlersShared<'a, K, V, Context, Shared>:
    OnUpdateShared<'a, K, V, Context, Shared>
    + OnRemoveShared<'a, K, V, Context, Shared>
    + OnClearShared<'a, K, V, Context, Shared>
{
}

pub trait MapLaneHandlers<'a, K, V, Context>:
    OnUpdate<'a, K, V, Context> + OnRemove<'a, K, V, Context> + OnClear<'a, K, V, Context>
{
}

impl<'a, K, V, Context, L> MapLaneHandlers<'a, K, V, Context> for L where
    L: OnUpdate<'a, K, V, Context> + OnRemove<'a, K, V, Context> + OnClear<'a, K, V, Context>
{
}

impl<K, V, Context, L> MapLaneLifecycle<K, V, Context> for L where
    L: for<'a> MapLaneHandlers<'a, K, V, Context>
{
}

pub trait MapLaneLifecycleShared<K, V, Context, Shared>:
    for<'a> MapLaneHandlersShared<'a, K, V, Context, Shared>
{
}

impl<L, K, V, Context, Shared> MapLaneLifecycleShared<K, V, Context, Shared> for L where
    L: for<'a> MapLaneHandlersShared<'a, K, V, Context, Shared>
{
}

impl<'a, L, K, V, Context, Shared> MapLaneHandlersShared<'a, K, V, Context, Shared> for L where
    L: OnUpdateShared<'a, K, V, Context, Shared>
        + OnRemoveShared<'a, K, V, Context, Shared>
        + OnClearShared<'a, K, V, Context, Shared>
{
}

/// A lifecycle for a map lane with some shared state (shard with other lifecycles in the same agent).
///
/// #Type Parameters
/// * `Context` - The contect for the event handlers (providing access to the agent lanes).
/// * `Shared` - The shared state to which the lifecycle has access.
/// * `FUpd` - The `on_update` event handler.
/// * `FRem` - The `on_remove` event handler.
/// * `FClr` - The `on_clear` event handler.
pub struct StatefulMapLaneLifecycle<
    Context,
    Shared,
    K,
    V,
    FUpd = NoHandler,
    FRem = NoHandler,
    FClr = NoHandler,
> {
    _value_type: PhantomData<fn(Context, Shared, K, V)>,
    on_update: FUpd,
    on_remove: FRem,
    on_clear: FClr,
}

impl<Context, Shared, K, V, FUpd: Clone, FRem: Clone, FClr: Clone> Clone
    for StatefulMapLaneLifecycle<Context, Shared, K, V, FUpd, FRem, FClr>
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

impl<Context, Shared, K, V> Default for StatefulMapLaneLifecycle<Context, Shared, K, V> {
    fn default() -> Self {
        Self {
            _value_type: Default::default(),
            on_update: Default::default(),
            on_remove: Default::default(),
            on_clear: Default::default(),
        }
    }
}

impl<Context, Shared, K, V, FUpd, FRem, FClr>
    StatefulMapLaneLifecycle<Context, Shared, K, V, FUpd, FRem, FClr>
{
    pub fn on_update<F>(
        self,
        f: F,
    ) -> StatefulMapLaneLifecycle<Context, Shared, K, V, FnHandler<F>, FRem, FClr>
    where
        FnHandler<F>: for<'a> OnUpdateShared<'a, K, V, Context, Shared>,
    {
        StatefulMapLaneLifecycle {
            _value_type: PhantomData,
            on_update: FnHandler(f),
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    pub fn on_remove<F>(
        self,
        f: F,
    ) -> StatefulMapLaneLifecycle<Context, Shared, K, V, FUpd, FnHandler<F>, FClr>
    where
        FnHandler<F>: for<'a> OnRemoveShared<'a, K, V, Context, Shared>,
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
    ) -> StatefulMapLaneLifecycle<Context, Shared, K, V, FUpd, FRem, FnHandler<F>>
    where
        FnHandler<F>: for<'a> OnClearShared<'a, K, V, Context, Shared>,
    {
        StatefulMapLaneLifecycle {
            _value_type: PhantomData,
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: FnHandler(f),
        }
    }
}

impl<'a, K, V, Context, Shared, FUpd, FRem, FClr> OnUpdateShared<'a, K, V, Context, Shared>
    for StatefulMapLaneLifecycle<Context, Shared, K, V, FUpd, FRem, FClr>
where
    FUpd: OnUpdateShared<'a, K, V, Context, Shared>,
    FRem: Send,
    FClr: Send,
{
    type OnUpdateHandler = FUpd::OnUpdateHandler;

    fn on_update(
        &'a self,
        shared: &'a Shared,
        handler_context: crate::agent_lifecycle::utility::HandlerContext<Context>,
        map: &std::collections::HashMap<K, V>,
        key: K,
        prev_value: Option<V>,
    ) -> Self::OnUpdateHandler {
        self.on_update
            .on_update(shared, handler_context, map, key, prev_value)
    }
}

impl<'a, K, V, Context, Shared, FUpd, FRem, FClr> OnRemoveShared<'a, K, V, Context, Shared>
    for StatefulMapLaneLifecycle<Context, Shared, K, V, FUpd, FRem, FClr>
where
    FUpd: Send,
    FRem: OnRemoveShared<'a, K, V, Context, Shared>,
    FClr: Send,
{
    type OnRemoveHandler = FRem::OnRemoveHandler;

    fn on_remove(
        &'a self,
        shared: &'a Shared,
        handler_context: crate::agent_lifecycle::utility::HandlerContext<Context>,
        map: &std::collections::HashMap<K, V>,
        key: K,
        prev_value: V,
    ) -> Self::OnRemoveHandler {
        self.on_remove
            .on_remove(shared, handler_context, map, key, prev_value)
    }
}

impl<'a, K, V, Context, Shared, FUpd, FRem, FClr> OnClearShared<'a, K, V, Context, Shared>
    for StatefulMapLaneLifecycle<Context, Shared, K, V, FUpd, FRem, FClr>
where
    FUpd: Send,
    FRem: Send,
    FClr: OnClearShared<'a, K, V, Context, Shared>,
{
    type OnClearHandler = FClr::OnClearHandler;

    fn on_clear(
        &'a self,
        shared: &'a Shared,
        handler_context: crate::agent_lifecycle::utility::HandlerContext<Context>,
        before: std::collections::HashMap<K, V>,
    ) -> Self::OnClearHandler {
        self.on_clear.on_clear(shared, handler_context, before)
    }
}
