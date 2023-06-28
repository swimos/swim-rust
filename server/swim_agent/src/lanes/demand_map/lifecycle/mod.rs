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

use std::marker::PhantomData;

use swim_api::handlers::{FnHandler, NoHandler};

use crate::agent_lifecycle::utility::HandlerContext;

use self::{
    keys::{Keys, KeysShared},
    on_cue_key::{OnCueKey, OnCueKeyShared},
};

pub mod keys;
pub mod on_cue_key;

/// Trait for the lifecycle of a demand-map lane.
///
/// #Type Parameters
/// * `K` - The type of the keys of the map.
/// * `V` - The type of the values of the map.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
pub trait DemandMapLaneLifecycle<K, V, Context>:
    Keys<K, Context> + OnCueKey<K, V, Context>
{
}

/// Trait for the lifecycle of a demand-map lane where the lifecycle has access to some shared state (shared
/// with all other lifecycles in the agent).
///
/// #Type Parameters
/// * `K` - The type of the keys of the map.
/// * `V` - The type of the values of the map.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `Shared` - The shared state to which the lifecycle has access.
pub trait DemandMapLaneLifecycleShared<K, V, Context, Shared>:
    KeysShared<K, Context, Shared> + OnCueKeyShared<K, V, Context, Shared>
{
}

impl<K, V, Context, L> DemandMapLaneLifecycle<K, V, Context> for L where
    L: Keys<K, Context> + OnCueKey<K, V, Context>
{
}

impl<K, V, Context, Shared, L> DemandMapLaneLifecycleShared<K, V, Context, Shared> for L where
    L: KeysShared<K, Context, Shared> + OnCueKeyShared<K, V, Context, Shared>
{
}

/// A lifecycle for a demand-map lane with some shared state (shard with other lifecycles in the same agent).
///
/// #Type Parameters
/// * `Context` - The context for the event handlers (providing access to the agent lanes).
/// * `Shared` - The shared state to which the lifecycle has access.
/// * `K` - The type of the keys of the map.
/// * `V` - The type of the values of the map.
/// * `Keys` - The type of the keys event.
/// * `OneCueKey` - The type of the `on_cue_key` event.
pub struct StatefulDemandMapLaneLifecycle<
    Context,
    Shared,
    K,
    V,
    Keys = NoHandler,
    OnCueKey = NoHandler,
> {
    _value_type: PhantomData<fn(Context, Shared, K) -> (K, V)>,
    keys: Keys,
    on_cue_key: OnCueKey,
}

impl<Context, Shared, K, V, Keys: Clone, OnCueKey: Clone> Clone
    for StatefulDemandMapLaneLifecycle<Context, Shared, K, V, Keys, OnCueKey>
{
    fn clone(&self) -> Self {
        Self {
            _value_type: PhantomData,
            keys: self.keys.clone(),
            on_cue_key: self.on_cue_key.clone(),
        }
    }
}

impl<Context, Shared, K, V> Default for StatefulDemandMapLaneLifecycle<Context, Shared, K, V> {
    fn default() -> Self {
        Self {
            _value_type: Default::default(),
            keys: Default::default(),
            on_cue_key: Default::default(),
        }
    }
}

impl<K, V, Context, Shared, Keys, OnCueKey> KeysShared<K, Context, Shared>
    for StatefulDemandMapLaneLifecycle<Context, Shared, K, V, Keys, OnCueKey>
where
    Keys: KeysShared<K, Context, Shared>,
    OnCueKey: Send,
    K: 'static,
{
    type KeysHandler<'a> = <Keys as KeysShared<K, Context, Shared>>::KeysHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn keys<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::KeysHandler<'a> {
        self.keys.keys(shared, handler_context)
    }
}

impl<K, V, Context, Shared, Keys, OnCueK> OnCueKeyShared<K, V, Context, Shared>
    for StatefulDemandMapLaneLifecycle<Context, Shared, K, V, Keys, OnCueK>
where
    Keys: Send,
    OnCueK: OnCueKeyShared<K, V, Context, Shared>,
{
    type OnCueKeyHandler<'a> = <OnCueK as OnCueKeyShared<K, V, Context, Shared>>::OnCueKeyHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_cue_key<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
    ) -> Self::OnCueKeyHandler<'a> {
        self.on_cue_key.on_cue_key(shared, handler_context, key)
    }
}

impl<Context, Shared, K, V, Keys, OnCueK>
    StatefulDemandMapLaneLifecycle<Context, Shared, K, V, Keys, OnCueK>
{
    /// Replace the `keys` handler with another derived from a closure.
    pub fn keys<F>(
        self,
        f: F,
    ) -> StatefulDemandMapLaneLifecycle<Context, Shared, K, V, FnHandler<F>, OnCueK>
    where
        K: 'static,
        FnHandler<F>: KeysShared<K, Context, Shared>,
    {
        StatefulDemandMapLaneLifecycle {
            _value_type: Default::default(),
            keys: FnHandler(f),
            on_cue_key: self.on_cue_key,
        }
    }

    /// Replace the `on_cue_key` handler with another derived from a closure.
    pub fn on_cue_key<F>(
        self,
        f: F,
    ) -> StatefulDemandMapLaneLifecycle<Context, Shared, K, V, Keys, FnHandler<F>>
    where
        V: 'static,
        FnHandler<F>: OnCueKeyShared<K, V, Context, Shared>,
    {
        StatefulDemandMapLaneLifecycle {
            _value_type: Default::default(),
            keys: self.keys,
            on_cue_key: FnHandler(f),
        }
    }
}
