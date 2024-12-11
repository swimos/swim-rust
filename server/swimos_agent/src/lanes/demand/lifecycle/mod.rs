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

use std::marker::PhantomData;

use swimos_utilities::handlers::{FnHandler, NoHandler};

use crate::agent_lifecycle::HandlerContext;

use self::on_cue::{OnCue, OnCueShared};

pub mod on_cue;

/// Trait for the lifecycle of a demand lane.
///
/// # Type Parameters
/// * `T` - The type of the values of the lane.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
pub trait DemandLaneLifecycle<T, Context>: OnCue<T, Context> {}

/// Trait for the lifecycle of a demand lane where the lifecycle has access to some shared state (shared
/// with all other lifecycles in the agent).
///
/// # Type Parameters
/// * `T` - The type of the values of the lane.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `Shared` - The shared state to which the lifecycle has access.
pub trait DemandLaneLifecycleShared<T, Context, Shared>: OnCueShared<T, Context, Shared> {}

impl<T, Context, L> DemandLaneLifecycle<T, Context> for L where L: OnCue<T, Context> {}

impl<T, Context, Shared, L> DemandLaneLifecycleShared<T, Context, Shared> for L where
    L: OnCueShared<T, Context, Shared>
{
}

/// A lifecycle for a demand lane with some shared state (shard with other lifecycles in the same agent).
///
/// # Type Parameters
/// * `Context` - The context for the event handlers (providing access to the agent lanes).
/// * `Shared` - The shared state to which the lifecycle has access.
/// * `T` - The type of the values of the lane,
/// * `OneCue` - The type of the `on_cue` event.
pub struct StatefulDemandLaneLifecycle<Context, Shared, T, OnCue = NoHandler> {
    _value_type: PhantomData<fn(Context, Shared, T)>,
    on_cue: OnCue,
}

impl<Context, Shared, T, OnCue: Clone> Clone
    for StatefulDemandLaneLifecycle<Context, Shared, T, OnCue>
{
    fn clone(&self) -> Self {
        Self {
            _value_type: PhantomData,
            on_cue: self.on_cue.clone(),
        }
    }
}

impl<Context, Shared, T> Default for StatefulDemandLaneLifecycle<Context, Shared, T> {
    fn default() -> Self {
        Self {
            _value_type: Default::default(),
            on_cue: Default::default(),
        }
    }
}

impl<T, Context, Shared, OnCue> OnCueShared<T, Context, Shared>
    for StatefulDemandLaneLifecycle<Context, Shared, T, OnCue>
where
    OnCue: OnCueShared<T, Context, Shared>,
    T: 'static,
{
    type OnCueHandler<'a>
        = OnCue::OnCueHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_cue<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::OnCueHandler<'a> {
        self.on_cue.on_cue(shared, handler_context)
    }
}

impl<Context, Shared, T, OnCue> StatefulDemandLaneLifecycle<Context, Shared, T, OnCue> {
    /// Replace the `on_cue` handler with another derived from a closure.
    pub fn on_cue<F>(self, f: F) -> StatefulDemandLaneLifecycle<Context, Shared, T, FnHandler<F>>
    where
        T: 'static,
        FnHandler<F>: OnCueShared<T, Context, Shared>,
    {
        StatefulDemandLaneLifecycle {
            _value_type: Default::default(),
            on_cue: FnHandler(f),
        }
    }
}
