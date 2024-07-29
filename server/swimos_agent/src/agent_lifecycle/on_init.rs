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

use std::{hash::Hash, marker::PhantomData};

use swimos_form::Form;
use swimos_utilities::handlers::{FnHandler, NoHandler};

use crate::{
    event_handler::{ActionContext, BoxJoinLaneInit},
    item::AgentItem,
    lanes::{
        join_map::{self, lifecycle::JoinMapLaneLifecycle},
        join_value::{self, lifecycle::JoinValueLaneLifecycle},
        JoinMapLane, JoinValueLane,
    },
    meta::AgentMetadata,
};

use super::utility::{JoinMapContext, JoinValueContext};

/// Pre-initialization function for a agent. Allows for the initialization of the agent context
/// before any event handlers run.
pub trait OnInit<Context>: Send {
    /// Provides an opportunity for the lifecycle to perform any initial setup. This will be
    /// called immediately before the `on_start` event handler is executed.
    fn initialize(
        &self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    );
}

/// Pre-initialization function for a agent which has access to shared state with the event handlers for the
/// agent. Allows for the initialization of the agent context before any event handlers run.
pub trait OnInitShared<Context, Shared>: Send {
    fn initialize(
        &self,
        shared: &Shared,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    );
}

impl<Context> OnInit<Context> for NoHandler {
    fn initialize(
        &self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) {
    }
}

impl<Context, Shared> OnInitShared<Context, Shared> for NoHandler {
    fn initialize(
        &self,
        _shared: &Shared,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) {
    }
}

impl<Context, F> OnInit<Context> for FnHandler<F>
where
    F: Fn(&mut ActionContext<Context>, AgentMetadata, &Context) + Send,
{
    fn initialize(
        &self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) {
        let FnHandler(f) = self;
        f(action_context, meta, context)
    }
}

impl<Context, Shared, F> OnInitShared<Context, Shared> for FnHandler<F>
where
    F: Fn(&Shared, &mut ActionContext<Context>, AgentMetadata, &Context) + Send,
{
    fn initialize(
        &self,
        shared: &Shared,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) {
        let FnHandler(f) = self;
        f(shared, action_context, meta, context)
    }
}

/// An initializer that does nothing.
pub type InitNil = NoHandler;

/// A heterogeneous list of initializers. This should be terminated with [`InitNil`]. Each
/// initializer will be called in the order in which they occur in the list. This is intended
/// for use by the lifecycle derivation macro.
#[derive(Clone, Copy)]
pub struct InitCons<L, R> {
    head: L,
    tail: R,
}

impl<L, R> InitCons<L, R> {
    pub fn cons(head: L, tail: R) -> Self {
        InitCons { head, tail }
    }
}

impl<Context, L, R> OnInit<Context> for InitCons<L, R>
where
    L: OnInit<Context>,
    R: OnInit<Context>,
{
    fn initialize(
        &self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) {
        let InitCons { head, tail } = self;
        head.initialize(action_context, meta, context);
        tail.initialize(action_context, meta, context);
    }
}

impl<Context, Shared, L, R> OnInitShared<Context, Shared> for InitCons<L, R>
where
    L: OnInitShared<Context, Shared>,
    R: OnInitShared<Context, Shared>,
{
    fn initialize(
        &self,
        shared: &Shared,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) {
        let InitCons { head, tail } = self;
        head.initialize(shared, action_context, meta, context);
        tail.initialize(shared, action_context, meta, context);
    }
}

/// An initializer that will register the downlink lifecycle for a join value lane in the context.
pub struct RegisterJoinValue<Context, Shared, K, V, F> {
    _type: PhantomData<fn(&Shared)>,
    projection: fn(&Context) -> &JoinValueLane<K, V>,
    lifecycle_fac: F,
}

impl<Context, Shared, K, V, F> Clone for RegisterJoinValue<Context, Shared, K, V, F>
where
    F: Clone,
{
    fn clone(&self) -> Self {
        Self {
            _type: PhantomData,
            projection: self.projection,
            lifecycle_fac: self.lifecycle_fac.clone(),
        }
    }
}

impl<Context, Shared, K, V, F> RegisterJoinValue<Context, Shared, K, V, F> {
    /// # Arguments
    /// * `projection` - Projection from the agent type to the lane.
    /// * `lifecycle_fac` - A factory that will create lifecycle instances for each key of the
    ///   join value lane.
    pub fn new(projection: fn(&Context) -> &JoinValueLane<K, V>, lifecycle_fac: F) -> Self {
        RegisterJoinValue {
            _type: PhantomData,
            projection,
            lifecycle_fac,
        }
    }
}

impl<Context, Shared, K, V, F, LC> OnInitShared<Context, Shared>
    for RegisterJoinValue<Context, Shared, K, V, F>
where
    Context: 'static,
    K: Clone + Eq + Hash + Send + 'static,
    V: Form + Send + Sync + 'static,
    V::Rec: Send,
    F: Fn(&Shared, JoinValueContext<Context, K, V>) -> LC + Send + Clone + 'static,
    LC: JoinValueLaneLifecycle<K, V, Context> + 'static,
{
    fn initialize(
        &self,
        shared: &Shared,
        action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        context: &Context,
    ) {
        let RegisterJoinValue {
            projection,
            lifecycle_fac,
            ..
        } = self;
        let lc = lifecycle_fac(shared, Default::default());
        let fac = move || lc.clone();
        let init: BoxJoinLaneInit<'static, Context> =
            Box::new(join_value::LifecycleInitializer::new(*projection, fac));
        let lane_id = projection(context).id();
        action_context.register_join_lane_initializer(lane_id, init);
    }
}

/// An initializer that will register the downlink lifecycle for a join map lane in the context.
pub struct RegisterJoinMap<Context, Shared, L, K, V, F> {
    _type: PhantomData<fn(&Shared)>,
    projection: fn(&Context) -> &JoinMapLane<L, K, V>,
    lifecycle_fac: F,
}

impl<Context, Shared, L, K, V, F> Clone for RegisterJoinMap<Context, Shared, L, K, V, F>
where
    F: Clone,
{
    fn clone(&self) -> Self {
        Self {
            _type: PhantomData,
            projection: self.projection,
            lifecycle_fac: self.lifecycle_fac.clone(),
        }
    }
}

impl<Context, Shared, L, K, V, F> RegisterJoinMap<Context, Shared, L, K, V, F> {
    /// # Arguments
    /// * `projection` - Projection from the agent type to the lane.
    /// * `lifecycle_fac` - A factory that will create lifecycle instances for each key of the
    ///   join map lane.
    pub fn new(projection: fn(&Context) -> &JoinMapLane<L, K, V>, lifecycle_fac: F) -> Self {
        RegisterJoinMap {
            _type: PhantomData,
            projection,
            lifecycle_fac,
        }
    }
}

impl<Context, Shared, L, K, V, F, LC> OnInitShared<Context, Shared>
    for RegisterJoinMap<Context, Shared, L, K, V, F>
where
    Context: 'static,
    L: Clone + Eq + Hash + Send + 'static,
    K: Clone + Form + Eq + Hash + Ord + Send + 'static,
    V: Form + Send + Sync + 'static,
    K::Rec: Send,
    V::BodyRec: Send,
    F: Fn(&Shared, JoinMapContext<Context, L, K, V>) -> LC + Send + Clone + 'static,
    LC: JoinMapLaneLifecycle<L, K, Context> + 'static,
{
    fn initialize(
        &self,
        shared: &Shared,
        action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        context: &Context,
    ) {
        let RegisterJoinMap {
            projection,
            lifecycle_fac,
            ..
        } = self;
        let lc = lifecycle_fac(shared, Default::default());
        let fac = move || lc.clone();
        let init: BoxJoinLaneInit<'static, Context> =
            Box::new(join_map::LifecycleInitializer::new(*projection, fac));
        let lane_id = projection(context).id();
        action_context.register_join_lane_initializer(lane_id, init);
    }
}
