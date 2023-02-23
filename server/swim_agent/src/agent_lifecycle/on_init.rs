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

use std::{hash::Hash, marker::PhantomData};

use swim_api::handlers::{FnHandler, NoHandler};
use swim_form::Form;

use crate::{
    event_handler::{ActionContext, BoxJoinValueInit},
    item::AgentItem,
    lanes::{
        join_value::{lifecycle::JoinValueLaneLifecycle, LifecycleInitializer},
        JoinValueLane,
    },
    meta::AgentMetadata,
};

use super::utility::JoinValueContext;

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

pub type InitNil = NoHandler;

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

pub struct JoinValueInit<Context, Shared, K, V, F> {
    _type: PhantomData<fn(&Shared)>,
    projection: fn(&Context) -> &JoinValueLane<K, V>,
    lifecycle_fac: F,
}

impl<Context, Shared, K, V, F> JoinValueInit<Context, Shared, K, V, F> {
    pub fn new(projection: fn(&Context) -> &JoinValueLane<K, V>, lifecycle_fac: F) -> Self {
        JoinValueInit {
            _type: PhantomData,
            projection,
            lifecycle_fac,
        }
    }
}

impl<Context, Shared, K, V, F, LC> OnInitShared<Context, Shared>
    for JoinValueInit<Context, Shared, K, V, F>
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
        let JoinValueInit {
            projection,
            lifecycle_fac,
            ..
        } = self;
        let lc = lifecycle_fac(shared, Default::default());
        let fac = move || lc.clone();
        let init: BoxJoinValueInit<'static, Context> =
            Box::new(LifecycleInitializer::new(*projection, fac));
        let lane_id = projection(context).id();
        action_context.register_join_value_initializer(lane_id, init);
    }
}
