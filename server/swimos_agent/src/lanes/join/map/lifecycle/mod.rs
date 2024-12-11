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

use std::{collections::HashSet, marker::PhantomData};

use swimos_api::address::Address;
use swimos_utilities::handlers::{FnHandler, NoHandler};

use crate::{
    agent_lifecycle::HandlerContext,
    event_handler::HandlerAction,
    lifecycle_fn::{LiftShared, WithHandlerContext},
};

use self::{
    on_failed::{OnJoinMapFailed, OnJoinMapFailedShared},
    on_linked::{OnJoinMapLinked, OnJoinMapLinkedShared},
    on_synced::{OnJoinMapSynced, OnJoinMapSyncedShared},
    on_unlinked::{OnJoinMapUnlinked, OnJoinMapUnlinkedShared},
};

pub mod on_failed;
pub mod on_linked;
pub mod on_synced;
pub mod on_unlinked;

/// Trait for the lifecycle of an join map lane lifecycle (in fact, a specialized map downlink
/// lifecycle).
///
/// #Node
/// All implementations of this interface must be [`Clone`] as it needs to be duplicated for each
/// entry in the join map lane map.
///
/// # Type Parameters
/// * `L` - The type of the labels for links.
/// * `K` - The key type of the join map lane.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
pub trait JoinMapLaneLifecycle<L, K, Context>:
    OnJoinMapLinked<L, Context>
    + OnJoinMapSynced<L, K, Context>
    + OnJoinMapUnlinked<L, K, Context>
    + OnJoinMapFailed<L, K, Context>
    + Clone
{
}

impl<L, K, Context, LC> JoinMapLaneLifecycle<L, K, Context> for LC where
    LC: OnJoinMapLinked<L, Context>
        + OnJoinMapSynced<L, K, Context>
        + OnJoinMapUnlinked<L, K, Context>
        + OnJoinMapFailed<L, K, Context>
        + Clone
{
}

/// A lifecycle for an join map downlink where the individual event handlers do not share state.
///
/// # Type Parameters
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `L` - The type of the labels for links.
/// * `K` - The key type of the join map lane.
pub trait StatelessJoinMapLifecycle<Context, L, K>: JoinMapLaneLifecycle<L, K, Context> {
    type WithOnLinked<H>: StatelessJoinMapLifecycle<Context, L, K>
    where
        H: OnJoinMapLinked<L, Context> + Clone;

    type WithOnSynced<H>: StatelessJoinMapLifecycle<Context, L, K>
    where
        H: OnJoinMapSynced<L, K, Context> + Clone;

    type WithOnUnlinked<H>: StatelessJoinMapLifecycle<Context, L, K>
    where
        H: OnJoinMapUnlinked<L, K, Context> + Clone;

    type WithOnFailed<H>: StatelessJoinMapLifecycle<Context, L, K>
    where
        H: OnJoinMapFailed<L, K, Context> + Clone;

    type WithShared<Shared>: StatefulJoinMapLifecycle<Context, Shared, L, K>
    where
        Shared: Send + Clone;

    fn on_linked<F>(self, handler: F) -> Self::WithOnLinked<WithHandlerContext<F>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinMapLinked<L, Context>;

    fn on_synced<F>(self, handler: F) -> Self::WithOnSynced<WithHandlerContext<F>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinMapSynced<L, K, Context>;

    fn on_unlinked<F>(self, handler: F) -> Self::WithOnUnlinked<WithHandlerContext<F>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinMapUnlinked<L, K, Context>;

    fn on_failed<F>(self, handler: F) -> Self::WithOnFailed<WithHandlerContext<F>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinMapFailed<L, K, Context>;

    fn with_shared_state<Shared: Send + Clone>(self, shared: Shared) -> Self::WithShared<Shared>;
}

/// A lifecycle for an join map downlink where the individual event handlers have shared state.
///
/// # Type Parameters
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `Shared` - The type of the shared state.
/// * `L` - The type of the labels for links.
/// * `K` - The key type of the join map lane.
pub trait StatefulJoinMapLifecycle<Context, Shared, L, K>:
    JoinMapLaneLifecycle<L, K, Context>
{
    type WithOnLinked<H>: StatefulJoinMapLifecycle<Context, Shared, L, K>
    where
        H: OnJoinMapLinkedShared<L, Context, Shared> + Clone;

    type WithOnSynced<H>: StatefulJoinMapLifecycle<Context, Shared, L, K>
    where
        H: OnJoinMapSyncedShared<L, K, Context, Shared> + Clone;

    type WithOnUnlinked<H>: StatefulJoinMapLifecycle<Context, Shared, L, K>
    where
        H: OnJoinMapUnlinkedShared<L, K, Context, Shared> + Clone;

    type WithOnFailed<H>: StatefulJoinMapLifecycle<Context, Shared, L, K>
    where
        H: OnJoinMapFailedShared<L, K, Context, Shared> + Clone;

    fn on_linked<F>(self, handler: F) -> Self::WithOnLinked<FnHandler<F>>
    where
        F: Clone,
        FnHandler<F>: OnJoinMapLinkedShared<L, Context, Shared> + Clone;

    fn on_synced<F>(self, handler: F) -> Self::WithOnSynced<FnHandler<F>>
    where
        F: Clone,
        FnHandler<F>: OnJoinMapSyncedShared<L, K, Context, Shared>;

    fn on_unlinked<F>(self, handler: F) -> Self::WithOnUnlinked<FnHandler<F>>
    where
        F: Clone,
        FnHandler<F>: OnJoinMapUnlinkedShared<L, K, Context, Shared>;

    fn on_failed<F>(self, handler: F) -> Self::WithOnFailed<FnHandler<F>>
    where
        F: Clone,
        FnHandler<F>: OnJoinMapFailedShared<L, K, Context, Shared>;
}

/// A lifecycle for a join map downlink where the event handlers do not share state.
///
/// # Type Parameters
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `L` - The type of the labels for links.
/// * `K` - The key type of the join map lane.
/// * `FLinked` - The type of the 'on_linked' handler.
/// * `FSynced` - The type of the 'on_synced' handler.
/// * `FUnlinked` - The type of the 'on_unlinked' handler.
/// * `FFailed` - The type of the 'on_failed handler.
#[derive(Debug)]
pub struct StatelessJoinMapLaneLifecycle<
    Context,
    L,
    K,
    FLinked = NoHandler,
    FSynced = NoHandler,
    FUnlinked = NoHandler,
    FFailed = NoHandler,
> {
    _type: PhantomData<fn(&Context, L, K)>,
    on_linked: FLinked,
    on_synced: FSynced,
    on_unlinked: FUnlinked,
    on_failed: FFailed,
}

impl<Context, L, K> Default for StatelessJoinMapLaneLifecycle<Context, L, K> {
    fn default() -> Self {
        Self {
            _type: Default::default(),
            on_linked: Default::default(),
            on_synced: Default::default(),
            on_unlinked: Default::default(),
            on_failed: Default::default(),
        }
    }
}

/// A lifecycle for a join map downlink where the event handlers can share state.
///
/// # Type Parameters
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `State` - The type of the shared state.
/// * `L` - The type of the labels for links.
/// * `K` - The key type of the join map lane.
/// * `FLinked` - The type of the 'on_linked' handler.
/// * `FSynced` - The type of the 'on_synced' handler.
/// * `FUnlinked` - The type of the 'on_unlinked' handler.
/// * `FFailed` - The type of the 'on_failed handler.
#[derive(Debug)]
pub struct StatefulJoinMapLaneLifecycle<
    Context,
    State,
    L,
    K,
    FLinked = NoHandler,
    FSynced = NoHandler,
    FUnlinked = NoHandler,
    FFailed = NoHandler,
> {
    _type: PhantomData<fn(L, K)>,
    state: State,
    handler_context: HandlerContext<Context>,
    on_linked: FLinked,
    on_synced: FSynced,
    on_unlinked: FUnlinked,
    on_failed: FFailed,
}

impl<Context, State, L, K> StatefulJoinMapLaneLifecycle<Context, State, L, K> {
    pub fn new(state: State) -> Self {
        StatefulJoinMapLaneLifecycle {
            _type: PhantomData,
            state,
            handler_context: Default::default(),
            on_linked: Default::default(),
            on_synced: Default::default(),
            on_unlinked: Default::default(),
            on_failed: Default::default(),
        }
    }
}

impl<Context, State, L, K, FLinked, FSynced, FUnlinked, FFailed> Clone
    for StatefulJoinMapLaneLifecycle<Context, State, L, K, FLinked, FSynced, FUnlinked, FFailed>
where
    State: Clone,
    FLinked: Clone,
    FSynced: Clone,
    FUnlinked: Clone,
    FFailed: Clone,
{
    fn clone(&self) -> Self {
        Self {
            _type: PhantomData,
            state: self.state.clone(),
            handler_context: Default::default(),
            on_linked: self.on_linked.clone(),
            on_synced: self.on_synced.clone(),
            on_unlinked: self.on_unlinked.clone(),
            on_failed: self.on_failed.clone(),
        }
    }
}

impl<Context, L, K, FLinked, FSynced, FUnlinked, FFailed> Clone
    for StatelessJoinMapLaneLifecycle<Context, L, K, FLinked, FSynced, FUnlinked, FFailed>
where
    FLinked: Clone,
    FSynced: Clone,
    FUnlinked: Clone,
    FFailed: Clone,
{
    fn clone(&self) -> Self {
        Self {
            _type: PhantomData,
            on_linked: self.on_linked.clone(),
            on_synced: self.on_synced.clone(),
            on_unlinked: self.on_unlinked.clone(),
            on_failed: self.on_failed.clone(),
        }
    }
}

impl<Context, State, L, K, FLinked, FSynced, FUnlinked, FFailed> OnJoinMapLinked<L, Context>
    for StatefulJoinMapLaneLifecycle<Context, State, L, K, FLinked, FSynced, FUnlinked, FFailed>
where
    State: Send,
    FLinked: OnJoinMapLinkedShared<L, Context, State>,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
{
    type OnJoinMapLinkedHandler<'a>
        = FLinked::OnJoinMapLinkedHandler<'a>
    where
        Self: 'a;

    fn on_linked<'a>(&'a self, link: L, remote: Address<&str>) -> Self::OnJoinMapLinkedHandler<'a> {
        let StatefulJoinMapLaneLifecycle {
            on_linked,
            state,
            handler_context,
            ..
        } = self;
        on_linked.on_linked(state, *handler_context, link, remote)
    }
}

impl<Context, State, L, K, FLinked, FSynced, FUnlinked, FFailed> OnJoinMapSynced<L, K, Context>
    for StatefulJoinMapLaneLifecycle<Context, State, L, K, FLinked, FSynced, FUnlinked, FFailed>
where
    State: Send,
    FLinked: Send,
    FSynced: OnJoinMapSyncedShared<L, K, Context, State>,
    FUnlinked: Send,
    FFailed: Send,
{
    type OnJoinMapSyncedHandler<'a>
        = FSynced::OnJoinMapSyncedHandler<'a>
    where
        Self: 'a;

    fn on_synced<'a>(
        &'a self,
        link: L,
        remote: Address<&str>,
        keys: &HashSet<K>,
    ) -> Self::OnJoinMapSyncedHandler<'a> {
        let StatefulJoinMapLaneLifecycle {
            on_synced,
            state,
            handler_context,
            ..
        } = self;
        on_synced.on_synced(state, *handler_context, link, remote, keys)
    }
}

impl<Context, State, L, K, FLinked, FSynced, FUnlinked, FFailed> OnJoinMapUnlinked<L, K, Context>
    for StatefulJoinMapLaneLifecycle<Context, State, L, K, FLinked, FSynced, FUnlinked, FFailed>
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: OnJoinMapUnlinkedShared<L, K, Context, State>,
    FFailed: Send,
{
    type OnJoinMapUnlinkedHandler<'a>
        = FUnlinked::OnJoinMapUnlinkedHandler<'a>
    where
        Self: 'a;

    fn on_unlinked<'a>(
        &'a self,
        link: L,
        remote: Address<&str>,
        keys: HashSet<K>,
    ) -> Self::OnJoinMapUnlinkedHandler<'a> {
        let StatefulJoinMapLaneLifecycle {
            on_unlinked,
            state,
            handler_context,
            ..
        } = self;
        on_unlinked.on_unlinked(state, *handler_context, link, remote, keys)
    }
}

impl<Context, State, L, K, FLinked, FSynced, FUnlinked, FFailed> OnJoinMapFailed<L, K, Context>
    for StatefulJoinMapLaneLifecycle<Context, State, L, K, FLinked, FSynced, FUnlinked, FFailed>
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: OnJoinMapFailedShared<L, K, Context, State>,
{
    type OnJoinMapFailedHandler<'a>
        = FFailed::OnJoinMapFailedHandler<'a>
    where
        Self: 'a;

    fn on_failed<'a>(
        &'a self,
        link: L,
        remote: Address<&str>,
        keys: HashSet<K>,
    ) -> Self::OnJoinMapFailedHandler<'a> {
        let StatefulJoinMapLaneLifecycle {
            on_failed,
            state,
            handler_context,
            ..
        } = self;
        on_failed.on_failed(state, *handler_context, link, remote, keys)
    }
}

impl<Context, State, L, K, FLinked, FSynced, FUnlinked, FFailed>
    StatefulJoinMapLifecycle<Context, State, L, K>
    for StatefulJoinMapLaneLifecycle<Context, State, L, K, FLinked, FSynced, FUnlinked, FFailed>
where
    State: Send + Clone,
    FLinked: OnJoinMapLinkedShared<L, Context, State> + Clone,
    FSynced: OnJoinMapSyncedShared<L, K, Context, State> + Clone,
    FUnlinked: OnJoinMapUnlinkedShared<L, K, Context, State> + Clone,
    FFailed: OnJoinMapFailedShared<L, K, Context, State> + Clone,
{
    type WithOnLinked<H>
        = StatefulJoinMapLaneLifecycle<Context, State, L, K, H, FSynced, FUnlinked, FFailed>
    where
        H: OnJoinMapLinkedShared<L, Context, State> + Clone;

    type WithOnSynced<H>
        = StatefulJoinMapLaneLifecycle<Context, State, L, K, FLinked, H, FUnlinked, FFailed>
    where
        H: OnJoinMapSyncedShared<L, K, Context, State> + Clone;

    type WithOnUnlinked<H>
        = StatefulJoinMapLaneLifecycle<Context, State, L, K, FLinked, FSynced, H, FFailed>
    where
        H: OnJoinMapUnlinkedShared<L, K, Context, State> + Clone;

    type WithOnFailed<H>
        = StatefulJoinMapLaneLifecycle<Context, State, L, K, FLinked, FSynced, FUnlinked, H>
    where
        H: OnJoinMapFailedShared<L, K, Context, State> + Clone;

    fn on_linked<F>(self, handler: F) -> Self::WithOnLinked<FnHandler<F>>
    where
        F: Clone,
        FnHandler<F>: OnJoinMapLinkedShared<L, Context, State>,
    {
        StatefulJoinMapLaneLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: FnHandler(handler),
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
        }
    }

    fn on_synced<F>(self, handler: F) -> Self::WithOnSynced<FnHandler<F>>
    where
        F: Clone,
        FnHandler<F>: OnJoinMapSyncedShared<L, K, Context, State>,
    {
        StatefulJoinMapLaneLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: FnHandler(handler),
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
        }
    }

    fn on_unlinked<F>(self, handler: F) -> Self::WithOnUnlinked<FnHandler<F>>
    where
        F: Clone,
        FnHandler<F>: OnJoinMapUnlinkedShared<L, K, Context, State>,
    {
        StatefulJoinMapLaneLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: FnHandler(handler),
            on_failed: self.on_failed,
        }
    }

    fn on_failed<F>(self, handler: F) -> Self::WithOnFailed<FnHandler<F>>
    where
        F: Clone,
        FnHandler<F>: OnJoinMapFailedShared<L, K, Context, State>,
    {
        StatefulJoinMapLaneLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: FnHandler(handler),
        }
    }
}

impl<Context, L, K, FLinked, FSynced, FUnlinked, FFailed> OnJoinMapLinked<L, Context>
    for StatelessJoinMapLaneLifecycle<Context, L, K, FLinked, FSynced, FUnlinked, FFailed>
where
    FLinked: OnJoinMapLinked<L, Context>,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
{
    type OnJoinMapLinkedHandler<'a>
        = FLinked::OnJoinMapLinkedHandler<'a>
    where
        Self: 'a;

    fn on_linked<'a>(&'a self, link: L, remote: Address<&str>) -> Self::OnJoinMapLinkedHandler<'a> {
        let StatelessJoinMapLaneLifecycle { on_linked, .. } = self;
        on_linked.on_linked(link, remote)
    }
}

impl<Context, L, K, FLinked, FSynced, FUnlinked, FFailed> OnJoinMapSynced<L, K, Context>
    for StatelessJoinMapLaneLifecycle<Context, L, K, FLinked, FSynced, FUnlinked, FFailed>
where
    FLinked: Send,
    FSynced: OnJoinMapSynced<L, K, Context>,
    FUnlinked: Send,
    FFailed: Send,
{
    type OnJoinMapSyncedHandler<'a>
        = FSynced::OnJoinMapSyncedHandler<'a>
    where
        Self: 'a;

    fn on_synced<'a>(
        &'a self,
        link: L,
        remote: Address<&str>,
        keys: &HashSet<K>,
    ) -> Self::OnJoinMapSyncedHandler<'a> {
        let StatelessJoinMapLaneLifecycle { on_synced, .. } = self;
        on_synced.on_synced(link, remote, keys)
    }
}

impl<Context, L, K, FLinked, FSynced, FUnlinked, FFailed> OnJoinMapUnlinked<L, K, Context>
    for StatelessJoinMapLaneLifecycle<Context, L, K, FLinked, FSynced, FUnlinked, FFailed>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: OnJoinMapUnlinked<L, K, Context>,
    FFailed: Send,
{
    type OnJoinMapUnlinkedHandler<'a>
        = FUnlinked::OnJoinMapUnlinkedHandler<'a>
    where
        Self: 'a;

    fn on_unlinked<'a>(
        &'a self,
        link: L,
        remote: Address<&str>,
        keys: HashSet<K>,
    ) -> Self::OnJoinMapUnlinkedHandler<'a> {
        let StatelessJoinMapLaneLifecycle { on_unlinked, .. } = self;
        on_unlinked.on_unlinked(link, remote, keys)
    }
}

impl<Context, L, K, FLinked, FSynced, FUnlinked, FFailed> OnJoinMapFailed<L, K, Context>
    for StatelessJoinMapLaneLifecycle<Context, L, K, FLinked, FSynced, FUnlinked, FFailed>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: OnJoinMapFailed<L, K, Context>,
{
    type OnJoinMapFailedHandler<'a>
        = FFailed::OnJoinMapFailedHandler<'a>
    where
        Self: 'a;

    fn on_failed<'a>(
        &'a self,
        link: L,
        remote: Address<&str>,
        keys: HashSet<K>,
    ) -> Self::OnJoinMapFailedHandler<'a> {
        let StatelessJoinMapLaneLifecycle { on_failed, .. } = self;
        on_failed.on_failed(link, remote, keys)
    }
}

impl<Context, L, K, FLinked, FSynced, FUnlinked, FFailed> StatelessJoinMapLifecycle<Context, L, K>
    for StatelessJoinMapLaneLifecycle<Context, L, K, FLinked, FSynced, FUnlinked, FFailed>
where
    FLinked: OnJoinMapLinked<L, Context> + Clone,
    FSynced: OnJoinMapSynced<L, K, Context> + Clone,
    FUnlinked: OnJoinMapUnlinked<L, K, Context> + Clone,
    FFailed: OnJoinMapFailed<L, K, Context> + Clone,
{
    type WithOnLinked<H>
        = StatelessJoinMapLaneLifecycle<Context, L, K, H, FSynced, FUnlinked, FFailed>
    where
        H: OnJoinMapLinked<L, Context> + Clone;

    type WithOnSynced<H>
        = StatelessJoinMapLaneLifecycle<Context, L, K, FLinked, H, FUnlinked, FFailed>
    where
        H: OnJoinMapSynced<L, K, Context> + Clone;

    type WithOnUnlinked<H>
        = StatelessJoinMapLaneLifecycle<Context, L, K, FLinked, FSynced, H, FFailed>
    where
        H: OnJoinMapUnlinked<L, K, Context> + Clone;

    type WithOnFailed<H>
        = StatelessJoinMapLaneLifecycle<Context, L, K, FLinked, FSynced, FUnlinked, H>
    where
        H: OnJoinMapFailed<L, K, Context> + Clone;

    type WithShared<Shared>
        = StatefulJoinMapLaneLifecycle<
        Context,
        Shared,
        L,
        K,
        LiftShared<FLinked, Shared>,
        LiftShared<FSynced, Shared>,
        LiftShared<FUnlinked, Shared>,
        LiftShared<FFailed, Shared>,
    >
    where
        Shared: Send + Clone;

    fn on_linked<F>(self, handler: F) -> Self::WithOnLinked<WithHandlerContext<F>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinMapLinked<L, Context>,
    {
        StatelessJoinMapLaneLifecycle {
            _type: PhantomData,
            on_linked: WithHandlerContext::new(handler),
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
        }
    }

    fn on_synced<F>(self, handler: F) -> Self::WithOnSynced<WithHandlerContext<F>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinMapSynced<L, K, Context>,
    {
        StatelessJoinMapLaneLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: WithHandlerContext::new(handler),
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
        }
    }

    fn on_unlinked<F>(self, handler: F) -> Self::WithOnUnlinked<WithHandlerContext<F>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinMapUnlinked<L, K, Context>,
    {
        StatelessJoinMapLaneLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: WithHandlerContext::new(handler),
            on_failed: self.on_failed,
        }
    }

    fn on_failed<F>(self, handler: F) -> Self::WithOnFailed<WithHandlerContext<F>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinMapFailed<L, K, Context>,
    {
        StatelessJoinMapLaneLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: WithHandlerContext::new(handler),
        }
    }

    fn with_shared_state<Shared: Send + Clone>(self, shared: Shared) -> Self::WithShared<Shared> {
        StatefulJoinMapLaneLifecycle {
            _type: PhantomData,
            state: shared,
            handler_context: Default::default(),
            on_linked: LiftShared::new(self.on_linked),
            on_synced: LiftShared::new(self.on_synced),
            on_unlinked: LiftShared::new(self.on_unlinked),
            on_failed: LiftShared::new(self.on_failed),
        }
    }
}

pub trait JoinMapHandlerSyncedFn<'a, Context, Shared, L, K, Out> {
    type Handler: HandlerAction<Context, Completion = Out> + 'a;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        link: L,
        remote: Address<&str>,
        keys: &HashSet<K>,
    ) -> Self::Handler;
}

impl<'a, Context, Shared, L, K, F, H, Out> JoinMapHandlerSyncedFn<'a, Context, Shared, L, K, Out>
    for F
where
    F: Fn(&'a Shared, HandlerContext<Context>, L, Address<&str>, &HashSet<K>) -> H,
    H: HandlerAction<Context, Completion = Out> + 'a,
{
    type Handler = H;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        link: L,
        remote: Address<&str>,
        keys: &HashSet<K>,
    ) -> Self::Handler {
        (*self)(shared, handler_context, link, remote, keys)
    }
}

pub trait JoinMapHandlerStoppedFn<'a, Context, Shared, L, K, Out> {
    type Handler: HandlerAction<Context, Completion = Out> + 'a;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        link: L,
        remote: Address<&str>,
        keys: HashSet<K>,
    ) -> Self::Handler;
}

impl<'a, Context, Shared, L, K, F, H, Out> JoinMapHandlerStoppedFn<'a, Context, Shared, L, K, Out>
    for F
where
    F: Fn(&'a Shared, HandlerContext<Context>, L, Address<&str>, HashSet<K>) -> H,
    H: HandlerAction<Context, Completion = Out> + 'a,
{
    type Handler = H;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        link: L,
        remote: Address<&str>,
        keys: HashSet<K>,
    ) -> Self::Handler {
        (*self)(shared, handler_context, link, remote, keys)
    }
}
