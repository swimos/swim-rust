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

use swimos_api::address::Address;
use swimos_utilities::handlers::{BorrowHandler, FnHandler, NoHandler};

use crate::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::EventHandler,
    lifecycle_fn::{LiftShared, WithHandlerContext, WithHandlerContextBorrow},
};

use self::{
    on_failed::{OnJoinValueFailed, OnJoinValueFailedShared},
    on_linked::{OnJoinValueLinked, OnJoinValueLinkedShared},
    on_synced::{OnJoinValueSynced, OnJoinValueSyncedShared},
    on_unlinked::{OnJoinValueUnlinked, OnJoinValueUnlinkedShared},
};

pub mod on_failed;
pub mod on_linked;
pub mod on_synced;
pub mod on_unlinked;

/// Trait for the lifecycle of an join value lane lifecycle (in fact, a specialized event downlink
/// lifecycle).
///
/// #Node
/// All implementations of this interface must be [`Clone`] as it needs to be duplicated for each
/// entry in the join value lane map.
///
/// # Type Parameters
/// * `K` - The key type of the join value lane.
/// * `V` - The value type of the join value lane.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
pub trait JoinValueLaneLifecycle<K, V, Context>:
    OnJoinValueLinked<K, Context>
    + OnJoinValueSynced<K, V, Context>
    + OnJoinValueUnlinked<K, Context>
    + OnJoinValueFailed<K, Context>
    + Clone
{
}

impl<K, V, Context, L> JoinValueLaneLifecycle<K, V, Context> for L where
    L: OnJoinValueLinked<K, Context>
        + OnJoinValueSynced<K, V, Context>
        + OnJoinValueUnlinked<K, Context>
        + OnJoinValueFailed<K, Context>
        + Clone
{
}

/// A lifecycle for an join value downlink where the individual event handlers do not share state.
///
/// # Type Parameters
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `K` - The key type of the join value lane.
/// * `V` - THe value type of the join value lane.
pub trait StatelessJoinValueLifecycle<Context, K, V>:
    JoinValueLaneLifecycle<K, V, Context>
{
    type WithOnLinked<H>: StatelessJoinValueLifecycle<Context, K, V>
    where
        H: OnJoinValueLinked<K, Context> + Clone;

    type WithOnSynced<H>: StatelessJoinValueLifecycle<Context, K, V>
    where
        H: OnJoinValueSynced<K, V, Context> + Clone;

    type WithOnUnlinked<H>: StatelessJoinValueLifecycle<Context, K, V>
    where
        H: OnJoinValueUnlinked<K, Context> + Clone;

    type WithOnFailed<H>: StatelessJoinValueLifecycle<Context, K, V>
    where
        H: OnJoinValueFailed<K, Context> + Clone;

    type WithShared<Shared>: StatefulJoinValueLifecycle<Context, Shared, K, V>
    where
        Shared: Send + Clone;

    fn on_linked<F>(self, handler: F) -> Self::WithOnLinked<WithHandlerContext<F>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinValueLinked<K, Context>;

    fn on_synced<F, B>(self, handler: F) -> Self::WithOnSynced<WithHandlerContextBorrow<F, B>>
    where
        F: Clone,
        B: ?Sized,
        V: Borrow<B>,
        WithHandlerContextBorrow<F, B>: OnJoinValueSynced<K, V, Context>;

    fn on_unlinked<F>(self, handler: F) -> Self::WithOnUnlinked<WithHandlerContext<F>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinValueUnlinked<K, Context>;

    fn on_failed<F>(self, handler: F) -> Self::WithOnFailed<WithHandlerContext<F>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinValueFailed<K, Context>;

    fn with_shared_state<Shared: Send + Clone>(self, shared: Shared) -> Self::WithShared<Shared>;
}

/// A lifecycle for an join value downlink where the individual event handlers have shared state.
///
/// # Type Parameters
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `Shared` - The type of the shared state.
/// * `K` - The key type of the join value lane.
/// * `V` - THe value type of the join value lane.
pub trait StatefulJoinValueLifecycle<Context, Shared, K, V>:
    JoinValueLaneLifecycle<K, V, Context>
{
    type WithOnLinked<H>: StatefulJoinValueLifecycle<Context, Shared, K, V>
    where
        H: OnJoinValueLinkedShared<K, Context, Shared> + Clone;

    type WithOnSynced<H>: StatefulJoinValueLifecycle<Context, Shared, K, V>
    where
        H: OnJoinValueSyncedShared<K, V, Context, Shared> + Clone;

    type WithOnUnlinked<H>: StatefulJoinValueLifecycle<Context, Shared, K, V>
    where
        H: OnJoinValueUnlinkedShared<K, Context, Shared> + Clone;

    type WithOnFailed<H>: StatefulJoinValueLifecycle<Context, Shared, K, V>
    where
        H: OnJoinValueFailedShared<K, Context, Shared> + Clone;

    fn on_linked<F>(self, handler: F) -> Self::WithOnLinked<FnHandler<F>>
    where
        F: Clone,
        FnHandler<F>: OnJoinValueLinkedShared<K, Context, Shared> + Clone;

    fn on_synced<F, B>(self, handler: F) -> Self::WithOnSynced<BorrowHandler<F, B>>
    where
        F: Clone,
        B: ?Sized,
        V: Borrow<B>,
        BorrowHandler<F, B>: OnJoinValueSyncedShared<K, V, Context, Shared>;

    fn on_unlinked<F>(self, handler: F) -> Self::WithOnUnlinked<FnHandler<F>>
    where
        F: Clone,
        FnHandler<F>: OnJoinValueUnlinkedShared<K, Context, Shared>;

    fn on_failed<F>(self, handler: F) -> Self::WithOnFailed<FnHandler<F>>
    where
        F: Clone,
        FnHandler<F>: OnJoinValueFailedShared<K, Context, Shared>;
}

/// A lifecycle for a join value downlink where the event handlers do not share state.
///
/// # Type Parameters
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `K` - The type of the keys of the join value lane.
/// * `V` - The type of the values of the join value lane.
/// * `FLinked` - The type of the 'on_linked' handler.
/// * `FSynced` - The type of the 'on_synced' handler.
/// * `FUnlinked` - The type of the 'on_unlinked' handler.
/// * `FFailed` - The type of the 'on_failed handler.
#[derive(Debug)]
pub struct StatelessJoinValueLaneLifecycle<
    Context,
    K,
    V,
    FLinked = NoHandler,
    FSynced = NoHandler,
    FUnlinked = NoHandler,
    FFailed = NoHandler,
> {
    _type: PhantomData<fn(&Context, K, V)>,
    on_linked: FLinked,
    on_synced: FSynced,
    on_unlinked: FUnlinked,
    on_failed: FFailed,
}

/// A lifecycle for a join value downlink where the event handlers can share state.
///
/// # Type Parameters
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `State` - The type of the shared state.
/// * `K` - The type of the keys of the join value lane.
/// * `V` - The type of the values of the join value lane.
/// * `FLinked` - The type of the 'on_linked' handler.
/// * `FSynced` - The type of the 'on_synced' handler.
/// * `FUnlinked` - The type of the 'on_unlinked' handler.
/// * `FFailed` - The type of the 'on_failed handler.
#[derive(Debug)]
pub struct StatefulJoinValueLaneLifecycle<
    Context,
    State,
    K,
    V,
    FLinked = NoHandler,
    FSynced = NoHandler,
    FUnlinked = NoHandler,
    FFailed = NoHandler,
> {
    _type: PhantomData<fn(K, V)>,
    state: State,
    handler_context: HandlerContext<Context>,
    on_linked: FLinked,
    on_synced: FSynced,
    on_unlinked: FUnlinked,
    on_failed: FFailed,
}

impl<Context, K, V> Default for StatelessJoinValueLaneLifecycle<Context, K, V> {
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

impl<Context, State, K, V> StatefulJoinValueLaneLifecycle<Context, State, K, V> {
    pub fn new(state: State) -> Self {
        StatefulJoinValueLaneLifecycle {
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

impl<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed> Clone
    for StatefulJoinValueLaneLifecycle<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed>
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

impl<Context, K, V, FLinked, FSynced, FUnlinked, FFailed> Clone
    for StatelessJoinValueLaneLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FFailed>
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

pub trait JoinValueSyncFn<'a, Context, Shared, K, V: ?Sized> {
    type Handler: EventHandler<Context> + 'a;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
        remote: Address<&str>,
        value: Option<&V>,
    ) -> Self::Handler;
}

impl<'a, Context, Shared, K, V, F, H> JoinValueSyncFn<'a, Context, Shared, K, V> for F
where
    V: ?Sized,
    F: Fn(&'a Shared, HandlerContext<Context>, K, Address<&str>, Option<&V>) -> H,
    H: EventHandler<Context> + 'a,
{
    type Handler = H;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
        remote: Address<&str>,
        value: Option<&V>,
    ) -> Self::Handler {
        (*self)(shared, handler_context, key, remote, value)
    }
}

impl<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed> OnJoinValueLinked<K, Context>
    for StatefulJoinValueLaneLifecycle<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed>
where
    State: Send,
    FLinked: OnJoinValueLinkedShared<K, Context, State>,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
{
    type OnJoinValueLinkedHandler<'a> = FLinked::OnJoinValueLinkedHandler<'a>
    where
        Self: 'a;

    fn on_linked<'a>(
        &'a self,
        key: K,
        remote: Address<&str>,
    ) -> Self::OnJoinValueLinkedHandler<'a> {
        let StatefulJoinValueLaneLifecycle {
            on_linked,
            state,
            handler_context,
            ..
        } = self;
        on_linked.on_linked(state, *handler_context, key, remote)
    }
}

impl<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed> OnJoinValueSynced<K, V, Context>
    for StatefulJoinValueLaneLifecycle<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed>
where
    State: Send,
    FLinked: Send,
    FSynced: OnJoinValueSyncedShared<K, V, Context, State>,
    FUnlinked: Send,
    FFailed: Send,
{
    type OnJoinValueSyncedHandler<'a> = FSynced::OnJoinValueSyncedHandler<'a>
    where
        Self: 'a;

    fn on_synced<'a>(
        &'a self,
        key: K,
        remote: Address<&str>,
        value: Option<&V>,
    ) -> Self::OnJoinValueSyncedHandler<'a> {
        let StatefulJoinValueLaneLifecycle {
            on_synced,
            state,
            handler_context,
            ..
        } = self;
        on_synced.on_synced(state, *handler_context, key, remote, value)
    }
}

impl<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed> OnJoinValueUnlinked<K, Context>
    for StatefulJoinValueLaneLifecycle<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed>
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: OnJoinValueUnlinkedShared<K, Context, State>,
    FFailed: Send,
{
    type OnJoinValueUnlinkedHandler<'a> = FUnlinked::OnJoinValueUnlinkedHandler<'a>
    where
        Self: 'a;

    fn on_unlinked<'a>(
        &'a self,
        key: K,
        remote: Address<&str>,
    ) -> Self::OnJoinValueUnlinkedHandler<'a> {
        let StatefulJoinValueLaneLifecycle {
            on_unlinked,
            state,
            handler_context,
            ..
        } = self;
        on_unlinked.on_unlinked(state, *handler_context, key, remote)
    }
}

impl<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed> OnJoinValueFailed<K, Context>
    for StatefulJoinValueLaneLifecycle<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed>
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: OnJoinValueFailedShared<K, Context, State>,
{
    type OnJoinValueFailedHandler<'a> = FFailed::OnJoinValueFailedHandler<'a>
    where
        Self: 'a;

    fn on_failed<'a>(
        &'a self,
        key: K,
        remote: Address<&str>,
    ) -> Self::OnJoinValueFailedHandler<'a> {
        let StatefulJoinValueLaneLifecycle {
            on_failed,
            state,
            handler_context,
            ..
        } = self;
        on_failed.on_failed(state, *handler_context, key, remote)
    }
}

impl<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed>
    StatefulJoinValueLifecycle<Context, State, K, V>
    for StatefulJoinValueLaneLifecycle<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed>
where
    State: Send + Clone,
    FLinked: OnJoinValueLinkedShared<K, Context, State> + Clone,
    FSynced: OnJoinValueSyncedShared<K, V, Context, State> + Clone,
    FUnlinked: OnJoinValueUnlinkedShared<K, Context, State> + Clone,
    FFailed: OnJoinValueFailedShared<K, Context, State> + Clone,
{
    type WithOnLinked<H> = StatefulJoinValueLaneLifecycle<Context, State, K, V, H, FSynced, FUnlinked, FFailed>
    where
        H: OnJoinValueLinkedShared<K, Context, State> + Clone;

    type WithOnSynced<H> = StatefulJoinValueLaneLifecycle<Context, State, K, V, FLinked, H, FUnlinked, FFailed>
    where
        H: OnJoinValueSyncedShared<K, V, Context, State> + Clone;

    type WithOnUnlinked<H> = StatefulJoinValueLaneLifecycle<Context, State, K, V, FLinked, FSynced, H, FFailed>
    where
        H: OnJoinValueUnlinkedShared<K, Context, State> + Clone;

    type WithOnFailed<H> = StatefulJoinValueLaneLifecycle<Context, State, K, V, FLinked, FSynced, FUnlinked, H>
    where
        H: OnJoinValueFailedShared<K, Context, State> + Clone;

    fn on_linked<F>(self, handler: F) -> Self::WithOnLinked<FnHandler<F>>
    where
        F: Clone,
        FnHandler<F>: OnJoinValueLinkedShared<K, Context, State>,
    {
        StatefulJoinValueLaneLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: FnHandler(handler),
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
        }
    }

    fn on_synced<F, B>(self, handler: F) -> Self::WithOnSynced<BorrowHandler<F, B>>
    where
        F: Clone,
        B: ?Sized,
        V: Borrow<B>,
        BorrowHandler<F, B>: OnJoinValueSyncedShared<K, V, Context, State>,
    {
        StatefulJoinValueLaneLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: BorrowHandler::new(handler),
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
        }
    }

    fn on_unlinked<F>(self, handler: F) -> Self::WithOnUnlinked<FnHandler<F>>
    where
        F: Clone,
        FnHandler<F>: OnJoinValueUnlinkedShared<K, Context, State>,
    {
        StatefulJoinValueLaneLifecycle {
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
        FnHandler<F>: OnJoinValueFailedShared<K, Context, State>,
    {
        StatefulJoinValueLaneLifecycle {
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

impl<Context, K, V, FLinked, FSynced, FUnlinked, FFailed> OnJoinValueLinked<K, Context>
    for StatelessJoinValueLaneLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FFailed>
where
    FLinked: OnJoinValueLinked<K, Context>,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
{
    type OnJoinValueLinkedHandler<'a> = FLinked::OnJoinValueLinkedHandler<'a>
    where
        Self: 'a;

    fn on_linked<'a>(
        &'a self,
        key: K,
        remote: Address<&str>,
    ) -> Self::OnJoinValueLinkedHandler<'a> {
        let StatelessJoinValueLaneLifecycle { on_linked, .. } = self;
        on_linked.on_linked(key, remote)
    }
}

impl<Context, K, V, FLinked, FSynced, FUnlinked, FFailed> OnJoinValueSynced<K, V, Context>
    for StatelessJoinValueLaneLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FFailed>
where
    FLinked: Send,
    FSynced: OnJoinValueSynced<K, V, Context>,
    FUnlinked: Send,
    FFailed: Send,
{
    type OnJoinValueSyncedHandler<'a> = FSynced::OnJoinValueSyncedHandler<'a>
    where
        Self: 'a;

    fn on_synced<'a>(
        &'a self,
        key: K,
        remote: Address<&str>,
        value: Option<&V>,
    ) -> Self::OnJoinValueSyncedHandler<'a> {
        let StatelessJoinValueLaneLifecycle { on_synced, .. } = self;
        on_synced.on_synced(key, remote, value)
    }
}

impl<Context, K, V, FLinked, FSynced, FUnlinked, FFailed> OnJoinValueUnlinked<K, Context>
    for StatelessJoinValueLaneLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FFailed>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: OnJoinValueUnlinked<K, Context>,
    FFailed: Send,
{
    type OnJoinValueUnlinkedHandler<'a> = FUnlinked::OnJoinValueUnlinkedHandler<'a>
    where
        Self: 'a;

    fn on_unlinked<'a>(
        &'a self,
        key: K,
        remote: Address<&str>,
    ) -> Self::OnJoinValueUnlinkedHandler<'a> {
        let StatelessJoinValueLaneLifecycle { on_unlinked, .. } = self;
        on_unlinked.on_unlinked(key, remote)
    }
}

impl<Context, K, V, FLinked, FSynced, FUnlinked, FFailed> OnJoinValueFailed<K, Context>
    for StatelessJoinValueLaneLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FFailed>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: OnJoinValueFailed<K, Context>,
{
    type OnJoinValueFailedHandler<'a> = FFailed::OnJoinValueFailedHandler<'a>
    where
        Self: 'a;

    fn on_failed<'a>(
        &'a self,
        key: K,
        remote: Address<&str>,
    ) -> Self::OnJoinValueFailedHandler<'a> {
        let StatelessJoinValueLaneLifecycle { on_failed, .. } = self;
        on_failed.on_failed(key, remote)
    }
}

impl<Context, K, V, FLinked, FSynced, FUnlinked, FFailed> StatelessJoinValueLifecycle<Context, K, V>
    for StatelessJoinValueLaneLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FFailed>
where
    FLinked: OnJoinValueLinked<K, Context> + Clone,
    FSynced: OnJoinValueSynced<K, V, Context> + Clone,
    FUnlinked: OnJoinValueUnlinked<K, Context> + Clone,
    FFailed: OnJoinValueFailed<K, Context> + Clone,
{
    type WithOnLinked<H> = StatelessJoinValueLaneLifecycle<Context, K, V, H, FSynced, FUnlinked, FFailed>
    where
        H: OnJoinValueLinked<K, Context> + Clone;

    type WithOnSynced<H> = StatelessJoinValueLaneLifecycle<Context, K, V, FLinked, H, FUnlinked, FFailed>
    where
        H: OnJoinValueSynced<K, V, Context> + Clone;

    type WithOnUnlinked<H> = StatelessJoinValueLaneLifecycle<Context, K, V, FLinked, FSynced, H, FFailed>
    where
        H: OnJoinValueUnlinked<K, Context> + Clone;

    type WithOnFailed<H> = StatelessJoinValueLaneLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, H>
    where
        H: OnJoinValueFailed<K, Context> + Clone;

    type WithShared<Shared> = StatefulJoinValueLaneLifecycle<
        Context,
        Shared,
        K,
        V,
        LiftShared<FLinked, Shared>,
        LiftShared<FSynced, Shared>,
        LiftShared<FUnlinked, Shared>,
        LiftShared<FFailed, Shared>>
    where
        Shared: Send + Clone;

    fn on_linked<F>(self, handler: F) -> Self::WithOnLinked<WithHandlerContext<F>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinValueLinked<K, Context>,
    {
        StatelessJoinValueLaneLifecycle {
            _type: PhantomData,
            on_linked: WithHandlerContext::new(handler),
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
        }
    }

    fn on_synced<F, B>(self, handler: F) -> Self::WithOnSynced<WithHandlerContextBorrow<F, B>>
    where
        F: Clone,
        B: ?Sized,
        V: Borrow<B>,
        WithHandlerContextBorrow<F, B>: OnJoinValueSynced<K, V, Context>,
    {
        StatelessJoinValueLaneLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: WithHandlerContextBorrow::new(handler),
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
        }
    }

    fn on_unlinked<F>(self, handler: F) -> Self::WithOnUnlinked<WithHandlerContext<F>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinValueUnlinked<K, Context>,
    {
        StatelessJoinValueLaneLifecycle {
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
        WithHandlerContext<F>: OnJoinValueFailed<K, Context>,
    {
        StatelessJoinValueLaneLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: WithHandlerContext::new(handler),
        }
    }

    fn with_shared_state<Shared: Send + Clone>(self, shared: Shared) -> Self::WithShared<Shared> {
        StatefulJoinValueLaneLifecycle {
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
