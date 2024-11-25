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

use crate::{
    agent_lifecycle::HandlerContext,
    lifecycle_fn::{LiftShared, WithHandlerContext, WithHandlerContextBorrow},
};

pub use self::{
    on_clear::{OnDownlinkClear, OnDownlinkClearShared},
    on_remove::{OnDownlinkRemove, OnDownlinkRemoveShared},
    on_update::{OnDownlinkUpdate, OnDownlinkUpdateShared},
};

use super::{
    on_failed::{OnFailed, OnFailedShared},
    on_linked::{OnLinked, OnLinkedShared},
    on_synced::OnSynced,
    on_unlinked::{OnUnlinked, OnUnlinkedShared},
    OnSyncedShared,
};

mod on_clear;
mod on_remove;
mod on_update;

/// Trait for the lifecycle of a map downlink.
///
/// # Type Parameters
/// * `K` - The type of the keys of the downlink.
/// * `V` - The type of the values of the downlink.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
pub trait MapDownlinkLifecycle<K, V, M, Context>:
    OnLinked<Context>
    + OnSynced<M, Context>
    + OnDownlinkUpdate<K, V, M, Context>
    + OnDownlinkRemove<K, V, M, Context>
    + OnDownlinkClear<M, Context>
    + OnUnlinked<Context>
    + OnFailed<Context>
{
}

impl<LC, K, V, M, Context> MapDownlinkLifecycle<K, V, M, Context> for LC where
    LC: OnLinked<Context>
        + OnSynced<M, Context>
        + OnDownlinkUpdate<K, V, M, Context>
        + OnDownlinkRemove<K, V, M, Context>
        + OnDownlinkClear<M, Context>
        + OnUnlinked<Context>
        + OnFailed<Context>
{
}

/// A lifecycle for a map downlink where the individual event handlers do not share state.
///
/// # Type Parameters
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `K` - The type of the keys for the map.
/// * `V` - The type of the values for the map.
pub trait StatelessMapLifecycle<Context, K, V, M>: MapDownlinkLifecycle<K, V, M, Context> {
    type WithOnLinked<H>: StatelessMapLifecycle<Context, K, V, M>
    where
        H: OnLinked<Context>;

    type WithOnSynced<H>: StatelessMapLifecycle<Context, K, V, M>
    where
        H: OnSynced<M, Context>;

    type WithOnUnlinked<H>: StatelessMapLifecycle<Context, K, V, M>
    where
        H: OnUnlinked<Context>;

    type WithOnFailed<H>: StatelessMapLifecycle<Context, K, V, M>
    where
        H: OnFailed<Context>;

    type WithOnUpdate<H>: StatelessMapLifecycle<Context, K, V, M>
    where
        H: OnDownlinkUpdate<K, V, M, Context>;

    type WithOnRemove<H>: StatelessMapLifecycle<Context, K, V, M>
    where
        H: OnDownlinkRemove<K, V, M, Context>;

    type WithOnClear<H>: StatelessMapLifecycle<Context, K, V, M>
    where
        H: OnDownlinkClear<M, Context>;

    type WithShared<Shared>: StatefulMapLifecycle<Context, Shared, K, V, M>
    where
        Shared: Send;

    fn on_linked<F>(self, handler: F) -> Self::WithOnLinked<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnLinked<Context>;

    fn on_synced<F>(self, handler: F) -> Self::WithOnSynced<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnSynced<M, Context>;

    fn on_unlinked<F>(self, handler: F) -> Self::WithOnUnlinked<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnUnlinked<Context>;

    fn on_failed<F>(self, handler: F) -> Self::WithOnFailed<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnFailed<Context>;

    fn on_update<F, B>(self, handler: F) -> Self::WithOnUpdate<WithHandlerContextBorrow<F, B>>
    where
        B: ?Sized,
        V: Borrow<B>,
        WithHandlerContextBorrow<F, B>: OnDownlinkUpdate<K, V, M, Context>;

    fn on_remove<F>(self, handler: F) -> Self::WithOnRemove<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnDownlinkRemove<K, V, M, Context>;

    fn on_clear<F>(self, handler: F) -> Self::WithOnClear<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnDownlinkClear<M, Context>;

    fn with_shared_state<Shared: Send>(self, shared: Shared) -> Self::WithShared<Shared>;
}

/// A lifecycle for a map downlink where the individual event handlers have shared state.
///
/// # Type Parameters
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `Shared` - The type of the shared state.
/// * `K` - The type of the keys for the map.
/// * `V` - The type of the values for the map.
pub trait StatefulMapLifecycle<Context, Shared, K, V, M>:
    MapDownlinkLifecycle<K, V, M, Context>
{
    type WithOnLinked<H>: StatefulMapLifecycle<Context, Shared, K, V, M>
    where
        H: OnLinkedShared<Context, Shared>;

    type WithOnSynced<H>: StatefulMapLifecycle<Context, Shared, K, V, M>
    where
        H: OnSyncedShared<M, Context, Shared>;

    type WithOnUnlinked<H>: StatefulMapLifecycle<Context, Shared, K, V, M>
    where
        H: OnUnlinkedShared<Context, Shared>;

    type WithOnFailed<H>: StatefulMapLifecycle<Context, Shared, K, V, M>
    where
        H: OnFailedShared<Context, Shared>;

    type WithOnUpdate<H>: StatefulMapLifecycle<Context, Shared, K, V, M>
    where
        H: OnDownlinkUpdateShared<K, V, M, Context, Shared>;

    type WithOnRemove<H>: StatefulMapLifecycle<Context, Shared, K, V, M>
    where
        H: OnDownlinkRemoveShared<K, V, M, Context, Shared>;

    type WithOnClear<H>: StatefulMapLifecycle<Context, Shared, K, V, M>
    where
        H: OnDownlinkClearShared<M, Context, Shared>;

    fn on_linked<F>(self, handler: F) -> Self::WithOnLinked<FnHandler<F>>
    where
        FnHandler<F>: OnLinkedShared<Context, Shared>;

    fn on_synced<F>(self, handler: F) -> Self::WithOnSynced<FnHandler<F>>
    where
        FnHandler<F>: OnSyncedShared<M, Context, Shared>;

    fn on_unlinked<F>(self, handler: F) -> Self::WithOnUnlinked<FnHandler<F>>
    where
        FnHandler<F>: OnUnlinkedShared<Context, Shared>;

    fn on_failed<F>(self, handler: F) -> Self::WithOnFailed<FnHandler<F>>
    where
        FnHandler<F>: OnFailedShared<Context, Shared>;

    fn on_update<F, B>(self, handler: F) -> Self::WithOnUpdate<BorrowHandler<F, B>>
    where
        B: ?Sized,
        V: Borrow<B>,
        BorrowHandler<F, B>: OnDownlinkUpdateShared<K, V, M, Context, Shared>;

    fn on_remove<F>(self, handler: F) -> Self::WithOnRemove<FnHandler<F>>
    where
        FnHandler<F>: OnDownlinkRemoveShared<K, V, M, Context, Shared>;

    fn on_clear<F>(self, handler: F) -> Self::WithOnClear<FnHandler<F>>
    where
        FnHandler<F>: OnDownlinkClearShared<M, Context, Shared>;
}

/// A lifecycle for a map downlink where the event handlers do not share state..
///
/// # Type Parameters
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `K` - The type of the keys of the downlink.
/// * `V` - The type of the values of the downlink.
/// * `FLinked` - The type of the 'on_linked' handler.
/// * `FSynced` - The type of the 'on_synced' handler.
/// * `FUnlinked` - The type of the 'on_unlinked' handler.
/// * `FFailed` - The type of the 'on_failed' handler.
/// * `FUpd` - The type of the 'on_update' handler.
/// * `FRem` - The type of the 'on_remove' handler.
/// * `FClr` - The type of the 'on_clear' handler.
#[derive(Debug)]
pub struct StatelessMapDownlinkLifecycle<
    Context,
    K,
    V,
    M,
    FLinked = NoHandler,
    FSynced = NoHandler,
    FUnlinked = NoHandler,
    FFailed = NoHandler,
    FUpd = NoHandler,
    FRem = NoHandler,
    FClr = NoHandler,
> {
    _type: PhantomData<fn(&Context, K, V, M)>,
    on_linked: FLinked,
    on_synced: FSynced,
    on_unlinked: FUnlinked,
    on_failed: FFailed,
    on_update: FUpd,
    on_remove: FRem,
    on_clear: FClr,
}

impl<Context, K, V, M> Default for StatelessMapDownlinkLifecycle<Context, K, V, M> {
    fn default() -> Self {
        Self {
            _type: Default::default(),
            on_linked: Default::default(),
            on_synced: Default::default(),
            on_unlinked: Default::default(),
            on_failed: Default::default(),
            on_update: Default::default(),
            on_remove: Default::default(),
            on_clear: Default::default(),
        }
    }
}

/// A lifecycle for a map downlink where the individual event handlers can share state.
///
/// # Type Parameters
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `State` - The type of the shared state.
/// * `K` - The type of the keys of the downlink.
/// * `V` - The type of the values of the downlink.
/// * `FLinked` - The type of the 'on_linked' handler.
/// * `FSynced` - The type of the 'on_synced' handler.
/// * `FUnlinked` - The type of the 'on_unlinked' handler.
/// * `FFailed` - The type of the 'on_failed' handler.
/// * `FUpd` - The type of the 'on_update' handler.
/// * `FRem` - The type of the 'on_remove' handler.
/// * `FClr` - The type of the 'on_clear' handler.
#[derive(Debug)]
pub struct StatefulMapDownlinkLifecycle<
    Context,
    State,
    K,
    V,
    M,
    FLinked = NoHandler,
    FSynced = NoHandler,
    FUnlinked = NoHandler,
    FFailed = NoHandler,
    FUpd = NoHandler,
    FRem = NoHandler,
    FClr = NoHandler,
> {
    _type: PhantomData<fn(K, V, M)>,
    state: State,
    handler_context: HandlerContext<Context>,
    on_linked: FLinked,
    on_synced: FSynced,
    on_unlinked: FUnlinked,
    on_failed: FFailed,
    on_update: FUpd,
    on_remove: FRem,
    on_clear: FClr,
}

impl<Context, State, K, V, M> StatefulMapDownlinkLifecycle<Context, State, K, V, M> {
    pub fn new(state: State) -> Self {
        StatefulMapDownlinkLifecycle {
            _type: PhantomData,
            state,
            handler_context: Default::default(),
            on_linked: Default::default(),
            on_synced: Default::default(),
            on_unlinked: Default::default(),
            on_failed: Default::default(),
            on_update: Default::default(),
            on_remove: Default::default(),
            on_clear: Default::default(),
        }
    }
}

impl<Context, State, K, V, M, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr> Clone
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        M,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
where
    State: Clone,
    FLinked: Clone,
    FSynced: Clone,
    FUnlinked: Clone,
    FFailed: Clone,
    FUpd: Clone,
    FRem: Clone,
    FClr: Clone,
{
    fn clone(&self) -> Self {
        Self {
            _type: PhantomData,
            state: self.state.clone(),
            handler_context: HandlerContext::default(),
            on_linked: self.on_linked.clone(),
            on_synced: self.on_synced.clone(),
            on_unlinked: self.on_unlinked.clone(),
            on_failed: self.on_failed.clone(),
            on_update: self.on_update.clone(),
            on_remove: self.on_remove.clone(),
            on_clear: self.on_clear.clone(),
        }
    }
}

impl<Context, K, V, M, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr> Clone
    for StatelessMapDownlinkLifecycle<
        Context,
        K,
        V,
        M,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
where
    FLinked: Clone,
    FSynced: Clone,
    FUnlinked: Clone,
    FFailed: Clone,
    FUpd: Clone,
    FRem: Clone,
    FClr: Clone,
{
    fn clone(&self) -> Self {
        Self {
            _type: PhantomData,
            on_linked: self.on_linked.clone(),
            on_synced: self.on_synced.clone(),
            on_unlinked: self.on_unlinked.clone(),
            on_failed: self.on_failed.clone(),
            on_update: self.on_update.clone(),
            on_remove: self.on_remove.clone(),
            on_clear: self.on_clear.clone(),
        }
    }
}

impl<Context, K, V, M, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr> OnLinked<Context>
    for StatelessMapDownlinkLifecycle<
        Context,
        K,
        V,
        M,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
where
    FLinked: OnLinked<Context>,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FUpd: Send,
    FRem: Send,
    FClr: Send,
{
    type OnLinkedHandler<'a> = FLinked::OnLinkedHandler<'a>
    where
        Self: 'a;

    fn on_linked(&self) -> Self::OnLinkedHandler<'_> {
        let StatelessMapDownlinkLifecycle { on_linked, .. } = self;
        on_linked.on_linked()
    }
}

impl<Context, K, V, M, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr> OnSynced<M, Context>
    for StatelessMapDownlinkLifecycle<
        Context,
        K,
        V,
        M,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
where
    FLinked: Send,
    FSynced: OnSynced<M, Context>,
    FUnlinked: Send,
    FFailed: Send,
    FUpd: Send,
    FRem: Send,
    FClr: Send,
{
    type OnSyncedHandler<'a> = FSynced::OnSyncedHandler<'a>
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, map: &M) -> Self::OnSyncedHandler<'a> {
        let StatelessMapDownlinkLifecycle { on_synced, .. } = self;
        on_synced.on_synced(map)
    }
}

impl<Context, K, V, M, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr> OnUnlinked<Context>
    for StatelessMapDownlinkLifecycle<
        Context,
        K,
        V,
        M,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: OnUnlinked<Context>,
    FFailed: Send,
    FUpd: Send,
    FRem: Send,
    FClr: Send,
{
    type OnUnlinkedHandler<'a> = FUnlinked::OnUnlinkedHandler<'a>
    where
        Self: 'a;

    fn on_unlinked(&self) -> Self::OnUnlinkedHandler<'_> {
        let StatelessMapDownlinkLifecycle { on_unlinked, .. } = self;
        on_unlinked.on_unlinked()
    }
}

impl<Context, K, V, M, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr> OnFailed<Context>
    for StatelessMapDownlinkLifecycle<
        Context,
        K,
        V,
        M,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: OnFailed<Context>,
    FUpd: Send,
    FRem: Send,
    FClr: Send,
{
    type OnFailedHandler<'a> = FFailed::OnFailedHandler<'a>
    where
        Self: 'a;

    fn on_failed(&self) -> Self::OnFailedHandler<'_> {
        let StatelessMapDownlinkLifecycle { on_failed, .. } = self;
        on_failed.on_failed()
    }
}

impl<Context, K, V, M, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    OnDownlinkUpdate<K, V, M, Context>
    for StatelessMapDownlinkLifecycle<
        Context,
        K,
        V,
        M,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FUpd: OnDownlinkUpdate<K, V, M, Context>,
    FRem: Send,
    FClr: Send,
{
    type OnUpdateHandler<'a> = FUpd::OnUpdateHandler<'a>
    where
        Self: 'a;

    fn on_update<'a>(
        &'a self,
        key: K,
        map: &M,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        let StatelessMapDownlinkLifecycle { on_update, .. } = self;
        on_update.on_update(key, map, previous, new_value)
    }
}

impl<Context, K, V, M, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    OnDownlinkRemove<K, V, M, Context>
    for StatelessMapDownlinkLifecycle<
        Context,
        K,
        V,
        M,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FUpd: Send,
    FRem: OnDownlinkRemove<K, V, M, Context>,
    FClr: Send,
{
    type OnRemoveHandler<'a> = FRem::OnRemoveHandler<'a>
    where
        Self: 'a;

    fn on_remove<'a>(&'a self, key: K, map: &M, removed: V) -> Self::OnRemoveHandler<'a> {
        let StatelessMapDownlinkLifecycle { on_remove, .. } = self;
        on_remove.on_remove(key, map, removed)
    }
}

impl<Context, K, V, M, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    OnDownlinkClear<M, Context>
    for StatelessMapDownlinkLifecycle<
        Context,
        K,
        V,
        M,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FUpd: Send,
    FRem: Send,
    FClr: OnDownlinkClear<M, Context>,
{
    type OnClearHandler<'a> = FClr::OnClearHandler<'a>
    where
        Self: 'a;

    fn on_clear(&self, map: M) -> Self::OnClearHandler<'_> {
        let StatelessMapDownlinkLifecycle { on_clear, .. } = self;
        on_clear.on_clear(map)
    }
}

#[doc(hidden)]
pub type LiftedMapLifecycle<
    Context,
    State,
    K,
    V,
    M,
    FLinked,
    FSynced,
    FUnlinked,
    FFailed,
    FUpd,
    FRem,
    FClr,
> = StatefulMapDownlinkLifecycle<
    Context,
    State,
    K,
    V,
    M,
    LiftShared<FLinked, State>,
    LiftShared<FSynced, State>,
    LiftShared<FUnlinked, State>,
    LiftShared<FFailed, State>,
    LiftShared<FUpd, State>,
    LiftShared<FRem, State>,
    LiftShared<FClr, State>,
>;

impl<Context, K, V, M, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    StatelessMapLifecycle<Context, K, V, M>
    for StatelessMapDownlinkLifecycle<
        Context,
        K,
        V,
        M,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
where
    FLinked: OnLinked<Context>,
    FSynced: OnSynced<M, Context>,
    FUnlinked: OnUnlinked<Context>,
    FFailed: OnFailed<Context>,
    FUpd: OnDownlinkUpdate<K, V, M, Context>,
    FRem: OnDownlinkRemove<K, V, M, Context>,
    FClr: OnDownlinkClear<M, Context>,
{
    type WithOnLinked<H> = StatelessMapDownlinkLifecycle<
    Context,
    K,
    V,
    M,
    H,
    FSynced,
    FUnlinked,
    FFailed,
    FUpd,
    FRem,
    FClr,
    >
    where
        H: OnLinked<Context>;

    type WithOnSynced<H> = StatelessMapDownlinkLifecycle<
    Context,
    K,
    V,
    M,
    FLinked,
    H,
    FUnlinked,
    FFailed,
    FUpd,
    FRem,
    FClr,
    >
    where
        H: OnSynced<M, Context>;

    type WithOnUnlinked<H> = StatelessMapDownlinkLifecycle<
    Context,
    K,
    V,
    M,
    FLinked,
    FSynced,
    H,
    FFailed,
    FUpd,
    FRem,
    FClr,
    >
    where
       H: OnUnlinked<Context>;

    type WithOnFailed<H> = StatelessMapDownlinkLifecycle<
    Context,
    K,
    V,
    M,
    FLinked,
    FSynced,
    FUnlinked,
    H,
    FUpd,
    FRem,
    FClr,
    >
    where
        H: OnFailed<Context>;

    type WithOnUpdate<H> = StatelessMapDownlinkLifecycle<
    Context,
    K,
    V,
    M,
    FLinked,
    FSynced,
    FUnlinked,
    FFailed,
    H,
    FRem,
    FClr,
    >
    where
        H: OnDownlinkUpdate<K, V, M, Context>;

    type WithOnRemove<H> = StatelessMapDownlinkLifecycle<
    Context,
    K,
    V,
    M,
    FLinked,
    FSynced,
    FUnlinked,
    FFailed,
    FUpd,
    H,
    FClr,
    >
    where
         H: OnDownlinkRemove<K, V, M, Context>;

    type WithOnClear<H> = StatelessMapDownlinkLifecycle<
    Context,
    K,
    V,
    M,
    FLinked,
    FSynced,
    FUnlinked,
    FFailed,
    FUpd,
    FRem,
    H,
    >
    where
        H: OnDownlinkClear<M, Context>;

    type WithShared<Shared> = StatefulMapDownlinkLifecycle<
        Context,
        Shared,
        K,
        V,
        M,
        LiftShared<FLinked, Shared>,
        LiftShared<FSynced, Shared>,
        LiftShared<FUnlinked, Shared>,
        LiftShared<FFailed, Shared>,
        LiftShared<FUpd, Shared>,
        LiftShared<FRem, Shared>,
        LiftShared<FClr, Shared>,
    >
        where
            Shared: Send;

    fn on_linked<F>(self, handler: F) -> Self::WithOnLinked<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnLinked<Context>,
    {
        StatelessMapDownlinkLifecycle {
            _type: PhantomData,
            on_linked: WithHandlerContext::new(handler),
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    fn on_synced<F>(self, handler: F) -> Self::WithOnSynced<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnSynced<M, Context>,
    {
        StatelessMapDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: WithHandlerContext::new(handler),
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    fn on_unlinked<F>(self, handler: F) -> Self::WithOnUnlinked<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnUnlinked<Context>,
    {
        StatelessMapDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: WithHandlerContext::new(handler),
            on_failed: self.on_failed,
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    fn on_failed<F>(self, handler: F) -> Self::WithOnFailed<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnFailed<Context>,
    {
        StatelessMapDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: WithHandlerContext::new(handler),
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    fn on_update<F, B>(self, handler: F) -> Self::WithOnUpdate<WithHandlerContextBorrow<F, B>>
    where
        B: ?Sized,
        V: Borrow<B>,
        WithHandlerContextBorrow<F, B>: OnDownlinkUpdate<K, V, M, Context>,
    {
        StatelessMapDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_update: WithHandlerContextBorrow::new(handler),
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    fn on_remove<F>(self, handler: F) -> Self::WithOnRemove<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnDownlinkRemove<K, V, M, Context>,
    {
        StatelessMapDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_update: self.on_update,
            on_remove: WithHandlerContext::new(handler),
            on_clear: self.on_clear,
        }
    }

    fn on_clear<F>(self, handler: F) -> Self::WithOnClear<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnDownlinkClear<M, Context>,
    {
        StatelessMapDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: WithHandlerContext::new(handler),
        }
    }

    fn with_shared_state<Shared: Send>(self, shared: Shared) -> Self::WithShared<Shared> {
        StatefulMapDownlinkLifecycle {
            _type: PhantomData,
            state: shared,
            handler_context: Default::default(),
            on_linked: LiftShared::new(self.on_linked),
            on_synced: LiftShared::new(self.on_synced),
            on_unlinked: LiftShared::new(self.on_unlinked),
            on_failed: LiftShared::new(self.on_failed),
            on_update: LiftShared::new(self.on_update),
            on_remove: LiftShared::new(self.on_remove),
            on_clear: LiftShared::new(self.on_clear),
        }
    }
}
impl<Context, K, V, M, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    StatelessMapDownlinkLifecycle<
        Context,
        K,
        V,
        M,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
{
    /// Add a state that is shared between all of the event handlers in the lifecycle.
    pub fn with_state<State>(
        self,
        state: State,
    ) -> LiftedMapLifecycle<
        Context,
        State,
        K,
        V,
        M,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    > {
        let StatelessMapDownlinkLifecycle {
            on_linked,
            on_synced,
            on_unlinked,
            on_failed,
            on_update,
            on_remove,
            on_clear,
            ..
        } = self;
        StatefulMapDownlinkLifecycle {
            _type: PhantomData,
            state,
            handler_context: Default::default(),
            on_linked: LiftShared::new(on_linked),
            on_synced: LiftShared::new(on_synced),
            on_unlinked: LiftShared::new(on_unlinked),
            on_failed: LiftShared::new(on_failed),
            on_update: LiftShared::new(on_update),
            on_remove: LiftShared::new(on_remove),
            on_clear: LiftShared::new(on_clear),
        }
    }
}

impl<Context, State, K, V, M, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    OnLinked<Context>
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        M,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
where
    State: Send,
    FLinked: OnLinkedShared<Context, State>,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FUpd: Send,
    FRem: Send,
    FClr: Send,
{
    type OnLinkedHandler<'a> = FLinked::OnLinkedHandler<'a>
    where
        Self: 'a;

    fn on_linked(&self) -> Self::OnLinkedHandler<'_> {
        let StatefulMapDownlinkLifecycle {
            on_linked,
            state,
            handler_context,
            ..
        } = self;
        on_linked.on_linked(state, *handler_context)
    }
}

impl<Context, State, K, V, M, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    OnSynced<M, Context>
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        M,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
where
    State: Send,
    FLinked: Send,
    FSynced: OnSyncedShared<M, Context, State>,
    FUnlinked: Send,
    FFailed: Send,
    FUpd: Send,
    FRem: Send,
    FClr: Send,
{
    type OnSyncedHandler<'a> = FSynced::OnSyncedHandler<'a>
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, value: &M) -> Self::OnSyncedHandler<'a> {
        let StatefulMapDownlinkLifecycle {
            on_synced,
            state,
            handler_context,
            ..
        } = self;
        on_synced.on_synced(state, *handler_context, value)
    }
}

impl<Context, State, K, V, M, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    OnUnlinked<Context>
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        M,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: OnUnlinkedShared<Context, State>,
    FFailed: Send,
    FUpd: Send,
    FRem: Send,
    FClr: Send,
{
    type OnUnlinkedHandler<'a> = FUnlinked::OnUnlinkedHandler<'a>
    where
        Self: 'a;

    fn on_unlinked(&self) -> Self::OnUnlinkedHandler<'_> {
        let StatefulMapDownlinkLifecycle {
            on_unlinked,
            state,
            handler_context,
            ..
        } = self;
        on_unlinked.on_unlinked(state, *handler_context)
    }
}

impl<Context, State, K, V, M, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    OnFailed<Context>
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        M,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: OnFailedShared<Context, State>,
    FUpd: Send,
    FRem: Send,
    FClr: Send,
{
    type OnFailedHandler<'a> = FFailed::OnFailedHandler<'a>
    where
        Self: 'a;

    fn on_failed(&self) -> Self::OnFailedHandler<'_> {
        let StatefulMapDownlinkLifecycle {
            on_failed,
            state,
            handler_context,
            ..
        } = self;
        on_failed.on_failed(state, *handler_context)
    }
}

impl<Context, State, K, V, M, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    OnDownlinkUpdate<K, V, M, Context>
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        M,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FUpd: OnDownlinkUpdateShared<K, V, M, Context, State>,
    FRem: Send,
    FClr: Send,
{
    type OnUpdateHandler<'a> = FUpd::OnUpdateHandler<'a>
    where
        Self: 'a;

    fn on_update<'a>(
        &'a self,
        key: K,
        map: &M,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        let StatefulMapDownlinkLifecycle {
            on_update,
            state,
            handler_context,
            ..
        } = self;
        on_update.on_update(state, *handler_context, key, map, previous, new_value)
    }
}

impl<Context, State, K, V, M, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    OnDownlinkRemove<K, V, M, Context>
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        M,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FUpd: Send,
    FRem: OnDownlinkRemoveShared<K, V, M, Context, State>,
    FClr: Send,
{
    type OnRemoveHandler<'a> = FRem::OnRemoveHandler<'a>
    where
        Self: 'a;

    fn on_remove<'a>(&'a self, key: K, map: &M, removed: V) -> Self::OnRemoveHandler<'a> {
        let StatefulMapDownlinkLifecycle {
            on_remove,
            state,
            handler_context,
            ..
        } = self;
        on_remove.on_remove(state, *handler_context, key, map, removed)
    }
}

impl<Context, State, K, V, M, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    OnDownlinkClear<M, Context>
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        M,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FUpd: Send,
    FRem: Send,
    FClr: OnDownlinkClearShared<M, Context, State>,
{
    type OnClearHandler<'a> = FClr::OnClearHandler<'a>
    where
        Self: 'a;

    fn on_clear(&self, map: M) -> Self::OnClearHandler<'_> {
        let StatefulMapDownlinkLifecycle {
            on_clear,
            state,
            handler_context,
            ..
        } = self;
        on_clear.on_clear(state, *handler_context, map)
    }
}

impl<Context, Shared, K, V, M, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    StatefulMapLifecycle<Context, Shared, K, V, M>
    for StatefulMapDownlinkLifecycle<
        Context,
        Shared,
        K,
        V,
        M,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
where
    Shared: Send,
    FLinked: OnLinkedShared<Context, Shared>,
    FSynced: OnSyncedShared<M, Context, Shared>,
    FUnlinked: OnUnlinkedShared<Context, Shared>,
    FFailed: OnFailedShared<Context, Shared>,
    FUpd: OnDownlinkUpdateShared<K, V, M, Context, Shared>,
    FRem: OnDownlinkRemoveShared<K, V, M, Context, Shared>,
    FClr: OnDownlinkClearShared<M, Context, Shared>,
{
    type WithOnLinked<H> = StatefulMapDownlinkLifecycle<
    Context,
    Shared,
    K,
    V,
    M,
    H,
    FSynced,
    FUnlinked,
    FFailed,
    FUpd,
    FRem,
    FClr,
    >
    where
        H: OnLinkedShared<Context, Shared>;

    type WithOnSynced<H> = StatefulMapDownlinkLifecycle<
    Context,
    Shared,
    K,
    V,
    M,
    FLinked,
    H,
    FUnlinked,
    FFailed,
    FUpd,
    FRem,
    FClr,
    >
    where
        H: OnSyncedShared<M, Context, Shared>;

    type WithOnUnlinked<H> = StatefulMapDownlinkLifecycle<
    Context,
    Shared,
    K,
    V,
    M,
    FLinked,
    FSynced,
    H,
    FFailed,
    FUpd,
    FRem,
    FClr,
    >
    where
        H: OnUnlinkedShared<Context, Shared>;

    type WithOnFailed<H> = StatefulMapDownlinkLifecycle<
    Context,
    Shared,
    K,
    V,
    M,
    FLinked,
    FSynced,
    FUnlinked,
    H,
    FUpd,
    FRem,
    FClr,
    >
    where
        H: OnFailedShared<Context, Shared>;

    type WithOnUpdate<H> = StatefulMapDownlinkLifecycle<
    Context,
    Shared,
    K,
    V,
    M,
    FLinked,
    FSynced,
    FUnlinked,
    FFailed,
    H,
    FRem,
    FClr,
    >
    where
        H: OnDownlinkUpdateShared<K, V, M, Context, Shared>;

    type WithOnRemove<H> = StatefulMapDownlinkLifecycle<
    Context,
    Shared,
    K,
    V,
    M,
    FLinked,
    FSynced,
    FUnlinked,
    FFailed,
    FUpd,
    H,
    FClr,
    >
    where
        H: OnDownlinkRemoveShared<K, V, M, Context, Shared>;

    type WithOnClear<H> = StatefulMapDownlinkLifecycle<
    Context,
    Shared,
    K,
    V,
    M,
    FLinked,
    FSynced,
    FUnlinked,
    FFailed,
    FUpd,
    FRem,
    H,
    >
    where
        H: OnDownlinkClearShared<M, Context, Shared>;

    fn on_linked<F>(self, handler: F) -> Self::WithOnLinked<FnHandler<F>>
    where
        FnHandler<F>: OnLinkedShared<Context, Shared>,
    {
        StatefulMapDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: FnHandler(handler),
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    fn on_synced<F>(self, handler: F) -> Self::WithOnSynced<FnHandler<F>>
    where
        FnHandler<F>: OnSyncedShared<M, Context, Shared>,
    {
        StatefulMapDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: FnHandler(handler),
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    fn on_unlinked<F>(self, handler: F) -> Self::WithOnUnlinked<FnHandler<F>>
    where
        FnHandler<F>: OnUnlinkedShared<Context, Shared>,
    {
        StatefulMapDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: FnHandler(handler),
            on_failed: self.on_failed,
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    fn on_failed<F>(self, handler: F) -> Self::WithOnFailed<FnHandler<F>>
    where
        FnHandler<F>: OnFailedShared<Context, Shared>,
    {
        StatefulMapDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: FnHandler(handler),
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    fn on_update<F, B>(self, handler: F) -> Self::WithOnUpdate<BorrowHandler<F, B>>
    where
        B: ?Sized,
        V: Borrow<B>,
        BorrowHandler<F, B>: OnDownlinkUpdateShared<K, V, M, Context, Shared>,
    {
        StatefulMapDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_update: BorrowHandler::new(handler),
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    fn on_remove<F>(self, handler: F) -> Self::WithOnRemove<FnHandler<F>>
    where
        FnHandler<F>: OnDownlinkRemoveShared<K, V, M, Context, Shared>,
    {
        StatefulMapDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_update: self.on_update,
            on_remove: FnHandler(handler),
            on_clear: self.on_clear,
        }
    }

    fn on_clear<F>(self, handler: F) -> Self::WithOnClear<FnHandler<F>>
    where
        FnHandler<F>: OnDownlinkClearShared<M, Context, Shared>,
    {
        StatefulMapDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: FnHandler(handler),
        }
    }
}
