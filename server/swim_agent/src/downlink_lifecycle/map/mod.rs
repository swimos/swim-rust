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

use std::{borrow::Borrow, collections::HashMap, marker::PhantomData};

use swim_api::handlers::{BorrowHandler, FnHandler, NoHandler};

use crate::agent_lifecycle::utility::HandlerContext;

use self::{
    on_clear::{OnDownlinkClear, OnDownlinkClearShared},
    on_remove::{OnDownlinkRemove, OnDownlinkRemoveShared},
    on_update::{OnDownlinkUpdate, OnDownlinkUpdateShared},
};

use super::{
    on_linked::{OnLinked, OnLinkedShared},
    on_synced::{OnSynced, OnSyncedShared},
    on_unlinked::{OnUnlinked, OnUnlinkedShared},
    LiftShared, WithHandlerContext, WithHandlerContextBorrow, on_failed::{OnFailed, OnFailedShared},
};

pub mod on_clear;
pub mod on_remove;
pub mod on_update;

/// Trait for the lifecycle of a map downlink.
///
/// #Type Parameters
/// * `K` - The type of the keys of the downlink.
/// * `V` - The type of the values of the downlink.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
pub trait MapDownlinkLifecycle<K, V, Context>:
    OnLinked<Context>
    + OnSynced<HashMap<K, V>, Context>
    + OnDownlinkUpdate<K, V, Context>
    + OnDownlinkRemove<K, V, Context>
    + OnDownlinkClear<K, V, Context>
    + OnUnlinked<Context>
    + OnFailed<Context>
{
}

impl<LC, K, V, Context> MapDownlinkLifecycle<K, V, Context> for LC where
    LC: OnLinked<Context>
        + OnSynced<HashMap<K, V>, Context>
        + OnDownlinkUpdate<K, V, Context>
        + OnDownlinkRemove<K, V, Context>
        + OnDownlinkClear<K, V, Context>
        + OnUnlinked<Context>
        + OnFailed<Context>,
{
}

/// A lifecycle for a map downlink .
///
/// #Type Parameters
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
///
#[derive(Debug)]
pub struct StatelessMapDownlinkLifecycle<
    Context,
    K,
    V,
    FLinked = NoHandler,
    FSynced = NoHandler,
    FUnlinked = NoHandler,
    FFailed = NoHandler,
    FUpd = NoHandler,
    FRem = NoHandler,
    FClr = NoHandler,
> {
    _type: PhantomData<fn(&Context, K, V)>,
    on_linked: FLinked,
    on_synced: FSynced,
    on_unlinked: FUnlinked,
    on_failed: FFailed,
    on_update: FUpd,
    on_remove: FRem,
    on_clear: FClr,
}

impl<Context, K, V> Default for StatelessMapDownlinkLifecycle<Context, K, V> {
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

/// A lifecycle for a map downlink where the individual event handlers can shared state.
///
/// #Type Parameters
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
///
#[derive(Debug)]
pub struct StatefulMapDownlinkLifecycle<
    Context,
    State,
    K,
    V,
    FLinked = NoHandler,
    FSynced = NoHandler,
    FUnlinked = NoHandler,
    FFailed = NoHandler,
    FUpd = NoHandler,
    FRem = NoHandler,
    FClr = NoHandler,
> {
    _type: PhantomData<fn(K, V)>,
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

impl<Context, State, K, V> StatefulMapDownlinkLifecycle<Context, State, K, V> {
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

impl<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr> Clone
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
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

impl<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr> Clone
    for StatelessMapDownlinkLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
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

impl<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr> OnLinked<Context>
    for StatelessMapDownlinkLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
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

impl<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr> OnSynced<HashMap<K, V>, Context>
    for StatelessMapDownlinkLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
where
    FLinked: Send,
    FSynced: OnSynced<HashMap<K, V>, Context>,
    FUnlinked: Send,
    FFailed: Send,
    FUpd: Send,
    FRem: Send,
    FClr: Send,
{
    type OnSyncedHandler<'a> = FSynced::OnSyncedHandler<'a>
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, map: &HashMap<K, V>) -> Self::OnSyncedHandler<'a> {
        let StatelessMapDownlinkLifecycle { on_synced, .. } = self;
        on_synced.on_synced(map)
    }
}

impl<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr> OnUnlinked<Context>
    for StatelessMapDownlinkLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
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

impl<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr> OnFailed<Context>
    for StatelessMapDownlinkLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
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

impl<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr> OnDownlinkUpdate<K, V, Context>
    for StatelessMapDownlinkLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FUpd: OnDownlinkUpdate<K, V, Context>,
    FRem: Send,
    FClr: Send,
{
    type OnUpdateHandler<'a> = FUpd::OnUpdateHandler<'a>
    where
        Self: 'a;

    fn on_update<'a>(
        &'a self,
        key: K,
        map: &HashMap<K, V>,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        let StatelessMapDownlinkLifecycle { on_update, .. } = self;
        on_update.on_update(key, map, previous, new_value)
    }
}

impl<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr> OnDownlinkRemove<K, V, Context>
    for StatelessMapDownlinkLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FUpd: Send,
    FRem: OnDownlinkRemove<K, V, Context>,
    FClr: Send,
{
    type OnRemoveHandler<'a> = FRem::OnRemoveHandler<'a>
    where
        Self: 'a;

    fn on_remove<'a>(
        &'a self,
        key: K,
        map: &HashMap<K, V>,
        removed: V,
    ) -> Self::OnRemoveHandler<'a> {
        let StatelessMapDownlinkLifecycle { on_remove, .. } = self;
        on_remove.on_remove(key, map, removed)
    }
}

impl<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr> OnDownlinkClear<K, V, Context>
    for StatelessMapDownlinkLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FUpd: Send,
    FRem: Send,
    FClr: OnDownlinkClear<K, V, Context>,
{
    type OnClearHandler<'a> = FClr::OnClearHandler<'a>
    where
        Self: 'a;

    fn on_clear(&self, map: HashMap<K, V>) -> Self::OnClearHandler<'_> {
        let StatelessMapDownlinkLifecycle { on_clear, .. } = self;
        on_clear.on_clear(map)
    }
}

pub type LiftedMapLifecycle<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr> =
    StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        LiftShared<FLinked, State>,
        LiftShared<FSynced, State>,
        LiftShared<FUnlinked, State>,
        LiftShared<FFailed, State>,
        LiftShared<FUpd, State>,
        LiftShared<FRem, State>,
        LiftShared<FClr, State>,
    >;

type StatelessWithContextAndBorrow<Context, K, V, Linked, Synced, Unlinked, Failed, Rem, Clr, F, B> =
    StatelessMapDownlinkLifecycle<
        Context,
        K,
        V,
        Linked,
        Synced,
        Unlinked,
        Failed,
        WithHandlerContextBorrow<Context, F, B>,
        Rem,
        Clr,
    >;

type StatefulWithBorrow<Context, State, K, V, Linked, Synced, Unlinked, Failed, Rem, Clr, F, B> =
    StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        Linked,
        Synced,
        Unlinked,
        Failed,
        BorrowHandler<F, B>,
        Rem,
        Clr,
    >;

impl<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    StatelessMapDownlinkLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
{
    /// Replace the 'on_linked' handler with another derived from a closure.
    pub fn on_linked<F>(
        self,
        f: F,
    ) -> StatelessMapDownlinkLifecycle<
        Context,
        K,
        V,
        WithHandlerContext<Context, F>,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
    where
        WithHandlerContext<Context, F>: OnLinked<Context>,
    {
        StatelessMapDownlinkLifecycle {
            _type: PhantomData,
            on_linked: WithHandlerContext::new(f),
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    /// Replace the 'on_synced' handler with another derived from a closure.
    pub fn on_synced<F>(
        self,
        f: F,
    ) -> StatelessMapDownlinkLifecycle<
        Context,
        K,
        V,
        FLinked,
        WithHandlerContext<Context, F>,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
    where
        WithHandlerContext<Context, F>: OnSynced<HashMap<K, V>, Context>,
    {
        StatelessMapDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: WithHandlerContext::new(f),
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    /// Replace the 'on_unlinked' handler with another derived from a closure.
    pub fn on_unlinked<F>(
        self,
        f: F,
    ) -> StatelessMapDownlinkLifecycle<
        Context,
        K,
        V,
        FLinked,
        FSynced,
        WithHandlerContext<Context, F>,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
    where
        WithHandlerContext<Context, F>: OnUnlinked<Context>,
    {
        StatelessMapDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: WithHandlerContext::new(f),
            on_failed: self.on_failed,
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    /// Replace the 'on_unlinked' handler with another derived from a closure.
    pub fn on_failed<F>(
        self,
        f: F,
    ) -> StatelessMapDownlinkLifecycle<
        Context,
        K,
        V,
        FLinked,
        FSynced,
        FUnlinked,
        WithHandlerContext<Context, F>,
        FUpd,
        FRem,
        FClr,
    >
    where
        WithHandlerContext<Context, F>: OnFailed<Context>,
    {
        StatelessMapDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: WithHandlerContext::new(f),
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    /// Replace the 'on_update' handler with another derived from a closure.
    pub fn on_update<F, B>(
        self,
        f: F,
    ) -> StatelessWithContextAndBorrow<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FRem, FClr, F, B>
    where
        B: ?Sized,
        V: Borrow<B>,
        WithHandlerContextBorrow<Context, F, B>: OnDownlinkUpdate<K, V, Context>,
    {
        StatelessMapDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_update: WithHandlerContextBorrow::new(f),
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    /// Replace the 'on_remove' handler with another derived from a closure.
    pub fn on_remove<F>(
        self,
        f: F,
    ) -> StatelessMapDownlinkLifecycle<
        Context,
        K,
        V,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        WithHandlerContext<Context, F>,
        FClr,
    >
    where
        WithHandlerContext<Context, F>: OnDownlinkRemove<K, V, Context>,
    {
        StatelessMapDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_update: self.on_update,
            on_remove: WithHandlerContext::new(f),
            on_clear: self.on_clear,
        }
    }

    /// Replace the 'on_clear' handler with another derived from a closure.
    pub fn on_clear<F>(
        self,
        f: F,
    ) -> StatelessMapDownlinkLifecycle<
        Context,
        K,
        V,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        WithHandlerContext<Context, F>,
    >
    where
        WithHandlerContext<Context, F>: OnDownlinkClear<K, V, Context>,
    {
        StatelessMapDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: WithHandlerContext::new(f),
        }
    }

    /// Add a state that is shared between all of the event handlers in the lifecycle.
    pub fn with_state<State>(
        self,
        state: State,
    ) -> LiftedMapLifecycle<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    {
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

impl<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr> OnLinked<Context>
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
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

impl<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    OnSynced<HashMap<K, V>, Context>
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
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
    FSynced: OnSyncedShared<HashMap<K, V>, Context, State>,
    FUnlinked: Send,
    FFailed: Send,
    FUpd: Send,
    FRem: Send,
    FClr: Send,
{
    type OnSyncedHandler<'a> = FSynced::OnSyncedHandler<'a>
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, value: &HashMap<K, V>) -> Self::OnSyncedHandler<'a> {
        let StatefulMapDownlinkLifecycle {
            on_synced,
            state,
            handler_context,
            ..
        } = self;
        on_synced.on_synced(state, *handler_context, value)
    }
}

impl<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr> OnUnlinked<Context>
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
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

impl<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr> OnFailed<Context>
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
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

impl<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    OnDownlinkUpdate<K, V, Context>
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
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
    FUpd: OnDownlinkUpdateShared<K, V, Context, State>,
    FRem: Send,
    FClr: Send,
{
    type OnUpdateHandler<'a> = FUpd::OnUpdateHandler<'a>
    where
        Self: 'a;

    fn on_update<'a>(
        &'a self,
        key: K,
        map: &HashMap<K, V>,
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

impl<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    OnDownlinkRemove<K, V, Context>
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
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
    FRem: OnDownlinkRemoveShared<K, V, Context, State>,
    FClr: Send,
{
    type OnRemoveHandler<'a> = FRem::OnRemoveHandler<'a>
    where
        Self: 'a;

    fn on_remove<'a>(
        &'a self,
        key: K,
        map: &HashMap<K, V>,
        removed: V,
    ) -> Self::OnRemoveHandler<'a> {
        let StatefulMapDownlinkLifecycle {
            on_remove,
            state,
            handler_context,
            ..
        } = self;
        on_remove.on_remove(state, *handler_context, key, map, removed)
    }
}

impl<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    OnDownlinkClear<K, V, Context>
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
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
    FClr: OnDownlinkClearShared<K, V, Context, State>,
{
    type OnClearHandler<'a> = FClr::OnClearHandler<'a>
    where
        Self: 'a;

    fn on_clear(&self, map: HashMap<K, V>) -> Self::OnClearHandler<'_> {
        let StatefulMapDownlinkLifecycle {
            on_clear,
            state,
            handler_context,
            ..
        } = self;
        on_clear.on_clear(state, *handler_context, map)
    }
}

impl<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
{
    /// Replace the 'on_linked' handler with another derived from a closure.
    pub fn on_linked<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        FnHandler<F>,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
    where
        FnHandler<F>: OnLinkedShared<Context, State>,
    {
        StatefulMapDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: FnHandler(f),
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    /// Replace the 'on_synced' handler with another derived from a closure.
    pub fn on_synced<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        FLinked,
        FnHandler<F>,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
    where
        FnHandler<F>: OnSyncedShared<HashMap<K, V>, Context, State>,
    {
        StatefulMapDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: FnHandler(f),
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    /// Replace the 'on_unlinked' handler with another derived from a closure.
    pub fn on_unlinked<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        FLinked,
        FSynced,
        FnHandler<F>,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
    where
        FnHandler<F>: OnUnlinkedShared<Context, State>,
    {
        StatefulMapDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: FnHandler(f),
            on_failed: self.on_failed,
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    /// Replace the 'on_failed' handler with another derived from a closure.
    pub fn on_failed<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        FLinked,
        FSynced,
        FUnlinked,
        FnHandler<F>,
        FUpd,
        FRem,
        FClr,
    >
    where
        FnHandler<F>: OnFailedShared<Context, State>,
    {
        StatefulMapDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: FnHandler(f),
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    /// Replace the 'on_update' handler with another derived from a closure.
    pub fn on_update<F, B>(
        self,
        f: F,
    ) -> StatefulWithBorrow<Context, State, K, V, FLinked, FSynced, FUnlinked, FFailed, FRem, FClr, F, B>
    where
        B: ?Sized,
        V: Borrow<B>,
        BorrowHandler<F, B>: OnDownlinkUpdateShared<K, V, Context, State>,
    {
        StatefulMapDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_update: BorrowHandler::new(f),
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    /// Replace the 'on_remove' handler with another derived from a closure.
    pub fn on_remove<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FnHandler<F>,
        FClr,
    >
    where
        FnHandler<F>: OnDownlinkRemoveShared<K, V, Context, State>,
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
            on_remove: FnHandler(f),
            on_clear: self.on_clear,
        }
    }

    /// Replace the 'on_clear' handler with another derived from a closure.
    pub fn on_clear<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FnHandler<F>,
    >
    where
        FnHandler<F>: OnDownlinkClearShared<K, V, Context, State>,
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
            on_clear: FnHandler(f),
        }
    }
}
