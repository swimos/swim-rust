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

use std::{collections::HashMap, marker::PhantomData};

use swim_api::handlers::{FnHandler, NoHandler};

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
    LiftShared, WithHandlerContext,
};

pub mod on_clear;
pub mod on_remove;
pub mod on_update;

pub trait MapDownlinkHandlers<'a, K, V, Context>:
    OnLinked<'a, Context>
    + OnSynced<'a, HashMap<K, V>, Context>
    + OnDownlinkUpdate<'a, K, V, Context>
    + OnDownlinkRemove<'a, K, V, Context>
    + OnDownlinkClear<'a, K, V, Context>
    + OnUnlinked<'a, Context>
{
}

impl<'a, K, V, Context, LC> MapDownlinkHandlers<'a, K, V, Context> for LC where
    LC: OnLinked<'a, Context>
        + OnSynced<'a, HashMap<K, V>, Context>
        + OnDownlinkUpdate<'a, K, V, Context>
        + OnDownlinkRemove<'a, K, V, Context>
        + OnDownlinkClear<'a, K, V, Context>
        + OnUnlinked<'a, Context>
{
}

/// Trait for the lifecycle of a map downlink.
///
/// #Type Parameters
/// * `K` - The type of the keys of the downlink.
/// * `V` - The type of the values of the downlink.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
pub trait MapDownlinkLifecycle<K, V, Context>:
    for<'a> MapDownlinkHandlers<'a, K, V, Context>
{
}

impl<LC, K, V, Context> MapDownlinkLifecycle<K, V, Context> for LC where
    LC: for<'a> MapDownlinkHandlers<'a, K, V, Context>
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
    FUpd = NoHandler,
    FRem = NoHandler,
    FClr = NoHandler,
> {
    _type: PhantomData<fn(&Context, K, V)>,
    on_linked: FLinked,
    on_synced: FSynced,
    on_unlinked: FUnlinked,
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
            on_update: Default::default(),
            on_remove: Default::default(),
            on_clear: Default::default(),
        }
    }
}

impl<Context, State, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr> Clone
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        FLinked,
        FSynced,
        FUnlinked,
        FUpd,
        FRem,
        FClr,
    >
where
    State: Clone,
    FLinked: Clone,
    FSynced: Clone,
    FUnlinked: Clone,
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
            on_update: self.on_update.clone(),
            on_remove: self.on_remove.clone(),
            on_clear: self.on_clear.clone(),
        }
    }
}

impl<Context, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr> Clone
    for StatelessMapDownlinkLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
where
    FLinked: Clone,
    FSynced: Clone,
    FUnlinked: Clone,
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
            on_update: self.on_update.clone(),
            on_remove: self.on_remove.clone(),
            on_clear: self.on_clear.clone(),
        }
    }
}

impl<'a, Context, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr> OnLinked<'a, Context>
    for StatelessMapDownlinkLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
where
    FLinked: OnLinked<'a, Context>,
    FSynced: Send,
    FUnlinked: Send,
    FUpd: Send,
    FRem: Send,
    FClr: Send,
{
    type OnLinkedHandler = FLinked::OnLinkedHandler;

    fn on_linked(&'a self) -> Self::OnLinkedHandler {
        let StatelessMapDownlinkLifecycle { on_linked, .. } = self;
        on_linked.on_linked()
    }
}

impl<'a, Context, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
    OnSynced<'a, HashMap<K, V>, Context>
    for StatelessMapDownlinkLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
where
    FLinked: Send,
    FSynced: OnSynced<'a, HashMap<K, V>, Context>,
    FUnlinked: Send,
    FUpd: Send,
    FRem: Send,
    FClr: Send,
{
    type OnSyncedHandler = FSynced::OnSyncedHandler;

    fn on_synced(&'a self, map: &HashMap<K, V>) -> Self::OnSyncedHandler {
        let StatelessMapDownlinkLifecycle { on_synced, .. } = self;
        on_synced.on_synced(map)
    }
}

impl<'a, Context, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr> OnUnlinked<'a, Context>
    for StatelessMapDownlinkLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: OnUnlinked<'a, Context>,
    FUpd: Send,
    FRem: Send,
    FClr: Send,
{
    type OnUnlinkedHandler = FUnlinked::OnUnlinkedHandler;

    fn on_unlinked(&'a self) -> Self::OnUnlinkedHandler {
        let StatelessMapDownlinkLifecycle { on_unlinked, .. } = self;
        on_unlinked.on_unlinked()
    }
}

impl<'a, Context, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
    OnDownlinkUpdate<'a, K, V, Context>
    for StatelessMapDownlinkLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FUpd: OnDownlinkUpdate<'a, K, V, Context>,
    FRem: Send,
    FClr: Send,
{
    type OnUpdateHandler = FUpd::OnUpdateHandler;

    fn on_update(
        &'a self,
        key: K,
        map: &HashMap<K, V>,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler {
        let StatelessMapDownlinkLifecycle { on_update, .. } = self;
        on_update.on_update(key, map, previous, new_value)
    }
}

impl<'a, Context, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
    OnDownlinkRemove<'a, K, V, Context>
    for StatelessMapDownlinkLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FUpd: Send,
    FRem: OnDownlinkRemove<'a, K, V, Context>,
    FClr: Send,
{
    type OnRemoveHandler = FRem::OnRemoveHandler;

    fn on_remove(&'a self, key: K, map: &HashMap<K, V>, removed: V) -> Self::OnRemoveHandler {
        let StatelessMapDownlinkLifecycle { on_remove, .. } = self;
        on_remove.on_remove(key, map, removed)
    }
}

impl<'a, Context, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
    OnDownlinkClear<'a, K, V, Context>
    for StatelessMapDownlinkLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FUpd: Send,
    FRem: Send,
    FClr: OnDownlinkClear<'a, K, V, Context>,
{
    type OnClearHandler = FClr::OnClearHandler;

    fn on_clear(&'a self, map: HashMap<K, V>) -> Self::OnClearHandler {
        let StatelessMapDownlinkLifecycle { on_clear, .. } = self;
        on_clear.on_clear(map)
    }
}

pub type LiftedMapLifecycle<Context, State, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr> =
    StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        LiftShared<FLinked, State>,
        LiftShared<FSynced, State>,
        LiftShared<FUnlinked, State>,
        LiftShared<FUpd, State>,
        LiftShared<FRem, State>,
        LiftShared<FClr, State>,
    >;

impl<Context, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
    StatelessMapDownlinkLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
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
        FUpd,
        FRem,
        FClr,
    >
    where
        WithHandlerContext<Context, F>: for<'a> OnLinked<'a, Context>,
    {
        StatelessMapDownlinkLifecycle {
            _type: PhantomData,
            on_linked: WithHandlerContext::new(f),
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
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
        FUpd,
        FRem,
        FClr,
    >
    where
        WithHandlerContext<Context, F>: for<'a> OnSynced<'a, HashMap<K, V>, Context>,
    {
        StatelessMapDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: WithHandlerContext::new(f),
            on_unlinked: self.on_unlinked,
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
        FUpd,
        FRem,
        FClr,
    >
    where
        WithHandlerContext<Context, F>: for<'a> OnUnlinked<'a, Context>,
    {
        StatelessMapDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: WithHandlerContext::new(f),
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    /// Replace the 'on_update' handler with another derived from a closure.
    pub fn on_update<F>(
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
        FRem,
        FClr,
    >
    where
        WithHandlerContext<Context, F>: for<'a> OnDownlinkUpdate<'a, K, V, Context>,
    {
        StatelessMapDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_update: WithHandlerContext::new(f),
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
        FUpd,
        WithHandlerContext<Context, F>,
        FClr,
    >
    where
        WithHandlerContext<Context, F>: for<'a> OnDownlinkRemove<'a, K, V, Context>,
    {
        StatelessMapDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
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
        FUpd,
        FRem,
        WithHandlerContext<Context, F>,
    >
    where
        WithHandlerContext<Context, F>: for<'a> OnDownlinkClear<'a, K, V, Context>,
    {
        StatelessMapDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: WithHandlerContext::new(f),
        }
    }

    /// Add a state that is shared between all of the event handlers in the lifecycle.
    pub fn with_state<State>(
        self,
        state: State,
    ) -> LiftedMapLifecycle<Context, State, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
    {
        let StatelessMapDownlinkLifecycle {
            on_linked,
            on_synced,
            on_unlinked,
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
            on_update: LiftShared::new(on_update),
            on_remove: LiftShared::new(on_remove),
            on_clear: LiftShared::new(on_clear),
        }
    }
}

impl<'a, Context, State, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr> OnLinked<'a, Context>
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        FLinked,
        FSynced,
        FUnlinked,
        FUpd,
        FRem,
        FClr,
    >
where
    State: Send,
    FLinked: OnLinkedShared<'a, Context, State>,
    FSynced: Send,
    FUnlinked: Send,
    FUpd: Send,
    FRem: Send,
    FClr: Send,
{
    type OnLinkedHandler = FLinked::OnLinkedHandler;

    fn on_linked(&'a self) -> Self::OnLinkedHandler {
        let StatefulMapDownlinkLifecycle {
            on_linked,
            state,
            handler_context,
            ..
        } = self;
        on_linked.on_linked(state, *handler_context)
    }
}

impl<'a, Context, State, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
    OnSynced<'a, HashMap<K, V>, Context>
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        FLinked,
        FSynced,
        FUnlinked,
        FUpd,
        FRem,
        FClr,
    >
where
    State: Send,
    FLinked: Send,
    FSynced: OnSyncedShared<'a, HashMap<K, V>, Context, State>,
    FUnlinked: Send,
    FUpd: Send,
    FRem: Send,
    FClr: Send,
{
    type OnSyncedHandler = FSynced::OnSyncedHandler;

    fn on_synced(&'a self, value: &HashMap<K, V>) -> Self::OnSyncedHandler {
        let StatefulMapDownlinkLifecycle {
            on_synced,
            state,
            handler_context,
            ..
        } = self;
        on_synced.on_synced(state, *handler_context, value)
    }
}

impl<'a, Context, State, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
    OnUnlinked<'a, Context>
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        FLinked,
        FSynced,
        FUnlinked,
        FUpd,
        FRem,
        FClr,
    >
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: OnUnlinkedShared<'a, Context, State>,
    FUpd: Send,
    FRem: Send,
    FClr: Send,
{
    type OnUnlinkedHandler = FUnlinked::OnUnlinkedHandler;

    fn on_unlinked(&'a self) -> Self::OnUnlinkedHandler {
        let StatefulMapDownlinkLifecycle {
            on_unlinked,
            state,
            handler_context,
            ..
        } = self;
        on_unlinked.on_unlinked(state, *handler_context)
    }
}

impl<'a, Context, State, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
    OnDownlinkUpdate<'a, K, V, Context>
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        FLinked,
        FSynced,
        FUnlinked,
        FUpd,
        FRem,
        FClr,
    >
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FUpd: OnDownlinkUpdateShared<'a, K, V, Context, State>,
    FRem: Send,
    FClr: Send,
{
    type OnUpdateHandler = FUpd::OnUpdateHandler;

    fn on_update(
        &'a self,
        key: K,
        map: &HashMap<K, V>,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler {
        let StatefulMapDownlinkLifecycle {
            on_update,
            state,
            handler_context,
            ..
        } = self;
        on_update.on_update(state, *handler_context, key, map, previous, new_value)
    }
}

impl<'a, Context, State, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
    OnDownlinkRemove<'a, K, V, Context>
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        FLinked,
        FSynced,
        FUnlinked,
        FUpd,
        FRem,
        FClr,
    >
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FUpd: Send,
    FRem: OnDownlinkRemoveShared<'a, K, V, Context, State>,
    FClr: Send,
{
    type OnRemoveHandler = FRem::OnRemoveHandler;

    fn on_remove(&'a self, key: K, map: &HashMap<K, V>, removed: V) -> Self::OnRemoveHandler {
        let StatefulMapDownlinkLifecycle {
            on_remove,
            state,
            handler_context,
            ..
        } = self;
        on_remove.on_remove(state, *handler_context, key, map, removed)
    }
}

impl<'a, Context, State, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
    OnDownlinkClear<'a, K, V, Context>
    for StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        FLinked,
        FSynced,
        FUnlinked,
        FUpd,
        FRem,
        FClr,
    >
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FUpd: Send,
    FRem: Send,
    FClr: OnDownlinkClearShared<'a, K, V, Context, State>,
{
    type OnClearHandler = FClr::OnClearHandler;

    fn on_clear(&'a self, map: HashMap<K, V>) -> Self::OnClearHandler {
        let StatefulMapDownlinkLifecycle {
            on_clear,
            state,
            handler_context,
            ..
        } = self;
        on_clear.on_clear(state, *handler_context, map)
    }
}

impl<Context, State, K, V, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
    StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        FLinked,
        FSynced,
        FUnlinked,
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
        FUpd,
        FRem,
        FClr,
    >
    where
        FnHandler<F>: for<'a> OnLinkedShared<'a, Context, State>,
    {
        StatefulMapDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: FnHandler(f),
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
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
        FUpd,
        FRem,
        FClr,
    >
    where
        FnHandler<F>: for<'a> OnSyncedShared<'a, HashMap<K, V>, Context, State>,
    {
        StatefulMapDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: FnHandler(f),
            on_unlinked: self.on_unlinked,
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
        FUpd,
        FRem,
        FClr,
    >
    where
        FnHandler<F>: for<'a> OnUnlinkedShared<'a, Context, State>,
    {
        StatefulMapDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: FnHandler(f),
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: self.on_clear,
        }
    }

    /// Replace the 'on_update' handler with another derived from a closure.
    pub fn on_update<F>(
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
        FRem,
        FClr,
    >
    where
        FnHandler<F>: for<'a> OnDownlinkUpdateShared<'a, K, V, Context, State>,
    {
        StatefulMapDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_update: FnHandler(f),
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
        FUpd,
        FnHandler<F>,
        FClr,
    >
    where
        FnHandler<F>: for<'a> OnDownlinkRemoveShared<'a, K, V, Context, State>,
    {
        StatefulMapDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
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
        FUpd,
        FRem,
        FnHandler<F>,
    >
    where
        FnHandler<F>: for<'a> OnDownlinkClearShared<'a, K, V, Context, State>,
    {
        StatefulMapDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_update: self.on_update,
            on_remove: self.on_remove,
            on_clear: FnHandler(f),
        }
    }
}
