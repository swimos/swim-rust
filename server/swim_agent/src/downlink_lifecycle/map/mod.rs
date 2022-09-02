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

use std::collections::HashMap;

use swim_api::handlers::NoHandler;

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

pub trait MapDownlinkLifecycle<K, V, Context>:
    for<'a> MapDownlinkHandlers<'a, K, V, Context>
{
}

impl<LC, K, V, Context> MapDownlinkLifecycle<K, V, Context> for LC where
    LC: for<'a> MapDownlinkHandlers<'a, K, V, Context>
{
}

#[derive(Debug)]
pub struct StatefulMapDownlinkLifecycle<
    Context,
    State,
    FLinked = NoHandler,
    FSynced = NoHandler,
    FUnlinked = NoHandler,
    FUpd = NoHandler,
    FRem = NoHandler,
    FClr = NoHandler,
> {
    state: State,
    handler_context: HandlerContext<Context>,
    on_linked: FLinked,
    on_synced: FSynced,
    on_unlinked: FUnlinked,
    on_update: FUpd,
    on_remove: FRem,
    on_clear: FClr,
}

impl<Context, State> StatefulMapDownlinkLifecycle<Context, State> {
    pub fn new(state: State) -> Self {
        StatefulMapDownlinkLifecycle {
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

impl<Context, State, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr> Clone
    for StatefulMapDownlinkLifecycle<Context, State, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
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

impl<'a, Context, State, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr> OnLinked<'a, Context>
    for StatefulMapDownlinkLifecycle<Context, State, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
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

impl<'a, K, V, Context, State, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
    OnSynced<'a, HashMap<K, V>, Context>
    for StatefulMapDownlinkLifecycle<Context, State, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
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

impl<'a, Context, State, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr> OnUnlinked<'a, Context>
    for StatefulMapDownlinkLifecycle<Context, State, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
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

impl<'a, K, V, Context, State, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
    OnDownlinkUpdate<'a, K, V, Context>
    for StatefulMapDownlinkLifecycle<Context, State, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
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

impl<'a, K, V, Context, State, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
    OnDownlinkRemove<'a, K, V, Context>
    for StatefulMapDownlinkLifecycle<Context, State, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
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

impl<'a, K, V, Context, State, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
    OnDownlinkClear<'a, K, V, Context>
    for StatefulMapDownlinkLifecycle<Context, State, FLinked, FSynced, FUnlinked, FUpd, FRem, FClr>
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
