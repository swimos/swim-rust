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

use swim_api::handlers::NoHandler;

use crate::{
    agent_lifecycle::utility::HandlerContext,
    lanes::value::lifecycle::{on_event::OnEvent, on_set::OnSet},
};

use self::{
    on_event::{OnDownlinkEvent, OnDownlinkEventShared},
    on_set::{OnDownlinkSet, OnDownlinkSetShared},
};

use super::{
    on_linked::{OnLinked, OnLinkedShared},
    on_synced::{OnSynced, OnSyncedShared},
    on_unlinked::{OnUnlinked, OnUnlinkedShared},
};

pub mod on_event;
pub mod on_set;

pub trait ValueDownlinkHandlers<'a, T, Context>:
    OnLinked<'a, Context> + OnSynced<'a, T, Context> + OnEvent<'a, T, Context> + OnSet<'a, T, Context> + OnUnlinked<'a, Context>
{
}

impl<'a, T, Context, LC> ValueDownlinkHandlers<'a, T, Context> for LC
where
    LC: OnLinked<'a, Context> + OnSynced<'a, T, Context> + OnEvent<'a, T, Context> + OnSet<'a, T, Context> + OnUnlinked<'a, Context>,
{}

pub trait ValueDownlinkLifecycle<T, Context>:
    for<'a> ValueDownlinkHandlers<'a, T, Context>
{
}

impl<LC, T, Context> ValueDownlinkLifecycle<T, Context> for LC where
    LC: for<'a> ValueDownlinkHandlers<'a, T, Context>
{
}

#[derive(Debug)]
pub struct StatefulValueDownlinkLifecycle<
    Context,
    State,
    FLinked = NoHandler,
    FSynced = NoHandler,
    FUnlinked = NoHandler,
    FEv = NoHandler,
    FSet = NoHandler,
> {
    state: State,
    handler_context: HandlerContext<Context>,
    on_linked: FLinked,
    on_synced: FSynced,
    on_unlinked: FUnlinked,
    on_event: FEv,
    on_set: FSet,
}

impl<Context, State> StatefulValueDownlinkLifecycle<Context, State> {
    pub fn new(state: State) -> Self {
        StatefulValueDownlinkLifecycle {
            state,
            handler_context: Default::default(),
            on_linked: Default::default(),
            on_synced: Default::default(),
            on_unlinked: Default::default(),
            on_event: Default::default(),
            on_set: Default::default(),
        }
    }
}

impl<Context, State, FLinked, FSynced, FUnlinked, FEv, FSet> Clone
    for StatefulValueDownlinkLifecycle<Context, State, FLinked, FSynced, FUnlinked, FEv, FSet>
where
    State: Clone,
    FLinked: Clone,
    FSynced: Clone,
    FUnlinked: Clone,
    FEv: Clone,
    FSet: Clone,
{
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            handler_context: HandlerContext::default(),
            on_linked: self.on_linked.clone(),
            on_synced: self.on_synced.clone(),
            on_unlinked: self.on_unlinked.clone(),
            on_event: self.on_event.clone(),
            on_set: self.on_set.clone(),
        }
    }
}

impl<'a, Context, State, FLinked, FSynced, FUnlinked, FEv, FSet> OnLinked<'a, Context>
    for StatefulValueDownlinkLifecycle<Context, State, FLinked, FSynced, FUnlinked, FEv, FSet>
where
    State: Send,
    FLinked: OnLinkedShared<'a, Context, State>,
    FSynced: Send,
    FUnlinked: Send,
    FEv: Send,
    FSet: Send,
{
    type OnLinkedHandler = FLinked::OnLinkedHandler;

    fn on_linked(&'a self) -> Self::OnLinkedHandler {
        let StatefulValueDownlinkLifecycle {
            on_linked,
            state,
            handler_context,
            ..
        } = self;
        on_linked.on_linked(state, *handler_context)
    }
}

impl<'a, T, Context, State, FLinked, FSynced, FUnlinked, FEv, FSet> OnSynced<'a, T, Context>
    for StatefulValueDownlinkLifecycle<Context, State, FLinked, FSynced, FUnlinked, FEv, FSet>
where
    State: Send,
    FLinked: Send,
    FSynced: OnSyncedShared<'a, T, Context, State>,
    FUnlinked: Send,
    FEv: Send,
    FSet: Send,
{
    type OnSyncedHandler = FSynced::OnSyncedHandler;

    fn on_synced(&'a self, value: &T) -> Self::OnSyncedHandler {
        let StatefulValueDownlinkLifecycle {
            on_synced,
            state,
            handler_context,
            ..
        } = self;
        on_synced.on_synced(state, *handler_context, value)
    }
}

impl<'a, Context, State, FLinked, FSynced, FUnlinked, FEv, FSet> OnUnlinked<'a, Context>
    for StatefulValueDownlinkLifecycle<Context, State, FLinked, FSynced, FUnlinked, FEv, FSet>
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: OnUnlinkedShared<'a, Context, State>,
    FEv: Send,
    FSet: Send,
{
    type OnUnlinkedHandler = FUnlinked::OnUnlinkedHandler;

    fn on_unlinked(&'a self) -> Self::OnUnlinkedHandler {
        let StatefulValueDownlinkLifecycle {
            on_unlinked,
            state,
            handler_context,
            ..
        } = self;
        on_unlinked.on_unlinked(state, *handler_context)
    }
}

impl<'a, T, Context, State, FLinked, FSynced, FUnlinked, FEv, FSet> OnDownlinkEvent<'a, T, Context>
    for StatefulValueDownlinkLifecycle<Context, State, FLinked, FSynced, FUnlinked, FEv, FSet>
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FEv: OnDownlinkEventShared<'a, T, Context, State>,
    FSet: Send,
{
    type OnEventHandler = FEv::OnEventHandler;

    fn on_event(&'a self, value: &T) -> Self::OnEventHandler {
        let StatefulValueDownlinkLifecycle {
            on_event,
            state,
            handler_context,
            ..
        } = self;
        on_event.on_event(state, *handler_context, value)
    }
}

impl<'a, T, Context, State, FLinked, FSynced, FUnlinked, FEv, FSet> OnDownlinkSet<'a, T, Context>
    for StatefulValueDownlinkLifecycle<Context, State, FLinked, FSynced, FUnlinked, FEv, FSet>
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FEv: Send,
    FSet: OnDownlinkSetShared<'a, T, Context, State>,
{
    type OnSetHandler = FSet::OnSetHandler;

    fn on_set(&'a self, previous: Option<T>, new_value: &T) -> Self::OnSetHandler {
        let StatefulValueDownlinkLifecycle {
            on_set,
            state,
            handler_context,
            ..
        } = self;
        on_set.on_set(state, *handler_context, previous, new_value)
    }
}
