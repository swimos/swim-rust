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

use std::marker::PhantomData;

use swim_api::handlers::{FnHandler, NoHandler};

use crate::agent_lifecycle::utility::HandlerContext;

use self::{
    on_event::{OnDownlinkEvent, OnDownlinkEventShared},
    on_set::{OnDownlinkSet, OnDownlinkSetShared},
};

use super::{
    on_linked::{OnLinked, OnLinkedShared},
    on_synced::{OnSynced, OnSyncedShared},
    on_unlinked::{OnUnlinked, OnUnlinkedShared},
    LiftShared, WithHandlerContext,
};

pub mod on_event;
pub mod on_set;

pub trait ValueDownlinkHandlers<'a, T, Context>:
    OnLinked<'a, Context>
    + OnSynced<'a, T, Context>
    + OnDownlinkEvent<'a, T, Context>
    + OnDownlinkSet<'a, T, Context>
    + OnUnlinked<'a, Context>
{
}

impl<'a, T, Context, LC> ValueDownlinkHandlers<'a, T, Context> for LC where
    LC: OnLinked<'a, Context>
        + OnSynced<'a, T, Context>
        + OnDownlinkEvent<'a, T, Context>
        + OnDownlinkSet<'a, T, Context>
        + OnUnlinked<'a, Context>
{
}

/// Trait for the lifecycle of a value downlink.
///
/// #Type Parameters
/// * `T` - The type of the state of the lane.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
pub trait ValueDownlinkLifecycle<T, Context>:
    for<'a> ValueDownlinkHandlers<'a, T, Context>
{
}

impl<LC, T, Context> ValueDownlinkLifecycle<T, Context> for LC where
    LC: for<'a> ValueDownlinkHandlers<'a, T, Context>
{
}

/// A lifecycle for a value downlink where the individual event handlers can shared state.
///
/// #Type Parameters
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `State` - The type of the shared state.
/// * `T` - The type of the downlink.
/// * `FLinked` - The type of the 'on_linked' handler.
/// * `FSynced` - The type of the 'on_synced' handler.
/// * `FUnlinked` - The type of the 'on_unlinked' handler.
/// * `FEv` - The type of the 'on_event' handler.
/// * `FSet` - The type of the 'on_set' handler.
///
#[derive(Debug)]
pub struct StatefulValueDownlinkLifecycle<
    Context,
    State,
    T,
    FLinked = NoHandler,
    FSynced = NoHandler,
    FUnlinked = NoHandler,
    FEv = NoHandler,
    FSet = NoHandler,
> {
    _type: PhantomData<fn(T)>,
    state: State,
    handler_context: HandlerContext<Context>,
    on_linked: FLinked,
    on_synced: FSynced,
    on_unlinked: FUnlinked,
    on_event: FEv,
    on_set: FSet,
}

impl<Context, State, T> StatefulValueDownlinkLifecycle<Context, State, T> {
    pub fn new(state: State) -> Self {
        StatefulValueDownlinkLifecycle {
            _type: PhantomData,
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

impl<Context, State, T, FLinked, FSynced, FUnlinked, FEv, FSet> Clone
    for StatefulValueDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FEv, FSet>
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
            _type: PhantomData,
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

impl<'a, Context, State, T, FLinked, FSynced, FUnlinked, FEv, FSet> OnLinked<'a, Context>
    for StatefulValueDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FEv, FSet>
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

impl<'a, Context, State, T, FLinked, FSynced, FUnlinked, FEv, FSet> OnSynced<'a, T, Context>
    for StatefulValueDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FEv, FSet>
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

impl<'a, Context, State, T, FLinked, FSynced, FUnlinked, FEv, FSet> OnUnlinked<'a, Context>
    for StatefulValueDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FEv, FSet>
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

impl<'a, Context, State, T, FLinked, FSynced, FUnlinked, FEv, FSet> OnDownlinkEvent<'a, T, Context>
    for StatefulValueDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FEv, FSet>
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

impl<'a, Context, State, T, FLinked, FSynced, FUnlinked, FEv, FSet> OnDownlinkSet<'a, T, Context>
    for StatefulValueDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FEv, FSet>
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

impl<Context, State, T, FLinked, FSynced, FUnlinked, FEv, FSet>
    StatefulValueDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FEv, FSet>
{
    /// Replace the 'on_linked' handler with another derived from a closure.
    pub fn on_linked<F>(
        self,
        f: F,
    ) -> StatefulValueDownlinkLifecycle<
        Context,
        State,
        T,
        FnHandler<F>,
        FSynced,
        FUnlinked,
        FEv,
        FSet,
    >
    where
        FnHandler<F>: for<'a> OnLinkedShared<'a, Context, State>,
    {
        StatefulValueDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: FnHandler(f),
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_event: self.on_event,
            on_set: self.on_set,
        }
    }

    /// Replace the 'on_synced' handler with another derived from a closure.
    pub fn on_synced<F>(
        self,
        f: F,
    ) -> StatefulValueDownlinkLifecycle<
        Context,
        State,
        T,
        FLinked,
        FnHandler<F>,
        FUnlinked,
        FEv,
        FSet,
    >
    where
        FnHandler<F>: for<'a> OnSyncedShared<'a, T, Context, State>,
    {
        StatefulValueDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: FnHandler(f),
            on_unlinked: self.on_unlinked,
            on_event: self.on_event,
            on_set: self.on_set,
        }
    }

    /// Replace the 'on_unlinked' handler with another derived from a closure.
    pub fn on_unlinked<F>(
        self,
        f: F,
    ) -> StatefulValueDownlinkLifecycle<Context, State, T, FLinked, FSynced, FnHandler<F>, FEv, FSet>
    where
        FnHandler<F>: for<'a> OnUnlinkedShared<'a, Context, State>,
    {
        StatefulValueDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: FnHandler(f),
            on_event: self.on_event,
            on_set: self.on_set,
        }
    }

    /// Replace the 'on_event' handler with another derived from a closure.
    pub fn on_event<F>(
        self,
        f: F,
    ) -> StatefulValueDownlinkLifecycle<
        Context,
        State,
        T,
        FLinked,
        FSynced,
        FUnlinked,
        FnHandler<F>,
        FSet,
    >
    where
        FnHandler<F>: for<'a> OnDownlinkEventShared<'a, T, Context, State>,
    {
        StatefulValueDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_event: FnHandler(f),
            on_set: self.on_set,
        }
    }

    /// Replace the 'on_set' handler with another derived from a closure.
    pub fn on_set<F>(
        self,
        f: F,
    ) -> StatefulValueDownlinkLifecycle<
        Context,
        State,
        T,
        FLinked,
        FSynced,
        FUnlinked,
        FEv,
        FnHandler<F>,
    >
    where
        FnHandler<F>: for<'a> OnDownlinkSetShared<'a, T, Context, State>,
    {
        StatefulValueDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_event: self.on_event,
            on_set: FnHandler(f),
        }
    }
}

/// A lifecycle for a value downlink where the individual event handlers can shared state.
///
/// #Type Parameters
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `State` - The type of the shared state.
/// * `T` - The type of the downlink.
/// * `FLinked` - The type of the 'on_linked' handler.
/// * `FSynced` - The type of the 'on_synced' handler.
/// * `FUnlinked` - The type of the 'on_unlinked' handler.
/// * `FEv` - The type of the 'on_event' handler.
/// * `FSet` - The type of the 'on_set' handler.
#[derive(Debug)]
pub struct StatelessValueDownlinkLifecycle<
    Context,
    T,
    FLinked = NoHandler,
    FSynced = NoHandler,
    FUnlinked = NoHandler,
    FEv = NoHandler,
    FSet = NoHandler,
> {
    _type: PhantomData<fn(&Context, T)>,
    on_linked: FLinked,
    on_synced: FSynced,
    on_unlinked: FUnlinked,
    on_event: FEv,
    on_set: FSet,
}

impl<Context, T> Default for StatelessValueDownlinkLifecycle<Context, T> {
    fn default() -> Self {
        Self {
            _type: Default::default(),
            on_linked: Default::default(),
            on_synced: Default::default(),
            on_unlinked: Default::default(),
            on_event: Default::default(),
            on_set: Default::default(),
        }
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FEv, FSet> Clone
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FEv, FSet>
where
    FLinked: Clone,
    FSynced: Clone,
    FUnlinked: Clone,
    FEv: Clone,
    FSet: Clone,
{
    fn clone(&self) -> Self {
        Self {
            _type: PhantomData,
            on_linked: self.on_linked.clone(),
            on_synced: self.on_synced.clone(),
            on_unlinked: self.on_unlinked.clone(),
            on_event: self.on_event.clone(),
            on_set: self.on_set.clone(),
        }
    }
}

impl<'a, Context, T, FLinked, FSynced, FUnlinked, FEv, FSet> OnLinked<'a, Context>
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FEv, FSet>
where
    FLinked: OnLinked<'a, Context>,
    FSynced: Send,
    FUnlinked: Send,
    FEv: Send,
    FSet: Send,
{
    type OnLinkedHandler = FLinked::OnLinkedHandler;

    fn on_linked(&'a self) -> Self::OnLinkedHandler {
        let StatelessValueDownlinkLifecycle { on_linked, .. } = self;
        on_linked.on_linked()
    }
}

impl<'a, Context, T, FLinked, FSynced, FUnlinked, FEv, FSet> OnSynced<'a, T, Context>
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FEv, FSet>
where
    FLinked: Send,
    FSynced: OnSynced<'a, T, Context>,
    FUnlinked: Send,
    FEv: Send,
    FSet: Send,
{
    type OnSyncedHandler = FSynced::OnSyncedHandler;

    fn on_synced(&'a self, value: &T) -> Self::OnSyncedHandler {
        let StatelessValueDownlinkLifecycle { on_synced, .. } = self;
        on_synced.on_synced(value)
    }
}

impl<'a, Context, T, FLinked, FSynced, FUnlinked, FEv, FSet> OnUnlinked<'a, Context>
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FEv, FSet>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: OnUnlinked<'a, Context>,
    FEv: Send,
    FSet: Send,
{
    type OnUnlinkedHandler = FUnlinked::OnUnlinkedHandler;

    fn on_unlinked(&'a self) -> Self::OnUnlinkedHandler {
        let StatelessValueDownlinkLifecycle { on_unlinked, .. } = self;
        on_unlinked.on_unlinked()
    }
}

impl<'a, Context, T, FLinked, FSynced, FUnlinked, FEv, FSet> OnDownlinkEvent<'a, T, Context>
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FEv, FSet>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FEv: OnDownlinkEvent<'a, T, Context>,
    FSet: Send,
{
    type OnEventHandler = FEv::OnEventHandler;

    fn on_event(&'a self, value: &T) -> Self::OnEventHandler {
        let StatelessValueDownlinkLifecycle { on_event, .. } = self;
        on_event.on_event(value)
    }
}

impl<'a, Context, T, FLinked, FSynced, FUnlinked, FEv, FSet> OnDownlinkSet<'a, T, Context>
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FEv, FSet>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FEv: Send,
    FSet: OnDownlinkSet<'a, T, Context>,
{
    type OnSetHandler = FSet::OnSetHandler;

    fn on_set(&'a self, previous: Option<T>, new_value: &T) -> Self::OnSetHandler {
        let StatelessValueDownlinkLifecycle { on_set, .. } = self;
        on_set.on_set(previous, new_value)
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FEv, FSet>
    StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FEv, FSet>
{
    /// Replace the 'on_linked' handler with another derived from a closure.
    pub fn on_linked<F>(
        self,
        f: F,
    ) -> StatelessValueDownlinkLifecycle<
        Context,
        T,
        WithHandlerContext<Context, F>,
        FSynced,
        FUnlinked,
        FEv,
        FSet,
    >
    where
        WithHandlerContext<Context, F>: for<'a> OnLinked<'a, Context>,
    {
        StatelessValueDownlinkLifecycle {
            _type: PhantomData,
            on_linked: WithHandlerContext::new(f),
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_event: self.on_event,
            on_set: self.on_set,
        }
    }

    /// Replace the 'on_synced' handler with another derived from a closure.
    pub fn on_synced<F>(
        self,
        f: F,
    ) -> StatelessValueDownlinkLifecycle<
        Context,
        T,
        FLinked,
        WithHandlerContext<Context, F>,
        FUnlinked,
        FEv,
        FSet,
    >
    where
        WithHandlerContext<Context, F>: for<'a> OnSynced<'a, T, Context>,
    {
        StatelessValueDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: WithHandlerContext::new(f),
            on_unlinked: self.on_unlinked,
            on_event: self.on_event,
            on_set: self.on_set,
        }
    }

    /// Replace the 'on_unlinked' handler with another derived from a closure.
    pub fn on_unlinked<F>(
        self,
        f: F,
    ) -> StatelessValueDownlinkLifecycle<
        Context,
        T,
        FLinked,
        FSynced,
        WithHandlerContext<Context, F>,
        FEv,
        FSet,
    >
    where
        WithHandlerContext<Context, F>: for<'a> OnUnlinked<'a, Context>,
    {
        StatelessValueDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: WithHandlerContext::new(f),
            on_event: self.on_event,
            on_set: self.on_set,
        }
    }

    /// Replace the 'on_event' handler with another derived from a closure.
    pub fn on_event<F>(
        self,
        f: F,
    ) -> StatelessValueDownlinkLifecycle<
        Context,
        T,
        FLinked,
        FSynced,
        FUnlinked,
        WithHandlerContext<Context, F>,
        FSet,
    >
    where
        WithHandlerContext<Context, F>: for<'a> OnDownlinkEvent<'a, T, Context>,
    {
        StatelessValueDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_event: WithHandlerContext::new(f),
            on_set: self.on_set,
        }
    }

    /// Replace the 'on_set' handler with another derived from a closure.
    pub fn on_set<F>(
        self,
        f: F,
    ) -> StatelessValueDownlinkLifecycle<
        Context,
        T,
        FLinked,
        FSynced,
        FUnlinked,
        FEv,
        WithHandlerContext<Context, F>,
    >
    where
        WithHandlerContext<Context, F>: for<'a> OnDownlinkSet<'a, T, Context>,
    {
        StatelessValueDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_event: self.on_event,
            on_set: WithHandlerContext::new(f),
        }
    }

    /// Add a state that is shared between the handlers of the lifeycle.
    pub fn with_state<State>(
        self,
        state: State,
    ) -> StatefulValueDownlinkLifecycle<
        Context,
        State,
        T,
        LiftShared<FLinked, State>,
        LiftShared<FSynced, State>,
        LiftShared<FUnlinked, State>,
        LiftShared<FEv, State>,
        LiftShared<FSet, State>,
    > {
        let StatelessValueDownlinkLifecycle {
            on_linked,
            on_synced,
            on_unlinked,
            on_event,
            on_set,
            ..
        } = self;
        StatefulValueDownlinkLifecycle {
            _type: PhantomData,
            state,
            handler_context: Default::default(),
            on_linked: LiftShared::new(on_linked),
            on_synced: LiftShared::new(on_synced),
            on_unlinked: LiftShared::new(on_unlinked),
            on_event: LiftShared::new(on_event),
            on_set: LiftShared::new(on_set),
        }
    }
}
