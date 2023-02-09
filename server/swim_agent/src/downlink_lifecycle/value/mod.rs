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

use std::{borrow::Borrow, marker::PhantomData};

use swim_api::handlers::{BorrowHandler, FnHandler, NoHandler};

use crate::agent_lifecycle::utility::HandlerContext;

use self::{
    on_event::{OnDownlinkEvent, OnDownlinkEventShared},
    on_set::{OnDownlinkSet, OnDownlinkSetShared},
};

use super::{
    on_failed::{OnFailed, OnFailedShared},
    on_linked::{OnLinked, OnLinkedShared},
    on_synced::{OnSynced, OnSyncedShared},
    on_unlinked::{OnUnlinked, OnUnlinkedShared},
    LiftShared, WithHandlerContext, WithHandlerContextBorrow,
};

pub mod on_event;
pub mod on_set;

/// Trait for the lifecycle of a value downlink.
///
/// #Type Parameters
/// * `T` - The type of the state of the lane.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
pub trait ValueDownlinkLifecycle<T, Context>:
    OnLinked<Context>
    + OnSynced<(), Context>
    + OnDownlinkEvent<T, Context>
    + OnUnlinked<Context>
    + OnFailed<Context>
{
}

impl<LC, T, Context> ValueDownlinkLifecycle<T, Context> for LC where
    LC: OnLinked<Context>
        + OnSynced<(), Context>
        + OnDownlinkEvent<T, Context>
        + OnUnlinked<Context>
        + OnFailed<Context>
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
/// * `FFailed` - The type of the 'on_failed' handler.
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
    FFailed = NoHandler,
    FEv = NoHandler,
    FSet = NoHandler,
> {
    _type: PhantomData<fn(T)>,
    state: State,
    handler_context: HandlerContext<Context>,
    on_linked: FLinked,
    on_synced: FSynced,
    on_unlinked: FUnlinked,
    on_failed: FFailed,
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
            on_failed: Default::default(),
            on_event: Default::default(),
            on_set: Default::default(),
        }
    }
}

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet> Clone
    for StatefulValueDownlinkLifecycle<
        Context,
        State,
        T,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FEv,
        FSet,
    >
where
    State: Clone,
    FLinked: Clone,
    FSynced: Clone,
    FUnlinked: Clone,
    FFailed: Clone,
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
            on_failed: self.on_failed.clone(),
            on_event: self.on_event.clone(),
            on_set: self.on_set.clone(),
        }
    }
}

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet> OnLinked<Context>
    for StatefulValueDownlinkLifecycle<
        Context,
        State,
        T,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FEv,
        FSet,
    >
where
    State: Send,
    FLinked: OnLinkedShared<Context, State>,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FEv: Send,
    FSet: Send,
{
    type OnLinkedHandler<'a> = FLinked::OnLinkedHandler<'a>
    where
        Self: 'a;

    fn on_linked(&self) -> Self::OnLinkedHandler<'_> {
        let StatefulValueDownlinkLifecycle {
            on_linked,
            state,
            handler_context,
            ..
        } = self;
        on_linked.on_linked(state, *handler_context)
    }
}

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet> OnSynced<(), Context>
    for StatefulValueDownlinkLifecycle<
        Context,
        State,
        T,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FEv,
        FSet,
    >
where
    State: Send,
    FLinked: Send,
    FSynced: OnSyncedShared<(), Context, State>,
    FUnlinked: Send,
    FFailed: Send,
    FEv: Send,
    FSet: Send,
{
    type OnSyncedHandler<'a> = FSynced::OnSyncedHandler<'a>
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, value: &()) -> Self::OnSyncedHandler<'a> {
        let StatefulValueDownlinkLifecycle {
            on_synced,
            state,
            handler_context,
            ..
        } = self;
        on_synced.on_synced(state, *handler_context, value)
    }
}

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet> OnUnlinked<Context>
    for StatefulValueDownlinkLifecycle<
        Context,
        State,
        T,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FEv,
        FSet,
    >
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: OnUnlinkedShared<Context, State>,
    FFailed: Send,
    FEv: Send,
    FSet: Send,
{
    type OnUnlinkedHandler<'a> = FUnlinked::OnUnlinkedHandler<'a>
    where
        Self: 'a;

    fn on_unlinked(&self) -> Self::OnUnlinkedHandler<'_> {
        let StatefulValueDownlinkLifecycle {
            on_unlinked,
            state,
            handler_context,
            ..
        } = self;
        on_unlinked.on_unlinked(state, *handler_context)
    }
}

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet> OnFailed<Context>
    for StatefulValueDownlinkLifecycle<
        Context,
        State,
        T,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FEv,
        FSet,
    >
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: OnFailedShared<Context, State>,
    FEv: Send,
    FSet: Send,
{
    type OnFailedHandler<'a> = FFailed::OnFailedHandler<'a>
    where
        Self: 'a;

    fn on_failed(&self) -> Self::OnFailedHandler<'_> {
        let StatefulValueDownlinkLifecycle {
            on_failed,
            state,
            handler_context,
            ..
        } = self;
        on_failed.on_failed(state, *handler_context)
    }
}

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet> OnDownlinkEvent<T, Context>
    for StatefulValueDownlinkLifecycle<
        Context,
        State,
        T,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FEv,
        FSet,
    >
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FEv: OnDownlinkEventShared<T, Context, State>,
    FSet: Send,
{
    type OnEventHandler<'a> = FEv::OnEventHandler<'a>
    where
        Self: 'a;

    fn on_event<'a>(&'a self, value: T) -> Self::OnEventHandler<'a> {
        let StatefulValueDownlinkLifecycle {
            on_event,
            state,
            handler_context,
            ..
        } = self;
        on_event.on_event(state, *handler_context, value)
    }
}

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet> OnDownlinkSet<T, Context>
    for StatefulValueDownlinkLifecycle<
        Context,
        State,
        T,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FEv,
        FSet,
    >
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FEv: Send,
    FSet: OnDownlinkSetShared<T, Context, State>,
{
    type OnSetHandler<'a> = FSet::OnSetHandler<'a>
    where
        Self: 'a;

    fn on_set<'a>(&'a self, previous: Option<T>, new_value: &T) -> Self::OnSetHandler<'a> {
        let StatefulValueDownlinkLifecycle {
            on_set,
            state,
            handler_context,
            ..
        } = self;
        on_set.on_set(state, *handler_context, previous, new_value)
    }
}

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet>
    StatefulValueDownlinkLifecycle<
        Context,
        State,
        T,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FEv,
        FSet,
    >
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
        FFailed,
        FEv,
        FSet,
    >
    where
        FnHandler<F>: OnLinkedShared<Context, State>,
    {
        StatefulValueDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: FnHandler(f),
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
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
        FFailed,
        FEv,
        FSet,
    >
    where
        FnHandler<F>: OnSyncedShared<(), Context, State>,
    {
        StatefulValueDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: FnHandler(f),
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_event: self.on_event,
            on_set: self.on_set,
        }
    }

    /// Replace the 'on_unlinked' handler with another derived from a closure.
    pub fn on_unlinked<F>(
        self,
        f: F,
    ) -> StatefulValueDownlinkLifecycle<
        Context,
        State,
        T,
        FLinked,
        FSynced,
        FnHandler<F>,
        FFailed,
        FEv,
        FSet,
    >
    where
        FnHandler<F>: OnUnlinkedShared<Context, State>,
    {
        StatefulValueDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: FnHandler(f),
            on_failed: self.on_failed,
            on_event: self.on_event,
            on_set: self.on_set,
        }
    }

    /// Replace the 'on_failed' handler with another derived from a closure.
    pub fn on_failed<F>(
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
        FEv,
        FSet,
    >
    where
        FnHandler<F>: OnFailedShared<Context, State>,
    {
        StatefulValueDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: FnHandler(f),
            on_event: self.on_event,
            on_set: self.on_set,
        }
    }

    /// Replace the 'on_event' handler with another derived from a closure.
    pub fn on_event<F, B>(
        self,
        f: F,
    ) -> StatefulValueDownlinkLifecycle<
        Context,
        State,
        T,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        BorrowHandler<F, B>,
        FSet,
    >
    where
        B: ?Sized,
        T: Borrow<B>,
        BorrowHandler<F, B>: OnDownlinkEventShared<T, Context, State>,
    {
        StatefulValueDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_event: BorrowHandler::new(f),
            on_set: self.on_set,
        }
    }

    /// Replace the 'on_set' handler with another derived from a closure.
    pub fn on_set<F, B>(
        self,
        f: F,
    ) -> StatefulValueDownlinkLifecycle<
        Context,
        State,
        T,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FEv,
        BorrowHandler<F, B>,
    >
    where
        B: ?Sized,
        T: Borrow<B>,
        BorrowHandler<F, B>: OnDownlinkSetShared<T, Context, State>,
    {
        StatefulValueDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_event: self.on_event,
            on_set: BorrowHandler::new(f),
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
/// * `FFailed` - The type of the 'on_failed' handler.
/// * `FEv` - The type of the 'on_event' handler.
/// * `FSet` - The type of the 'on_set' handler.
#[derive(Debug)]
pub struct StatelessValueDownlinkLifecycle<
    Context,
    T,
    FLinked = NoHandler,
    FSynced = NoHandler,
    FUnlinked = NoHandler,
    FFailed = NoHandler,
    FEv = NoHandler,
    FSet = NoHandler,
> {
    _type: PhantomData<fn(&Context, T)>,
    on_linked: FLinked,
    on_synced: FSynced,
    on_unlinked: FUnlinked,
    on_failed: FFailed,
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
            on_failed: Default::default(),
            on_event: Default::default(),
            on_set: Default::default(),
        }
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet> Clone
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet>
where
    FLinked: Clone,
    FSynced: Clone,
    FUnlinked: Clone,
    FFailed: Clone,
    FEv: Clone,
    FSet: Clone,
{
    fn clone(&self) -> Self {
        Self {
            _type: PhantomData,
            on_linked: self.on_linked.clone(),
            on_synced: self.on_synced.clone(),
            on_unlinked: self.on_unlinked.clone(),
            on_failed: self.on_failed.clone(),
            on_event: self.on_event.clone(),
            on_set: self.on_set.clone(),
        }
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet> OnLinked<Context>
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet>
where
    FLinked: OnLinked<Context>,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FEv: Send,
    FSet: Send,
{
    type OnLinkedHandler<'a> = FLinked::OnLinkedHandler<'a>
    where
        Self: 'a;

    fn on_linked(&self) -> Self::OnLinkedHandler<'_> {
        let StatelessValueDownlinkLifecycle { on_linked, .. } = self;
        on_linked.on_linked()
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet> OnSynced<(), Context>
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet>
where
    FLinked: Send,
    FSynced: OnSynced<(), Context>,
    FUnlinked: Send,
    FFailed: Send,
    FEv: Send,
    FSet: Send,
{
    type OnSyncedHandler<'a> = FSynced::OnSyncedHandler<'a>
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, value: &()) -> Self::OnSyncedHandler<'a> {
        let StatelessValueDownlinkLifecycle { on_synced, .. } = self;
        on_synced.on_synced(value)
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet> OnUnlinked<Context>
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: OnUnlinked<Context>,
    FFailed: Send,
    FEv: Send,
    FSet: Send,
{
    type OnUnlinkedHandler<'a> = FUnlinked::OnUnlinkedHandler<'a>
    where
        Self: 'a;

    fn on_unlinked(&self) -> Self::OnUnlinkedHandler<'_> {
        let StatelessValueDownlinkLifecycle { on_unlinked, .. } = self;
        on_unlinked.on_unlinked()
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet> OnFailed<Context>
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: OnFailed<Context>,
    FFailed: Send,
    FEv: Send,
    FSet: Send,
{
    type OnFailedHandler<'a> = FFailed::OnFailedHandler<'a>
    where
        Self: 'a;

    fn on_failed(&self) -> Self::OnFailedHandler<'_> {
        let StatelessValueDownlinkLifecycle { on_failed, .. } = self;
        on_failed.on_failed()
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet> OnDownlinkEvent<T, Context>
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FEv: OnDownlinkEvent<T, Context>,
    FSet: Send,
{
    type OnEventHandler<'a> = FEv::OnEventHandler<'a>
    where
        Self: 'a;

    fn on_event<'a>(&'a self, value: T) -> Self::OnEventHandler<'a> {
        let StatelessValueDownlinkLifecycle { on_event, .. } = self;
        on_event.on_event(value)
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet> OnDownlinkSet<T, Context>
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FEv: Send,
    FSet: OnDownlinkSet<T, Context>,
{
    type OnSetHandler<'a> = FSet::OnSetHandler<'a>
    where
        Self: 'a;

    fn on_set<'a>(&'a self, previous: Option<T>, new_value: &T) -> Self::OnSetHandler<'a> {
        let StatelessValueDownlinkLifecycle { on_set, .. } = self;
        on_set.on_set(previous, new_value)
    }
}

pub type LiftedValueLifecycle<Context, T, State, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet> =
    StatefulValueDownlinkLifecycle<
        Context,
        State,
        T,
        LiftShared<FLinked, State>,
        LiftShared<FSynced, State>,
        LiftShared<FUnlinked, State>,
        LiftShared<FFailed, State>,
        LiftShared<FEv, State>,
        LiftShared<FSet, State>,
    >;

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet>
    StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet>
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
        FFailed,
        FEv,
        FSet,
    >
    where
        WithHandlerContext<Context, F>: OnLinked<Context>,
    {
        StatelessValueDownlinkLifecycle {
            _type: PhantomData,
            on_linked: WithHandlerContext::new(f),
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_event: self.on_event,
            on_set: self.on_set,
        }
    }

    /// Replace the 'on_synced' handler with another derived from a closure.
    pub fn on_synced<F, B>(
        self,
        f: F,
    ) -> StatelessValueDownlinkLifecycle<
        Context,
        T,
        FLinked,
        WithHandlerContextBorrow<Context, F, B>,
        FUnlinked,
        FFailed,
        FEv,
        FSet,
    >
    where
        B: ?Sized,
        T: Borrow<B>,
        WithHandlerContextBorrow<Context, F, B>: OnSynced<T, Context>,
    {
        StatelessValueDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: WithHandlerContextBorrow::new(f),
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
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
        FFailed,
        FEv,
        FSet,
    >
    where
        WithHandlerContext<Context, F>: OnUnlinked<Context>,
    {
        StatelessValueDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: WithHandlerContext::new(f),
            on_failed: self.on_failed,
            on_event: self.on_event,
            on_set: self.on_set,
        }
    }

    /// Replace the 'on_failed' handler with another derived from a closure.
    pub fn on_failed<F>(
        self,
        f: F,
    ) -> StatelessValueDownlinkLifecycle<
        Context,
        T,
        FLinked,
        FSynced,
        FUnlinked,
        WithHandlerContext<Context, F>,
        FEv,
        FSet,
    >
    where
        WithHandlerContext<Context, F>: OnFailed<Context>,
    {
        StatelessValueDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: WithHandlerContext::new(f),
            on_event: self.on_event,
            on_set: self.on_set,
        }
    }

    /// Replace the 'on_event' handler with another derived from a closure.
    pub fn on_event<F, B>(
        self,
        f: F,
    ) -> StatelessValueDownlinkLifecycle<
        Context,
        T,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        WithHandlerContextBorrow<Context, F, B>,
        FSet,
    >
    where
        B: ?Sized,
        T: Borrow<B>,
        WithHandlerContextBorrow<Context, F, B>: OnDownlinkEvent<T, Context>,
    {
        StatelessValueDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_event: WithHandlerContextBorrow::new(f),
            on_set: self.on_set,
        }
    }

    /// Replace the 'on_set' handler with another derived from a closure.
    pub fn on_set<F, B>(
        self,
        f: F,
    ) -> StatelessValueDownlinkLifecycle<
        Context,
        T,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FEv,
        WithHandlerContextBorrow<Context, F, B>,
    >
    where
        B: ?Sized,
        T: Borrow<B>,
        WithHandlerContextBorrow<Context, F, B>: OnDownlinkSet<T, Context>,
    {
        StatelessValueDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_event: self.on_event,
            on_set: WithHandlerContextBorrow::new(f),
        }
    }

    /// Add a state that is shared between the handlers of the lifeycle.
    pub fn with_state<State>(
        self,
        state: State,
    ) -> LiftedValueLifecycle<Context, T, State, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet>
    {
        let StatelessValueDownlinkLifecycle {
            on_linked,
            on_synced,
            on_unlinked,
            on_failed,
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
            on_failed: LiftShared::new(on_failed),
            on_event: LiftShared::new(on_event),
            on_set: LiftShared::new(on_set),
        }
    }
}
