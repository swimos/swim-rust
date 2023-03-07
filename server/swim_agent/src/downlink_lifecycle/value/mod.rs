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

use swim_api::handlers::{BorrowHandler, FnHandler, NoHandler};

use crate::{agent_lifecycle::utility::HandlerContext, lifecycle_fn::WithHandlerContextBorrow};

use self::{
    on_event::{OnDownlinkEvent, OnDownlinkEventShared},
    on_set::{OnDownlinkSet, OnDownlinkSetShared},
};

use super::{
    on_failed::{OnFailed, OnFailedShared},
    on_linked::{OnLinked, OnLinkedShared},
    on_synced::{OnSynced, OnSyncedShared},
    on_unlinked::{OnUnlinked, OnUnlinkedShared},
};
use crate::lifecycle_fn::{LiftShared, WithHandlerContext};

pub mod on_event;
pub mod on_set;

/// Trait for the lifecycle of a value downlink.
///
/// #Type Parameters
/// * `T` - The type of the state of the lane.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
pub trait ValueDownlinkLifecycle<T, Context>:
    OnLinked<Context>
    + OnSynced<T, Context>
    + OnDownlinkEvent<T, Context>
    + OnDownlinkSet<T, Context>
    + OnUnlinked<Context>
    + OnFailed<Context>
{
}

impl<LC, T, Context> ValueDownlinkLifecycle<T, Context> for LC where
    LC: OnLinked<Context>
        + OnSynced<T, Context>
        + OnDownlinkEvent<T, Context>
        + OnDownlinkSet<T, Context>
        + OnUnlinked<Context>
        + OnFailed<Context>
{
}

/// A lifecycle for a value downlink where the individual event handlers can share state.
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

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet> OnSynced<T, Context>
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
    FSynced: OnSyncedShared<T, Context, State>,
    FUnlinked: Send,
    FFailed: Send,
    FEv: Send,
    FSet: Send,
{
    type OnSyncedHandler<'a> = FSynced::OnSyncedHandler<'a>
    where
    Self: 'a;

    fn on_synced<'a>(&'a self, value: &T) -> Self::OnSyncedHandler<'a> {
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

    fn on_event(&self, value: &T) -> Self::OnEventHandler<'_> {
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
        OnDownlinkSetShared::on_set(on_set, state, *handler_context, previous, new_value)
        //on_set.on_set(state, *handler_context, previous, new_value)
    }
}

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet>
    StatefulValueLifecycle<Context, State, T>
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
    FSynced: OnSyncedShared<T, Context, State>,
    FUnlinked: OnUnlinkedShared<Context, State>,
    FFailed: OnFailedShared<Context, State>,
    FEv: OnDownlinkEventShared<T, Context, State>,
    FSet: OnDownlinkSetShared<T, Context, State>,
{
    type WithOnLinked<H> = StatefulValueDownlinkLifecycle<
    Context,
    State,
    T,
    H,
    FSynced,
    FUnlinked,
    FFailed,
    FEv,
    FSet
    >
    where
        H: OnLinkedShared<Context, State>;

    type WithOnSynced<H> = StatefulValueDownlinkLifecycle<
    Context,
    State,
    T,
    FLinked,
    H,
    FUnlinked,
    FFailed,
    FEv,
    FSet
    >
    where
        H: OnSyncedShared<T, Context, State>;

    type WithOnUnlinked<H> = StatefulValueDownlinkLifecycle<
    Context,
    State,
    T,
    FLinked,
    FSynced,
    H,
    FFailed,
    FEv,
    FSet
    >
    where
        H: OnUnlinkedShared<Context, State>;

    type WithOnFailed<H> = StatefulValueDownlinkLifecycle<
    Context,
    State,
    T,
    FLinked,
    FSynced,
    FUnlinked,
    H,
    FEv,
    FSet
    >
    where
        H: OnFailedShared<Context, State>;

    type WithOnEvent<H> = StatefulValueDownlinkLifecycle<
    Context,
    State,
    T,
    FLinked,
    FSynced,
    FUnlinked,
    FFailed,
    H,
    FSet
    >
    where
        H: OnDownlinkEventShared<T, Context, State>;

    type WithOnSet<H> = StatefulValueDownlinkLifecycle<
        Context,
        State,
        T,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FEv,
        H,
        >
        where
            H: OnDownlinkSetShared<T, Context, State>;

    fn on_linked<F>(self, handler: F) -> Self::WithOnLinked<FnHandler<F>>
    where
        FnHandler<F>: OnLinkedShared<Context, State>,
    {
        StatefulValueDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: FnHandler(handler),
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_event: self.on_event,
            on_set: self.on_set,
        }
    }

    fn on_unlinked<F>(self, handler: F) -> Self::WithOnUnlinked<FnHandler<F>>
    where
        FnHandler<F>: OnUnlinkedShared<Context, State>,
    {
        StatefulValueDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: FnHandler(handler),
            on_failed: self.on_failed,
            on_event: self.on_event,
            on_set: self.on_set,
        }
    }

    fn on_failed<F>(self, handler: F) -> Self::WithOnFailed<FnHandler<F>>
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
            on_failed: FnHandler(handler),
            on_event: self.on_event,
            on_set: self.on_set,
        }
    }

    fn on_synced<F, B>(self, handler: F) -> Self::WithOnSynced<BorrowHandler<F, B>>
    where
        B: ?Sized,
        T: Borrow<B>,
        BorrowHandler<F, B>: OnSyncedShared<T, Context, State>,
    {
        StatefulValueDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: BorrowHandler::new(handler),
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_event: self.on_event,
            on_set: self.on_set,
        }
    }

    fn on_event<F, B>(self, handler: F) -> Self::WithOnEvent<BorrowHandler<F, B>>
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
            on_event: BorrowHandler::new(handler),
            on_set: self.on_set,
        }
    }

    fn on_set<F, B>(self, handler: F) -> Self::WithOnSet<BorrowHandler<F, B>>
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
            on_set: BorrowHandler::new(handler),
        }
    }
}

/// A lifecycle for a value downlink where the individual event handlers do not share state.
///
/// #Type Parameters
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `T` - The type of the downlink.
pub trait StatelessValueLifecycle<Context, T>: ValueDownlinkLifecycle<T, Context> {
    type WithOnLinked<H>: StatelessValueLifecycle<Context, T>
    where
        H: OnLinked<Context>;

    type WithOnSynced<H>: StatelessValueLifecycle<Context, T>
    where
        H: OnSynced<T, Context>;

    type WithOnUnlinked<H>: StatelessValueLifecycle<Context, T>
    where
        H: OnUnlinked<Context>;

    type WithOnFailed<H>: StatelessValueLifecycle<Context, T>
    where
        H: OnFailed<Context>;

    type WithOnEvent<H>: StatelessValueLifecycle<Context, T>
    where
        H: OnDownlinkEvent<T, Context>;

    type WithOnSet<H>: StatelessValueLifecycle<Context, T>
    where
        H: OnDownlinkSet<T, Context>;

    type WithShared<Shared>: StatefulValueLifecycle<Context, Shared, T>
    where
        Shared: Send;

    fn on_linked<F>(self, handler: F) -> Self::WithOnLinked<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnLinked<Context>;

    fn on_synced<F, B>(self, handler: F) -> Self::WithOnSynced<WithHandlerContextBorrow<F, B>>
    where
        B: ?Sized,
        T: Borrow<B>,
        WithHandlerContextBorrow<F, B>: OnSynced<T, Context>;

    fn on_unlinked<F>(self, handler: F) -> Self::WithOnUnlinked<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnUnlinked<Context>;

    fn on_failed<F>(self, handler: F) -> Self::WithOnFailed<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnFailed<Context>;

    fn on_event<F, B>(self, handler: F) -> Self::WithOnEvent<WithHandlerContextBorrow<F, B>>
    where
        B: ?Sized,
        T: Borrow<B>,
        WithHandlerContextBorrow<F, B>: OnDownlinkEvent<T, Context>;

    fn on_set<F, B>(self, handler: F) -> Self::WithOnSet<WithHandlerContextBorrow<F, B>>
    where
        B: ?Sized,
        T: Borrow<B>,
        WithHandlerContextBorrow<F, B>: OnDownlinkSet<T, Context>;

    fn with_shared_state<Shared: Send>(self, shared: Shared) -> Self::WithShared<Shared>;
}

/// A lifecycle for a value downlink where the individual event handlers have shared state.
///
/// #Type Parameters
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `Shared` - The type of the shared state.
/// * `T` - The type of the downlink.
pub trait StatefulValueLifecycle<Context, Shared, T>: ValueDownlinkLifecycle<T, Context> {
    type WithOnLinked<H>: StatefulValueLifecycle<Context, Shared, T>
    where
        H: OnLinkedShared<Context, Shared>;

    type WithOnSynced<H>: StatefulValueLifecycle<Context, Shared, T>
    where
        H: OnSyncedShared<T, Context, Shared>;

    type WithOnUnlinked<H>: StatefulValueLifecycle<Context, Shared, T>
    where
        H: OnUnlinkedShared<Context, Shared>;

    type WithOnFailed<H>: StatefulValueLifecycle<Context, Shared, T>
    where
        H: OnFailedShared<Context, Shared>;

    type WithOnEvent<H>: StatefulValueLifecycle<Context, Shared, T>
    where
        H: OnDownlinkEventShared<T, Context, Shared>;

    type WithOnSet<H>: StatefulValueLifecycle<Context, Shared, T>
    where
        H: OnDownlinkSetShared<T, Context, Shared>;

    fn on_linked<F>(self, handler: F) -> Self::WithOnLinked<FnHandler<F>>
    where
        FnHandler<F>: OnLinkedShared<Context, Shared>;

    fn on_synced<F, B>(self, handler: F) -> Self::WithOnSynced<BorrowHandler<F, B>>
    where
        B: ?Sized,
        T: Borrow<B>,
        BorrowHandler<F, B>: OnSyncedShared<T, Context, Shared>;

    fn on_unlinked<F>(self, handler: F) -> Self::WithOnUnlinked<FnHandler<F>>
    where
        FnHandler<F>: OnUnlinkedShared<Context, Shared>;

    fn on_failed<F>(self, handler: F) -> Self::WithOnFailed<FnHandler<F>>
    where
        FnHandler<F>: OnFailedShared<Context, Shared>;

    fn on_event<F, B>(self, handler: F) -> Self::WithOnEvent<BorrowHandler<F, B>>
    where
        B: ?Sized,
        T: Borrow<B>,
        BorrowHandler<F, B>: OnDownlinkEventShared<T, Context, Shared>;

    fn on_set<F, B>(self, handler: F) -> Self::WithOnSet<BorrowHandler<F, B>>
    where
        B: ?Sized,
        T: Borrow<B>,
        BorrowHandler<F, B>: OnDownlinkSetShared<T, Context, Shared>;
}

/// A lifecycle for a value downlink where the individual event handlers do not share state.
///
/// #Type Parameters
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
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

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet> OnSynced<T, Context>
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet>
where
    FLinked: Send,
    FSynced: OnSynced<T, Context>,
    FUnlinked: Send,
    FFailed: Send,
    FEv: Send,
    FSet: Send,
{
    type OnSyncedHandler<'a> = FSynced::OnSyncedHandler<'a>
    where
    Self: 'a;

    fn on_synced<'a>(&'a self, value: &T) -> Self::OnSyncedHandler<'a> {
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

    fn on_event(&self, value: &T) -> Self::OnEventHandler<'_> {
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
    StatelessValueLifecycle<Context, T>
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet>
where
    FLinked: OnLinked<Context>,
    FSynced: OnSynced<T, Context>,
    FUnlinked: OnUnlinked<Context>,
    FFailed: OnFailed<Context>,
    FEv: OnDownlinkEvent<T, Context>,
    FSet: OnDownlinkSet<T, Context>,
{
    type WithOnLinked<H> = StatelessValueDownlinkLifecycle<Context, T, H, FSynced, FUnlinked, FFailed, FEv, FSet>
    where
    H: OnLinked<Context>;

    type WithOnSynced<H> = StatelessValueDownlinkLifecycle<Context, T, FLinked, H, FUnlinked, FFailed, FEv, FSet>
    where
    H: OnSynced<T, Context>;

    type WithOnUnlinked<H> = StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, H, FFailed, FEv, FSet>
    where
    H: OnUnlinked<Context>;

    type WithOnFailed<H> = StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, H, FEv, FSet>
    where
    H: OnFailed<Context>;

    type WithOnEvent<H> = StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, H, FSet>
    where
    H: OnDownlinkEvent<T, Context>;

    type WithOnSet<H> = StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, H>
    where
        H: OnDownlinkSet<T, Context>;

    type WithShared<Shared> = StatefulValueDownlinkLifecycle<
        Context,
        Shared,
        T,
        LiftShared<FLinked, Shared>,
        LiftShared<FSynced, Shared>,
        LiftShared<FUnlinked, Shared>,
        LiftShared<FFailed, Shared>,
        LiftShared<FEv, Shared>,
        LiftShared<FSet, Shared>,
    >
    where
        Shared: Send;

    fn on_linked<F>(self, handler: F) -> Self::WithOnLinked<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnLinked<Context>,
    {
        StatelessValueDownlinkLifecycle {
            _type: PhantomData,
            on_linked: WithHandlerContext::new(handler),
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_event: self.on_event,
            on_set: self.on_set,
        }
    }

    fn on_unlinked<F>(self, handler: F) -> Self::WithOnUnlinked<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnUnlinked<Context>,
    {
        StatelessValueDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: WithHandlerContext::new(handler),
            on_failed: self.on_failed,
            on_event: self.on_event,
            on_set: self.on_set,
        }
    }

    fn on_failed<F>(self, handler: F) -> Self::WithOnFailed<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnFailed<Context>,
    {
        StatelessValueDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: WithHandlerContext::new(handler),
            on_event: self.on_event,
            on_set: self.on_set,
        }
    }

    fn with_shared_state<Shared: Send>(self, shared: Shared) -> Self::WithShared<Shared> {
        StatefulValueDownlinkLifecycle {
            _type: PhantomData,
            state: shared,
            handler_context: Default::default(),
            on_linked: LiftShared::new(self.on_linked),
            on_synced: LiftShared::new(self.on_synced),
            on_unlinked: LiftShared::new(self.on_unlinked),
            on_failed: LiftShared::new(self.on_failed),
            on_event: LiftShared::new(self.on_event),
            on_set: LiftShared::new(self.on_set),
        }
    }

    fn on_synced<F, B>(self, handler: F) -> Self::WithOnSynced<WithHandlerContextBorrow<F, B>>
    where
        B: ?Sized,
        T: Borrow<B>,
        WithHandlerContextBorrow<F, B>: OnSynced<T, Context>,
    {
        StatelessValueDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: WithHandlerContextBorrow::new(handler),
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_event: self.on_event,
            on_set: self.on_set,
        }
    }

    fn on_event<F, B>(self, handler: F) -> Self::WithOnEvent<WithHandlerContextBorrow<F, B>>
    where
        B: ?Sized,
        T: Borrow<B>,
        WithHandlerContextBorrow<F, B>: OnDownlinkEvent<T, Context>,
    {
        StatelessValueDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_event: WithHandlerContextBorrow::new(handler),
            on_set: self.on_set,
        }
    }

    fn on_set<F, B>(self, handler: F) -> Self::WithOnSet<WithHandlerContextBorrow<F, B>>
    where
        B: ?Sized,
        T: Borrow<B>,
        WithHandlerContextBorrow<F, B>: OnDownlinkSet<T, Context>,
    {
        StatelessValueDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_event: self.on_event,
            on_set: WithHandlerContextBorrow::new(handler),
        }
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet>
    StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv, FSet>
{
    /// Add a state that is shared between the handlers of the lifecycle.
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
