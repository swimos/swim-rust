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

use self::on_event::{OnDownlinkEvent, OnDownlinkEventShared};

use super::{
    on_failed::{OnFailed, OnFailedShared},
    on_linked::{OnLinked, OnLinkedShared},
    on_synced::{OnSynced, OnSyncedShared},
    on_unlinked::{OnUnlinked, OnUnlinkedShared},
};
use crate::lifecycle_fn::{LiftShared, WithHandlerContext};

pub mod on_event;

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
> {
    _type: PhantomData<fn(T)>,
    state: State,
    handler_context: HandlerContext<Context>,
    on_linked: FLinked,
    on_synced: FSynced,
    on_unlinked: FUnlinked,
    on_failed: FFailed,
    on_event: FEv,
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
        }
    }
}

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv> Clone
    for StatefulValueDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
where
    State: Clone,
    FLinked: Clone,
    FSynced: Clone,
    FUnlinked: Clone,
    FFailed: Clone,
    FEv: Clone,
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
        }
    }
}

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv> OnLinked<Context>
    for StatefulValueDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
where
    State: Send,
    FLinked: OnLinkedShared<Context, State>,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FEv: Send,
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

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv> OnSynced<(), Context>
    for StatefulValueDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
where
    State: Send,
    FLinked: Send,
    FSynced: OnSyncedShared<(), Context, State>,
    FUnlinked: Send,
    FFailed: Send,
    FEv: Send,
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

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv> OnUnlinked<Context>
    for StatefulValueDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: OnUnlinkedShared<Context, State>,
    FFailed: Send,
    FEv: Send,
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

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv> OnFailed<Context>
    for StatefulValueDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: OnFailedShared<Context, State>,
    FEv: Send,
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

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv> OnDownlinkEvent<T, Context>
    for StatefulValueDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FEv: OnDownlinkEventShared<T, Context, State>,
{
    type OnEventHandler<'a> = FEv::OnEventHandler<'a>
    where
    Self: 'a;

    fn on_event(&self, value: T) -> Self::OnEventHandler<'_> {
        let StatefulValueDownlinkLifecycle {
            on_event,
            state,
            handler_context,
            ..
        } = self;
        on_event.on_event(state, *handler_context, value)
    }
}

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
    StatefulValueLifecycle<Context, State, T>
    for StatefulValueDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
where
    State: Send,
    FLinked: OnLinkedShared<Context, State>,
    FSynced: OnSyncedShared<(), Context, State>,
    FUnlinked: OnUnlinkedShared<Context, State>,
    FFailed: OnFailedShared<Context, State>,
    FEv: OnDownlinkEventShared<T, Context, State>,
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
    >
    where
        H: OnSyncedShared<(), Context, State>;

    type WithOnUnlinked<H> = StatefulValueDownlinkLifecycle<
    Context,
    State,
    T,
    FLinked,
    FSynced,
    H,
    FFailed,
    FEv,
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
    >
    where
        H: OnDownlinkEventShared<T, Context, State>;

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
        }
    }

    fn on_synced<F>(self, handler: F) -> Self::WithOnSynced<FnHandler<F>>
    where
        FnHandler<F>: OnSyncedShared<(), Context, State>,
    {
        StatefulValueDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: FnHandler(handler),
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_event: self.on_event,
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
        }
    }

    fn on_event<F>(self, handler: F) -> Self::WithOnEvent<FnHandler<F>>
    where
        FnHandler<F>: OnDownlinkEventShared<T, Context, State>,
    {
        StatefulValueDownlinkLifecycle {
            _type: PhantomData,
            state: self.state,
            handler_context: self.handler_context,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_event: FnHandler(handler),
        }
    }
}

pub trait StatelessValueLifecycle<Context, T>:
    OnLinked<Context>
    + OnSynced<(), Context>
    + OnUnlinked<Context>
    + OnFailed<Context>
    + OnDownlinkEvent<T, Context>
{
    type WithOnLinked<H>: StatelessValueLifecycle<Context, T>
    where
        H: OnLinked<Context>;

    type WithOnSynced<H>: StatelessValueLifecycle<Context, T>
    where
        H: OnSynced<(), Context>;

    type WithOnUnlinked<H>: StatelessValueLifecycle<Context, T>
    where
        H: OnUnlinked<Context>;

    type WithOnFailed<H>: StatelessValueLifecycle<Context, T>
    where
        H: OnFailed<Context>;

    type WithOnEvent<H>: StatelessValueLifecycle<Context, T>
    where
        H: OnDownlinkEvent<T, Context>;

    type WithShared<Shared>: StatefulValueLifecycle<Context, Shared, T>
    where
        Shared: Send;

    fn on_linked<F>(self, handler: F) -> Self::WithOnLinked<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnLinked<Context>;

    fn on_synced<F>(self, handler: F) -> Self::WithOnSynced<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnSynced<(), Context>;

    fn on_unlinked<F>(self, handler: F) -> Self::WithOnUnlinked<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnUnlinked<Context>;

    fn on_failed<F>(self, handler: F) -> Self::WithOnFailed<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnFailed<Context>;

    fn on_event<F>(self, handler: F) -> Self::WithOnEvent<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnDownlinkEvent<T, Context>;

    fn with_shared_state<Shared: Send>(self, shared: Shared) -> Self::WithShared<Shared>;
}

pub trait StatefulValueLifecycle<Context, Shared, T>:
    OnLinked<Context>
    + OnSynced<(), Context>
    + OnUnlinked<Context>
    + OnFailed<Context>
    + OnDownlinkEvent<T, Context>
{
    type WithOnLinked<H>: StatefulValueLifecycle<Context, Shared, T>
    where
        H: OnLinkedShared<Context, Shared>;

    type WithOnSynced<H>: StatefulValueLifecycle<Context, Shared, T>
    where
        H: OnSyncedShared<(), Context, Shared>;

    type WithOnUnlinked<H>: StatefulValueLifecycle<Context, Shared, T>
    where
        H: OnUnlinkedShared<Context, Shared>;

    type WithOnFailed<H>: StatefulValueLifecycle<Context, Shared, T>
    where
        H: OnFailedShared<Context, Shared>;

    type WithOnEvent<H>: StatefulValueLifecycle<Context, Shared, T>
    where
        H: OnDownlinkEventShared<T, Context, Shared>;

    fn on_linked<F>(self, handler: F) -> Self::WithOnLinked<FnHandler<F>>
    where
        FnHandler<F>: OnLinkedShared<Context, Shared>;

    fn on_synced<F>(self, handler: F) -> Self::WithOnSynced<FnHandler<F>>
    where
        FnHandler<F>: OnSyncedShared<(), Context, Shared>;

    fn on_unlinked<F>(self, handler: F) -> Self::WithOnUnlinked<FnHandler<F>>
    where
        FnHandler<F>: OnUnlinkedShared<Context, Shared>;

    fn on_failed<F>(self, handler: F) -> Self::WithOnFailed<FnHandler<F>>
    where
        FnHandler<F>: OnFailedShared<Context, Shared>;

    fn on_event<F>(self, handler: F) -> Self::WithOnEvent<FnHandler<F>>
    where
        FnHandler<F>: OnDownlinkEventShared<T, Context, Shared>;
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
#[derive(Debug)]
pub struct StatelessValueDownlinkLifecycle<
    Context,
    T,
    FLinked = NoHandler,
    FSynced = NoHandler,
    FUnlinked = NoHandler,
    FFailed = NoHandler,
    FEv = NoHandler,
> {
    _type: PhantomData<fn(&Context, T)>,
    on_linked: FLinked,
    on_synced: FSynced,
    on_unlinked: FUnlinked,
    on_failed: FFailed,
    on_event: FEv,
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
        }
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv> Clone
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
where
    FLinked: Clone,
    FSynced: Clone,
    FUnlinked: Clone,
    FFailed: Clone,
    FEv: Clone,
{
    fn clone(&self) -> Self {
        Self {
            _type: PhantomData,
            on_linked: self.on_linked.clone(),
            on_synced: self.on_synced.clone(),
            on_unlinked: self.on_unlinked.clone(),
            on_failed: self.on_failed.clone(),
            on_event: self.on_event.clone(),
        }
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv> OnLinked<Context>
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
where
    FLinked: OnLinked<Context>,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FEv: Send,
{
    type OnLinkedHandler<'a> = FLinked::OnLinkedHandler<'a>
    where
    Self: 'a;

    fn on_linked(&self) -> Self::OnLinkedHandler<'_> {
        let StatelessValueDownlinkLifecycle { on_linked, .. } = self;
        on_linked.on_linked()
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv> OnSynced<(), Context>
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
where
    FLinked: Send,
    FSynced: OnSynced<(), Context>,
    FUnlinked: Send,
    FFailed: Send,
    FEv: Send,
{
    type OnSyncedHandler<'a> = FSynced::OnSyncedHandler<'a>
    where
    Self: 'a;

    fn on_synced<'a>(&'a self, value: &()) -> Self::OnSyncedHandler<'a> {
        let StatelessValueDownlinkLifecycle { on_synced, .. } = self;
        on_synced.on_synced(value)
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv> OnUnlinked<Context>
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: OnUnlinked<Context>,
    FFailed: Send,
    FEv: Send,
{
    type OnUnlinkedHandler<'a> = FUnlinked::OnUnlinkedHandler<'a>
    where
    Self: 'a;

    fn on_unlinked(&self) -> Self::OnUnlinkedHandler<'_> {
        let StatelessValueDownlinkLifecycle { on_unlinked, .. } = self;
        on_unlinked.on_unlinked()
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv> OnFailed<Context>
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: OnFailed<Context>,
    FFailed: Send,
    FEv: Send,
{
    type OnFailedHandler<'a> = FFailed::OnFailedHandler<'a>
    where
    Self: 'a;

    fn on_failed(&self) -> Self::OnFailedHandler<'_> {
        let StatelessValueDownlinkLifecycle { on_failed, .. } = self;
        on_failed.on_failed()
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv> OnDownlinkEvent<T, Context>
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FEv: OnDownlinkEvent<T, Context>,
{
    type OnEventHandler<'a> = FEv::OnEventHandler<'a>
    where
    Self: 'a;

    fn on_event(&self, value: T) -> Self::OnEventHandler<'_> {
        let StatelessValueDownlinkLifecycle { on_event, .. } = self;
        on_event.on_event(value)
    }
}

pub type LiftedValueLifecycle<Context, T, State, FLinked, FSynced, FUnlinked, FFailed, FEv> =
    StatefulValueDownlinkLifecycle<
        Context,
        State,
        T,
        LiftShared<FLinked, State>,
        LiftShared<FSynced, State>,
        LiftShared<FUnlinked, State>,
        LiftShared<FFailed, State>,
        LiftShared<FEv, State>,
    >;

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv> StatelessValueLifecycle<Context, T>
    for StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
where
    FLinked: OnLinked<Context>,
    FSynced: OnSynced<(), Context>,
    FUnlinked: OnUnlinked<Context>,
    FFailed: OnFailed<Context>,
    FEv: OnDownlinkEvent<T, Context>,
{
    type WithOnLinked<H> = StatelessValueDownlinkLifecycle<Context, T, H, FSynced, FUnlinked, FFailed, FEv>
    where
    H: OnLinked<Context>;

    type WithOnSynced<H> = StatelessValueDownlinkLifecycle<Context, T, FLinked, H, FUnlinked, FFailed, FEv>
    where
    H: OnSynced<(), Context>;

    type WithOnUnlinked<H> = StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, H, FFailed, FEv>
    where
    H: OnUnlinked<Context>;

    type WithOnFailed<H> = StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, H, FEv>
    where
    H: OnFailed<Context>;

    type WithOnEvent<H> = StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, H>
    where
    H: OnDownlinkEvent<T, Context>;

    type WithShared<Shared> = StatefulValueDownlinkLifecycle<
        Context,
        Shared,
        T,
        LiftShared<FLinked, Shared>,
        LiftShared<FSynced, Shared>,
        LiftShared<FUnlinked, Shared>,
        LiftShared<FFailed, Shared>,
        LiftShared<FEv, Shared>,
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
        }
    }

    fn on_synced<F>(self, handler: F) -> Self::WithOnSynced<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnSynced<(), Context>,
    {
        StatelessValueDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: WithHandlerContext::new(handler),
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_event: self.on_event,
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
        }
    }

    fn on_event<F>(self, handler: F) -> Self::WithOnEvent<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnDownlinkEvent<T, Context>,
    {
        StatelessValueDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_event: WithHandlerContext::new(handler),
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
        }
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
    StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
{
    /// Add a state that is shared between the handlers of the lifeycle.
    pub fn with_state<State>(
        self,
        state: State,
    ) -> LiftedValueLifecycle<Context, T, State, FLinked, FSynced, FUnlinked, FFailed, FEv> {
        let StatelessValueDownlinkLifecycle {
            on_linked,
            on_synced,
            on_unlinked,
            on_failed,
            on_event,
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
        }
    }
}
