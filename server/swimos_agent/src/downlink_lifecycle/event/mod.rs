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

use std::marker::PhantomData;

use swimos_utilities::handlers::{FnHandler, NoHandler};

use crate::{
    agent_lifecycle::HandlerContext,
    lifecycle_fn::{LiftShared, WithHandlerContext},
};

use super::{
    on_failed::{OnFailed, OnFailedShared},
    on_linked::{OnLinked, OnLinkedShared},
    on_synced::{OnSynced, OnSyncedShared},
    on_unlinked::{OnUnlinked, OnUnlinkedShared},
};

mod on_event;

pub use on_event::{OnConsumeEvent, OnConsumeEventShared};

/// Trait for the lifecycle of an event downlink.
///
/// # Type Parameters
/// * `T` - The type of the events.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
pub trait EventDownlinkLifecycle<T, Context>:
    OnLinked<Context>
    + OnSynced<(), Context>
    + OnConsumeEvent<T, Context>
    + OnUnlinked<Context>
    + OnFailed<Context>
{
}

impl<LC, T, Context> EventDownlinkLifecycle<T, Context> for LC where
    LC: OnLinked<Context>
        + OnSynced<(), Context>
        + OnConsumeEvent<T, Context>
        + OnUnlinked<Context>
        + OnFailed<Context>
{
}

/// A lifecycle for an event downlink where the individual event handlers can share state.
///
/// # Type Parameters
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `State` - The type of the shared state.
/// * `T` - The type of the downlink.
/// * `FLinked` - The type of the 'on_linked' handler.
/// * `FSynced` - The type of the 'on_synced' handler.
/// * `FUnlinked` - The type of the 'on_unlinked' handler.
/// * `FFailed` - The type of the 'on_failed' handler.
/// * `FEv` - The type of the 'on_event' handler.
#[derive(Debug)]
pub struct StatefulEventDownlinkLifecycle<
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

impl<Context, State, T> StatefulEventDownlinkLifecycle<Context, State, T> {
    pub fn new(state: State) -> Self {
        StatefulEventDownlinkLifecycle {
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
    for StatefulEventDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
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
    for StatefulEventDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
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
        let StatefulEventDownlinkLifecycle {
            on_linked,
            state,
            handler_context,
            ..
        } = self;
        on_linked.on_linked(state, *handler_context)
    }
}

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv> OnSynced<(), Context>
    for StatefulEventDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
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
        let StatefulEventDownlinkLifecycle {
            on_synced,
            state,
            handler_context,
            ..
        } = self;
        on_synced.on_synced(state, *handler_context, value)
    }
}

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv> OnUnlinked<Context>
    for StatefulEventDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
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
        let StatefulEventDownlinkLifecycle {
            on_unlinked,
            state,
            handler_context,
            ..
        } = self;
        on_unlinked.on_unlinked(state, *handler_context)
    }
}

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv> OnFailed<Context>
    for StatefulEventDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
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
        let StatefulEventDownlinkLifecycle {
            on_failed,
            state,
            handler_context,
            ..
        } = self;
        on_failed.on_failed(state, *handler_context)
    }
}

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv> OnConsumeEvent<T, Context>
    for StatefulEventDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
where
    State: Send,
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FEv: OnConsumeEventShared<T, Context, State>,
{
    type OnEventHandler<'a> = FEv::OnEventHandler<'a>
    where
    Self: 'a;

    fn on_event(&self, value: T) -> Self::OnEventHandler<'_> {
        let StatefulEventDownlinkLifecycle {
            on_event,
            state,
            handler_context,
            ..
        } = self;
        on_event.on_event(state, *handler_context, value)
    }
}

impl<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
    StatefulEventLifecycle<Context, State, T>
    for StatefulEventDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
where
    State: Send,
    FLinked: OnLinkedShared<Context, State>,
    FSynced: OnSyncedShared<(), Context, State>,
    FUnlinked: OnUnlinkedShared<Context, State>,
    FFailed: OnFailedShared<Context, State>,
    FEv: OnConsumeEventShared<T, Context, State>,
{
    type WithOnLinked<H> = StatefulEventDownlinkLifecycle<
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

    type WithOnSynced<H> = StatefulEventDownlinkLifecycle<
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

    type WithOnUnlinked<H> = StatefulEventDownlinkLifecycle<
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

    type WithOnFailed<H> = StatefulEventDownlinkLifecycle<
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

    type WithOnEvent<H> = StatefulEventDownlinkLifecycle<
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
        H: OnConsumeEventShared<T, Context, State>;

    fn on_linked<F>(self, handler: F) -> Self::WithOnLinked<FnHandler<F>>
    where
        FnHandler<F>: OnLinkedShared<Context, State>,
    {
        StatefulEventDownlinkLifecycle {
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

    fn on_unlinked<F>(self, handler: F) -> Self::WithOnUnlinked<FnHandler<F>>
    where
        FnHandler<F>: OnUnlinkedShared<Context, State>,
    {
        StatefulEventDownlinkLifecycle {
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
        StatefulEventDownlinkLifecycle {
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

    fn on_synced<F>(self, handler: F) -> Self::WithOnSynced<FnHandler<F>>
    where
        FnHandler<F>: OnSyncedShared<(), Context, State>,
    {
        StatefulEventDownlinkLifecycle {
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

    fn on_event<F>(self, handler: F) -> Self::WithOnEvent<FnHandler<F>>
    where
        FnHandler<F>: OnConsumeEventShared<T, Context, State>,
    {
        StatefulEventDownlinkLifecycle {
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

/// A lifecycle for an event downlink where the individual event handlers do not share state.
///
/// # Type Parameters
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `T` - The type of the downlink.
pub trait StatelessEventLifecycle<Context, T>: EventDownlinkLifecycle<T, Context> {
    type WithOnLinked<H>: StatelessEventLifecycle<Context, T>
    where
        H: OnLinked<Context>;

    type WithOnSynced<H>: StatelessEventLifecycle<Context, T>
    where
        H: OnSynced<(), Context>;

    type WithOnUnlinked<H>: StatelessEventLifecycle<Context, T>
    where
        H: OnUnlinked<Context>;

    type WithOnFailed<H>: StatelessEventLifecycle<Context, T>
    where
        H: OnFailed<Context>;

    type WithOnEvent<H>: StatelessEventLifecycle<Context, T>
    where
        H: OnConsumeEvent<T, Context>;

    type WithShared<Shared>: StatefulEventLifecycle<Context, Shared, T>
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
        WithHandlerContext<F>: OnConsumeEvent<T, Context>;

    fn with_shared_state<Shared: Send>(self, shared: Shared) -> Self::WithShared<Shared>;
}

/// A lifecycle for an event downlink where the individual event handlers have shared state.
///
/// # Type Parameters
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `Shared` - The type of the shared state.
/// * `T` - The type of the downlink.
pub trait StatefulEventLifecycle<Context, Shared, T>: EventDownlinkLifecycle<T, Context> {
    type WithOnLinked<H>: StatefulEventLifecycle<Context, Shared, T>
    where
        H: OnLinkedShared<Context, Shared>;

    type WithOnSynced<H>: StatefulEventLifecycle<Context, Shared, T>
    where
        H: OnSyncedShared<(), Context, Shared>;

    type WithOnUnlinked<H>: StatefulEventLifecycle<Context, Shared, T>
    where
        H: OnUnlinkedShared<Context, Shared>;

    type WithOnFailed<H>: StatefulEventLifecycle<Context, Shared, T>
    where
        H: OnFailedShared<Context, Shared>;

    type WithOnEvent<H>: StatefulEventLifecycle<Context, Shared, T>
    where
        H: OnConsumeEventShared<T, Context, Shared>;

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
        FnHandler<F>: OnConsumeEventShared<T, Context, Shared>;
}

/// A lifecycle for an event downlink where the individual event handlers dno not share state.
///
/// # Type Parameters
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `T` - The type of the downlink.
/// * `FLinked` - The type of the 'on_linked' handler.
/// * `FSynced` - The type of the 'on_synced' handler.
/// * `FUnlinked` - The type of the 'on_unlinked' handler.
/// * `FFailed` - The type of the 'on_failed' handler.
/// * `FEv` - The type of the 'on_event' handler.
#[derive(Debug)]
pub struct StatelessEventDownlinkLifecycle<
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

impl<Context, T> Default for StatelessEventDownlinkLifecycle<Context, T> {
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
    for StatelessEventDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
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
    for StatelessEventDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
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
        let StatelessEventDownlinkLifecycle { on_linked, .. } = self;
        on_linked.on_linked()
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv> OnSynced<(), Context>
    for StatelessEventDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
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
        let StatelessEventDownlinkLifecycle { on_synced, .. } = self;
        on_synced.on_synced(value)
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv> OnUnlinked<Context>
    for StatelessEventDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
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
        let StatelessEventDownlinkLifecycle { on_unlinked, .. } = self;
        on_unlinked.on_unlinked()
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv> OnFailed<Context>
    for StatelessEventDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
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
        let StatelessEventDownlinkLifecycle { on_failed, .. } = self;
        on_failed.on_failed()
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv> OnConsumeEvent<T, Context>
    for StatelessEventDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
where
    FLinked: Send,
    FSynced: Send,
    FUnlinked: Send,
    FFailed: Send,
    FEv: OnConsumeEvent<T, Context>,
{
    type OnEventHandler<'a> = FEv::OnEventHandler<'a>
    where
    Self: 'a;

    fn on_event(&self, value: T) -> Self::OnEventHandler<'_> {
        let StatelessEventDownlinkLifecycle { on_event, .. } = self;
        on_event.on_event(value)
    }
}

#[doc(hidden)]
pub type LiftedEventLifecycle<Context, T, State, FLinked, FSynced, FUnlinked, FFailed, FEv> =
    StatefulEventDownlinkLifecycle<
        Context,
        State,
        T,
        LiftShared<FLinked, State>,
        LiftShared<FSynced, State>,
        LiftShared<FUnlinked, State>,
        LiftShared<FFailed, State>,
        LiftShared<FEv, State>,
    >;

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv> StatelessEventLifecycle<Context, T>
    for StatelessEventDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
where
    FLinked: OnLinked<Context>,
    FSynced: OnSynced<(), Context>,
    FUnlinked: OnUnlinked<Context>,
    FFailed: OnFailed<Context>,
    FEv: OnConsumeEvent<T, Context>,
{
    type WithOnLinked<H> = StatelessEventDownlinkLifecycle<Context, T, H, FSynced, FUnlinked, FFailed, FEv>
    where
    H: OnLinked<Context>;

    type WithOnSynced<H> = StatelessEventDownlinkLifecycle<Context, T, FLinked, H, FUnlinked, FFailed, FEv>
    where
    H: OnSynced<(), Context>;

    type WithOnUnlinked<H> = StatelessEventDownlinkLifecycle<Context, T, FLinked, FSynced, H, FFailed, FEv>
    where
    H: OnUnlinked<Context>;

    type WithOnFailed<H> = StatelessEventDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, H, FEv>
    where
    H: OnFailed<Context>;

    type WithOnEvent<H> = StatelessEventDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, H>
    where
    H: OnConsumeEvent<T, Context>;

    type WithShared<Shared> = StatefulEventDownlinkLifecycle<
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
        StatelessEventDownlinkLifecycle {
            _type: PhantomData,
            on_linked: WithHandlerContext::new(handler),
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_event: self.on_event,
        }
    }

    fn on_unlinked<F>(self, handler: F) -> Self::WithOnUnlinked<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnUnlinked<Context>,
    {
        StatelessEventDownlinkLifecycle {
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
        StatelessEventDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: WithHandlerContext::new(handler),
            on_event: self.on_event,
        }
    }

    fn with_shared_state<Shared: Send>(self, shared: Shared) -> Self::WithShared<Shared> {
        StatefulEventDownlinkLifecycle {
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

    fn on_synced<F>(self, handler: F) -> Self::WithOnSynced<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnSynced<(), Context>,
    {
        StatelessEventDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: WithHandlerContext::new(handler),
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_event: self.on_event,
        }
    }

    fn on_event<F>(self, handler: F) -> Self::WithOnEvent<WithHandlerContext<F>>
    where
        WithHandlerContext<F>: OnConsumeEvent<T, Context>,
    {
        StatelessEventDownlinkLifecycle {
            _type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_unlinked: self.on_unlinked,
            on_failed: self.on_failed,
            on_event: WithHandlerContext::new(handler),
        }
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
    StatelessEventDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FFailed, FEv>
{
    /// Add a state that is shared between the handlers of the lifecycle.
    pub fn with_state<State>(
        self,
        state: State,
    ) -> LiftedEventLifecycle<Context, T, State, FLinked, FSynced, FUnlinked, FFailed, FEv> {
        let StatelessEventDownlinkLifecycle {
            on_linked,
            on_synced,
            on_unlinked,
            on_failed,
            on_event,
            ..
        } = self;
        StatefulEventDownlinkLifecycle {
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
