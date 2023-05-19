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

pub use handler_fn::*;
pub use on_event::{OnEvent, OnEventShared};
pub use on_linked::{OnLinked, OnLinkedShared};
pub use on_set::{OnSet, OnSetShared};
pub use on_synced::{OnSynced, OnSyncedShared};
pub use on_unlinked::{OnUnlinked, OnUnlinkedShared};
use swim_api::handlers::{BlockingHandler, FnMutHandler, NoHandler, WithShared};

mod handler_fn;
mod on_event;
mod on_linked;
mod on_set;
mod on_synced;
mod on_unlinked;

/// Description of a lifecycle for a value downlink.
pub trait ValueDownlinkLifecycle<T>:
    OnLinked + OnSynced<T> + OnEvent<T> + OnSet<T> + OnUnlinked
{
}

/// Description of a lifecycle for an event downlink.
pub trait EventDownlinkLifecycle<T>: OnLinked + OnEvent<T> + OnUnlinked {}

impl<T, L> ValueDownlinkLifecycle<T> for L where
    L: OnLinked + OnSynced<T> + OnEvent<T> + OnSet<T> + OnUnlinked
{
}

impl<T, L> EventDownlinkLifecycle<T> for L where L: OnLinked + OnEvent<T> + OnUnlinked {}
/// A basic lifecycle for a value downlink where the event handlers do not share any state.
pub struct BasicValueDownlinkLifecycle<
    T,
    FLink = NoHandler,
    FSync = NoHandler,
    FEv = NoHandler,
    FSet = NoHandler,
    FUnlink = NoHandler,
> {
    _value_type: PhantomData<fn(T)>,
    on_linked: FLink,
    on_synced: FSync,
    on_event: FEv,
    on_set: FSet,
    on_unlinked: FUnlink,
}

impl<T> Default for BasicValueDownlinkLifecycle<T> {
    fn default() -> Self {
        Self {
            _value_type: PhantomData,
            on_linked: Default::default(),
            on_event: Default::default(),
            on_unlinked: Default::default(),
            on_set: Default::default(),
            on_synced: Default::default(),
        }
    }
}

impl<T> Default for BasicEventDownlinkLifecycle<T> {
    fn default() -> Self {
        Self {
            _value_type: PhantomData,
            on_linked: Default::default(),
            on_event: Default::default(),
            on_unlinked: Default::default(),
        }
    }
}

type WithSharedValueDownlinkLifecycle<T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked> =
    StatefulValueDownlinkLifecycle<
        T,
        Shared,
        WithShared<FLinked>,
        WithShared<FSynced>,
        WithShared<FEv>,
        WithShared<FSet>,
        WithShared<FUnlinked>,
    >;

type WithSharedEventDownlinkLifecycle<T, Shared, FLinked, FEv, FUnlinked> =
    StatefulEventDownlinkLifecycle<
        T,
        Shared,
        WithShared<FLinked>,
        WithShared<FEv>,
        WithShared<FUnlinked>,
    >;

impl<T, FLinked, FSynced, FEv, FSet, FUnlinked>
    BasicValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
{
    /// Replace the handler that is called when the downlink connects.
    pub fn on_linked<F>(
        self,
        f: F,
    ) -> BasicValueDownlinkLifecycle<T, FnMutHandler<F>, FSynced, FEv, FSet, FUnlinked>
    where
        FnMutHandler<F>: OnLinked,
    {
        BasicValueDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: FnMutHandler(f),
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_set: self.on_set,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink connects with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_linked_blocking<F>(
        self,
        f: F,
    ) -> BasicValueDownlinkLifecycle<T, BlockingHandler<F>, FSynced, FEv, FSet, FUnlinked>
    where
        F: FnMut() + Send,
    {
        BasicValueDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: BlockingHandler(f),
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_set: self.on_set,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink synchronizes.
    pub fn on_synced<F>(
        self,
        f: F,
    ) -> BasicValueDownlinkLifecycle<T, FLinked, FnMutHandler<F>, FEv, FSet, FUnlinked>
    where
        FnMutHandler<F>: for<'a> OnSynced<T>,
    {
        BasicValueDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: FnMutHandler(f),
            on_event: self.on_event,
            on_set: self.on_set,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink synchronizes with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_synced_blocking<F>(
        self,
        f: F,
    ) -> BasicValueDownlinkLifecycle<T, FLinked, BlockingHandler<F>, FEv, FSet, FUnlinked>
    where
        F: FnMut(&T) + Send,
    {
        BasicValueDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: BlockingHandler(f),
            on_event: self.on_event,
            on_set: self.on_set,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink receives an event.
    pub fn on_event<F>(
        self,
        f: F,
    ) -> BasicValueDownlinkLifecycle<T, FLinked, FSynced, FnMutHandler<F>, FSet, FUnlinked>
    where
        FnMutHandler<F>: OnEvent<T>,
    {
        BasicValueDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: FnMutHandler(f),
            on_set: self.on_set,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink receives and event with the specified
    /// synchronous closure. Running this closure will block the task so it should complete quickly.
    pub fn on_event_blocking<F>(
        self,
        f: F,
    ) -> BasicValueDownlinkLifecycle<T, FLinked, FSynced, BlockingHandler<F>, FSet, FUnlinked>
    where
        F: FnMut(&T) + Send,
    {
        BasicValueDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: BlockingHandler(f),
            on_set: self.on_set,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink value changes.
    pub fn on_set<F>(
        self,
        f: F,
    ) -> BasicValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FnMutHandler<F>, FUnlinked>
    where
        FnMutHandler<F>: OnSet<T>,
    {
        BasicValueDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_set: FnMutHandler(f),
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink value changes with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_set_blocking<F>(
        self,
        f: F,
    ) -> BasicValueDownlinkLifecycle<T, FLinked, FSynced, FEv, BlockingHandler<F>, FUnlinked>
    where
        F: FnMut(Option<&T>, &T) + Send,
    {
        BasicValueDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_set: BlockingHandler(f),
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink disconnects.
    pub fn on_unlinked<F>(
        self,
        f: F,
    ) -> BasicValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FSet, FnMutHandler<F>>
    where
        FnMutHandler<F>: OnUnlinked,
    {
        BasicValueDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_set: self.on_set,
            on_unlinked: FnMutHandler(f),
        }
    }

    /// Replace the handler that is called when the downlink disconnects with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_unlinked_blocking<F>(
        self,
        f: F,
    ) -> BasicValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FSet, BlockingHandler<F>>
    where
        F: FnMut() + Send,
    {
        BasicValueDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_set: self.on_set,
            on_unlinked: BlockingHandler(f),
        }
    }

    /// Adds shared state this is accessible to all handlers for this downlink.
    pub fn with<Shared>(
        self,
        shared_state: Shared,
    ) -> WithSharedValueDownlinkLifecycle<T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked> {
        StatefulValueDownlinkLifecycle {
            _value_type: PhantomData,
            shared: shared_state,
            on_linked: WithShared::new(self.on_linked),
            on_synced: WithShared::new(self.on_synced),
            on_event: WithShared::new(self.on_event),
            on_set: WithShared::new(self.on_set),
            on_unlinked: WithShared::new(self.on_unlinked),
        }
    }
}

impl<T, FLinked, FSynced, FEv, FSet, FUnlinked> OnLinked
    for BasicValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
    FLinked: OnLinked,
    FSynced: Send,
    FEv: Send,
    FSet: Send,
    FUnlinked: Send,
{
    type OnLinkedFut<'a> = FLinked::OnLinkedFut<'a>
    where
        Self: 'a;

    fn on_linked(&mut self) -> Self::OnLinkedFut<'_> {
        self.on_linked.on_linked()
    }
}

impl<T, FLinked, FSynced, FEv, FSet, FUnlinked> OnSynced<T>
    for BasicValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
    FLinked: Send,
    FSynced: OnSynced<T>,
    FEv: Send,
    FSet: Send,
    FUnlinked: Send,
{
    type OnSyncedFut<'a> = FSynced::OnSyncedFut<'a>
    where
        Self: 'a,
        T: 'a;

    fn on_synced<'a>(&'a mut self, value: &'a T) -> Self::OnSyncedFut<'a> {
        self.on_synced.on_synced(value)
    }
}

impl<T, FLinked, FSynced, FEv, FSet, FUnlinked> OnEvent<T>
    for BasicValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
    FLinked: Send,
    FSynced: Send,
    FEv: OnEvent<T>,
    FSet: Send,
    FUnlinked: Send,
{
    type OnEventFut<'a> = FEv::OnEventFut<'a>
    where
        Self: 'a;

    fn on_event<'a>(&'a mut self, value: &'a T) -> Self::OnEventFut<'a> {
        self.on_event.on_event(value)
    }
}

impl<T, FLinked, FSynced, FEv, FSet, FUnlinked> OnSet<T>
    for BasicValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync,
    FLinked: Send,
    FSynced: Send,
    FEv: Send,
    FSet: OnSet<T>,
    FUnlinked: Send,
{
    type OnSetFut<'a> = FSet::OnSetFut<'a>
    where
        Self: 'a,
        T: 'a;

    fn on_set<'a>(&'a mut self, existing: Option<&'a T>, new_value: &'a T) -> Self::OnSetFut<'a> {
        self.on_set.on_set(existing, new_value)
    }
}

impl<T, FLinked, FSynced, FEv, FSet, FUnlinked> OnUnlinked
    for BasicValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
    FLinked: Send,
    FSynced: Send,
    FEv: Send,
    FSet: Send,
    FUnlinked: OnUnlinked,
{
    type OnUnlinkedFut<'a> = FUnlinked::OnUnlinkedFut<'a>
    where
        Self: 'a;

    fn on_unlinked(&mut self) -> Self::OnUnlinkedFut<'_> {
        self.on_unlinked.on_unlinked()
    }
}

/// A lifecycle for a value downlink where the handlers for each event share state.
pub struct StatefulValueDownlinkLifecycle<
    T,
    Shared,
    FLink = NoHandler,
    FSync = NoHandler,
    FEv = NoHandler,
    FSet = NoHandler,
    FUnlink = NoHandler,
> {
    _value_type: PhantomData<fn(T)>,
    shared: Shared,
    on_linked: FLink,
    on_synced: FSync,
    on_event: FEv,
    on_set: FSet,
    on_unlinked: FUnlink,
}

impl<T, Shared> StatefulEventDownlinkLifecycle<T, Shared> {
    pub fn new(shared: Shared) -> Self {
        StatefulEventDownlinkLifecycle {
            _value_type: PhantomData,
            shared,
            on_linked: Default::default(),
            on_event: Default::default(),
            on_unlinked: Default::default(),
        }
    }
}

impl<T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked>
    StatefulValueDownlinkLifecycle<T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
{
    /// Replace the handler that is called when the downlink connects.
    pub fn on_linked<F>(
        self,
        f: F,
    ) -> StatefulValueDownlinkLifecycle<T, Shared, FnMutHandler<F>, FSynced, FEv, FSet, FUnlinked>
    where
        FnMutHandler<F>: OnLinkedShared<Shared>,
    {
        StatefulValueDownlinkLifecycle {
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: FnMutHandler(f),
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_set: self.on_set,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink connects with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_linked_blocking<F>(
        self,
        f: F,
    ) -> StatefulValueDownlinkLifecycle<T, Shared, BlockingHandler<F>, FSynced, FEv, FSet, FUnlinked>
    where
        F: FnMut(&mut Shared) + Send,
    {
        StatefulValueDownlinkLifecycle {
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: BlockingHandler(f),
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_set: self.on_set,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink synchronizes.
    pub fn on_synced<F>(
        self,
        f: F,
    ) -> StatefulValueDownlinkLifecycle<T, Shared, FLinked, FnMutHandler<F>, FEv, FSet, FUnlinked>
    where
        FnMutHandler<F>: OnSyncedShared<T, Shared>,
    {
        StatefulValueDownlinkLifecycle {
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_synced: FnMutHandler(f),
            on_event: self.on_event,
            on_set: self.on_set,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink synchronizes with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_synced_blocking<F>(
        self,
        f: F,
    ) -> StatefulValueDownlinkLifecycle<T, Shared, FLinked, BlockingHandler<F>, FEv, FSet, FUnlinked>
    where
        F: FnMut(&mut Shared, &T),
    {
        StatefulValueDownlinkLifecycle {
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_synced: BlockingHandler(f),
            on_event: self.on_event,
            on_set: self.on_set,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink receives a new event.
    pub fn on_event<F>(
        self,
        f: F,
    ) -> StatefulValueDownlinkLifecycle<T, Shared, FLinked, FSynced, FnMutHandler<F>, FSet, FUnlinked>
    where
        FnMutHandler<F>: OnEventShared<T, Shared>,
    {
        StatefulValueDownlinkLifecycle {
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: FnMutHandler(f),
            on_set: self.on_set,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink receives a new event with the specified
    /// synchronous closure. Running this closure will block the task so it should complete quickly.
    pub fn on_event_blocking<F>(
        self,
        f: F,
    ) -> StatefulValueDownlinkLifecycle<
        T,
        Shared,
        FLinked,
        FSynced,
        BlockingHandler<F>,
        FSet,
        FUnlinked,
    >
    where
        F: FnMut(&mut Shared, &T),
    {
        StatefulValueDownlinkLifecycle {
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: BlockingHandler(f),
            on_set: self.on_set,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink value changes.
    pub fn on_set<F>(
        self,
        f: F,
    ) -> StatefulValueDownlinkLifecycle<T, Shared, FLinked, FSynced, FEv, FnMutHandler<F>, FUnlinked>
    where
        FnMutHandler<F>: OnSetShared<T, Shared>,
    {
        StatefulValueDownlinkLifecycle {
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_set: FnMutHandler(f),
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink value changes with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_set_blocking<F>(
        self,
        f: F,
    ) -> StatefulValueDownlinkLifecycle<
        T,
        Shared,
        FLinked,
        FSynced,
        FEv,
        BlockingHandler<F>,
        FUnlinked,
    >
    where
        F: FnMut(&mut Shared, Option<&T>, &T),
    {
        StatefulValueDownlinkLifecycle {
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_set: BlockingHandler(f),
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink disconnects.
    pub fn on_unlinked<F>(
        self,
        f: F,
    ) -> StatefulValueDownlinkLifecycle<T, Shared, FLinked, FSynced, FEv, FSet, FnMutHandler<F>>
    where
        FnMutHandler<F>: OnUnlinkedShared<Shared>,
    {
        StatefulValueDownlinkLifecycle {
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_set: self.on_set,
            on_unlinked: FnMutHandler(f),
        }
    }

    /// Replace the handler that is called when the downlink disconnects with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_unlinked_blocking<F>(
        self,
        f: F,
    ) -> StatefulValueDownlinkLifecycle<T, Shared, FLinked, FSynced, FEv, FSet, BlockingHandler<F>>
    where
        F: FnMut(&mut Shared),
    {
        StatefulValueDownlinkLifecycle {
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_set: self.on_set,
            on_unlinked: BlockingHandler(f),
        }
    }
}

impl<T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked> OnLinked
    for StatefulValueDownlinkLifecycle<T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
    FLinked: OnLinkedShared<Shared>,
    FSynced: Send,
    FEv: Send,
    FSet: Send,
    FUnlinked: Send,
{
    type OnLinkedFut<'a> = FLinked::OnLinkedFut<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_linked(&mut self) -> Self::OnLinkedFut<'_> {
        let StatefulValueDownlinkLifecycle {
            shared, on_linked, ..
        } = self;
        on_linked.on_linked(shared)
    }
}

impl<T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked> OnSynced<T>
    for StatefulValueDownlinkLifecycle<T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
    FLinked: Send,
    FSynced: OnSyncedShared<T, Shared>,
    FEv: Send,
    FSet: Send,
    FUnlinked: Send,
{
    type OnSyncedFut<'a> = FSynced::OnSyncedFut<'a>
    where
        Self: 'a,
        T: 'a;

    fn on_synced<'a>(&'a mut self, value: &'a T) -> Self::OnSyncedFut<'a> {
        let StatefulValueDownlinkLifecycle {
            shared, on_synced, ..
        } = self;
        on_synced.on_synced(shared, value)
    }
}

impl<T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked> OnEvent<T>
    for StatefulValueDownlinkLifecycle<T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
    Shared: Send + Sync,
    FLinked: Send,
    FSynced: Send,
    FEv: OnEventShared<T, Shared>,
    FSet: Send,
    FUnlinked: Send,
{
    type OnEventFut<'a> = FEv::OnEventFut<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_event<'a>(&'a mut self, value: &'a T) -> Self::OnEventFut<'a> {
        let StatefulValueDownlinkLifecycle {
            shared, on_event, ..
        } = self;
        on_event.on_event(shared, value)
    }
}

impl<T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked> OnSet<T>
    for StatefulValueDownlinkLifecycle<T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
    FLinked: Send,
    FSynced: Send,
    FEv: Send,
    FSet: OnSetShared<T, Shared>,
    FUnlinked: Send,
{
    type OnSetFut<'a> = FSet::OnSetFut<'a>
    where
        Self: 'a,
        T: 'a;

    fn on_set<'a>(&'a mut self, existing: Option<&'a T>, new_value: &'a T) -> Self::OnSetFut<'a> {
        let StatefulValueDownlinkLifecycle { shared, on_set, .. } = self;
        on_set.on_set(shared, existing, new_value)
    }
}

impl<T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked> OnUnlinked
    for StatefulValueDownlinkLifecycle<T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
    FLinked: Send,
    FSynced: Send,
    FEv: Send,
    FSet: Send,
    FUnlinked: OnUnlinkedShared<Shared>,
{
    type OnUnlinkedFut<'a> = FUnlinked::OnUnlinkedFut<'a>
    where
        Self: 'a;

    fn on_unlinked(&mut self) -> Self::OnUnlinkedFut<'_> {
        let StatefulValueDownlinkLifecycle {
            shared,
            on_unlinked,
            ..
        } = self;
        on_unlinked.on_unlinked(shared)
    }
}

/// A basic lifecycle for an event downlink where the event handlers do not share any state.
pub struct BasicEventDownlinkLifecycle<T, FLink = NoHandler, FEv = NoHandler, FUnlink = NoHandler> {
    _value_type: PhantomData<fn(T)>,
    on_linked: FLink,
    on_event: FEv,
    on_unlinked: FUnlink,
}

/// A lifecycle for an event downlink where the handlers for each event share state.
pub struct StatefulEventDownlinkLifecycle<
    T,
    Shared,
    FLink = NoHandler,
    FEv = NoHandler,
    FUnlink = NoHandler,
> {
    _value_type: PhantomData<fn(T)>,
    shared: Shared,
    on_linked: FLink,
    on_event: FEv,
    on_unlinked: FUnlink,
}

impl<T, FLinked, FEv, FUnlinked> BasicEventDownlinkLifecycle<T, FLinked, FEv, FUnlinked>
where
    T: Send + Sync + 'static,
{
    /// Replace the handler that is called when the downlink connects.
    pub fn on_linked<F>(
        self,
        f: F,
    ) -> BasicEventDownlinkLifecycle<T, FnMutHandler<F>, FEv, FUnlinked>
    where
        FnMutHandler<F>: OnLinked,
    {
        BasicEventDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: FnMutHandler(f),
            on_event: self.on_event,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink connects with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_linked_blocking<F>(
        self,
        f: F,
    ) -> BasicEventDownlinkLifecycle<T, BlockingHandler<F>, FEv, FUnlinked>
    where
        F: FnMut() + Send,
    {
        BasicEventDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: BlockingHandler(f),
            on_event: self.on_event,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink receives a new event.
    pub fn on_event<F>(
        self,
        f: F,
    ) -> BasicEventDownlinkLifecycle<T, FLinked, FnMutHandler<F>, FUnlinked>
    where
        FnMutHandler<F>: OnEvent<T>,
    {
        BasicEventDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_event: FnMutHandler(f),
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink receives a new event with the specified
    /// synchronous closure. Running this closure will block the task so it should complete quickly.
    pub fn on_event_blocking<F>(
        self,
        f: F,
    ) -> BasicEventDownlinkLifecycle<T, FLinked, BlockingHandler<F>, FUnlinked>
    where
        F: FnMut(&T) + Send,
    {
        BasicEventDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_event: BlockingHandler(f),
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink disconnects.
    pub fn on_unlinked<F>(
        self,
        f: F,
    ) -> BasicEventDownlinkLifecycle<T, FLinked, FEv, FnMutHandler<F>>
    where
        FnMutHandler<F>: OnUnlinked,
    {
        BasicEventDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_event: self.on_event,
            on_unlinked: FnMutHandler(f),
        }
    }

    /// Replace the handler that is called when the downlink disconnects with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_unlinked_blocking<F>(
        self,
        f: F,
    ) -> BasicEventDownlinkLifecycle<T, FLinked, FEv, BlockingHandler<F>>
    where
        F: FnMut() + Send,
    {
        BasicEventDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_event: self.on_event,
            on_unlinked: BlockingHandler(f),
        }
    }

    /// Adds shared state this is accessible to all handlers for this downlink.
    pub fn with<Shared>(
        self,
        shared_state: Shared,
    ) -> WithSharedEventDownlinkLifecycle<T, Shared, FLinked, FEv, FUnlinked> {
        StatefulEventDownlinkLifecycle {
            _value_type: PhantomData,
            shared: shared_state,
            on_linked: WithShared::new(self.on_linked),
            on_event: WithShared::new(self.on_event),
            on_unlinked: WithShared::new(self.on_unlinked),
        }
    }
}

impl<T, FLinked, FEv, FUnlinked> OnLinked
    for BasicEventDownlinkLifecycle<T, FLinked, FEv, FUnlinked>
where
    T: Send + Sync + 'static,
    FLinked: OnLinked,
    FEv: Send,
    FUnlinked: Send,
{
    type OnLinkedFut<'a> = FLinked::OnLinkedFut<'a>
    where
        Self: 'a;

    fn on_linked(&mut self) -> Self::OnLinkedFut<'_> {
        self.on_linked.on_linked()
    }
}

impl<T, FLinked, FEv, FUnlinked> OnEvent<T>
    for BasicEventDownlinkLifecycle<T, FLinked, FEv, FUnlinked>
where
    T: Send + Sync + 'static,
    FLinked: Send,
    FEv: OnEvent<T>,
    FUnlinked: Send,
{
    type OnEventFut<'a> = FEv::OnEventFut<'a>
    where
        Self: 'a;

    fn on_event<'a>(&'a mut self, value: &'a T) -> Self::OnEventFut<'a> {
        self.on_event.on_event(value)
    }
}

impl<T, FLinked, FEv, FUnlinked> OnUnlinked
    for BasicEventDownlinkLifecycle<T, FLinked, FEv, FUnlinked>
where
    T: Send + Sync + 'static,
    FLinked: Send,
    FEv: Send,
    FUnlinked: OnUnlinked,
{
    type OnUnlinkedFut<'a> = FUnlinked::OnUnlinkedFut<'a>
    where
        Self: 'a;

    fn on_unlinked(&mut self) -> Self::OnUnlinkedFut<'_> {
        self.on_unlinked.on_unlinked()
    }
}

impl<T, Shared, FLinked, FEv, FUnlinked>
    StatefulEventDownlinkLifecycle<T, Shared, FLinked, FEv, FUnlinked>
where
    T: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
{
    /// Replace the handler that is called when the downlink connects.
    pub fn on_linked<F>(
        self,
        f: F,
    ) -> StatefulEventDownlinkLifecycle<T, Shared, FnMutHandler<F>, FEv, FUnlinked>
    where
        FnMutHandler<F>: OnLinkedShared<Shared>,
    {
        StatefulEventDownlinkLifecycle {
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: FnMutHandler(f),
            on_event: self.on_event,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink connects. Running this closure
    /// will block the task so it should complete quickly.
    pub fn on_linked_blocking<F>(
        self,
        f: F,
    ) -> StatefulEventDownlinkLifecycle<T, Shared, BlockingHandler<F>, FEv, FUnlinked>
    where
        F: FnMut(&mut Shared) + Send,
    {
        StatefulEventDownlinkLifecycle {
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: BlockingHandler(f),
            on_event: self.on_event,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink receives a new event.
    pub fn on_event<F>(
        self,
        f: F,
    ) -> StatefulEventDownlinkLifecycle<T, Shared, FLinked, FnMutHandler<F>, FUnlinked>
    where
        FnMutHandler<F>: OnEventShared<T, Shared>,
    {
        StatefulEventDownlinkLifecycle {
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_event: FnMutHandler(f),
            on_unlinked: self.on_unlinked,
        }
    }
    /// Replace the handler that is called when the downlink receives a new event. Running this closure
    /// will block the task so it should complete quickly.
    pub fn on_event_blocking<F>(
        self,
        f: F,
    ) -> StatefulEventDownlinkLifecycle<T, Shared, FLinked, BlockingHandler<F>, FUnlinked>
    where
        F: FnMut(&mut Shared, &T),
    {
        StatefulEventDownlinkLifecycle {
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_event: BlockingHandler(f),
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink disconnects.
    pub fn on_unlinked<F>(
        self,
        f: F,
    ) -> StatefulEventDownlinkLifecycle<T, Shared, FLinked, FEv, FnMutHandler<F>>
    where
        FnMutHandler<F>: OnUnlinkedShared<Shared>,
    {
        StatefulEventDownlinkLifecycle {
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_event: self.on_event,
            on_unlinked: FnMutHandler(f),
        }
    }

    /// Replace the handler that is called when the downlink disconnects. Running this closure
    /// will block the task so it should complete quickly.
    pub fn on_unlinked_blocking<F>(
        self,
        f: F,
    ) -> StatefulEventDownlinkLifecycle<T, Shared, FLinked, FEv, BlockingHandler<F>>
    where
        F: FnMut(&mut Shared),
    {
        StatefulEventDownlinkLifecycle {
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_event: self.on_event,
            on_unlinked: BlockingHandler(f),
        }
    }
}

impl<T, Shared, FLinked, FEv, FUnlinked> OnLinked
    for StatefulEventDownlinkLifecycle<T, Shared, FLinked, FEv, FUnlinked>
where
    T: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
    FLinked: OnLinkedShared<Shared>,
    FEv: Send,
    FUnlinked: Send,
{
    type OnLinkedFut<'a> = FLinked::OnLinkedFut<'a>
    where
        Self: 'a;

    fn on_linked(&mut self) -> Self::OnLinkedFut<'_> {
        let StatefulEventDownlinkLifecycle {
            shared, on_linked, ..
        } = self;
        on_linked.on_linked(shared)
    }
}

impl<T, Shared, FLinked, FEv, FUnlinked> OnEvent<T>
    for StatefulEventDownlinkLifecycle<T, Shared, FLinked, FEv, FUnlinked>
where
    T: Send + Sync + 'static,
    Shared: Send + Sync,
    FLinked: Send,
    FEv: OnEventShared<T, Shared>,
    FUnlinked: Send,
{
    type OnEventFut<'a> = FEv::OnEventFut<'a>
    where
        Self: 'a;

    fn on_event<'a>(&'a mut self, value: &'a T) -> Self::OnEventFut<'a> {
        let StatefulEventDownlinkLifecycle {
            shared, on_event, ..
        } = self;
        on_event.on_event(shared, value)
    }
}

impl<T, Shared, FLinked, FEv, FUnlinked> OnUnlinked
    for StatefulEventDownlinkLifecycle<T, Shared, FLinked, FEv, FUnlinked>
where
    T: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
    FLinked: Send,
    FEv: Send,
    FUnlinked: OnUnlinkedShared<Shared>,
{
    type OnUnlinkedFut<'a> = FUnlinked::OnUnlinkedFut<'a>
    where
        Self: 'a;

    fn on_unlinked(&mut self) -> Self::OnUnlinkedFut<'_> {
        let StatefulEventDownlinkLifecycle {
            shared,
            on_unlinked,
            ..
        } = self;
        on_unlinked.on_unlinked(shared)
    }
}
