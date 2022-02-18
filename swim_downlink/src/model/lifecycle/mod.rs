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

pub use on_event::{OnEvent, OnEventShared};
pub use on_linked::{OnLinked, OnLinkedShared};
pub use on_set::{OnSet, OnSetShared};
pub use on_synced::{OnSynced, OnSyncedShared};
pub use on_unlinked::{OnUnlinked, OnUnlinkedShared};
use swim_api::handlers::{BlockingHandler, FnMutHandler, NoHandler, WithShared};

mod on_event;
mod on_linked;
mod on_set;
mod on_synced;
mod on_unlinked;
#[cfg(test)]
mod tests;

/// The set of handlers that a value downlink lifecycle supports.
pub trait ValueDownlinkHandlers<'a, T>:
    OnLinked<'a> + OnSynced<'a, T> + OnEvent<'a, T> + OnSet<'a, T> + OnUnlinked<'a>
{
}

/// The set of handlers that a value downlink lifecycle supports.
pub trait EventDownlinkHandlers<'a, T>: OnLinked<'a> + OnEvent<'a, T> + OnUnlinked<'a> {}

/// Description of a lifecycle for a value downlink.
pub trait ValueDownlinkLifecycle<T>: for<'a> ValueDownlinkHandlers<'a, T> {}

/// Description of a lifecycle for an event downlink.
pub trait EventDownlinkLifecycle<T>: for<'a> EventDownlinkHandlers<'a, T> {}

impl<T, L> ValueDownlinkLifecycle<T> for L where L: for<'a> ValueDownlinkHandlers<'a, T> {}

impl<T, L> EventDownlinkLifecycle<T> for L where L: for<'a> EventDownlinkHandlers<'a, T> {}

impl<'a, T, L> ValueDownlinkHandlers<'a, T> for L where
    L: OnLinked<'a> + OnSynced<'a, T> + OnEvent<'a, T> + OnSet<'a, T> + OnUnlinked<'a>
{
}

impl<'a, T, L> EventDownlinkHandlers<'a, T> for L where
    L: OnLinked<'a> + OnEvent<'a, T> + OnUnlinked<'a>
{
}

/// A basic lifecycle for a value downlink where the event handlers do not share any state.
pub struct BasicValueDownlinkLifecycle<T, FLink, FSync, FEv, FSet, FUnlink> {
    _value_type: PhantomData<fn(T)>,
    on_linked: FLink,
    on_synced: FSync,
    on_event: FEv,
    on_set: FSet,
    on_unlinked: FUnlink,
}

/// Create a default lifecycle for a value downlink (where all of the event handlers do nothing).
pub fn for_value_downlink<T>(
) -> BasicValueDownlinkLifecycle<T, NoHandler, NoHandler, NoHandler, NoHandler, NoHandler> {
    BasicValueDownlinkLifecycle {
        _value_type: PhantomData,
        on_linked: NoHandler,
        on_synced: NoHandler,
        on_event: NoHandler,
        on_set: NoHandler,
        on_unlinked: NoHandler,
    }
}

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
        FnMutHandler<F>: for<'a> OnLinked<'a>,
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
    /// closure. This operation should be guaranteed to complete quickly to avoid blocking the task
    /// executing the downlink.
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

    /// Replace the handler that is called when the synchronizes connects.
    pub fn on_synced<F>(
        self,
        f: F,
    ) -> BasicValueDownlinkLifecycle<T, FLinked, FnMutHandler<F>, FEv, FSet, FUnlinked>
    where
        FnMutHandler<F>: for<'a> OnSynced<'a, T>,
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
    /// closure. This operation should be guaranteed to complete quickly to avoid blocking the task
    /// executing the downlink.
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
        FnMutHandler<F>: for<'a> OnEvent<'a, T>,
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
    /// synchronous closure. This operation should be guaranteed to complete quickly to avoid blocking
    /// the task executing the downlink.
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
        FnMutHandler<F>: for<'a> OnSet<'a, T>,
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
    /// closure. This operation should be guaranteed to complete quickly to avoid blocking the task
    /// executing the downlink.
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
        FnMutHandler<F>: for<'a> OnUnlinked<'a>,
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
    /// closure. This operation should be guaranteed to complete quickly to avoid blocking the task
    /// executing the downlink.
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
    #[allow(clippy::type_complexity)] //There is no way this type can be decomposed.
    pub fn with<Shared>(
        self,
        shared_state: Shared,
    ) -> StatefulValueDownlinkLifecycle<
        T,
        Shared,
        WithShared<FLinked>,
        WithShared<FSynced>,
        WithShared<FEv>,
        WithShared<FSet>,
        WithShared<FUnlinked>,
    > {
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

impl<'a, T, FLinked, FSynced, FEv, FSet, FUnlinked> OnLinked<'a>
    for BasicValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
    FLinked: OnLinked<'a>,
    FSynced: Send,
    FEv: Send,
    FSet: Send,
    FUnlinked: Send,
{
    type OnLinkedFut = FLinked::OnLinkedFut;

    fn on_linked(&'a mut self) -> Self::OnLinkedFut {
        self.on_linked.on_linked()
    }
}

impl<'a, T, FLinked, FSynced, FEv, FSet, FUnlinked> OnSynced<'a, T>
    for BasicValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
    FLinked: Send,
    FSynced: OnSynced<'a, T>,
    FEv: Send,
    FSet: Send,
    FUnlinked: Send,
{
    type OnSyncedFut = FSynced::OnSyncedFut;

    fn on_synced(&'a mut self, value: &'a T) -> Self::OnSyncedFut {
        self.on_synced.on_synced(value)
    }
}

impl<'a, T, FLinked, FSynced, FEv, FSet, FUnlinked> OnEvent<'a, T>
    for BasicValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
    FLinked: Send,
    FSynced: Send,
    FEv: OnEvent<'a, T>,
    FSet: Send,
    FUnlinked: Send,
{
    type OnEventFut = FEv::OnEventFut;

    fn on_event(&'a mut self, value: &'a T) -> Self::OnEventFut {
        self.on_event.on_event(value)
    }
}

impl<'a, T, FLinked, FSynced, FEv, FSet, FUnlinked> OnSet<'a, T>
    for BasicValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
    FLinked: Send,
    FSynced: Send,
    FEv: Send,
    FSet: OnSet<'a, T>,
    FUnlinked: Send,
{
    type OnSetFut = FSet::OnSetFut;

    fn on_set(&'a mut self, existing: Option<&'a T>, new_value: &'a T) -> Self::OnSetFut {
        self.on_set.on_set(existing, new_value)
    }
}

impl<'a, T, FLinked, FSynced, FEv, FSet, FUnlinked> OnUnlinked<'a>
    for BasicValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
    FLinked: Send,
    FSynced: Send,
    FEv: Send,
    FSet: Send,
    FUnlinked: OnUnlinked<'a>,
{
    type OnUnlinkedFut = FUnlinked::OnUnlinkedFut;

    fn on_unlinked(&'a mut self) -> Self::OnUnlinkedFut {
        self.on_unlinked.on_unlinked()
    }
}

/// A lifecycle for a value downlink where the handlers for each event share state.
pub struct StatefulValueDownlinkLifecycle<T, Shared, FLink, FSync, FEv, FSet, FUnlink> {
    _value_type: PhantomData<fn(T)>,
    shared: Shared,
    on_linked: FLink,
    on_synced: FSync,
    on_event: FEv,
    on_set: FSet,
    on_unlinked: FUnlink,
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
        FnMutHandler<F>: for<'a> OnLinkedShared<'a, Shared>,
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
    /// closure. This operation should be guaranteed to complete quickly to avoid blocking the task
    /// executing the downlink.
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
        FnMutHandler<F>: for<'a> OnSyncedShared<'a, T, Shared>,
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
    /// closure. This operation should be guaranteed to complete quickly to avoid blocking the task
    /// executing the downlink.
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
        FnMutHandler<F>: for<'a> OnEventShared<'a, T, Shared>,
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
    /// synchronous closure. This operation should be guaranteed to complete quickly to avoid blocking the task
    /// executing the downlink.
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
        FnMutHandler<F>: for<'a> OnSetShared<'a, T, Shared>,
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
    /// closure. This operation should be guaranteed to complete quickly to avoid blocking the task
    /// executing the downlink.
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
        FnMutHandler<F>: for<'a> OnUnlinkedShared<'a, Shared>,
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
    /// closure. This operation should be guaranteed to complete quickly to avoid blocking the task
    /// executing the downlink.
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

impl<'a, T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked> OnLinked<'a>
    for StatefulValueDownlinkLifecycle<T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
    FLinked: OnLinkedShared<'a, Shared>,
    FSynced: Send,
    FEv: Send,
    FSet: Send,
    FUnlinked: Send,
{
    type OnLinkedFut = FLinked::OnLinkedFut;

    fn on_linked(&'a mut self) -> Self::OnLinkedFut {
        let StatefulValueDownlinkLifecycle {
            shared, on_linked, ..
        } = self;
        on_linked.on_linked(shared)
    }
}

impl<'a, T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked> OnSynced<'a, T>
    for StatefulValueDownlinkLifecycle<T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
    FLinked: Send,
    FSynced: OnSyncedShared<'a, T, Shared>,
    FEv: Send,
    FSet: Send,
    FUnlinked: Send,
{
    type OnSyncedFut = FSynced::OnSyncedFut;

    fn on_synced(&'a mut self, value: &'a T) -> Self::OnSyncedFut {
        let StatefulValueDownlinkLifecycle {
            shared, on_synced, ..
        } = self;
        on_synced.on_synced(shared, value)
    }
}

impl<'a, T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked> OnEvent<'a, T>
    for StatefulValueDownlinkLifecycle<T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
    FLinked: Send,
    FSynced: Send,
    FEv: OnEventShared<'a, T, Shared>,
    FSet: Send,
    FUnlinked: Send,
{
    type OnEventFut = FEv::OnEventFut;

    fn on_event(&'a mut self, value: &'a T) -> Self::OnEventFut {
        let StatefulValueDownlinkLifecycle {
            shared, on_event, ..
        } = self;
        on_event.on_event(shared, value)
    }
}

impl<'a, T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked> OnSet<'a, T>
    for StatefulValueDownlinkLifecycle<T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
    FLinked: Send,
    FSynced: Send,
    FEv: Send,
    FSet: OnSetShared<'a, T, Shared>,
    FUnlinked: Send,
{
    type OnSetFut = FSet::OnSetFut;

    fn on_set(&'a mut self, existing: Option<&'a T>, new_value: &'a T) -> Self::OnSetFut {
        let StatefulValueDownlinkLifecycle { shared, on_set, .. } = self;
        on_set.on_set(shared, existing, new_value)
    }
}

impl<'a, T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked> OnUnlinked<'a>
    for StatefulValueDownlinkLifecycle<T, Shared, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
    FLinked: Send,
    FSynced: Send,
    FEv: Send,
    FSet: Send,
    FUnlinked: OnUnlinkedShared<'a, Shared>,
{
    type OnUnlinkedFut = FUnlinked::OnUnlinkedFut;

    fn on_unlinked(&'a mut self) -> Self::OnUnlinkedFut {
        let StatefulValueDownlinkLifecycle {
            shared,
            on_unlinked,
            ..
        } = self;
        on_unlinked.on_unlinked(shared)
    }
}

/// A basic lifecycle for an event downlink where the event handlers do not share any state.
pub struct BasicEventDownlinkLifecycle<T, FLink, FEv, FUnlink> {
    _value_type: PhantomData<fn(T)>,
    on_linked: FLink,
    on_event: FEv,
    on_unlinked: FUnlink,
}

/// Create a default event downlink lifecycle where all of the event handlers do nothing.
pub fn for_event_downlink<T>() -> BasicEventDownlinkLifecycle<T, NoHandler, NoHandler, NoHandler> {
    BasicEventDownlinkLifecycle {
        _value_type: PhantomData,
        on_linked: NoHandler,
        on_event: NoHandler,
        on_unlinked: NoHandler,
    }
}

/// A lifecycle for an event downlink where the handlers for each event share state.
pub struct StatefulEventDownlinkLifecycle<T, Shared, FLink, FEv, FUnlink> {
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
        FnMutHandler<F>: for<'a> OnLinked<'a>,
    {
        BasicEventDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: FnMutHandler(f),
            on_event: self.on_event,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink connects with the specified synchronous
    /// closure. This operation should be guaranteed to complete quickly to avoid blocking the task
    /// executing the downlink.
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
        FnMutHandler<F>: for<'a> OnEvent<'a, T>,
    {
        BasicEventDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_event: FnMutHandler(f),
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink receives a new event with the specified
    /// synchronous closure. This operation should be guaranteed to complete quickly to avoid blocking the task
    /// executing the downlink.
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
        FnMutHandler<F>: for<'a> OnUnlinked<'a>,
    {
        BasicEventDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_event: self.on_event,
            on_unlinked: FnMutHandler(f),
        }
    }

    /// Replace the handler that is called when the downlink disconnects with the specified synchronous
    /// closure. This operation should be guaranteed to complete quickly to avoid blocking the task
    /// executing the downlink.
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
    #[allow(clippy::type_complexity)] //There is no way this type can be decomposed.
    pub fn with<Shared>(
        self,
        shared_state: Shared,
    ) -> StatefulEventDownlinkLifecycle<
        T,
        Shared,
        WithShared<FLinked>,
        WithShared<FEv>,
        WithShared<FUnlinked>,
    > {
        StatefulEventDownlinkLifecycle {
            _value_type: PhantomData,
            shared: shared_state,
            on_linked: WithShared::new(self.on_linked),
            on_event: WithShared::new(self.on_event),
            on_unlinked: WithShared::new(self.on_unlinked),
        }
    }
}

impl<'a, T, FLinked, FEv, FUnlinked> OnLinked<'a>
    for BasicEventDownlinkLifecycle<T, FLinked, FEv, FUnlinked>
where
    T: Send + Sync + 'static,
    FLinked: OnLinked<'a>,
    FEv: Send,
    FUnlinked: Send,
{
    type OnLinkedFut = FLinked::OnLinkedFut;

    fn on_linked(&'a mut self) -> Self::OnLinkedFut {
        self.on_linked.on_linked()
    }
}

impl<'a, T, FLinked, FEv, FUnlinked> OnEvent<'a, T>
    for BasicEventDownlinkLifecycle<T, FLinked, FEv, FUnlinked>
where
    T: Send + Sync + 'static,
    FLinked: Send,
    FEv: OnEvent<'a, T>,
    FUnlinked: Send,
{
    type OnEventFut = FEv::OnEventFut;

    fn on_event(&'a mut self, value: &'a T) -> Self::OnEventFut {
        self.on_event.on_event(value)
    }
}

impl<'a, T, FLinked, FEv, FUnlinked> OnUnlinked<'a>
    for BasicEventDownlinkLifecycle<T, FLinked, FEv, FUnlinked>
where
    T: Send + Sync + 'static,
    FLinked: Send,
    FEv: Send,
    FUnlinked: OnUnlinked<'a>,
{
    type OnUnlinkedFut = FUnlinked::OnUnlinkedFut;

    fn on_unlinked(&'a mut self) -> Self::OnUnlinkedFut {
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
        FnMutHandler<F>: for<'a> OnLinkedShared<'a, Shared>,
    {
        StatefulEventDownlinkLifecycle {
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: FnMutHandler(f),
            on_event: self.on_event,
            on_unlinked: self.on_unlinked,
        }
    }

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
        FnMutHandler<F>: for<'a> OnEventShared<'a, T, Shared>,
    {
        StatefulEventDownlinkLifecycle {
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_event: FnMutHandler(f),
            on_unlinked: self.on_unlinked,
        }
    }

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
        FnMutHandler<F>: for<'a> OnUnlinkedShared<'a, Shared>,
    {
        StatefulEventDownlinkLifecycle {
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_event: self.on_event,
            on_unlinked: FnMutHandler(f),
        }
    }

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

impl<'a, T, Shared, FLinked, FEv, FUnlinked> OnLinked<'a>
    for StatefulEventDownlinkLifecycle<T, Shared, FLinked, FEv, FUnlinked>
where
    T: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
    FLinked: OnLinkedShared<'a, Shared>,
    FEv: Send,
    FUnlinked: Send,
{
    type OnLinkedFut = FLinked::OnLinkedFut;

    fn on_linked(&'a mut self) -> Self::OnLinkedFut {
        let StatefulEventDownlinkLifecycle {
            shared, on_linked, ..
        } = self;
        on_linked.on_linked(shared)
    }
}

impl<'a, T, Shared, FLinked, FEv, FUnlinked> OnEvent<'a, T>
    for StatefulEventDownlinkLifecycle<T, Shared, FLinked, FEv, FUnlinked>
where
    T: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
    FLinked: Send,
    FEv: OnEventShared<'a, T, Shared>,
    FUnlinked: Send,
{
    type OnEventFut = FEv::OnEventFut;

    fn on_event(&'a mut self, value: &'a T) -> Self::OnEventFut {
        let StatefulEventDownlinkLifecycle {
            shared, on_event, ..
        } = self;
        on_event.on_event(shared, value)
    }
}

impl<'a, T, Shared, FLinked, FEv, FUnlinked> OnUnlinked<'a>
    for StatefulEventDownlinkLifecycle<T, Shared, FLinked, FEv, FUnlinked>
where
    T: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
    FLinked: Send,
    FEv: Send,
    FUnlinked: OnUnlinkedShared<'a, Shared>,
{
    type OnUnlinkedFut = FUnlinked::OnUnlinkedFut;

    fn on_unlinked(&'a mut self) -> Self::OnUnlinkedFut {
        let StatefulEventDownlinkLifecycle {
            shared,
            on_unlinked,
            ..
        } = self;
        on_unlinked.on_unlinked(shared)
    }
}
