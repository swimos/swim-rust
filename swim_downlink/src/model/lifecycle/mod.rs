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

use im::OrdMap;
use std::marker::PhantomData;

pub use on_cleared::{OnCleared, OnClearedShared};
pub use on_event::{OnEvent, OnEventShared};
pub use on_linked::{OnLinked, OnLinkedShared};
pub use on_removed::{OnRemoved, OnRemovedShared};
pub use on_set::{OnSet, OnSetShared};
pub use on_synced::{OnSynced, OnSyncedShared};
pub use on_unlinked::{OnUnlinked, OnUnlinkedShared};
pub use on_updated::{OnUpdated, OnUpdatedShared};
use swim_api::handlers::{BlockingHandler, FnMutHandler, NoHandler, WithShared};
use swim_api::protocol::map::MapMessage;

mod on_cleared;
mod on_event;
mod on_linked;
mod on_removed;
mod on_set;
mod on_synced;
mod on_unlinked;
mod on_updated;

/// The set of handlers that a value downlink lifecycle supports.
pub trait ValueDownlinkHandlers<'a, T>:
    OnLinked<'a> + OnSynced<'a, T> + OnEvent<'a, T> + OnSet<'a, T> + OnUnlinked<'a>
{
}

/// The set of handlers that an event downlink lifecycle supports.
pub trait EventDownlinkHandlers<'a, T>: OnLinked<'a> + OnEvent<'a, T> + OnUnlinked<'a> {}

/// The set of handlers that a map downlink lifecycle supports.
pub trait MapDownlinkHandlers<'a, K, V>:
    OnLinked<'a>
    + OnSynced<'a, OrdMap<K, V>>
    + OnEvent<'a, MapMessage<K, V>>
    + OnUpdated<'a, K, V>
    + OnRemoved<'a, K, V>
    + OnCleared<'a, K, V>
    + OnUnlinked<'a>
{
}

/// Description of a lifecycle for a value downlink.
pub trait ValueDownlinkLifecycle<T>: for<'a> ValueDownlinkHandlers<'a, T> {}

/// Description of a lifecycle for an event downlink.
pub trait EventDownlinkLifecycle<T>: for<'a> EventDownlinkHandlers<'a, T> {}

/// Description of a lifecycle for a map downlink.
pub trait MapDownlinkLifecycle<K, V>: for<'a> MapDownlinkHandlers<'a, K, V> {}

impl<T, L> ValueDownlinkLifecycle<T> for L where L: for<'a> ValueDownlinkHandlers<'a, T> {}

impl<T, L> EventDownlinkLifecycle<T> for L where L: for<'a> EventDownlinkHandlers<'a, T> {}

impl<K, V, L> MapDownlinkLifecycle<K, V> for L where L: for<'a> MapDownlinkHandlers<'a, K, V> {}

impl<'a, T, L> ValueDownlinkHandlers<'a, T> for L where
    L: OnLinked<'a> + OnSynced<'a, T> + OnEvent<'a, T> + OnSet<'a, T> + OnUnlinked<'a>
{
}

impl<'a, T, L> EventDownlinkHandlers<'a, T> for L where
    L: OnLinked<'a> + OnEvent<'a, T> + OnUnlinked<'a>
{
}

impl<'a, K, V, L> MapDownlinkHandlers<'a, K, V> for L where
    L: OnLinked<'a>
        + OnSynced<'a, OrdMap<K, V>>
        + OnEvent<'a, MapMessage<K, V>>
        + OnUpdated<'a, K, V>
        + OnRemoved<'a, K, V>
        + OnCleared<'a, K, V>
        + OnUnlinked<'a>
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

type WithSharedMapDownlinkLifecycle<K, V, Shared, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink> =
    StatefulMapDownlinkLifecycle<
        K,
        V,
        Shared,
        WithShared<FLink>,
        WithShared<FSync>,
        WithShared<FEv>,
        WithShared<FUpd>,
        WithShared<FRmv>,
        WithShared<FClr>,
        WithShared<FUnlink>,
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

/// A basic lifecycle for a map downlink where the event handlers do not share any state.
pub struct BasicMapDownlinkLifecycle<K, V, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink> {
    _key_type: PhantomData<fn(K)>,
    _value_type: PhantomData<fn(V)>,
    on_linked: FLink,
    on_synced: FSync,
    on_event: FEv,
    on_updated: FUpd,
    on_removed: FRmv,
    on_cleared: FClr,
    on_unlinked: FUnlink,
}

/// Create a default lifecycle for a map downlink (where all of the event handlers do nothing).
pub fn for_map_downlink<K, V>() -> BasicMapDownlinkLifecycle<
    K,
    V,
    NoHandler,
    NoHandler,
    NoHandler,
    NoHandler,
    NoHandler,
    NoHandler,
    NoHandler,
> {
    BasicMapDownlinkLifecycle {
        _key_type: PhantomData,
        _value_type: PhantomData,
        on_linked: NoHandler,
        on_synced: NoHandler,
        on_event: NoHandler,
        on_updated: NoHandler,
        on_removed: NoHandler,
        on_cleared: NoHandler,
        on_unlinked: NoHandler,
    }
}

impl<K, V, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
    BasicMapDownlinkLifecycle<K, V, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Replace the handler that is called when the downlink connects.
    pub fn on_linked<F>(
        self,
        f: F,
    ) -> BasicMapDownlinkLifecycle<K, V, FnMutHandler<F>, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
    where
        FnMutHandler<F>: for<'a> OnLinked<'a>,
    {
        BasicMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            on_linked: FnMutHandler(f),
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_updated: self.on_updated,
            on_removed: self.on_removed,
            on_cleared: self.on_cleared,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink connects with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_linked_blocking<F>(
        self,
        f: F,
    ) -> BasicMapDownlinkLifecycle<K, V, BlockingHandler<F>, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
    where
        F: FnMut() + Send,
    {
        BasicMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            on_linked: BlockingHandler(f),
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_updated: self.on_updated,
            on_removed: self.on_removed,
            on_cleared: self.on_cleared,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink synchronizes.
    pub fn on_synced<F>(
        self,
        f: F,
    ) -> BasicMapDownlinkLifecycle<K, V, FLink, FnMutHandler<F>, FEv, FUpd, FRmv, FClr, FUnlink>
    where
        FnMutHandler<F>: for<'a> OnSynced<'a, OrdMap<K, V>>,
    {
        BasicMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: FnMutHandler(f),
            on_event: self.on_event,
            on_updated: self.on_updated,
            on_removed: self.on_removed,
            on_cleared: self.on_cleared,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink synchronizes with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_synced_blocking<F>(
        self,
        f: F,
    ) -> BasicMapDownlinkLifecycle<K, V, FLink, BlockingHandler<F>, FEv, FUpd, FRmv, FClr, FUnlink>
    where
        F: FnMut(&OrdMap<K, V>) + Send,
    {
        BasicMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: BlockingHandler(f),
            on_event: self.on_event,
            on_updated: self.on_updated,
            on_removed: self.on_removed,
            on_cleared: self.on_cleared,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink receives an event.
    pub fn on_event<F>(
        self,
        f: F,
    ) -> BasicMapDownlinkLifecycle<K, V, FLink, FSync, FnMutHandler<F>, FUpd, FRmv, FClr, FUnlink>
    where
        FnMutHandler<F>: for<'a> OnEvent<'a, MapMessage<K, V>>,
    {
        BasicMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: FnMutHandler(f),
            on_updated: self.on_updated,
            on_removed: self.on_removed,
            on_cleared: self.on_cleared,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink receives and event with the specified
    /// synchronous closure. Running this closure will block the task so it should complete quickly.
    pub fn on_event_blocking<F>(
        self,
        f: F,
    ) -> BasicMapDownlinkLifecycle<K, V, FLink, FSync, BlockingHandler<F>, FUpd, FRmv, FClr, FUnlink>
    where
        F: FnMut(&MapMessage<K, V>) + Send,
    {
        BasicMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: BlockingHandler(f),
            on_updated: self.on_updated,
            on_removed: self.on_removed,
            on_cleared: self.on_cleared,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when a downlink entry is updated.
    pub fn on_updated<F>(
        self,
        f: F,
    ) -> BasicMapDownlinkLifecycle<K, V, FLink, FSync, FEv, FnMutHandler<F>, FRmv, FClr, FUnlink>
    where
        FnMutHandler<F>: for<'a> OnUpdated<'a, K, V>,
    {
        BasicMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_updated: FnMutHandler(f),
            on_removed: self.on_removed,
            on_cleared: self.on_cleared,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when a downlink entry is updated with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_updated_blocking<F>(
        self,
        f: F,
    ) -> BasicMapDownlinkLifecycle<K, V, FLink, FSync, FEv, BlockingHandler<F>, FRmv, FClr, FUnlink>
    where
        F: FnMut(K, Option<&V>, &V) + Send,
    {
        BasicMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_updated: BlockingHandler(f),
            on_removed: self.on_removed,
            on_cleared: self.on_cleared,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when a downlink entry is removed.
    pub fn on_removed<F>(
        self,
        f: F,
    ) -> BasicMapDownlinkLifecycle<K, V, FLink, FSync, FEv, FUpd, FnMutHandler<F>, FClr, FUnlink>
    where
        FnMutHandler<F>: for<'a> OnRemoved<'a, K, V>,
    {
        BasicMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_updated: self.on_updated,
            on_removed: FnMutHandler(f),
            on_cleared: self.on_cleared,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when a downlink entry is removed with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_removed_blocking<F>(
        self,
        f: F,
    ) -> BasicMapDownlinkLifecycle<K, V, FLink, FSync, FEv, FUpd, BlockingHandler<F>, FClr, FUnlink>
    where
        F: FnMut(K, Option<&V>) + Send,
    {
        BasicMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_updated: self.on_updated,
            on_removed: BlockingHandler(f),
            on_cleared: self.on_cleared,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when a map downlink is cleared.
    pub fn on_cleared<F>(
        self,
        f: F,
    ) -> BasicMapDownlinkLifecycle<K, V, FLink, FSync, FEv, FUpd, FRmv, FnMutHandler<F>, FUnlink>
    where
        FnMutHandler<F>: for<'a> OnCleared<'a, K, V>,
    {
        BasicMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_updated: self.on_updated,
            on_removed: self.on_removed,
            on_cleared: FnMutHandler(f),
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when a map downlink is cleared with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_cleared_blocking<F>(
        self,
        f: F,
    ) -> BasicMapDownlinkLifecycle<K, V, FLink, FSync, FEv, FUpd, FRmv, BlockingHandler<F>, FUnlink>
    where
        F: FnMut(OrdMap<K, V>) + Send,
    {
        BasicMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_updated: self.on_updated,
            on_removed: self.on_removed,
            on_cleared: BlockingHandler(f),
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink disconnects.
    pub fn on_unlinked<F>(
        self,
        f: F,
    ) -> BasicMapDownlinkLifecycle<K, V, FLink, FSync, FEv, FUpd, FRmv, FClr, FnMutHandler<F>>
    where
        FnMutHandler<F>: for<'a> OnUnlinked<'a>,
    {
        BasicMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_updated: self.on_updated,
            on_removed: self.on_removed,
            on_cleared: self.on_cleared,
            on_unlinked: FnMutHandler(f),
        }
    }

    /// Replace the handler that is called when the downlink disconnects with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_unlinked_blocking<F>(
        self,
        f: F,
    ) -> BasicMapDownlinkLifecycle<K, V, FLink, FSync, FEv, FUpd, FRmv, FClr, BlockingHandler<F>>
    where
        F: FnMut() + Send,
    {
        BasicMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_updated: self.on_updated,
            on_removed: self.on_removed,
            on_cleared: self.on_cleared,
            on_unlinked: BlockingHandler(f),
        }
    }

    /// Adds shared state this is accessible to all handlers for this downlink.
    pub fn with<Shared>(
        self,
        shared_state: Shared,
    ) -> WithSharedMapDownlinkLifecycle<K, V, Shared, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
    {
        WithSharedMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            shared: shared_state,
            on_linked: WithShared::new(self.on_linked),
            on_synced: WithShared::new(self.on_synced),
            on_event: WithShared::new(self.on_event),
            on_updated: WithShared::new(self.on_updated),
            on_removed: WithShared::new(self.on_removed),
            on_cleared: WithShared::new(self.on_cleared),
            on_unlinked: WithShared::new(self.on_unlinked),
        }
    }
}

impl<'a, K, V, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink> OnLinked<'a>
    for BasicMapDownlinkLifecycle<K, V, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    FLink: OnLinked<'a>,
    FSync: Send,
    FEv: Send,
    FUpd: Send,
    FRmv: Send,
    FClr: Send,
    FUnlink: Send,
{
    type OnLinkedFut = FLink::OnLinkedFut;

    fn on_linked(&'a mut self) -> Self::OnLinkedFut {
        self.on_linked.on_linked()
    }
}

impl<'a, K, V, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink> OnSynced<'a, OrdMap<K, V>>
    for BasicMapDownlinkLifecycle<K, V, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    FLink: Send,
    FSync: OnSynced<'a, OrdMap<K, V>>,
    FEv: Send,
    FUpd: Send,
    FRmv: Send,
    FClr: Send,
    FUnlink: Send,
{
    type OnSyncedFut = FSync::OnSyncedFut;

    fn on_synced(&'a mut self, value: &'a OrdMap<K, V>) -> Self::OnSyncedFut {
        self.on_synced.on_synced(value)
    }
}

impl<'a, K, V, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink> OnEvent<'a, MapMessage<K, V>>
    for BasicMapDownlinkLifecycle<K, V, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    FLink: Send,
    FSync: Send,
    FEv: OnEvent<'a, MapMessage<K, V>>,
    FUpd: Send,
    FRmv: Send,
    FClr: Send,
    FUnlink: Send,
{
    type OnEventFut = FEv::OnEventFut;

    fn on_event(&'a mut self, value: &'a MapMessage<K, V>) -> Self::OnEventFut {
        self.on_event.on_event(value)
    }
}

impl<'a, K, V, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink> OnUpdated<'a, K, V>
    for BasicMapDownlinkLifecycle<K, V, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    FLink: Send,
    FSync: Send,
    FEv: Send,
    FUpd: OnUpdated<'a, K, V>,
    FRmv: Send,
    FClr: Send,
    FUnlink: Send,
{
    type OnUpdatedFut = FUpd::OnUpdatedFut;

    fn on_updated(
        &'a mut self,
        key: &'a K,
        existing: Option<&'a V>,
        new_value: &'a V,
    ) -> Self::OnUpdatedFut {
        self.on_updated.on_updated(key, existing, new_value)
    }
}

impl<'a, K, V, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink> OnRemoved<'a, K, V>
    for BasicMapDownlinkLifecycle<K, V, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    FLink: Send,
    FSync: Send,
    FEv: Send,
    FUpd: Send,
    FRmv: OnRemoved<'a, K, V>,
    FClr: Send,
    FUnlink: Send,
{
    type OnRemovedFut = FRmv::OnRemovedFut;

    fn on_removed(&'a mut self, key: &'a K, existing: Option<&'a V>) -> Self::OnRemovedFut {
        self.on_removed.on_removed(key, existing)
    }
}

impl<'a, K, V, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink> OnCleared<'a, K, V>
    for BasicMapDownlinkLifecycle<K, V, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    FLink: Send,
    FSync: Send,
    FEv: Send,
    FUpd: Send,
    FRmv: Send,
    FClr: OnCleared<'a, K, V>,
    FUnlink: Send,
{
    type OnClearedFut = FClr::OnClearedFut;

    fn on_cleared(&'a mut self, map: &'a OrdMap<K, V>) -> Self::OnClearedFut {
        self.on_cleared.on_cleared(map)
    }
}

impl<'a, K, V, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink> OnUnlinked<'a>
    for BasicMapDownlinkLifecycle<K, V, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    FLink: Send,
    FSync: Send,
    FEv: Send,
    FUpd: Send,
    FRmv: Send,
    FClr: Send,
    FUnlink: OnUnlinked<'a>,
{
    type OnUnlinkedFut = FUnlink::OnUnlinkedFut;

    fn on_unlinked(&'a mut self) -> Self::OnUnlinkedFut {
        self.on_unlinked.on_unlinked()
    }
}

/// A lifecycle for a map downlink where the handlers for each event share state.
pub struct StatefulMapDownlinkLifecycle<K, V, Shared, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
{
    _key_type: PhantomData<fn(K)>,
    _value_type: PhantomData<fn(V)>,
    shared: Shared,
    on_linked: FLink,
    on_synced: FSync,
    on_event: FEv,
    on_updated: FUpd,
    on_removed: FRmv,
    on_cleared: FClr,
    on_unlinked: FUnlink,
}

impl<K, V, Shared, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
    StatefulMapDownlinkLifecycle<K, V, Shared, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
{
    /// Replace the handler that is called when the downlink connects.
    pub fn on_linked<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkLifecycle<
        K,
        V,
        Shared,
        FnMutHandler<F>,
        FSync,
        FEv,
        FUpd,
        FRmv,
        FClr,
        FUnlink,
    >
    where
        FnMutHandler<F>: for<'a> OnLinkedShared<'a, Shared>,
    {
        StatefulMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: FnMutHandler(f),
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_updated: self.on_updated,
            on_removed: self.on_removed,
            on_cleared: self.on_cleared,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink connects with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_linked_blocking<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkLifecycle<
        K,
        V,
        Shared,
        BlockingHandler<F>,
        FSync,
        FEv,
        FUpd,
        FRmv,
        FClr,
        FUnlink,
    >
    where
        F: FnMut(&mut Shared) + Send,
    {
        StatefulMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: BlockingHandler(f),
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_updated: self.on_updated,
            on_removed: self.on_removed,
            on_cleared: self.on_cleared,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink synchronizes.
    pub fn on_synced<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkLifecycle<
        K,
        V,
        Shared,
        FLink,
        FnMutHandler<F>,
        FEv,
        FUpd,
        FRmv,
        FClr,
        FUnlink,
    >
    where
        FnMutHandler<F>: for<'a> OnSyncedShared<'a, OrdMap<K, V>, Shared>,
    {
        StatefulMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_synced: FnMutHandler(f),
            on_event: self.on_event,
            on_updated: self.on_updated,
            on_removed: self.on_removed,
            on_cleared: self.on_cleared,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink synchronizes with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_synced_blocking<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkLifecycle<
        K,
        V,
        Shared,
        FLink,
        BlockingHandler<F>,
        FEv,
        FUpd,
        FRmv,
        FClr,
        FUnlink,
    >
    where
        F: FnMut(&mut Shared, &OrdMap<K, V>),
    {
        StatefulMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_synced: BlockingHandler(f),
            on_event: self.on_event,
            on_updated: self.on_updated,
            on_removed: self.on_removed,
            on_cleared: self.on_cleared,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink receives a new event.
    pub fn on_event<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkLifecycle<
        K,
        V,
        Shared,
        FLink,
        FSync,
        FnMutHandler<F>,
        FUpd,
        FRmv,
        FClr,
        FUnlink,
    >
    where
        FnMutHandler<F>: for<'a> OnEventShared<'a, MapMessage<K, V>, Shared>,
    {
        StatefulMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: FnMutHandler(f),
            on_updated: self.on_updated,
            on_removed: self.on_removed,
            on_cleared: self.on_cleared,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink receives a new event with the specified
    /// synchronous closure. Running this closure will block the task so it should complete quickly.
    pub fn on_event_blocking<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkLifecycle<
        K,
        V,
        Shared,
        FLink,
        FSync,
        BlockingHandler<F>,
        FUpd,
        FRmv,
        FClr,
        FUnlink,
    >
    where
        F: FnMut(&mut Shared, &MapMessage<K, V>),
    {
        StatefulMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: BlockingHandler(f),
            on_updated: self.on_updated,
            on_removed: self.on_removed,
            on_cleared: self.on_cleared,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when a downlink entry is updated.
    pub fn on_updated<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkLifecycle<
        K,
        V,
        Shared,
        FLink,
        FSync,
        FEv,
        FnMutHandler<F>,
        FRmv,
        FClr,
        FUnlink,
    >
    where
        FnMutHandler<F>: for<'a> OnUpdatedShared<'a, K, V, Shared>,
    {
        StatefulMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_updated: FnMutHandler(f),
            on_removed: self.on_removed,
            on_cleared: self.on_cleared,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when a downlink entry is updated with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_updated_blocking<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkLifecycle<
        K,
        V,
        Shared,
        FLink,
        FSync,
        FEv,
        BlockingHandler<F>,
        FRmv,
        FClr,
        FUnlink,
    >
    where
        F: FnMut(&mut Shared, &K, Option<&V>, &V),
    {
        StatefulMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_updated: BlockingHandler(f),
            on_removed: self.on_removed,
            on_cleared: self.on_cleared,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when a downlink entry is removed.
    pub fn on_removed<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkLifecycle<
        K,
        V,
        Shared,
        FLink,
        FSync,
        FEv,
        FUpd,
        FnMutHandler<F>,
        FClr,
        FUnlink,
    >
    where
        FnMutHandler<F>: for<'a> OnRemovedShared<'a, K, V, Shared>,
    {
        StatefulMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_updated: self.on_updated,
            on_removed: FnMutHandler(f),
            on_cleared: self.on_cleared,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when a downlink entry is removed with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_removed_blocking<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkLifecycle<
        K,
        V,
        Shared,
        FLink,
        FSync,
        FEv,
        FUpd,
        BlockingHandler<F>,
        FClr,
        FUnlink,
    >
    where
        F: FnMut(&mut Shared, &K, Option<&V>),
    {
        StatefulMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_updated: self.on_updated,
            on_removed: BlockingHandler(f),
            on_cleared: self.on_cleared,
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when a map downlink is cleared.
    pub fn on_cleared<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkLifecycle<
        K,
        V,
        Shared,
        FLink,
        FSync,
        FEv,
        FUpd,
        FRmv,
        FnMutHandler<F>,
        FUnlink,
    >
    where
        FnMutHandler<F>: for<'a> OnClearedShared<'a, K, V, Shared>,
    {
        StatefulMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_updated: self.on_updated,
            on_removed: self.on_removed,
            on_cleared: FnMutHandler(f),
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when a map downlink is cleared with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_cleared_blocking<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkLifecycle<
        K,
        V,
        Shared,
        FLink,
        FSync,
        FEv,
        FUpd,
        FRmv,
        BlockingHandler<F>,
        FUnlink,
    >
    where
        F: FnMut(&mut Shared, &OrdMap<K, V>),
    {
        StatefulMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_updated: self.on_updated,
            on_removed: self.on_removed,
            on_cleared: BlockingHandler(f),
            on_unlinked: self.on_unlinked,
        }
    }

    /// Replace the handler that is called when the downlink disconnects.
    pub fn on_unlinked<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkLifecycle<
        K,
        V,
        Shared,
        FLink,
        FSync,
        FEv,
        FUpd,
        FRmv,
        FClr,
        FnMutHandler<F>,
    >
    where
        FnMutHandler<F>: for<'a> OnUnlinkedShared<'a, Shared>,
    {
        StatefulMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_updated: self.on_updated,
            on_removed: self.on_removed,
            on_cleared: self.on_cleared,
            on_unlinked: FnMutHandler(f),
        }
    }

    /// Replace the handler that is called when the downlink disconnects with the specified synchronous
    /// closure. Running this closure will block the task so it should complete quickly.
    pub fn on_unlinked_blocking<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkLifecycle<
        K,
        V,
        Shared,
        FLink,
        FSync,
        FEv,
        FUpd,
        FRmv,
        FClr,
        BlockingHandler<F>,
    >
    where
        F: FnMut(&mut Shared),
    {
        StatefulMapDownlinkLifecycle {
            _key_type: PhantomData,
            _value_type: PhantomData,
            shared: self.shared,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_updated: self.on_updated,
            on_removed: self.on_removed,
            on_cleared: self.on_cleared,
            on_unlinked: BlockingHandler(f),
        }
    }
}

impl<'a, K, V, Shared, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink> OnLinked<'a>
    for StatefulMapDownlinkLifecycle<K, V, Shared, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
    FLink: OnLinkedShared<'a, Shared>,
    FSync: Send,
    FEv: Send,
    FUpd: Send,
    FRmv: Send,
    FClr: Send,
    FUnlink: Send,
{
    type OnLinkedFut = FLink::OnLinkedFut;

    fn on_linked(&'a mut self) -> Self::OnLinkedFut {
        let StatefulMapDownlinkLifecycle {
            shared, on_linked, ..
        } = self;
        on_linked.on_linked(shared)
    }
}

impl<'a, K, V, Shared, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink> OnSynced<'a, OrdMap<K, V>>
    for StatefulMapDownlinkLifecycle<K, V, Shared, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
    FLink: Send,
    FSync: OnSyncedShared<'a, OrdMap<K, V>, Shared>,
    FEv: Send,
    FUpd: Send,
    FRmv: Send,
    FClr: Send,
    FUnlink: Send,
{
    type OnSyncedFut = FSync::OnSyncedFut;

    fn on_synced(&'a mut self, value: &'a OrdMap<K, V>) -> Self::OnSyncedFut {
        let StatefulMapDownlinkLifecycle {
            shared, on_synced, ..
        } = self;
        on_synced.on_synced(shared, value)
    }
}

impl<'a, K, V, Shared, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink> OnEvent<'a, MapMessage<K, V>>
    for StatefulMapDownlinkLifecycle<K, V, Shared, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
    FLink: Send,
    FSync: Send,
    FEv: OnEventShared<'a, MapMessage<K, V>, Shared>,
    FUpd: Send,
    FRmv: Send,
    FClr: Send,
    FUnlink: Send,
{
    type OnEventFut = FEv::OnEventFut;

    fn on_event(&'a mut self, value: &'a MapMessage<K, V>) -> Self::OnEventFut {
        let StatefulMapDownlinkLifecycle {
            shared, on_event, ..
        } = self;
        on_event.on_event(shared, value)
    }
}

impl<'a, K, V, Shared, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink> OnUpdated<'a, K, V>
    for StatefulMapDownlinkLifecycle<K, V, Shared, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
    FLink: Send,
    FSync: Send,
    FEv: Send,
    FUpd: OnUpdatedShared<'a, K, V, Shared>,
    FRmv: Send,
    FClr: Send,
    FUnlink: Send,
{
    type OnUpdatedFut = FUpd::OnUpdatedFut;

    fn on_updated(
        &'a mut self,
        key: &'a K,
        existing: Option<&'a V>,
        new_value: &'a V,
    ) -> Self::OnUpdatedFut {
        let StatefulMapDownlinkLifecycle {
            shared, on_updated, ..
        } = self;
        on_updated.on_updated(shared, key, existing, new_value)
    }
}

impl<'a, K, V, Shared, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink> OnRemoved<'a, K, V>
    for StatefulMapDownlinkLifecycle<K, V, Shared, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
    FLink: Send,
    FSync: Send,
    FEv: Send,
    FUpd: Send,
    FRmv: OnRemovedShared<'a, K, V, Shared>,
    FClr: Send,
    FUnlink: Send,
{
    type OnRemovedFut = FRmv::OnRemovedFut;

    fn on_removed(&'a mut self, key: &'a K, existing: Option<&'a V>) -> Self::OnRemovedFut {
        let StatefulMapDownlinkLifecycle {
            shared, on_removed, ..
        } = self;
        on_removed.on_removed(shared, key, existing)
    }
}

impl<'a, K, V, Shared, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink> OnCleared<'a, K, V>
    for StatefulMapDownlinkLifecycle<K, V, Shared, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
    FLink: Send,
    FSync: Send,
    FEv: Send,
    FUpd: Send,
    FRmv: Send,
    FClr: OnClearedShared<'a, K, V, Shared>,
    FUnlink: Send,
{
    type OnClearedFut = FClr::OnClearedFut;

    fn on_cleared(&'a mut self, map: &'a OrdMap<K, V>) -> Self::OnClearedFut {
        let StatefulMapDownlinkLifecycle {
            shared, on_cleared, ..
        } = self;
        on_cleared.on_cleared(shared, map)
    }
}

impl<'a, K, V, Shared, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink> OnUnlinked<'a>
    for StatefulMapDownlinkLifecycle<K, V, Shared, FLink, FSync, FEv, FUpd, FRmv, FClr, FUnlink>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    Shared: Send + Sync + 'static,
    FLink: Send,
    FSync: Send,
    FEv: Send,
    FUpd: Send,
    FRmv: Send,
    FClr: Send,
    FUnlink: OnUnlinkedShared<'a, Shared>,
{
    type OnUnlinkedFut = FUnlink::OnUnlinkedFut;

    fn on_unlinked(&'a mut self) -> Self::OnUnlinkedFut {
        let StatefulMapDownlinkLifecycle {
            shared,
            on_unlinked,
            ..
        } = self;
        on_unlinked.on_unlinked(shared)
    }
}
