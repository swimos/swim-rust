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

use super::{FnMutHandler, NoHandler, WithShared};
pub use on_event::{OnEvent, OnEventShared};
pub use on_linked::{OnLinked, OnLinkedShared};
pub use on_set::{OnSet, OnSetShared};
pub use on_synced::{OnSynced, OnSyncedShared};
pub use on_unlinked::{OnUnlinked, OnUnlinkedShared};

mod on_event;
mod on_linked;
mod on_set;
mod on_synced;
mod on_unlinked;
#[cfg(test)]
mod tests;

pub trait ValueDownlinkLifecycle<'a, T>:
    OnLinked<'a> + OnSynced<'a, T> + OnEvent<'a, T> + OnSet<'a, T> + OnUnlinked<'a>
{
}

impl<'a, T, L> ValueDownlinkLifecycle<'a, T> for L where
    L: OnLinked<'a> + OnSynced<'a, T> + OnEvent<'a, T> + OnSet<'a, T> + OnUnlinked<'a>
{
}

pub struct StatelessValueDownlinkLifecycle<T, FLink, FSync, FEv, FSet, FUnlink> {
    _value_type: PhantomData<fn(T)>,
    on_linked: FLink,
    on_synced: FSync,
    on_event: FEv,
    on_set: FSet,
    on_unlinked: FUnlink,
}

pub fn for_value_downlink<T>(
) -> StatelessValueDownlinkLifecycle<T, NoHandler, NoHandler, NoHandler, NoHandler, NoHandler> {
    StatelessValueDownlinkLifecycle {
        _value_type: PhantomData,
        on_linked: NoHandler,
        on_synced: NoHandler,
        on_event: NoHandler,
        on_set: NoHandler,
        on_unlinked: NoHandler,
    }
}

impl<T, FLinked, FSynced, FEv, FSet, FUnlinked>
    StatelessValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FSet, FUnlinked>
where
    T: Send + Sync + 'static,
{
    pub fn on_linked<F>(
        self,
        f: F,
    ) -> StatelessValueDownlinkLifecycle<T, FnMutHandler<F>, FSynced, FEv, FSet, FUnlinked>
    where
        FnMutHandler<F>: for<'a> OnLinked<'a>,
    {
        StatelessValueDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: FnMutHandler(f),
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_set: self.on_set,
            on_unlinked: self.on_unlinked,
        }
    }

    pub fn on_synced<F>(
        self,
        f: F,
    ) -> StatelessValueDownlinkLifecycle<T, FLinked, FnMutHandler<F>, FEv, FSet, FUnlinked>
    where
        FnMutHandler<F>: for<'a> OnSynced<'a, T>,
    {
        StatelessValueDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: FnMutHandler(f),
            on_event: self.on_event,
            on_set: self.on_set,
            on_unlinked: self.on_unlinked,
        }
    }

    pub fn on_event<F>(
        self,
        f: F,
    ) -> StatelessValueDownlinkLifecycle<T, FLinked, FSynced, FnMutHandler<F>, FSet, FUnlinked>
    where
        FnMutHandler<F>: for<'a> OnEvent<'a, T>,
    {
        StatelessValueDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: FnMutHandler(f),
            on_set: self.on_set,
            on_unlinked: self.on_unlinked,
        }
    }

    pub fn on_set<F>(
        self,
        f: F,
    ) -> StatelessValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FnMutHandler<F>, FUnlinked>
    where
        FnMutHandler<F>: for<'a> OnSet<'a, T>,
    {
        StatelessValueDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_set: FnMutHandler(f),
            on_unlinked: self.on_unlinked,
        }
    }

    pub fn on_unlinked<F>(
        self,
        f: F,
    ) -> StatelessValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FSet, FnMutHandler<F>>
    where
        FnMutHandler<F>: for<'a> OnUnlinked<'a>,
    {
        StatelessValueDownlinkLifecycle {
            _value_type: PhantomData,
            on_linked: self.on_linked,
            on_synced: self.on_synced,
            on_event: self.on_event,
            on_set: self.on_set,
            on_unlinked: FnMutHandler(f),
        }
    }

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
    for StatelessValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FSet, FUnlinked>
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
    for StatelessValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FSet, FUnlinked>
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
    for StatelessValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FSet, FUnlinked>
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
    for StatelessValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FSet, FUnlinked>
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
    for StatelessValueDownlinkLifecycle<T, FLinked, FSynced, FEv, FSet, FUnlinked>
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
