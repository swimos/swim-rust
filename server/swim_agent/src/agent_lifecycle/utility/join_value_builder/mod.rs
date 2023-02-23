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

use swim_api::handlers::{BorrowHandler, FnHandler};

use crate::{
    lanes::join_value::lifecycle::{
        on_failed::{OnJoinValueFailed, OnJoinValueFailedShared},
        on_linked::{OnJoinValueLinked, OnJoinValueLinkedShared},
        on_synced::{OnJoinValueSynced, OnJoinValueSyncedShared},
        on_unlinked::{OnJoinValueUnlinked, OnJoinValueUnlinkedShared},
        JoinValueLaneLifecycle, StatefulJoinValueLaneLifecycle, StatefulJoinValueLifecycle,
        StatelessJoinValueLaneLifecycle, StatelessJoinValueLifecycle,
    },
    lifecycle_fn::{WithHandlerContext, WithHandlerContextBorrow},
};

#[derive(Debug)]
pub struct StatelessJoinValueLaneBuilder<
    Context,
    K,
    V,
    LC = StatelessJoinValueLaneLifecycle<Context, K, V>,
> {
    _type: PhantomData<fn(&Context, K, V)>,
    inner: LC,
}

impl<Context, K, V, LC: Default> Default for StatelessJoinValueLaneBuilder<Context, K, V, LC> {
    fn default() -> Self {
        Self {
            _type: Default::default(),
            inner: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct StatefulJoinValueLaneBuilder<
    Context,
    State,
    K,
    V,
    LC = StatefulJoinValueLaneLifecycle<Context, State, K, V>,
> {
    _type: PhantomData<fn(&Context, &State, K, V)>,
    inner: LC,
}

impl<Context, K, V, LC> StatelessJoinValueLaneBuilder<Context, K, V, LC>
where
    LC: StatelessJoinValueLifecycle<Context, K, V> + 'static,
{
    pub fn on_linked<F>(
        self,
        handler: F,
    ) -> StatelessJoinValueLaneBuilder<Context, K, V, LC::WithOnLinked<WithHandlerContext<F>>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinValueLinked<K, Context>,
    {
        let StatelessJoinValueLaneBuilder { inner, .. } = self;
        StatelessJoinValueLaneBuilder {
            _type: PhantomData,
            inner: inner.on_linked(handler),
        }
    }

    pub fn on_synced<F, B>(
        self,
        handler: F,
    ) -> StatelessJoinValueLaneBuilder<
        Context,
        K,
        V,
        LC::WithOnSynced<WithHandlerContextBorrow<F, B>>,
    >
    where
        B: ?Sized,
        V: Borrow<B>,
        F: Clone,
        WithHandlerContextBorrow<F, B>: OnJoinValueSynced<K, V, Context>,
    {
        let StatelessJoinValueLaneBuilder { inner, .. } = self;
        StatelessJoinValueLaneBuilder {
            _type: PhantomData,
            inner: inner.on_synced(handler),
        }
    }

    pub fn on_unlinked<F>(
        self,
        handler: F,
    ) -> StatelessJoinValueLaneBuilder<Context, K, V, LC::WithOnUnlinked<WithHandlerContext<F>>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinValueUnlinked<K, Context>,
    {
        let StatelessJoinValueLaneBuilder { inner, .. } = self;
        StatelessJoinValueLaneBuilder {
            _type: PhantomData,
            inner: inner.on_unlinked(handler),
        }
    }

    pub fn on_failed<F>(
        self,
        handler: F,
    ) -> StatelessJoinValueLaneBuilder<Context, K, V, LC::WithOnFailed<WithHandlerContext<F>>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinValueFailed<K, Context>,
    {
        let StatelessJoinValueLaneBuilder { inner, .. } = self;
        StatelessJoinValueLaneBuilder {
            _type: PhantomData,
            inner: inner.on_failed(handler),
        }
    }

    pub fn with_shared_state<State: Send + Clone>(
        self,
        state: State,
    ) -> StatefulJoinValueLaneBuilder<Context, State, K, V, LC::WithShared<State>> {
        StatefulJoinValueLaneBuilder {
            _type: PhantomData,
            inner: self.inner.with_shared_state(state),
        }
    }

    pub fn done(self) -> impl JoinValueLaneLifecycle<K, V, Context> + Send + 'static {
        self.inner
    }
}

impl<Context, State, K, V> StatefulJoinValueLaneBuilder<Context, State, K, V> {
    pub fn new(state: State) -> Self {
        StatefulJoinValueLaneBuilder {
            _type: PhantomData,
            inner: StatefulJoinValueLaneLifecycle::new(state),
        }
    }
}

impl<Context, State, K, V, LC> StatefulJoinValueLaneBuilder<Context, State, K, V, LC>
where
    LC: StatefulJoinValueLifecycle<Context, State, K, V> + 'static,
    State: Send + 'static,
{
    pub fn on_linked<F>(
        self,
        handler: F,
    ) -> StatefulJoinValueLaneBuilder<Context, State, K, V, LC::WithOnLinked<FnHandler<F>>>
    where
        F: Clone,
        FnHandler<F>: OnJoinValueLinkedShared<K, Context, State>,
    {
        let StatefulJoinValueLaneBuilder { inner, .. } = self;
        StatefulJoinValueLaneBuilder {
            _type: PhantomData,
            inner: inner.on_linked(handler),
        }
    }

    pub fn on_synced<F, B>(
        self,
        handler: F,
    ) -> StatefulJoinValueLaneBuilder<Context, State, K, V, LC::WithOnSynced<BorrowHandler<F, B>>>
    where
        B: ?Sized,
        V: Borrow<B>,
        F: Clone,
        BorrowHandler<F, B>: OnJoinValueSyncedShared<K, V, Context, State>,
    {
        let StatefulJoinValueLaneBuilder { inner, .. } = self;
        StatefulJoinValueLaneBuilder {
            _type: PhantomData,
            inner: inner.on_synced(handler),
        }
    }

    pub fn on_unlinked<F>(
        self,
        handler: F,
    ) -> StatefulJoinValueLaneBuilder<Context, State, K, V, LC::WithOnUnlinked<FnHandler<F>>>
    where
        F: Clone,
        FnHandler<F>: OnJoinValueUnlinkedShared<K, Context, State>,
    {
        let StatefulJoinValueLaneBuilder { inner, .. } = self;
        StatefulJoinValueLaneBuilder {
            _type: PhantomData,
            inner: inner.on_unlinked(handler),
        }
    }

    pub fn on_failed<F>(
        self,
        handler: F,
    ) -> StatefulJoinValueLaneBuilder<Context, State, K, V, LC::WithOnFailed<FnHandler<F>>>
    where
        F: Clone,
        FnHandler<F>: OnJoinValueFailedShared<K, Context, State>,
    {
        let StatefulJoinValueLaneBuilder { inner, .. } = self;
        StatefulJoinValueLaneBuilder {
            _type: PhantomData,
            inner: inner.on_failed(handler),
        }
    }

    pub fn done(self) -> impl JoinValueLaneLifecycle<K, V, Context> + Send + 'static {
        self.inner
    }
}
