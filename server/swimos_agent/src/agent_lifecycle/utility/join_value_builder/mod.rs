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

use swimos_utilities::handlers::{BorrowHandler, FnHandler};

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

/// Builder type for constructing join value lane lifecycles where the event handlers do not
/// share state.
#[derive(Debug)]
pub struct StatelessJoinValueLifecycleBuilder<
    Context,
    K,
    V,
    LC = StatelessJoinValueLaneLifecycle<Context, K, V>,
> {
    _type: PhantomData<fn(&Context, K, V)>,
    inner: LC,
}

impl<Context, K, V, LC: Default> Default for StatelessJoinValueLifecycleBuilder<Context, K, V, LC> {
    fn default() -> Self {
        Self {
            _type: Default::default(),
            inner: Default::default(),
        }
    }
}

/// Builder type for constructing join value lane lifecycles where the event handlers have shared state.
#[derive(Debug)]
pub struct StatefulJoinValueLifecycleBuilder<
    Context,
    State,
    K,
    V,
    LC = StatefulJoinValueLaneLifecycle<Context, State, K, V>,
> {
    _type: PhantomData<fn(&Context, &State, K, V)>,
    inner: LC,
}

impl<Context, K, V, LC> StatelessJoinValueLifecycleBuilder<Context, K, V, LC>
where
    LC: StatelessJoinValueLifecycle<Context, K, V> + 'static,
{
    /// Specify a handler for the `on_linked` event.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_linked<F>(
        self,
        handler: F,
    ) -> StatelessJoinValueLifecycleBuilder<Context, K, V, LC::WithOnLinked<WithHandlerContext<F>>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinValueLinked<K, Context>,
    {
        let StatelessJoinValueLifecycleBuilder { inner, .. } = self;
        StatelessJoinValueLifecycleBuilder {
            _type: PhantomData,
            inner: inner.on_linked(handler),
        }
    }

    /// Specify a handler for the `on_synced` event.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_synced<F, B>(
        self,
        handler: F,
    ) -> StatelessJoinValueLifecycleBuilder<
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
        let StatelessJoinValueLifecycleBuilder { inner, .. } = self;
        StatelessJoinValueLifecycleBuilder {
            _type: PhantomData,
            inner: inner.on_synced(handler),
        }
    }

    /// Specify a handler for the `on_unlinked` event.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_unlinked<F>(
        self,
        handler: F,
    ) -> StatelessJoinValueLifecycleBuilder<Context, K, V, LC::WithOnUnlinked<WithHandlerContext<F>>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinValueUnlinked<K, Context>,
    {
        let StatelessJoinValueLifecycleBuilder { inner, .. } = self;
        StatelessJoinValueLifecycleBuilder {
            _type: PhantomData,
            inner: inner.on_unlinked(handler),
        }
    }

    /// Specify a handler for the `on_failed` event (called if the downlink terminates with an error).
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_failed<F>(
        self,
        handler: F,
    ) -> StatelessJoinValueLifecycleBuilder<Context, K, V, LC::WithOnFailed<WithHandlerContext<F>>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinValueFailed<K, Context>,
    {
        let StatelessJoinValueLifecycleBuilder { inner, .. } = self;
        StatelessJoinValueLifecycleBuilder {
            _type: PhantomData,
            inner: inner.on_failed(handler),
        }
    }

    /// Augment the lifecycle with some state that is shared between the event handlers.
    ///
    /// # Arguments
    /// * `state` - The shared state.
    pub fn with_shared_state<State: Send + Clone>(
        self,
        state: State,
    ) -> StatefulJoinValueLifecycleBuilder<Context, State, K, V, LC::WithShared<State>> {
        StatefulJoinValueLifecycleBuilder {
            _type: PhantomData,
            inner: self.inner.with_shared_state(state),
        }
    }

    /// Complete the lifecycle.
    pub fn done(self) -> impl JoinValueLaneLifecycle<K, V, Context> + Send + 'static {
        self.inner
    }
}

impl<Context, State, K, V> StatefulJoinValueLifecycleBuilder<Context, State, K, V> {
    pub fn new(state: State) -> Self {
        StatefulJoinValueLifecycleBuilder {
            _type: PhantomData,
            inner: StatefulJoinValueLaneLifecycle::new(state),
        }
    }
}

impl<Context, State, K, V, LC> StatefulJoinValueLifecycleBuilder<Context, State, K, V, LC>
where
    LC: StatefulJoinValueLifecycle<Context, State, K, V> + 'static,
    State: Send + 'static,
{
    /// Specify a handler for the `on_linked` event.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_linked<F>(
        self,
        handler: F,
    ) -> StatefulJoinValueLifecycleBuilder<Context, State, K, V, LC::WithOnLinked<FnHandler<F>>>
    where
        F: Clone,
        FnHandler<F>: OnJoinValueLinkedShared<K, Context, State>,
    {
        let StatefulJoinValueLifecycleBuilder { inner, .. } = self;
        StatefulJoinValueLifecycleBuilder {
            _type: PhantomData,
            inner: inner.on_linked(handler),
        }
    }

    /// Specify a handler for the `on_synced` event.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_synced<F, B>(
        self,
        handler: F,
    ) -> StatefulJoinValueLifecycleBuilder<
        Context,
        State,
        K,
        V,
        LC::WithOnSynced<BorrowHandler<F, B>>,
    >
    where
        B: ?Sized,
        V: Borrow<B>,
        F: Clone,
        BorrowHandler<F, B>: OnJoinValueSyncedShared<K, V, Context, State>,
    {
        let StatefulJoinValueLifecycleBuilder { inner, .. } = self;
        StatefulJoinValueLifecycleBuilder {
            _type: PhantomData,
            inner: inner.on_synced(handler),
        }
    }

    /// Specify a handler for the `on_unlinked` event.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_unlinked<F>(
        self,
        handler: F,
    ) -> StatefulJoinValueLifecycleBuilder<Context, State, K, V, LC::WithOnUnlinked<FnHandler<F>>>
    where
        F: Clone,
        FnHandler<F>: OnJoinValueUnlinkedShared<K, Context, State>,
    {
        let StatefulJoinValueLifecycleBuilder { inner, .. } = self;
        StatefulJoinValueLifecycleBuilder {
            _type: PhantomData,
            inner: inner.on_unlinked(handler),
        }
    }

    /// Specify a handler for the `on_failed` event (called if the downlink terminates with an error).
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_failed<F>(
        self,
        handler: F,
    ) -> StatefulJoinValueLifecycleBuilder<Context, State, K, V, LC::WithOnFailed<FnHandler<F>>>
    where
        F: Clone,
        FnHandler<F>: OnJoinValueFailedShared<K, Context, State>,
    {
        let StatefulJoinValueLifecycleBuilder { inner, .. } = self;
        StatefulJoinValueLifecycleBuilder {
            _type: PhantomData,
            inner: inner.on_failed(handler),
        }
    }

    /// Complete the lifecycle.
    pub fn done(self) -> impl JoinValueLaneLifecycle<K, V, Context> + Send + 'static {
        self.inner
    }
}
