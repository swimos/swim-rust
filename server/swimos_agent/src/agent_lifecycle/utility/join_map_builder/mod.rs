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

use swimos_utilities::handlers::FnHandler;

use crate::{
    lanes::join_map::lifecycle::{
        on_failed::{OnJoinMapFailed, OnJoinMapFailedShared},
        on_linked::{OnJoinMapLinked, OnJoinMapLinkedShared},
        on_synced::{OnJoinMapSynced, OnJoinMapSyncedShared},
        on_unlinked::{OnJoinMapUnlinked, OnJoinMapUnlinkedShared},
        JoinMapLaneLifecycle, StatefulJoinMapLaneLifecycle, StatefulJoinMapLifecycle,
        StatelessJoinMapLaneLifecycle, StatelessJoinMapLifecycle,
    },
    lifecycle_fn::WithHandlerContext,
};

/// Builder type for constructing [`JoinMapLaneLifecycle`]s where the event handlers do not
/// share state.
#[derive(Debug)]
pub struct StatelessJoinMapLifecycleBuilder<
    Context,
    L,
    K,
    V,
    LC = StatelessJoinMapLaneLifecycle<Context, L, K>,
> {
    _type: PhantomData<fn(&Context, L, K, V)>,
    inner: LC,
}

impl<Context, L, K, V, LC: Default> Default
    for StatelessJoinMapLifecycleBuilder<Context, L, K, V, LC>
{
    fn default() -> Self {
        Self {
            _type: Default::default(),
            inner: Default::default(),
        }
    }
}

type JoinMapType<Context, State, L, K, V> = for<'a> fn(&'a Context, &'a State, L, K, V);

/// Builder type for constructing [`JoinMapLaneLifecycle`]s where the event handlers have shared state.
#[derive(Debug)]
pub struct StatefulJoinMapLifecycleBuilder<
    Context,
    State,
    L,
    K,
    V,
    LC = StatefulJoinMapLaneLifecycle<Context, State, L, K>,
> {
    _type: PhantomData<JoinMapType<Context, State, L, K, V>>,
    inner: LC,
}

impl<Context, L, K, V, LC> StatelessJoinMapLifecycleBuilder<Context, L, K, V, LC>
where
    LC: StatelessJoinMapLifecycle<Context, L, K> + 'static,
{
    /// Specify a handler for the `on_linked` event.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_linked<F>(
        self,
        handler: F,
    ) -> StatelessJoinMapLifecycleBuilder<Context, L, K, V, LC::WithOnLinked<WithHandlerContext<F>>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinMapLinked<L, Context>,
    {
        let StatelessJoinMapLifecycleBuilder { inner, .. } = self;
        StatelessJoinMapLifecycleBuilder {
            _type: PhantomData,
            inner: inner.on_linked(handler),
        }
    }

    /// Specify a handler for the `on_synced` event.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_synced<F>(
        self,
        handler: F,
    ) -> StatelessJoinMapLifecycleBuilder<Context, L, K, V, LC::WithOnSynced<WithHandlerContext<F>>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinMapSynced<L, K, Context>,
    {
        let StatelessJoinMapLifecycleBuilder { inner, .. } = self;
        StatelessJoinMapLifecycleBuilder {
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
    ) -> StatelessJoinMapLifecycleBuilder<Context, L, K, V, LC::WithOnUnlinked<WithHandlerContext<F>>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinMapUnlinked<L, K, Context>,
    {
        let StatelessJoinMapLifecycleBuilder { inner, .. } = self;
        StatelessJoinMapLifecycleBuilder {
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
    ) -> StatelessJoinMapLifecycleBuilder<Context, L, K, V, LC::WithOnFailed<WithHandlerContext<F>>>
    where
        F: Clone,
        WithHandlerContext<F>: OnJoinMapFailed<L, K, Context>,
    {
        let StatelessJoinMapLifecycleBuilder { inner, .. } = self;
        StatelessJoinMapLifecycleBuilder {
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
    ) -> StatefulJoinMapLifecycleBuilder<Context, State, L, K, V, LC::WithShared<State>> {
        StatefulJoinMapLifecycleBuilder {
            _type: PhantomData,
            inner: self.inner.with_shared_state(state),
        }
    }

    /// Complete the lifecycle.
    pub fn done(self) -> impl JoinMapLaneLifecycle<L, K, Context> + Send + 'static {
        self.inner
    }
}

impl<Context, State, L, K, V> StatefulJoinMapLifecycleBuilder<Context, State, L, K, V> {
    pub fn new(state: State) -> Self {
        StatefulJoinMapLifecycleBuilder {
            _type: PhantomData,
            inner: StatefulJoinMapLaneLifecycle::new(state),
        }
    }
}

impl<Context, State, L, K, V, LC> StatefulJoinMapLifecycleBuilder<Context, State, L, K, V, LC>
where
    LC: StatefulJoinMapLifecycle<Context, State, L, K> + 'static,
    State: Send + 'static,
{
    /// Specify a handler for the `on_linked` event.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_linked<F>(
        self,
        handler: F,
    ) -> StatefulJoinMapLifecycleBuilder<Context, State, L, K, V, LC::WithOnLinked<FnHandler<F>>>
    where
        F: Clone,
        FnHandler<F>: OnJoinMapLinkedShared<L, Context, State>,
    {
        let StatefulJoinMapLifecycleBuilder { inner, .. } = self;
        StatefulJoinMapLifecycleBuilder {
            _type: PhantomData,
            inner: inner.on_linked(handler),
        }
    }

    /// Specify a handler for the `on_synced` event.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_synced<F>(
        self,
        handler: F,
    ) -> StatefulJoinMapLifecycleBuilder<Context, State, L, K, V, LC::WithOnSynced<FnHandler<F>>>
    where
        F: Clone,
        FnHandler<F>: OnJoinMapSyncedShared<L, K, Context, State>,
    {
        let StatefulJoinMapLifecycleBuilder { inner, .. } = self;
        StatefulJoinMapLifecycleBuilder {
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
    ) -> StatefulJoinMapLifecycleBuilder<Context, State, L, K, V, LC::WithOnUnlinked<FnHandler<F>>>
    where
        F: Clone,
        FnHandler<F>: OnJoinMapUnlinkedShared<L, K, Context, State>,
    {
        let StatefulJoinMapLifecycleBuilder { inner, .. } = self;
        StatefulJoinMapLifecycleBuilder {
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
    ) -> StatefulJoinMapLifecycleBuilder<Context, State, L, K, V, LC::WithOnFailed<FnHandler<F>>>
    where
        F: Clone,
        FnHandler<F>: OnJoinMapFailedShared<L, K, Context, State>,
    {
        let StatefulJoinMapLifecycleBuilder { inner, .. } = self;
        StatefulJoinMapLifecycleBuilder {
            _type: PhantomData,
            inner: inner.on_failed(handler),
        }
    }

    /// Complete the lifecycle.
    pub fn done(self) -> impl JoinMapLaneLifecycle<L, K, Context> + Send + 'static {
        self.inner
    }
}
