// Copyright 2015-2024 Swim Inc.
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

use std::borrow::Borrow;

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use swimos_api::address::Address;
use swimos_form::Form;
use swimos_model::Text;
use swimos_utilities::handlers::{BorrowHandler, FnHandler};

use crate::downlink_lifecycle::{
    OnFailed, OnFailedShared, OnSynced, OnSyncedShared, StatefulMapLifecycle, StatelessMapLifecycle,
};
use crate::lifecycle_fn::{WithHandlerContext, WithHandlerContextBorrow};
use crate::{
    agent_model::downlink::{MapDownlinkHandle, OpenMapDownlinkAction},
    config::MapDownlinkConfig,
    downlink_lifecycle::{
        OnDownlinkClear, OnDownlinkClearShared, OnDownlinkRemove, OnDownlinkRemoveShared,
        OnDownlinkUpdate, OnDownlinkUpdateShared, OnLinked, OnLinkedShared, OnUnlinked,
        OnUnlinkedShared, StatelessMapDownlinkLifecycle,
    },
    event_handler::HandlerAction,
};

/// A builder for constructing a map downlink. Each lifecycle event handler is independent and, by
/// default, they all do nothing.
pub struct StatelessMapDownlinkBuilder<
    Context,
    K,
    V,
    LC = StatelessMapDownlinkLifecycle<Context, K, V>,
> {
    _type: PhantomData<fn(Context) -> (K, V)>,
    address: Address<Text>,
    config: MapDownlinkConfig,
    inner: LC,
}

impl<Context, K, V> StatelessMapDownlinkBuilder<Context, K, V> {
    pub fn new(address: Address<Text>, config: MapDownlinkConfig) -> Self {
        StatelessMapDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: StatelessMapDownlinkLifecycle::default(),
        }
    }
}

type StatefulBuilderVar<Context, K, V, State> = (fn(Context, State) -> (K, V), State);

/// A builder for constructing a map downlink. The lifecycle event handlers share state and, by default,
/// they all do nothing.
pub struct StatefulMapDownlinkBuilder<Context, K, V, State, LC> {
    _type: PhantomData<StatefulBuilderVar<Context, K, V, State>>,
    address: Address<Text>,
    config: MapDownlinkConfig,
    inner: LC,
}

impl<Context, K, V, LC> StatelessMapDownlinkBuilder<Context, K, V, LC>
where
    LC: StatelessMapLifecycle<Context, K, V>,
{
    /// Specify a handler for the `on_linked` event.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_linked<F>(
        self,
        handler: F,
    ) -> StatelessMapDownlinkBuilder<Context, K, V, LC::WithOnLinked<WithHandlerContext<F>>>
    where
        WithHandlerContext<F>: OnLinked<Context>,
    {
        let StatelessMapDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatelessMapDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
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
    ) -> StatelessMapDownlinkBuilder<Context, K, V, LC::WithOnSynced<WithHandlerContext<F>>>
    where
        WithHandlerContext<F>: OnSynced<HashMap<K, V>, Context>,
    {
        let StatelessMapDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatelessMapDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
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
    ) -> StatelessMapDownlinkBuilder<Context, K, V, LC::WithOnUnlinked<WithHandlerContext<F>>>
    where
        WithHandlerContext<F>: OnUnlinked<Context>,
    {
        let StatelessMapDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatelessMapDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
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
    ) -> StatelessMapDownlinkBuilder<Context, K, V, LC::WithOnFailed<WithHandlerContext<F>>>
    where
        WithHandlerContext<F>: OnFailed<Context>,
    {
        let StatelessMapDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatelessMapDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_failed(handler),
        }
    }

    /// Specify a new event handler to be executed when an entry in the map is updated.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_update<F, B>(
        self,
        handler: F,
    ) -> StatelessMapDownlinkBuilder<Context, K, V, LC::WithOnUpdate<WithHandlerContextBorrow<F, B>>>
    where
        B: ?Sized,
        V: Borrow<B>,
        WithHandlerContextBorrow<F, B>: OnDownlinkUpdate<K, V, Context>,
    {
        let StatelessMapDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatelessMapDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_update(handler),
        }
    }

    /// Specify a new event handler to be executed when an entry in the map is removed.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_remove<F>(
        self,
        handler: F,
    ) -> StatelessMapDownlinkBuilder<Context, K, V, LC::WithOnRemove<WithHandlerContext<F>>>
    where
        WithHandlerContext<F>: OnDownlinkRemove<K, V, Context>,
    {
        let StatelessMapDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatelessMapDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_remove(handler),
        }
    }

    /// Specify a new event handler to be executed when the map is cleared.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_clear<F>(
        self,
        handler: F,
    ) -> StatelessMapDownlinkBuilder<Context, K, V, LC::WithOnClear<WithHandlerContext<F>>>
    where
        WithHandlerContext<F>: OnDownlinkClear<K, V, Context>,
    {
        let StatelessMapDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatelessMapDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_clear(handler),
        }
    }
    /// Augment the lifecycle with some state that is shared between the event handlers.
    ///
    /// # Arguments
    /// * `shared` - The shared state.
    pub fn with_state<State: Send>(
        self,
        state: State,
    ) -> StatefulMapDownlinkBuilder<Context, K, V, State, LC::WithShared<State>> {
        let StatelessMapDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatefulMapDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.with_shared_state(state),
        }
    }
}

impl<Context, K, V, LC> StatelessMapDownlinkBuilder<Context, K, V, LC>
where
    Context: 'static,
    K: Form + Hash + Eq + Ord + Clone + Send + Sync + 'static,
    K::Rec: Send,
    V: Form + Send + Sync + 'static,
    V::Rec: Send,
    LC: StatelessMapLifecycle<Context, K, V> + 'static,
{
    /// Complete the downlink and create a [`HandlerAction`] that will open the downlink when it is
    /// executed.
    pub fn done(
        self,
    ) -> impl HandlerAction<Context, Completion = MapDownlinkHandle<K, V>> + Send + 'static {
        let StatelessMapDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        OpenMapDownlinkAction::new(address, inner, config)
    }
}

impl<Context, K, V, State, LC> StatefulMapDownlinkBuilder<Context, K, V, State, LC>
where
    LC: StatefulMapLifecycle<Context, State, K, V> + 'static,
{
    /// Specify a handler for the `on_linked` event.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_linked<F>(
        self,
        handler: F,
    ) -> StatefulMapDownlinkBuilder<Context, K, V, State, LC::WithOnLinked<FnHandler<F>>>
    where
        FnHandler<F>: OnLinkedShared<Context, State>,
    {
        let StatefulMapDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatefulMapDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
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
    ) -> StatefulMapDownlinkBuilder<Context, K, V, State, LC::WithOnSynced<FnHandler<F>>>
    where
        FnHandler<F>: OnSyncedShared<HashMap<K, V>, Context, State>,
    {
        let StatefulMapDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatefulMapDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
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
    ) -> StatefulMapDownlinkBuilder<Context, K, V, State, LC::WithOnUnlinked<FnHandler<F>>>
    where
        FnHandler<F>: OnUnlinkedShared<Context, State>,
    {
        let StatefulMapDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatefulMapDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
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
    ) -> StatefulMapDownlinkBuilder<Context, K, V, State, LC::WithOnFailed<FnHandler<F>>>
    where
        FnHandler<F>: OnFailedShared<Context, State>,
    {
        let StatefulMapDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatefulMapDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_failed(handler),
        }
    }

    /// Specify a new event handler to be executed when an entry in the map is updated.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_update<F, B>(
        self,
        handler: F,
    ) -> StatefulMapDownlinkBuilder<Context, K, V, State, LC::WithOnUpdate<BorrowHandler<F, B>>>
    where
        B: ?Sized,
        V: Borrow<B>,
        BorrowHandler<F, B>: OnDownlinkUpdateShared<K, V, Context, State>,
    {
        let StatefulMapDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatefulMapDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_update(handler),
        }
    }

    /// Specify a new event handler to be executed when an entry in the map is removed.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_remove<F>(
        self,
        handler: F,
    ) -> StatefulMapDownlinkBuilder<Context, K, V, State, LC::WithOnRemove<FnHandler<F>>>
    where
        FnHandler<F>: OnDownlinkRemoveShared<K, V, Context, State>,
    {
        let StatefulMapDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatefulMapDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_remove(handler),
        }
    }

    /// Specify a new event handler to be executed when the map is cleared.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_clear<F>(
        self,
        handler: F,
    ) -> StatefulMapDownlinkBuilder<Context, K, V, State, LC::WithOnClear<FnHandler<F>>>
    where
        FnHandler<F>: OnDownlinkClearShared<K, V, Context, State>,
    {
        let StatefulMapDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatefulMapDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_clear(handler),
        }
    }
}

impl<Context, K, V, State, LC> StatefulMapDownlinkBuilder<Context, K, V, State, LC>
where
    Context: 'static,
    K: Form + Hash + Eq + Ord + Clone + Send + Sync + 'static,
    K::Rec: Send,
    V: Form + Send + Sync + 'static,
    V::Rec: Send,
    State: Send + 'static,
    LC: StatefulMapLifecycle<Context, State, K, V> + 'static,
{
    /// Complete the downlink and create a [`HandlerAction`] that will open the downlink when it is
    /// executed.
    pub fn done(
        self,
    ) -> impl HandlerAction<Context, Completion = MapDownlinkHandle<K, V>> + Send + 'static {
        let StatefulMapDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        OpenMapDownlinkAction::new(address, inner, config)
    }
}
