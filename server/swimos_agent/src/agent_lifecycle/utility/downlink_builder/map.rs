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

use std::borrow::Borrow;

use std::hash::Hash;
use std::marker::PhantomData;
use swimos_form::Form;
use swimos_model::{address::Address, Text};
use swimos_utilities::handlers::{BorrowHandler, FnHandler};

use crate::downlink_lifecycle::map::on_synced::{OnMapSynced, OnMapSyncedShared};
use crate::downlink_lifecycle::map::{StatefulMapLifecycle, StatelessMapLifecycle};
use crate::downlink_lifecycle::on_failed::{OnFailed, OnFailedShared};
use crate::lifecycle_fn::{WithHandlerContext, WithHandlerContextBorrow};
use crate::{
    agent_model::downlink::{hosted::MapDownlinkHandle, OpenMapDownlinkAction},
    config::MapDownlinkConfig,
    downlink_lifecycle::{
        map::{
            on_clear::{OnDownlinkClear, OnDownlinkClearShared},
            on_remove::{OnDownlinkRemove, OnDownlinkRemoveShared},
            on_update::{OnDownlinkUpdate, OnDownlinkUpdateShared},
            StatelessMapDownlinkLifecycle,
        },
        on_linked::{OnLinked, OnLinkedShared},
        on_unlinked::{OnUnlinked, OnUnlinkedShared},
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
    /// Specify a new event handler to be executed when the downlink enters the linked state.
    pub fn on_linked<F>(
        self,
        f: F,
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
            inner: inner.on_linked(f),
        }
    }

    /// Specify a new event handler to be executed when the downlink enters the synced state.
    pub fn on_synced<F>(
        self,
        f: F,
    ) -> StatelessMapDownlinkBuilder<Context, K, V, LC::WithOnSynced<WithHandlerContext<F>>>
    where
        WithHandlerContext<F>: OnMapSynced<K, V, Context>,
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
            inner: inner.on_synced(f),
        }
    }

    /// Specify a new event handler to be executed when the downlink enters the unlinked state.
    pub fn on_unlinked<F>(
        self,
        f: F,
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
            inner: inner.on_unlinked(f),
        }
    }

    /// Specify a new event handler to be executed when the downlink fails.
    pub fn on_failed<F>(
        self,
        f: F,
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
            inner: inner.on_failed(f),
        }
    }

    /// Specify a new event handler to be executed when an entry in the map is updated.
    pub fn on_update<F, B>(
        self,
        f: F,
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
            inner: inner.on_update(f),
        }
    }

    /// Specify a new event handler to be executed when an entry in the map is removed.
    pub fn on_remove<F>(
        self,
        f: F,
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
            inner: inner.on_remove(f),
        }
    }

    /// Specify a new event handler to be executed when the map is cleared.
    pub fn on_clear<F>(
        self,
        f: F,
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
            inner: inner.on_clear(f),
        }
    }
    /// Add a state that can be shared between the event handlers for the downlink.
    ///
    /// #Arguments
    /// * `state` - The value of the state.
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
    LC: StatefulMapLifecycle<Context, State, K, V>,
{
    /// Specify a new event handler to be executed when the downlink enters the linked state.
    pub fn on_linked<F>(
        self,
        f: F,
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
            inner: inner.on_linked(f),
        }
    }

    /// Specify a new event handler to be executed when the downlink enters the synced state.
    pub fn on_synced<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkBuilder<Context, K, V, State, LC::WithOnSynced<FnHandler<F>>>
    where
        FnHandler<F>: OnMapSyncedShared<K, V, Context, State>,
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
            inner: inner.on_synced(f),
        }
    }

    /// Specify a new event handler to be executed when the downlink enters the unlinked state.
    pub fn on_unlinked<F>(
        self,
        f: F,
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
            inner: inner.on_unlinked(f),
        }
    }

    /// Specify a new event handler to be executed when the downlink enters the unlinked state.
    pub fn on_failed<F>(
        self,
        f: F,
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
            inner: inner.on_failed(f),
        }
    }

    /// Specify a new event handler to be executed when an entry in the map is updated.
    pub fn on_update<F, B>(
        self,
        f: F,
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
            inner: inner.on_update(f),
        }
    }

    /// Specify a new event handler to be executed when an entry in the map is removed.
    pub fn on_remove<F>(
        self,
        f: F,
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
            inner: inner.on_remove(f),
        }
    }

    /// Specify a new event handler to be executed when the map is cleared.
    pub fn on_clear<F>(
        self,
        f: F,
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
            inner: inner.on_clear(f),
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
