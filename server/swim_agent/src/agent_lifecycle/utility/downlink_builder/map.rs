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

use std::{borrow::Borrow, collections::HashMap};

use std::hash::Hash;
use swim_api::handlers::{BorrowHandler, FnHandler, NoHandler};
use swim_form::Form;
use swim_model::{address::Address, Text};

use crate::downlink_lifecycle::on_failed::{OnFailedShared, OnFailed};
use crate::{
    agent_model::downlink::{hosted::MapDownlinkHandle, OpenMapDownlinkAction},
    config::MapDownlinkConfig,
    downlink_lifecycle::{
        map::{
            on_clear::{OnDownlinkClear, OnDownlinkClearShared},
            on_remove::{OnDownlinkRemove, OnDownlinkRemoveShared},
            on_update::{OnDownlinkUpdate, OnDownlinkUpdateShared},
            StatefulMapDownlinkLifecycle, StatelessMapDownlinkLifecycle,
        },
        on_linked::{OnLinked, OnLinkedShared},
        on_synced::{OnSynced, OnSyncedShared},
        on_unlinked::{OnUnlinked, OnUnlinkedShared},
        LiftShared, WithHandlerContext, WithHandlerContextBorrow,
    },
    event_handler::HandlerAction,
};

/// A builder for constructing a map downlink. Each lifecycle event handler is independent and, by
/// default, they all do nothing.
pub struct StatelessMapDownlinkBuilder<
    Context,
    K,
    V,
    FLinked = NoHandler,
    FSynced = NoHandler,
    FUnlinked = NoHandler,
    FFailed = NoHandler,
    FUpd = NoHandler,
    FRem = NoHandler,
    FClr = NoHandler,
> {
    address: Address<Text>,
    config: MapDownlinkConfig,
    inner:
        StatelessMapDownlinkLifecycle<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>,
}

impl<Context, K, V> StatelessMapDownlinkBuilder<Context, K, V> {
    pub fn new(address: Address<Text>, config: MapDownlinkConfig) -> Self {
        StatelessMapDownlinkBuilder {
            address,
            config,
            inner: StatelessMapDownlinkLifecycle::default(),
        }
    }
}

/// A builder for constructing a map downlink. The lifecycle event handlers share state and, by default,
/// they all do nothing.
pub struct StatefulMapDownlinkBuilder<
    Context,
    K,
    V,
    State,
    FLinked = NoHandler,
    FSynced = NoHandler,
    FUnlinked = NoHandler,
    FFailed = NoHandler,
    FUpd = NoHandler,
    FRem = NoHandler,
    FClr = NoHandler,
> {
    address: Address<Text>,
    config: MapDownlinkConfig,
    inner: StatefulMapDownlinkLifecycle<
        Context,
        State,
        K,
        V,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >,
}

pub type LiftedMapBuilder<Context, K, V, State, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr> =
    StatefulMapDownlinkBuilder<
        Context,
        K,
        V,
        State,
        LiftShared<FLinked, State>,
        LiftShared<FSynced, State>,
        LiftShared<FUnlinked, State>,
        LiftShared<FFailed, State>,
        LiftShared<FUpd, State>,
        LiftShared<FRem, State>,
        LiftShared<FClr, State>,
    >;

type StatelessWithContextAndBorrow<Context, K, V, Linked, Synced, Unlinked, Failed, Rem, Clr, F, B> =
    StatelessMapDownlinkBuilder<
        Context,
        K,
        V,
        Linked,
        Synced,
        Unlinked,
        Failed,
        WithHandlerContextBorrow<Context, F, B>,
        Rem,
        Clr,
    >;

type StatefulWithBorrow<Context, K, V, State, Linked, Synced, Unlinked, Failed, Rem, Clr, F, B> =
    StatefulMapDownlinkBuilder<
        Context,
        K,
        V,
        State,
        Linked,
        Synced,
        Unlinked,
        Failed,
        BorrowHandler<F, B>,
        Rem,
        Clr,
    >;

impl<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    StatelessMapDownlinkBuilder<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
{
    /// Specify a new event handler to be executed when the downlink enters the linked state.
    pub fn on_linked<F>(
        self,
        f: F,
    ) -> StatelessMapDownlinkBuilder<
        Context,
        K,
        V,
        WithHandlerContext<Context, F>,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
    where
        WithHandlerContext<Context, F>: OnLinked<Context>,
    {
        let StatelessMapDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatelessMapDownlinkBuilder {
            address,
            config,
            inner: inner.on_linked(f),
        }
    }

    /// Specify a new event handler to be executed when the downlink enters the synced state.
    pub fn on_synced<F>(
        self,
        f: F,
    ) -> StatelessMapDownlinkBuilder<
        Context,
        K,
        V,
        FLinked,
        WithHandlerContext<Context, F>,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
    where
        WithHandlerContext<Context, F>: OnSynced<HashMap<K, V>, Context>,
    {
        let StatelessMapDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatelessMapDownlinkBuilder {
            address,
            config,
            inner: inner.on_synced(f),
        }
    }

    /// Specify a new event handler to be executed when the downlink enters the unlinked state.
    pub fn on_unlinked<F>(
        self,
        f: F,
    ) -> StatelessMapDownlinkBuilder<
        Context,
        K,
        V,
        FLinked,
        FSynced,
        WithHandlerContext<Context, F>,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
    where
        WithHandlerContext<Context, F>: OnUnlinked<Context>,
    {
        let StatelessMapDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatelessMapDownlinkBuilder {
            address,
            config,
            inner: inner.on_unlinked(f),
        }
    }

    /// Specify a new event handler to be executed when the downlink enters the unlinked state.
    pub fn on_failed<F>(
        self,
        f: F,
    ) -> StatelessMapDownlinkBuilder<
        Context,
        K,
        V,
        FLinked,
        FSynced,
        FUnlinked,
        WithHandlerContext<Context, F>,
        FUpd,
        FRem,
        FClr,
    >
    where
        WithHandlerContext<Context, F>: OnFailed<Context>,
    {
        let StatelessMapDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatelessMapDownlinkBuilder {
            address,
            config,
            inner: inner.on_failed(f),
        }
    }

    /// Specify a new event handler to be executed when an entry in the map is updated.
    pub fn on_update<F, B>(
        self,
        f: F,
    ) -> StatelessWithContextAndBorrow<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FRem, FClr, F, B>
    where
        B: ?Sized,
        V: Borrow<B>,
        WithHandlerContextBorrow<Context, F, B>: OnDownlinkUpdate<K, V, Context>,
    {
        let StatelessMapDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatelessMapDownlinkBuilder {
            address,
            config,
            inner: inner.on_update(f),
        }
    }

    /// Specify a new event handler to be executed when an entry in the map is removed.
    pub fn on_remove<F>(
        self,
        f: F,
    ) -> StatelessMapDownlinkBuilder<
        Context,
        K,
        V,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        WithHandlerContext<Context, F>,
        FClr,
    >
    where
        WithHandlerContext<Context, F>: OnDownlinkRemove<K, V, Context>,
    {
        let StatelessMapDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatelessMapDownlinkBuilder {
            address,
            config,
            inner: inner.on_remove(f),
        }
    }

    /// Specify a new event handler to be executed when the map is cleared.
    pub fn on_clear<F>(
        self,
        f: F,
    ) -> StatelessMapDownlinkBuilder<
        Context,
        K,
        V,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        WithHandlerContext<Context, F>,
    >
    where
        WithHandlerContext<Context, F>: OnDownlinkClear<K, V, Context>,
    {
        let StatelessMapDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatelessMapDownlinkBuilder {
            address,
            config,
            inner: inner.on_clear(f),
        }
    }

    /// Add a state that can be shared between the event handlers for the downlink.
    ///
    /// #Arguments
    /// * `state` - The value of the state.
    pub fn with_state<State>(
        self,
        state: State,
    ) -> LiftedMapBuilder<Context, K, V, State, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr> {
        let StatelessMapDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatefulMapDownlinkBuilder {
            address,
            config,
            inner: inner.with_state(state),
        }
    }
}

impl<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    StatelessMapDownlinkBuilder<Context, K, V, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
where
    Context: 'static,
    K: Form + Hash + Eq + Ord + Clone + Send + Sync + 'static,
    K::Rec: Send,
    V: Form + Send + Sync + 'static,
    V::Rec: Send,
    FLinked: OnLinked<Context> + 'static,
    FSynced: OnSynced<HashMap<K, V>, Context> + 'static,
    FUnlinked: OnUnlinked<Context> + 'static,
    FFailed: OnFailed<Context> + 'static,
    FUpd: OnDownlinkUpdate<K, V, Context> + 'static,
    FRem: OnDownlinkRemove<K, V, Context> + 'static,
    FClr: OnDownlinkClear<K, V, Context> + 'static,
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
        } = self;
        OpenMapDownlinkAction::new(address, inner, config)
    }
}

impl<Context, K, V, State, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    StatefulMapDownlinkBuilder<Context, K, V, State, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
{
    /// Specify a new event handler to be executed when the downlink enters the linked state.
    pub fn on_linked<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkBuilder<
        Context,
        K,
        V,
        State,
        FnHandler<F>,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
    where
        FnHandler<F>: OnLinkedShared<Context, State>,
    {
        let StatefulMapDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatefulMapDownlinkBuilder {
            address,
            config,
            inner: inner.on_linked(f),
        }
    }

    /// Specify a new event handler to be executed when the downlink enters the synced state.
    pub fn on_synced<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkBuilder<
        Context,
        K,
        V,
        State,
        FLinked,
        FnHandler<F>,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
    where
        FnHandler<F>: OnSyncedShared<HashMap<K, V>, Context, State>,
    {
        let StatefulMapDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatefulMapDownlinkBuilder {
            address,
            config,
            inner: inner.on_synced(f),
        }
    }

    /// Specify a new event handler to be executed when the downlink enters the unlinked state.
    pub fn on_unlinked<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkBuilder<
        Context,
        K,
        V,
        State,
        FLinked,
        FSynced,
        FnHandler<F>,
        FFailed,
        FUpd,
        FRem,
        FClr,
    >
    where
        FnHandler<F>: OnUnlinkedShared<Context, State>,
    {
        let StatefulMapDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatefulMapDownlinkBuilder {
            address,
            config,
            inner: inner.on_unlinked(f),
        }
    }

    /// Specify a new event handler to be executed when the downlink enters the unlinked state.
    pub fn on_failed<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkBuilder<
        Context,
        K,
        V,
        State,
        FLinked,
        FSynced,
        FUnlinked,
        FnHandler<F>,
        FUpd,
        FRem,
        FClr,
    >
    where
        FnHandler<F>: OnFailedShared<Context, State>,
    {
        let StatefulMapDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatefulMapDownlinkBuilder {
            address,
            config,
            inner: inner.on_failed(f),
        }
    }

    /// Specify a new event handler to be executed when an entry in the map is updated.
    pub fn on_update<F, B>(
        self,
        f: F,
    ) -> StatefulWithBorrow<Context, K, V, State, FLinked, FSynced, FUnlinked, FFailed, FRem, FClr, F, B>
    where
        B: ?Sized,
        V: Borrow<B>,
        BorrowHandler<F, B>: OnDownlinkUpdateShared<K, V, Context, State>,
    {
        let StatefulMapDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatefulMapDownlinkBuilder {
            address,
            config,
            inner: inner.on_update(f),
        }
    }

    /// Specify a new event handler to be executed when an entry in the map is removed.
    pub fn on_remove<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkBuilder<
        Context,
        K,
        V,
        State,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FnHandler<F>,
        FClr,
    >
    where
        FnHandler<F>: OnDownlinkRemoveShared<K, V, Context, State>,
    {
        let StatefulMapDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatefulMapDownlinkBuilder {
            address,
            config,
            inner: inner.on_remove(f),
        }
    }

    /// Specify a new event handler to be executed when the map is cleared.
    pub fn on_clear<F>(
        self,
        f: F,
    ) -> StatefulMapDownlinkBuilder<
        Context,
        K,
        V,
        State,
        FLinked,
        FSynced,
        FUnlinked,
        FFailed,
        FUpd,
        FRem,
        FnHandler<F>,
    >
    where
        FnHandler<F>: OnDownlinkClearShared<K, V, Context, State>,
    {
        let StatefulMapDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatefulMapDownlinkBuilder {
            address,
            config,
            inner: inner.on_clear(f),
        }
    }
}

impl<Context, K, V, State, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
    StatefulMapDownlinkBuilder<Context, K, V, State, FLinked, FSynced, FUnlinked, FFailed, FUpd, FRem, FClr>
where
    Context: 'static,
    K: Form + Hash + Eq + Ord + Clone + Send + Sync + 'static,
    K::Rec: Send,
    V: Form + Send + Sync + 'static,
    V::Rec: Send,
    State: Send + 'static,
    FLinked: OnLinkedShared<Context, State> + 'static,
    FSynced: OnSyncedShared<HashMap<K, V>, Context, State> + 'static,
    FUnlinked: OnUnlinkedShared<Context, State> + 'static,
    FFailed: OnFailedShared<Context, State> + 'static,
    FUpd: OnDownlinkUpdateShared<K, V, Context, State> + 'static,
    FRem: OnDownlinkRemoveShared<K, V, Context, State> + 'static,
    FClr: OnDownlinkClearShared<K, V, Context, State> + 'static,
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
        } = self;
        OpenMapDownlinkAction::new(address, inner, config)
    }
}
