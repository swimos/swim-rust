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

use swim_api::handlers::{FnHandler, NoHandler};
use swim_form::Form;
use swim_model::{address::Address, Text};

use crate::{
    agent_model::downlink::{hosted::ValueDownlinkHandle, OpenValueDownlink},
    config::ValueDownlinkConfig,
    downlink_lifecycle::{
        on_linked::{OnLinked, OnLinkedShared},
        on_synced::{OnSynced, OnSyncedShared},
        on_unlinked::{OnUnlinked, OnUnlinkedShared},
        value::{
            on_event::{OnDownlinkEvent, OnDownlinkEventShared},
            on_set::{OnDownlinkSet, OnDownlinkSetShared},
            StatefulValueDownlinkLifecycle, StatelessValueDownlinkLifecycle,
        },
        LiftShared, WithHandlerContext,
    },
    event_handler::HandlerAction,
};

/// A builder for constructing a value downlink. Each lifecycle event handler is independent and, by
/// default, they all do nothing.
pub struct StatelessValueDownlinkBuilder<
    Context,
    T,
    FLinked = NoHandler,
    FSynced = NoHandler,
    FUnlinked = NoHandler,
    FEv = NoHandler,
    FSet = NoHandler,
> {
    address: Address<Text>,
    config: ValueDownlinkConfig,
    inner: StatelessValueDownlinkLifecycle<Context, T, FLinked, FSynced, FUnlinked, FEv, FSet>,
}

/// A builder for constructing a value downlink. The lifecycle event handlers share state and, by default,
/// they all do nothing.
pub struct StatefulValueDownlinkBuilder<
    Context,
    T,
    State,
    FLinked = NoHandler,
    FSynced = NoHandler,
    FUnlinked = NoHandler,
    FEv = NoHandler,
    FSet = NoHandler,
> {
    address: Address<Text>,
    config: ValueDownlinkConfig,
    inner:
        StatefulValueDownlinkLifecycle<Context, State, T, FLinked, FSynced, FUnlinked, FEv, FSet>,
}

impl<Context, T> StatelessValueDownlinkBuilder<Context, T> {
    pub fn new(address: Address<Text>, config: ValueDownlinkConfig) -> Self {
        StatelessValueDownlinkBuilder {
            address,
            config,
            inner: StatelessValueDownlinkLifecycle::default(),
        }
    }
}

impl<Context, T, State> StatefulValueDownlinkBuilder<Context, T, State> {
    pub fn new(address: Address<Text>, config: ValueDownlinkConfig, state: State) -> Self {
        StatefulValueDownlinkBuilder {
            address,
            config,
            inner: StatefulValueDownlinkLifecycle::new(state),
        }
    }
}

pub type LiftedValueBuilder<Context, T, State, FLinked, FSynced, FUnlinked, FEv, FSet> =
    StatefulValueDownlinkBuilder<
        Context,
        T,
        State,
        LiftShared<FLinked, State>,
        LiftShared<FSynced, State>,
        LiftShared<FUnlinked, State>,
        LiftShared<FEv, State>,
        LiftShared<FSet, State>,
    >;

impl<Context, T, FLinked, FSynced, FUnlinked, FEv, FSet>
    StatelessValueDownlinkBuilder<Context, T, FLinked, FSynced, FUnlinked, FEv, FSet>
{
    /// Specify a new event handler to be executed when the downlink enters the linked state.
    pub fn on_linked<F>(
        self,
        f: F,
    ) -> StatelessValueDownlinkBuilder<
        Context,
        T,
        WithHandlerContext<Context, F>,
        FSynced,
        FUnlinked,
        FEv,
        FSet,
    >
    where
        WithHandlerContext<Context, F>: for<'a> OnLinked<'a, Context>,
    {
        let StatelessValueDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatelessValueDownlinkBuilder {
            address,
            config,
            inner: inner.on_linked(f),
        }
    }

    /// Specify a new event handler to be executed when the downlink enters the synced state.
    pub fn on_synced<F>(
        self,
        f: F,
    ) -> StatelessValueDownlinkBuilder<
        Context,
        T,
        FLinked,
        WithHandlerContext<Context, F>,
        FUnlinked,
        FEv,
        FSet,
    >
    where
        WithHandlerContext<Context, F>: for<'a> OnSynced<'a, T, Context>,
    {
        let StatelessValueDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatelessValueDownlinkBuilder {
            address,
            config,
            inner: inner.on_synced(f),
        }
    }

    /// Specify a new event handler to be executed when the downlink enters the unlinked state.
    pub fn on_unlinked<F>(
        self,
        f: F,
    ) -> StatelessValueDownlinkBuilder<
        Context,
        T,
        FLinked,
        FSynced,
        WithHandlerContext<Context, F>,
        FEv,
        FSet,
    >
    where
        WithHandlerContext<Context, F>: for<'a> OnUnlinked<'a, Context>,
    {
        let StatelessValueDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatelessValueDownlinkBuilder {
            address,
            config,
            inner: inner.on_unlinked(f),
        }
    }

    /// Specify a new event handler to be executed when an event is received for the downlink.
    pub fn on_event<F>(
        self,
        f: F,
    ) -> StatelessValueDownlinkBuilder<
        Context,
        T,
        FLinked,
        FSynced,
        FUnlinked,
        WithHandlerContext<Context, F>,
        FSet,
    >
    where
        WithHandlerContext<Context, F>: for<'a> OnDownlinkEvent<'a, T, Context>,
    {
        let StatelessValueDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatelessValueDownlinkBuilder {
            address,
            config,
            inner: inner.on_event(f),
        }
    }

    /// Specify a new event handler to be executed when the value of the downlink changes.
    pub fn on_set<F>(
        self,
        f: F,
    ) -> StatelessValueDownlinkBuilder<
        Context,
        T,
        FLinked,
        FSynced,
        FUnlinked,
        FEv,
        WithHandlerContext<Context, F>,
    >
    where
        WithHandlerContext<Context, F>: for<'a> OnDownlinkSet<'a, T, Context>,
    {
        let StatelessValueDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatelessValueDownlinkBuilder {
            address,
            config,
            inner: inner.on_set(f),
        }
    }

    /// Add a state that can be shared between the event handlers for the downlink.
    ///
    /// #Arguments
    /// * `state` - The value of the state.
    pub fn with_state<State>(
        self,
        state: State,
    ) -> LiftedValueBuilder<Context, T, State, FLinked, FSynced, FUnlinked, FEv, FSet> {
        let StatelessValueDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatefulValueDownlinkBuilder {
            address,
            config,
            inner: inner.with_state(state),
        }
    }
}

impl<Context, T, FLinked, FSynced, FUnlinked, FEv, FSet>
    StatelessValueDownlinkBuilder<Context, T, FLinked, FSynced, FUnlinked, FEv, FSet>
where
    Context: 'static,
    T: Form + Send + Sync + 'static,
    T::Rec: Send,
    FLinked: for<'a> OnLinked<'a, Context> + 'static,
    FSynced: for<'a> OnSynced<'a, T, Context> + 'static,
    FUnlinked: for<'a> OnUnlinked<'a, Context> + 'static,
    FEv: for<'a> OnDownlinkEvent<'a, T, Context> + 'static,
    FSet: for<'a> OnDownlinkSet<'a, T, Context> + 'static,
{
    /// Complete the downlink and create a [`HandlerAction`] that will open the downlink when it is
    /// executed.
    pub fn done(
        self,
    ) -> impl HandlerAction<Context, Completion = ValueDownlinkHandle<T>> + Send + 'static {
        let StatelessValueDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        OpenValueDownlink::new(address, inner, config)
    }
}

impl<Context, T, State, FLinked, FSynced, FUnlinked, FEv, FSet>
    StatefulValueDownlinkBuilder<Context, T, State, FLinked, FSynced, FUnlinked, FEv, FSet>
{
    /// Specify a new event handler to be executed when the downlink enters the linked state.
    pub fn on_linked<F>(
        self,
        f: F,
    ) -> StatefulValueDownlinkBuilder<Context, T, State, FnHandler<F>, FSynced, FUnlinked, FEv, FSet>
    where
        FnHandler<F>: for<'a> OnLinkedShared<'a, Context, State>,
    {
        let StatefulValueDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatefulValueDownlinkBuilder {
            address,
            config,
            inner: inner.on_linked(f),
        }
    }

    /// Specify a new event handler to be executed when the downlink enters the synced state.
    pub fn on_synced<F>(
        self,
        f: F,
    ) -> StatefulValueDownlinkBuilder<Context, T, State, FLinked, FnHandler<F>, FUnlinked, FEv, FSet>
    where
        FnHandler<F>: for<'a> OnSyncedShared<'a, T, Context, State>,
    {
        let StatefulValueDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatefulValueDownlinkBuilder {
            address,
            config,
            inner: inner.on_synced(f),
        }
    }

    /// Specify a new event handler to be executed when the downlink enters the unlinked state.
    pub fn on_unlinked<F>(
        self,
        f: F,
    ) -> StatefulValueDownlinkBuilder<Context, T, State, FLinked, FSynced, FnHandler<F>, FEv, FSet>
    where
        FnHandler<F>: for<'a> OnUnlinkedShared<'a, Context, State>,
    {
        let StatefulValueDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatefulValueDownlinkBuilder {
            address,
            config,
            inner: inner.on_unlinked(f),
        }
    }

    /// Specify a new event handler to be executed when an event is received for the downlink.
    pub fn on_event<F>(
        self,
        f: F,
    ) -> StatefulValueDownlinkBuilder<
        Context,
        T,
        State,
        FLinked,
        FSynced,
        FUnlinked,
        FnHandler<F>,
        FSet,
    >
    where
        FnHandler<F>: for<'a> OnDownlinkEventShared<'a, T, Context, State>,
    {
        let StatefulValueDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatefulValueDownlinkBuilder {
            address,
            config,
            inner: inner.on_event(f),
        }
    }

    /// Specify a new event handler to be executed when the value of the downlink changes.
    pub fn on_set<F>(
        self,
        f: F,
    ) -> StatefulValueDownlinkBuilder<
        Context,
        T,
        State,
        FLinked,
        FSynced,
        FUnlinked,
        FEv,
        FnHandler<F>,
    >
    where
        FnHandler<F>: for<'a> OnDownlinkSetShared<'a, T, Context, State>,
    {
        let StatefulValueDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        StatefulValueDownlinkBuilder {
            address,
            config,
            inner: inner.on_set(f),
        }
    }
}

impl<Context, T, State, FLinked, FSynced, FUnlinked, FEv, FSet>
    StatefulValueDownlinkBuilder<Context, T, State, FLinked, FSynced, FUnlinked, FEv, FSet>
where
    Context: 'static,
    State: Send + 'static,
    T: Form + Send + Sync + 'static,
    T::Rec: Send,
    FLinked: for<'a> OnLinkedShared<'a, Context, State> + 'static,
    FSynced: for<'a> OnSyncedShared<'a, T, Context, State> + 'static,
    FUnlinked: for<'a> OnUnlinkedShared<'a, Context, State> + 'static,
    FEv: for<'a> OnDownlinkEventShared<'a, T, Context, State> + 'static,
    FSet: for<'a> OnDownlinkSetShared<'a, T, Context, State> + 'static,
{
    /// Complete the downlink and create a [`HandlerAction`] that will open the downlink when it is
    /// executed.
    pub fn done(
        self,
    ) -> impl HandlerAction<Context, Completion = ValueDownlinkHandle<T>> + Send + 'static {
        let StatefulValueDownlinkBuilder {
            address,
            config,
            inner,
        } = self;
        OpenValueDownlink::new(address, inner, config)
    }
}
