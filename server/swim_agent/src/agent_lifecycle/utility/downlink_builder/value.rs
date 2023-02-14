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

use swim_api::handlers::FnHandler;
use swim_form::Form;
use swim_model::{address::Address, Text};

use crate::{
    agent_model::downlink::{hosted::ValueDownlinkHandle, OpenValueDownlinkAction},
    config::ValueDownlinkConfig,
    downlink_lifecycle::{
        on_failed::{OnFailed, OnFailedShared},
        on_linked::{OnLinked, OnLinkedShared},
        on_synced::{OnSynced, OnSyncedShared},
        on_unlinked::{OnUnlinked, OnUnlinkedShared},
        value::{
            on_event::{OnDownlinkEvent, OnDownlinkEventShared},
            StatefulValueDownlinkLifecycle, StatefulValueLifecycle,
            StatelessValueDownlinkLifecycle, StatelessValueLifecycle,
        },
        WithHandlerContext,
    },
    event_handler::HandlerAction,
};

/// A builder for constructing a value downlink. Each lifecycle event handler is independent and, by
/// default, they all do nothing.
pub struct StatelessValueDownlinkBuilder<
    Context,
    T,
    LC = StatelessValueDownlinkLifecycle<Context, T>,
> {
    _type: PhantomData<fn(Context, T) -> T>,
    address: Address<Text>,
    config: ValueDownlinkConfig,
    inner: LC,
}

/// A builder for constructing a value downlink. The lifecycle event handlers share state and, by default,
/// they all do nothing.
pub struct StatefulValueDownlinkBuilder<
    Context,
    T,
    State,
    LC = StatefulValueDownlinkLifecycle<Context, State, T>,
> {
    _type: PhantomData<fn(Context, State, T) -> T>,
    address: Address<Text>,
    config: ValueDownlinkConfig,
    inner: LC,
}

impl<Context, T> StatelessValueDownlinkBuilder<Context, T> {
    pub fn new(address: Address<Text>, config: ValueDownlinkConfig) -> Self {
        StatelessValueDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: StatelessValueDownlinkLifecycle::default(),
        }
    }
}

impl<Context, T, State> StatefulValueDownlinkBuilder<Context, T, State> {
    pub fn new(address: Address<Text>, config: ValueDownlinkConfig, state: State) -> Self {
        StatefulValueDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: StatefulValueDownlinkLifecycle::new(state),
        }
    }
}

impl<Context, T, LC> StatelessValueDownlinkBuilder<Context, T, LC>
where
    LC: StatelessValueLifecycle<Context, T>,
{
    pub fn on_linked<F>(
        self,
        handler: F,
    ) -> StatelessValueDownlinkBuilder<Context, T, LC::WithOnLinked<WithHandlerContext<F>>>
    where
        WithHandlerContext<F>: OnLinked<Context>,
    {
        let StatelessValueDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatelessValueDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_linked(handler),
        }
    }

    pub fn on_synced<F>(
        self,
        handler: F,
    ) -> StatelessValueDownlinkBuilder<Context, T, LC::WithOnSynced<WithHandlerContext<F>>>
    where
        WithHandlerContext<F>: OnSynced<(), Context>,
    {
        let StatelessValueDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatelessValueDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_synced(handler),
        }
    }

    pub fn on_unlinked<F>(
        self,
        handler: F,
    ) -> StatelessValueDownlinkBuilder<Context, T, LC::WithOnUnlinked<WithHandlerContext<F>>>
    where
        WithHandlerContext<F>: OnUnlinked<Context>,
    {
        let StatelessValueDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatelessValueDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_unlinked(handler),
        }
    }

    pub fn on_failed<F>(
        self,
        handler: F,
    ) -> StatelessValueDownlinkBuilder<Context, T, LC::WithOnFailed<WithHandlerContext<F>>>
    where
        WithHandlerContext<F>: OnFailed<Context>,
    {
        let StatelessValueDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatelessValueDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_failed(handler),
        }
    }

    pub fn on_event<F>(
        self,
        handler: F,
    ) -> StatelessValueDownlinkBuilder<Context, T, LC::WithOnEvent<WithHandlerContext<F>>>
    where
        WithHandlerContext<F>: OnDownlinkEvent<T, Context>,
    {
        let StatelessValueDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatelessValueDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_event(handler),
        }
    }

    pub fn with_shared_state<Shared: Send>(
        self,
        shared: Shared,
    ) -> StatefulValueDownlinkBuilder<Context, T, Shared, LC::WithShared<Shared>> {
        let StatelessValueDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatefulValueDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.with_shared_state(shared),
        }
    }
}

impl<Context, T, LC> StatelessValueDownlinkBuilder<Context, T, LC>
where
    Context: 'static,
    LC: StatelessValueLifecycle<Context, T> + 'static,
    T: Form + Send + 'static,
    T::Rec: Send,
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
            ..
        } = self;
        OpenValueDownlinkAction::new(address, inner, config)
    }
}

impl<Context, T, State, LC> StatefulValueDownlinkBuilder<Context, T, State, LC>
where
    LC: StatefulValueLifecycle<Context, State, T>,
{
    pub fn on_linked<F>(
        self,
        handler: F,
    ) -> StatefulValueDownlinkBuilder<Context, T, State, LC::WithOnLinked<FnHandler<F>>>
    where
        FnHandler<F>: OnLinkedShared<Context, State>,
    {
        let StatefulValueDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatefulValueDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_linked(handler),
        }
    }

    pub fn on_synced<F>(
        self,
        handler: F,
    ) -> StatefulValueDownlinkBuilder<Context, T, State, LC::WithOnSynced<FnHandler<F>>>
    where
        FnHandler<F>: OnSyncedShared<(), Context, State>,
    {
        let StatefulValueDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatefulValueDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_synced(handler),
        }
    }

    pub fn on_unlinked<F>(
        self,
        handler: F,
    ) -> StatefulValueDownlinkBuilder<Context, T, State, LC::WithOnUnlinked<FnHandler<F>>>
    where
        FnHandler<F>: OnUnlinkedShared<Context, State>,
    {
        let StatefulValueDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatefulValueDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_unlinked(handler),
        }
    }

    pub fn on_failed<F>(
        self,
        handler: F,
    ) -> StatefulValueDownlinkBuilder<Context, T, State, LC::WithOnFailed<FnHandler<F>>>
    where
        FnHandler<F>: OnFailedShared<Context, State>,
    {
        let StatefulValueDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatefulValueDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_failed(handler),
        }
    }

    pub fn on_event<F>(
        self,
        handler: F,
    ) -> StatefulValueDownlinkBuilder<Context, T, State, LC::WithOnEvent<FnHandler<F>>>
    where
        FnHandler<F>: OnDownlinkEventShared<T, Context, State>,
    {
        let StatefulValueDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatefulValueDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_event(handler),
        }
    }
}

impl<Context, State, T, LC> StatefulValueDownlinkBuilder<Context, T, State, LC>
where
    Context: 'static,
    LC: StatefulValueLifecycle<Context, State, T> + 'static,
    T: Form + Send + 'static,
    T::Rec: Send,
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
            ..
        } = self;
        OpenValueDownlinkAction::new(address, inner, config)
    }
}
