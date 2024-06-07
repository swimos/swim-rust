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

use swimos_form::read::RecognizerReadable;
use swimos_model::{address::Address, Text};
use swimos_utilities::handlers::FnHandler;

use crate::{
    agent_model::downlink::{hosted::EventDownlinkHandle, OpenEventDownlinkAction},
    config::SimpleDownlinkConfig,
    downlink_lifecycle::{
        event::{
            on_event::{OnConsumeEvent, OnConsumeEventShared},
            StatefulEventDownlinkLifecycle, StatefulEventLifecycle,
            StatelessEventDownlinkLifecycle, StatelessEventLifecycle,
        },
        on_failed::{OnFailed, OnFailedShared},
        on_linked::{OnLinked, OnLinkedShared},
        on_synced::{OnSynced, OnSyncedShared},
        on_unlinked::{OnUnlinked, OnUnlinkedShared},
    },
    event_handler::HandlerAction,
    lifecycle_fn::WithHandlerContext,
};

/// A builder for constructing an event downlink. Each lifecycle event handler is independent and, by
/// default, they all do nothing.
pub struct StatelessEventDownlinkBuilder<
    Context,
    T,
    LC = StatelessEventDownlinkLifecycle<Context, T>,
> {
    _type: PhantomData<fn(Context, T) -> T>,
    address: Address<Text>,
    config: SimpleDownlinkConfig,
    inner: LC,
}

/// A builder for constructing an event downlink. The lifecycle event handlers share state and, by default,
/// they all do nothing.
pub struct StatefulEventDownlinkBuilder<
    Context,
    T,
    State,
    LC = StatefulEventDownlinkLifecycle<Context, State, T>,
> {
    _type: PhantomData<fn(Context, State, T) -> T>,
    address: Address<Text>,
    config: SimpleDownlinkConfig,
    inner: LC,
}

impl<Context, T> StatelessEventDownlinkBuilder<Context, T> {
    pub fn new(address: Address<Text>, config: SimpleDownlinkConfig) -> Self {
        StatelessEventDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: StatelessEventDownlinkLifecycle::default(),
        }
    }
}

impl<Context, T, State> StatefulEventDownlinkBuilder<Context, T, State> {
    pub fn new(address: Address<Text>, config: SimpleDownlinkConfig, state: State) -> Self {
        StatefulEventDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: StatefulEventDownlinkLifecycle::new(state),
        }
    }
}

impl<Context, T, LC> StatelessEventDownlinkBuilder<Context, T, LC>
where
    LC: StatelessEventLifecycle<Context, T>,
{
    pub fn on_linked<F>(
        self,
        handler: F,
    ) -> StatelessEventDownlinkBuilder<Context, T, LC::WithOnLinked<WithHandlerContext<F>>>
    where
        WithHandlerContext<F>: OnLinked<Context>,
    {
        let StatelessEventDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatelessEventDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_linked(handler),
        }
    }

    pub fn on_synced<F>(
        self,
        handler: F,
    ) -> StatelessEventDownlinkBuilder<Context, T, LC::WithOnSynced<WithHandlerContext<F>>>
    where
        WithHandlerContext<F>: OnSynced<(), Context>,
    {
        let StatelessEventDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatelessEventDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_synced(handler),
        }
    }

    pub fn on_unlinked<F>(
        self,
        handler: F,
    ) -> StatelessEventDownlinkBuilder<Context, T, LC::WithOnUnlinked<WithHandlerContext<F>>>
    where
        WithHandlerContext<F>: OnUnlinked<Context>,
    {
        let StatelessEventDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatelessEventDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_unlinked(handler),
        }
    }

    pub fn on_failed<F>(
        self,
        handler: F,
    ) -> StatelessEventDownlinkBuilder<Context, T, LC::WithOnFailed<WithHandlerContext<F>>>
    where
        WithHandlerContext<F>: OnFailed<Context>,
    {
        let StatelessEventDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatelessEventDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_failed(handler),
        }
    }

    pub fn on_event<F>(
        self,
        handler: F,
    ) -> StatelessEventDownlinkBuilder<Context, T, LC::WithOnEvent<WithHandlerContext<F>>>
    where
        WithHandlerContext<F>: OnConsumeEvent<T, Context>,
    {
        let StatelessEventDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatelessEventDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_event(handler),
        }
    }

    pub fn with_shared_state<Shared: Send>(
        self,
        shared: Shared,
    ) -> StatefulEventDownlinkBuilder<Context, T, Shared, LC::WithShared<Shared>> {
        let StatelessEventDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatefulEventDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.with_shared_state(shared),
        }
    }
}

impl<Context, T, LC> StatelessEventDownlinkBuilder<Context, T, LC>
where
    Context: 'static,
    LC: StatelessEventLifecycle<Context, T> + 'static,
    T: RecognizerReadable + Send + 'static,
    T::Rec: Send,
{
    /// Complete the downlink and create a [`HandlerAction`] that will open the downlink when it is
    /// executed.
    pub fn done(
        self,
    ) -> impl HandlerAction<Context, Completion = EventDownlinkHandle> + Send + 'static {
        let StatelessEventDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        OpenEventDownlinkAction::new(address, inner, config, false)
    }
}

impl<Context, T, State, LC> StatefulEventDownlinkBuilder<Context, T, State, LC>
where
    LC: StatefulEventLifecycle<Context, State, T>,
{
    pub fn on_linked<F>(
        self,
        handler: F,
    ) -> StatefulEventDownlinkBuilder<Context, T, State, LC::WithOnLinked<FnHandler<F>>>
    where
        FnHandler<F>: OnLinkedShared<Context, State>,
    {
        let StatefulEventDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatefulEventDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_linked(handler),
        }
    }

    pub fn on_synced<F>(
        self,
        handler: F,
    ) -> StatefulEventDownlinkBuilder<Context, T, State, LC::WithOnSynced<FnHandler<F>>>
    where
        FnHandler<F>: OnSyncedShared<(), Context, State>,
    {
        let StatefulEventDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatefulEventDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_synced(handler),
        }
    }

    pub fn on_unlinked<F>(
        self,
        handler: F,
    ) -> StatefulEventDownlinkBuilder<Context, T, State, LC::WithOnUnlinked<FnHandler<F>>>
    where
        FnHandler<F>: OnUnlinkedShared<Context, State>,
    {
        let StatefulEventDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatefulEventDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_unlinked(handler),
        }
    }

    pub fn on_failed<F>(
        self,
        handler: F,
    ) -> StatefulEventDownlinkBuilder<Context, T, State, LC::WithOnFailed<FnHandler<F>>>
    where
        FnHandler<F>: OnFailedShared<Context, State>,
    {
        let StatefulEventDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatefulEventDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_failed(handler),
        }
    }

    pub fn on_event<F>(
        self,
        handler: F,
    ) -> StatefulEventDownlinkBuilder<Context, T, State, LC::WithOnEvent<FnHandler<F>>>
    where
        FnHandler<F>: OnConsumeEventShared<T, Context, State>,
    {
        let StatefulEventDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        StatefulEventDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: inner.on_event(handler),
        }
    }
}

impl<Context, State, T, LC> StatefulEventDownlinkBuilder<Context, T, State, LC>
where
    Context: 'static,
    LC: StatefulEventLifecycle<Context, State, T> + 'static,
    T: RecognizerReadable + Send + 'static,
    T::Rec: Send,
{
    /// Complete the downlink and create a [`HandlerAction`] that will open the downlink when it is
    /// executed.
    pub fn done(
        self,
    ) -> impl HandlerAction<Context, Completion = EventDownlinkHandle> + Send + 'static {
        let StatefulEventDownlinkBuilder {
            address,
            config,
            inner,
            ..
        } = self;
        OpenEventDownlinkAction::new(address, inner, config, false)
    }
}
