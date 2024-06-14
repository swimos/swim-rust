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

use swimos_api::address::Address;
use swimos_form::Form;
use swimos_model::Text;
use swimos_utilities::handlers::{BorrowHandler, FnHandler};

use crate::{
    agent_model::downlink::{OpenValueDownlinkAction, ValueDownlinkHandle},
    config::SimpleDownlinkConfig,
    downlink_lifecycle::{
        OnDownlinkEvent, OnDownlinkEventShared, OnDownlinkSet, OnDownlinkSetShared, OnFailed,
        OnFailedShared, OnLinked, OnLinkedShared, OnSynced, OnSyncedShared, OnUnlinked,
        OnUnlinkedShared, StatefulValueDownlinkLifecycle, StatefulValueLifecycle,
        StatelessValueDownlinkLifecycle, StatelessValueLifecycle,
    },
    event_handler::HandlerAction,
    lifecycle_fn::{WithHandlerContext, WithHandlerContextBorrow},
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
    config: SimpleDownlinkConfig,
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
    config: SimpleDownlinkConfig,
    inner: LC,
}

impl<Context, T> StatelessValueDownlinkBuilder<Context, T> {
    pub fn new(address: Address<Text>, config: SimpleDownlinkConfig) -> Self {
        StatelessValueDownlinkBuilder {
            _type: PhantomData,
            address,
            config,
            inner: StatelessValueDownlinkLifecycle::default(),
        }
    }
}

impl<Context, T, State> StatefulValueDownlinkBuilder<Context, T, State> {
    pub fn new(address: Address<Text>, config: SimpleDownlinkConfig, state: State) -> Self {
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
    /// Specify a handler for the `on_linked` event.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
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

    /// Specify a handler for the `on_synced` event.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_synced<F, B>(
        self,
        handler: F,
    ) -> StatelessValueDownlinkBuilder<Context, T, LC::WithOnSynced<WithHandlerContextBorrow<F, B>>>
    where
        B: ?Sized,
        T: Borrow<B>,
        WithHandlerContextBorrow<F, B>: OnSynced<T, Context>,
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

    /// Specify a handler for the `on_unlinked` event.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
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

    /// Specify a handler for the `on_failed` event (called if the downlink terminates with an error).
    ///
    /// # Arguments
    /// * `handler` - The event handler.
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

    /// Specify a handler for the `on_event` event, called when the downlink receives a new value.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_event<F, B>(
        self,
        handler: F,
    ) -> StatelessValueDownlinkBuilder<Context, T, LC::WithOnEvent<WithHandlerContextBorrow<F, B>>>
    where
        B: ?Sized,
        T: Borrow<B>,
        WithHandlerContextBorrow<F, B>: OnDownlinkEvent<T, Context>,
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

    /// Specify a handler for the `on_set` event, called when the downlink receives a new value.
    /// This differs from `on_event` in that the previous value is also provided to the event handler.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_set<F, B>(
        self,
        handler: F,
    ) -> StatelessValueDownlinkBuilder<Context, T, LC::WithOnSet<WithHandlerContextBorrow<F, B>>>
    where
        B: ?Sized,
        T: Borrow<B>,
        WithHandlerContextBorrow<F, B>: OnDownlinkSet<T, Context>,
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
            inner: inner.on_set(handler),
        }
    }

    /// Augment the lifecycle with some state that is shared between the event handlers.
    ///
    /// # Arguments
    /// * `shared` - The shared state.
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
    /// Specify a handler for the `on_linked` event.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
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

    /// Specify a handler for the `on_synced` event.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_synced<F, B>(
        self,
        handler: F,
    ) -> StatefulValueDownlinkBuilder<Context, T, State, LC::WithOnSynced<BorrowHandler<F, B>>>
    where
        B: ?Sized,
        T: Borrow<B>,
        BorrowHandler<F, B>: OnSyncedShared<T, Context, State>,
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

    /// Specify a handler for the `on_unlinked` event.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
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

    /// Specify a handler for the `on_failed` event (called if the downlink terminates with an error).
    ///
    /// # Arguments
    /// * `handler` - The event handler.
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

    /// Specify a handler for the `on_event` event, called when the downlink receives a new value.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_event<F, B>(
        self,
        handler: F,
    ) -> StatefulValueDownlinkBuilder<Context, T, State, LC::WithOnEvent<BorrowHandler<F, B>>>
    where
        B: ?Sized,
        T: Borrow<B>,
        BorrowHandler<F, B>: OnDownlinkEventShared<T, Context, State>,
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

    /// Specify a handler for the `on_set` event, called when the downlink receives a new value.
    /// This differs from `on_event` in that the previous value is also provided to the event handler.
    ///
    /// # Arguments
    /// * `handler` - The event handler.
    pub fn on_set<F, B>(
        self,
        handler: F,
    ) -> StatefulValueDownlinkBuilder<Context, T, State, LC::WithOnSet<BorrowHandler<F, B>>>
    where
        B: ?Sized,
        T: Borrow<B>,
        BorrowHandler<F, B>: OnDownlinkSetShared<T, Context, State>,
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
            inner: inner.on_set(handler),
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
