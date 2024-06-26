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

use swimos_utilities::handlers::{FnHandler, NoHandler};

use crate::{
    agent_lifecycle::HandlerContext,
    event_handler::{EventConsumeFn, EventHandler, UnitHandler},
    lifecycle_fn::{LiftShared, WithHandlerContext},
};

/// Lifecycle event for the `on_event` event of an event downlink, from an agent.
pub trait OnConsumeEvent<T, Context>: Send {
    type OnEventHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    /// # Arguments
    /// * `value` - The event value.
    fn on_event(&self, value: T) -> Self::OnEventHandler<'_>;
}

/// Lifecycle event for the `on_event` event of an event downlink, from an agent,where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnConsumeEventShared<T, Context, Shared>: Send {
    type OnEventHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// # Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `value` - The event value.
    fn on_event<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: T,
    ) -> Self::OnEventHandler<'a>;
}

impl<T, Context> OnConsumeEvent<T, Context> for NoHandler {
    type OnEventHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn on_event(&self, _value: T) -> Self::OnEventHandler<'_> {
        UnitHandler::default()
    }
}

impl<T, Context, Shared> OnConsumeEventShared<T, Context, Shared> for NoHandler {
    type OnEventHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_event<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _value: T,
    ) -> Self::OnEventHandler<'a> {
        UnitHandler::default()
    }
}

impl<T, Context, F, H> OnConsumeEvent<T, Context> for FnHandler<F>
where
    F: Fn(T) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnEventHandler<'a> = H where
        Self: 'a;

    fn on_event(&self, value: T) -> Self::OnEventHandler<'_> {
        let FnHandler(f) = self;
        f(value)
    }
}

impl<T, Context, Shared, F> OnConsumeEventShared<T, Context, Shared> for FnHandler<F>
where
    F: for<'a> EventConsumeFn<'a, Context, Shared, T> + Send,
{
    type OnEventHandler<'a> = <F as EventConsumeFn<'a, Context, Shared, T>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_event<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: T,
    ) -> Self::OnEventHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, value)
    }
}

impl<Context, T, F, H> OnConsumeEvent<T, Context> for WithHandlerContext<F>
where
    F: Fn(HandlerContext<Context>, T) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnEventHandler<'a> = H
    where
        Self: 'a;

    fn on_event(&self, value: T) -> Self::OnEventHandler<'_> {
        let WithHandlerContext { inner } = self;
        inner(Default::default(), value)
    }
}

impl<T, Context, Shared, F> OnConsumeEventShared<T, Context, Shared> for LiftShared<F, Shared>
where
    F: OnConsumeEvent<T, Context> + Send,
{
    type OnEventHandler<'a> = F::OnEventHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_event<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        value: T,
    ) -> Self::OnEventHandler<'a> {
        let LiftShared { inner, .. } = self;
        inner.on_event(value)
    }
}
