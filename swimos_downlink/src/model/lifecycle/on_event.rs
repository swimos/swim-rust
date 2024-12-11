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

use futures::future::{ready, Ready};
use std::future::Future;
use swimos_utilities::handlers::{BlockingHandler, FnMutHandler, NoHandler, WithShared};

use super::{EventFn, SharedEventFn};

/// Trait for event handlers to be called when a downlink receives a new event.
pub trait OnEvent<T>: Send {
    type OnEventFut<'a>: Future<Output = ()> + Send + 'a
    where
        Self: 'a,
        T: 'a;

    fn on_event<'a>(&'a mut self, value: &'a T) -> Self::OnEventFut<'a>;
}

/// Trait for event handlers, that share state with other handlers, called when a downlink
/// receives a new event.
pub trait OnEventShared<T, Shared>: Send {
    type OnEventFut<'a>: Future<Output = ()> + Send + 'a
    where
        Self: 'a,
        T: 'a,
        Shared: 'a;

    fn on_event<'a>(&'a mut self, shared: &'a mut Shared, value: &'a T) -> Self::OnEventFut<'a>;
}

impl<T> OnEvent<T> for NoHandler {
    type OnEventFut<'a>
        = Ready<()>
    where
        T: 'a,
        Self: 'a;

    fn on_event<'a>(&'a mut self, _value: &'a T) -> Self::OnEventFut<'a> {
        ready(())
    }
}

impl<T, F> OnEvent<T> for FnMutHandler<F>
where
    T: 'static,
    F: for<'a> EventFn<'a, T> + Send,
{
    type OnEventFut<'a>
        = <F as EventFn<'a, T>>::Fut
    where
        Self: 'a;

    fn on_event<'a>(&'a mut self, value: &'a T) -> Self::OnEventFut<'a> {
        let FnMutHandler(f) = self;
        f.apply(value)
    }
}

impl<T, Shared> OnEventShared<T, Shared> for NoHandler {
    type OnEventFut<'a>
        = Ready<()>
    where
        Self: 'a,
        Shared: 'a,
        T: 'a;

    fn on_event<'a>(&'a mut self, _shared: &'a mut Shared, _value: &'a T) -> Self::OnEventFut<'a> {
        ready(())
    }
}

impl<T, Shared, F> OnEventShared<T, Shared> for FnMutHandler<F>
where
    T: 'static,
    F: for<'a> SharedEventFn<'a, Shared, T> + Send,
{
    type OnEventFut<'a>
        = <F as SharedEventFn<'a, Shared, T>>::Fut
    where
        Self: 'a,
        Shared: 'a;

    fn on_event<'a>(&'a mut self, shared: &'a mut Shared, value: &'a T) -> Self::OnEventFut<'a> {
        let FnMutHandler(f) = self;
        f.apply(shared, value)
    }
}

impl<T, H, Shared> OnEventShared<T, Shared> for WithShared<H>
where
    H: OnEvent<T>,
{
    type OnEventFut<'a>
        = H::OnEventFut<'a>
    where
        Self: 'a,
        Shared: 'a,
        T: 'a;

    fn on_event<'a>(&'a mut self, _shared: &'a mut Shared, value: &'a T) -> Self::OnEventFut<'a> {
        self.0.on_event(value)
    }
}

impl<F, T> OnEvent<T> for BlockingHandler<F>
where
    T: 'static,
    F: for<'a> FnMut(&'a T) + Send,
{
    type OnEventFut<'a>
        = Ready<()>
    where
        Self: 'a;

    fn on_event<'a>(&'a mut self, value: &'a T) -> Self::OnEventFut<'a> {
        let BlockingHandler(f) = self;
        f(value);
        ready(())
    }
}

impl<F, T, Shared> OnEventShared<T, Shared> for BlockingHandler<F>
where
    T: 'static,
    F: for<'a> FnMut(&'a mut Shared, &'a T) + Send,
{
    type OnEventFut<'a>
        = Ready<()>
    where
        Self: 'a,
        Shared: 'a;

    fn on_event<'a>(&'a mut self, shared: &'a mut Shared, value: &'a T) -> Self::OnEventFut<'a> {
        let BlockingHandler(f) = self;
        f(shared, value);
        ready(())
    }
}

#[macro_export]
macro_rules! on_event_handler {
    ($t:ty, |$param:ident| $body:expr) => {{
        async fn handler($param: &$t) {
            $body
        }
        handler
    }};
    ($t:ty, $s:ty, |$shared:ident, $param:ident| $body:expr) => {{
        async fn handler($shared: &mut $s, $param: &$t) {
            $body
        }
        handler
    }};
}
