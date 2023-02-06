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

use futures::future::{ready, Ready};
use std::future::Future;
use swim_api::handlers::{BlockingHandler, FnMutHandler, NoHandler, WithShared};

use super::{SetFn, SharedSetFn};

/// Trait for event handlers to be called when a value changes.
pub trait OnSet<T>: Send {
    type OnSetFut<'a>: Future<Output = ()> + Send + 'a
    where
        Self: 'a,
        T: 'a;

    /// #Arguments
    /// * `existing` - The existing value, if it is defined.
    /// * `new_value` - The replacement value.
    fn on_set<'a>(&'a mut self, existing: Option<&'a T>, new_value: &'a T) -> Self::OnSetFut<'a>;
}

/// Trait for event handlers, that share state with other handlers, called when a downlink
/// receives a new event.
pub trait OnSetShared<T, Shared>: Send {
    type OnSetFut<'a>: Future<Output = ()> + Send + 'a
    where
        Self: 'a,
        T: 'a,
        Shared: 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `existing` - The existing value, if it is defined.
    /// * `new_value` - The replacement value.
    fn on_set<'a>(
        &'a mut self,
        shared: &'a mut Shared,
        existing: Option<&'a T>,
        new_value: &'a T,
    ) -> Self::OnSetFut<'a>;
}

impl<T> OnSet<T> for NoHandler {
    type OnSetFut<'a> = Ready<()>
    where
        Self: 'a,
        T: 'a;

    fn on_set<'a>(&'a mut self, _existing: Option<&'a T>, _new_value: &'a T) -> Self::OnSetFut<'a> {
        ready(())
    }
}

impl<T, F> OnSet<T> for FnMutHandler<F>
where
    F: for<'a> SetFn<'a, T> + Send,
{
    type OnSetFut<'a> = <F as SetFn<'a, T>>::Fut
    where
        Self: 'a,
        T: 'a;

    fn on_set<'a>(&'a mut self, existing: Option<&'a T>, new_value: &'a T) -> Self::OnSetFut<'a> {
        let FnMutHandler(f) = self;
        f.apply(existing, new_value)
    }
}

impl<T, Shared> OnSetShared<T, Shared> for NoHandler {
    type OnSetFut<'a> = Ready<()>
    where
        Self: 'a,
        Shared: 'a,
        T: 'a;

    fn on_set<'a>(
        &'a mut self,
        _shared: &'a mut Shared,
        _existing: Option<&'a T>,
        _new_value: &'a T,
    ) -> Self::OnSetFut<'a> {
        ready(())
    }
}

impl<T, Shared, F> OnSetShared<T, Shared> for FnMutHandler<F>
where
    F: for<'a> SharedSetFn<'a, Shared, T> + Send,
{
    type OnSetFut<'a> = <F as SharedSetFn<'a, Shared, T>>::Fut
    where
        Self: 'a,
        Shared: 'a,
        T: 'a;

    fn on_set<'a>(
        &'a mut self,
        shared: &'a mut Shared,
        existing: Option<&'a T>,
        new_value: &'a T,
    ) -> Self::OnSetFut<'a> {
        let FnMutHandler(f) = self;
        f.apply(shared, existing, new_value)
    }
}

impl<T, H, Shared> OnSetShared<T, Shared> for WithShared<H>
where
    H: OnSet<T>,
{
    type OnSetFut<'a> = H::OnSetFut<'a>
    where
        Self: 'a,
        Shared: 'a,
        T: 'a;

    fn on_set<'a>(
        &'a mut self,
        _shared: &'a mut Shared,
        existing: Option<&'a T>,
        new_value: &'a T,
    ) -> Self::OnSetFut<'a> {
        self.0.on_set(existing, new_value)
    }
}

impl<F, T> OnSet<T> for BlockingHandler<F>
where
    F: FnMut(Option<&T>, &T) + Send,
{
    type OnSetFut<'a> = Ready<()>
    where
        Self: 'a,
        T: 'a;

    fn on_set<'a>(&'a mut self, existing: Option<&'a T>, new_value: &'a T) -> Self::OnSetFut<'a> {
        let BlockingHandler(f) = self;
        f(existing, new_value);
        ready(())
    }
}

impl<T, F, Shared> OnSetShared<T, Shared> for BlockingHandler<F>
where
    F: FnMut(&mut Shared, Option<&T>, &T) + Send,
{
    type OnSetFut<'a> = Ready<()>
    where
        Self: 'a,
        Shared: 'a,
        T: 'a;

    fn on_set<'a>(
        &'a mut self,
        shared: &'a mut Shared,
        existing: Option<&'a T>,
        new_value: &'a T,
    ) -> Self::OnSetFut<'a> {
        let BlockingHandler(f) = self;
        f(shared, existing, new_value);
        ready(())
    }
}

#[macro_export]
macro_rules! on_set_handler {
    ($t:ty, |$before:ident, $after:ident| $body:expr) => {{
        async fn handler($before: core::option::Option<&$t>, $after: &$t) {
            $body
        }
        handler
    }};
    ($t:ty, $s:ty, |$shared:ident, $before:ident, $after:ident| $body:expr) => {{
        async fn handler($shared: &mut $s, $before: core::option::Option<&$t>, $after: &$t) {
            $body
        }
        handler
    }};
}
