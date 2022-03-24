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

/// Trait for event handlers to be called when a map entry is updated.
pub trait OnUpdated<'a, K, V>: Send {
    type OnUpdatedFut: Future<Output = ()> + Send + 'a;

    /// # Arguments
    /// * `key` - The key of the map entry.
    /// * `existing` - The existing value of the map entry, if it is defined.
    /// * `new_value` - The replacement value of the map entry.
    fn on_updated(
        &'a mut self,
        key: &'a K,
        existing: Option<&'a V>,
        new_value: &'a V,
    ) -> Self::OnUpdatedFut;
}

/// Trait for event handlers, that share state with other handlers, called when
/// a map entry is updated.
pub trait OnUpdatedShared<'a, K, V, Shared>: Send {
    type OnUpdatedFut: Future<Output = ()> + Send + 'a;

    /// # Arguments
    /// * `shared` - The shared state.
    /// * `key` - The key of the map entry.
    /// * `existing` - The existing value of the map entry, if it is defined.
    /// * `new_value` - The replacement value of the map entry.
    fn on_updated(
        &'a mut self,
        shared: &'a mut Shared,
        key: &'a K,
        existing: Option<&'a V>,
        new_value: &'a V,
    ) -> Self::OnUpdatedFut;
}

impl<'a, K, V> OnUpdated<'a, K, V> for NoHandler {
    type OnUpdatedFut = Ready<()>;

    fn on_updated(
        &'a mut self,
        _key: &'a K,
        _existing: Option<&'a V>,
        _new_value: &'a V,
    ) -> Self::OnUpdatedFut {
        ready(())
    }
}

impl<'a, K, V, F, Fut> OnUpdated<'a, K, V> for FnMutHandler<F>
where
    K: 'static,
    V: 'static,
    F: FnMut(&'a K, Option<&'a V>, &'a V) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
{
    type OnUpdatedFut = Fut;

    fn on_updated(
        &'a mut self,
        key: &'a K,
        existing: Option<&'a V>,
        new_value: &'a V,
    ) -> Self::OnUpdatedFut {
        let FnMutHandler(f) = self;
        f(key, existing, new_value)
    }
}

impl<'a, K, V, Shared> OnUpdatedShared<'a, K, V, Shared> for NoHandler {
    type OnUpdatedFut = Ready<()>;

    fn on_updated(
        &'a mut self,
        _shared: &'a mut Shared,
        _key: &'a K,
        _existing: Option<&'a V>,
        _new_value: &'a V,
    ) -> Self::OnUpdatedFut {
        ready(())
    }
}

impl<'a, K, V, Shared, F, Fut> OnUpdatedShared<'a, K, V, Shared> for FnMutHandler<F>
where
    K: 'static,
    V: 'static,
    Shared: 'static,
    F: FnMut(&'a mut Shared, &'a K, Option<&'a V>, &'a V) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
{
    type OnUpdatedFut = Fut;

    fn on_updated(
        &'a mut self,
        shared: &'a mut Shared,
        key: &'a K,
        existing: Option<&'a V>,
        new_value: &'a V,
    ) -> Self::OnUpdatedFut {
        let FnMutHandler(f) = self;
        f(shared, key, existing, new_value)
    }
}

impl<'a, K, V, H, Shared> OnUpdatedShared<'a, K, V, Shared> for WithShared<H>
where
    H: OnUpdated<'a, K, V>,
{
    type OnUpdatedFut = H::OnUpdatedFut;

    fn on_updated(
        &'a mut self,
        _shared: &'a mut Shared,
        key: &'a K,
        existing: Option<&'a V>,
        new_value: &'a V,
    ) -> Self::OnUpdatedFut {
        self.0.on_updated(key, existing, new_value)
    }
}

impl<'a, F, K, V> OnUpdated<'a, K, V> for BlockingHandler<F>
where
    K: 'static,
    V: 'static,
    F: FnMut(&'a K, Option<&'a V>, &'a V) + Send,
{
    type OnUpdatedFut = Ready<()>;

    fn on_updated(
        &'a mut self,
        key: &'a K,
        existing: Option<&'a V>,
        new_value: &'a V,
    ) -> Self::OnUpdatedFut {
        let BlockingHandler(f) = self;
        f(key, existing, new_value);
        ready(())
    }
}

impl<'a, K, V, F, Shared> OnUpdatedShared<'a, K, V, Shared> for BlockingHandler<F>
where
    Shared: 'static,
    K: 'static,
    V: 'static,
    F: FnMut(&'a mut Shared, &'a K, Option<&'a V>, &'a V) + Send,
{
    type OnUpdatedFut = Ready<()>;

    fn on_updated(
        &'a mut self,
        shared: &'a mut Shared,
        key: &'a K,
        existing: Option<&'a V>,
        new_value: &'a V,
    ) -> Self::OnUpdatedFut {
        let BlockingHandler(f) = self;
        f(shared, key, existing, new_value);
        ready(())
    }
}

#[macro_export]
macro_rules! on_updated_handler {
    ($k:ty, $v:ty, |$key:ident, $before:ident, $after:ident| $body:expr) => {{
        async fn handler($key: &$k, $before: core::option::Option<&$v>, $after: &$v) {
            $body
        }
        handler
    }};
    ($k:ty, $v:ty, $s:ty, |$shared:ident, $key:ident, $before:ident, $after:ident| $body:expr) => {{
        async fn handler(
            $shared: &mut $s,
            $key: &$k,
            $before: core::option::Option<&$v>,
            $after: &$v,
        ) {
            $body
        }
        handler
    }};
}
