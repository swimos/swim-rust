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

/// Trait for event handlers to be called when a map entry is removed.
pub trait OnRemoved<'a, K, V>: Send {
    type OnRemovedFut: Future<Output = ()> + Send + 'a;

    /// # Arguments
    /// * `key` - The key of the map entry.
    /// * `existing` - The existing value of the map entry, if it is defined.
    fn on_removed(&'a mut self, key: &'a K, existing: Option<&'a V>) -> Self::OnRemovedFut;
}

/// Trait for event handlers, that share state with other handlers, called when
/// a map entry is removed.
pub trait OnRemovedShared<'a, K, V, Shared>: Send {
    type OnRemovedFut: Future<Output = ()> + Send + 'a;

    /// # Arguments
    /// * `shared` - The shared state.
    /// * `key` - The key of the map entry.
    /// * `existing` - The existing value of the map entry, if it is defined.
    fn on_removed(
        &'a mut self,
        shared: &'a mut Shared,
        key: &'a K,
        existing: Option<&'a V>,
    ) -> Self::OnRemovedFut;
}

impl<'a, K, V> OnRemoved<'a, K, V> for NoHandler {
    type OnRemovedFut = Ready<()>;

    fn on_removed(&'a mut self, _key: &'a K, _existing: Option<&'a V>) -> Self::OnRemovedFut {
        ready(())
    }
}

impl<'a, K, V, F, Fut> OnRemoved<'a, K, V> for FnMutHandler<F>
where
    K: 'static,
    V: 'static,
    F: FnMut(&'a K, Option<&'a V>) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
{
    type OnRemovedFut = Fut;

    fn on_removed(&'a mut self, key: &'a K, existing: Option<&'a V>) -> Self::OnRemovedFut {
        let FnMutHandler(f) = self;
        f(key, existing)
    }
}

impl<'a, K, V, Shared> OnRemovedShared<'a, K, V, Shared> for NoHandler {
    type OnRemovedFut = Ready<()>;

    fn on_removed(
        &'a mut self,
        _shared: &'a mut Shared,
        _key: &'a K,
        _existing: Option<&'a V>,
    ) -> Self::OnRemovedFut {
        ready(())
    }
}

impl<'a, K, V, Shared, F, Fut> OnRemovedShared<'a, K, V, Shared> for FnMutHandler<F>
where
    K: 'static,
    V: 'static,
    Shared: 'static,
    F: FnMut(&'a mut Shared, &'a K, Option<&'a V>) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
{
    type OnRemovedFut = Fut;

    fn on_removed(
        &'a mut self,
        shared: &'a mut Shared,
        key: &'a K,
        existing: Option<&'a V>,
    ) -> Self::OnRemovedFut {
        let FnMutHandler(f) = self;
        f(shared, key, existing)
    }
}

impl<'a, K, V, H, Shared> OnRemovedShared<'a, K, V, Shared> for WithShared<H>
where
    H: OnRemoved<'a, K, V>,
{
    type OnRemovedFut = H::OnRemovedFut;

    fn on_removed(
        &'a mut self,
        _shared: &'a mut Shared,
        key: &'a K,
        existing: Option<&'a V>,
    ) -> Self::OnRemovedFut {
        self.0.on_removed(key, existing)
    }
}

impl<'a, F, K, V> OnRemoved<'a, K, V> for BlockingHandler<F>
where
    K: 'static,
    V: 'static,
    F: FnMut(&'a K, Option<&'a V>) + Send,
{
    type OnRemovedFut = Ready<()>;

    fn on_removed(&'a mut self, key: &'a K, existing: Option<&'a V>) -> Self::OnRemovedFut {
        let BlockingHandler(f) = self;
        f(key, existing);
        ready(())
    }
}

impl<'a, K, V, F, Shared> OnRemovedShared<'a, K, V, Shared> for BlockingHandler<F>
where
    Shared: 'static,
    K: 'static,
    V: 'static,
    F: FnMut(&'a mut Shared, &'a K, Option<&'a V>) + Send,
{
    type OnRemovedFut = Ready<()>;

    fn on_removed(
        &'a mut self,
        shared: &'a mut Shared,
        key: &'a K,
        existing: Option<&'a V>,
    ) -> Self::OnRemovedFut {
        let BlockingHandler(f) = self;
        f(shared, key, existing);
        ready(())
    }
}

#[macro_export]
macro_rules! on_removed_handler {
    ($k:ty, $v:ty, |$key:ident, $before:ident| $body:expr) => {{
        async fn handler($key: &$k, $before: core::option::Option<&$v>) {
            $body
        }
        handler
    }};
    ($k:ty, $v:ty, $s:ty, |$shared:ident, $key:ident, $before:ident| $body:expr) => {{
        async fn handler($shared: &mut $s, $key: &$k, $before: core::option::Option<&$v>) {
            $body
        }
        handler
    }};
}
