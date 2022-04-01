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

use std::collections::BTreeMap;
use futures::future::{ready, Ready};
use std::future::Future;
use swim_api::handlers::{BlockingHandler, FnMutHandler, NoHandler, WithShared};

/// Trait for event handlers to be called when a map is cleared.
pub trait OnCleared<'a, K, V>: Send {
    type OnClearedFut: Future<Output = ()> + Send + 'a;

    /// # Arguments
    /// * `map` - The entries of the map before the clear.
    fn on_cleared(&'a mut self, map: &'a BTreeMap<K, V>) -> Self::OnClearedFut;
}

/// Trait for event handlers, that share state with other handlers, called when
/// a map is cleared.
pub trait OnClearedShared<'a, K, V, Shared>: Send {
    type OnClearedFut: Future<Output = ()> + Send + 'a;

    /// # Arguments
    /// * `shared` - The shared state.
    /// * `map` - The entries of the map before the clear.
    fn on_cleared(
        &'a mut self,
        shared: &'a mut Shared,
        map: &'a BTreeMap<K, V>,
    ) -> Self::OnClearedFut;
}

impl<'a, K, V> OnCleared<'a, K, V> for NoHandler {
    type OnClearedFut = Ready<()>;

    fn on_cleared(&'a mut self, _map: &'a BTreeMap<K, V>) -> Self::OnClearedFut {
        ready(())
    }
}

impl<'a, K, V, F, Fut> OnCleared<'a, K, V> for FnMutHandler<F>
where
    K: 'static,
    V: 'static,
    F: FnMut(&'a BTreeMap<K, V>) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
{
    type OnClearedFut = Fut;

    fn on_cleared(&'a mut self, map: &'a BTreeMap<K, V>) -> Self::OnClearedFut {
        let FnMutHandler(f) = self;
        f(map)
    }
}

impl<'a, K, V, Shared> OnClearedShared<'a, K, V, Shared> for NoHandler {
    type OnClearedFut = Ready<()>;

    fn on_cleared(
        &'a mut self,
        _shared: &'a mut Shared,
        _map: &'a BTreeMap<K, V>,
    ) -> Self::OnClearedFut {
        ready(())
    }
}

impl<'a, K, V, Shared, F, Fut> OnClearedShared<'a, K, V, Shared> for FnMutHandler<F>
where
    K: 'static,
    V: 'static,
    Shared: 'static,
    F: FnMut(&'a mut Shared, &'a BTreeMap<K, V>) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
{
    type OnClearedFut = Fut;

    fn on_cleared(
        &'a mut self,
        shared: &'a mut Shared,
        map: &'a BTreeMap<K, V>,
    ) -> Self::OnClearedFut {
        let FnMutHandler(f) = self;
        f(shared, map)
    }
}

impl<'a, K, V, H, Shared> OnClearedShared<'a, K, V, Shared> for WithShared<H>
where
    H: OnCleared<'a, K, V>,
{
    type OnClearedFut = H::OnClearedFut;

    fn on_cleared(
        &'a mut self,
        _shared: &'a mut Shared,
        map: &'a BTreeMap<K, V>,
    ) -> Self::OnClearedFut {
        self.0.on_cleared(map)
    }
}

impl<'a, F, K, V> OnCleared<'a, K, V> for BlockingHandler<F>
where
    K: 'static,
    V: 'static,
    F: FnMut(&'a BTreeMap<K, V>) + Send,
{
    type OnClearedFut = Ready<()>;

    fn on_cleared(&'a mut self, map: &'a BTreeMap<K, V>) -> Self::OnClearedFut {
        let BlockingHandler(f) = self;
        f(map);
        ready(())
    }
}

impl<'a, K, V, F, Shared> OnClearedShared<'a, K, V, Shared> for BlockingHandler<F>
where
    Shared: 'static,
    K: 'static,
    V: 'static,
    F: FnMut(&'a mut Shared, &'a BTreeMap<K, V>) + Send,
{
    type OnClearedFut = Ready<()>;

    fn on_cleared(
        &'a mut self,
        shared: &'a mut Shared,
        map: &'a BTreeMap<K, V>,
    ) -> Self::OnClearedFut {
        let BlockingHandler(f) = self;
        f(shared, map);
        ready(())
    }
}

#[macro_export]
macro_rules! on_cleared_handler {
    ($k:ty, $v:ty, |$map:ident| $body:expr) => {{
        async fn handler($map: &'a BTreeMap<$k, $v>) {
            $body
        }
        handler
    }};
    ($k:ty, $v:ty, $s:ty, |$map:ident| $body:expr) => {{
        async fn handler($shared: &mut $s, $map: &'a BTreeMap<$k, $v>) {
            $body
        }
        handler
    }};
}
