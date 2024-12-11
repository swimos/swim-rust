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

use crate::model::lifecycle::{MapRemoveFn, SharedMapRemoveFn};
use futures::future::{ready, Ready};
use std::collections::BTreeMap;
use std::future::Future;
use swimos_utilities::handlers::{BlockingHandler, FnMutHandler, NoHandler, WithShared};

/// Trait for event handlers to be called when a map downlink removes an entry.
pub trait OnRemove<K, V>: Send {
    type OnRemoveFut<'a>: Future<Output = ()> + Send + 'a
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    fn on_remove<'a>(
        &'a mut self,
        key: K,
        map: &'a BTreeMap<K, V>,
        removed: V,
    ) -> Self::OnRemoveFut<'a>;
}

/// Trait for event handlers, that share state with other handlers, called when a map downlink
/// removes an entry.
pub trait OnRemoveShared<K, V, Shared>: Send {
    type OnRemoveFut<'a>: Future<Output = ()> + Send + 'a
    where
        Self: 'a,
        K: 'a,
        V: 'a,
        Shared: 'a;

    fn on_remove<'a>(
        &'a mut self,
        shared: &'a mut Shared,
        key: K,
        map: &'a BTreeMap<K, V>,
        removed: V,
    ) -> Self::OnRemoveFut<'a>;
}

impl<K, V> OnRemove<K, V> for NoHandler {
    type OnRemoveFut<'a>
        = Ready<()>
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    fn on_remove<'a>(
        &'a mut self,
        _key: K,
        _map: &'a BTreeMap<K, V>,
        _removed: V,
    ) -> Self::OnRemoveFut<'a> {
        ready(())
    }
}

impl<K, V, F> OnRemove<K, V> for FnMutHandler<F>
where
    F: for<'a> MapRemoveFn<'a, K, V> + Send,
{
    type OnRemoveFut<'a>
        = <F as MapRemoveFn<'a, K, V>>::Fut
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    fn on_remove<'a>(
        &'a mut self,
        key: K,
        map: &'a BTreeMap<K, V>,
        removed: V,
    ) -> Self::OnRemoveFut<'a> {
        let FnMutHandler(f) = self;
        f.apply(key, map, removed)
    }
}

impl<K, V, Shared> OnRemoveShared<K, V, Shared> for NoHandler {
    type OnRemoveFut<'a>
        = Ready<()>
    where
        Self: 'a,
        K: 'a,
        V: 'a,
        Shared: 'a;

    fn on_remove<'a>(
        &'a mut self,
        _shared: &'a mut Shared,
        _key: K,
        _map: &'a BTreeMap<K, V>,
        _removed: V,
    ) -> Self::OnRemoveFut<'a> {
        ready(())
    }
}

impl<K, V, Shared, F> OnRemoveShared<K, V, Shared> for FnMutHandler<F>
where
    F: for<'a> SharedMapRemoveFn<'a, Shared, K, V> + Send,
{
    type OnRemoveFut<'a>
        = <F as SharedMapRemoveFn<'a, Shared, K, V>>::Fut
    where
        Self: 'a,
        K: 'a,
        V: 'a,
        Shared: 'a;

    fn on_remove<'a>(
        &'a mut self,
        shared: &'a mut Shared,
        key: K,
        map: &'a BTreeMap<K, V>,
        removed: V,
    ) -> Self::OnRemoveFut<'a> {
        let FnMutHandler(f) = self;
        f.apply(shared, key, map, removed)
    }
}

impl<K, V, H, Shared> OnRemoveShared<K, V, Shared> for WithShared<H>
where
    H: OnRemoveShared<K, V, Shared>,
{
    type OnRemoveFut<'a>
        = H::OnRemoveFut<'a>
    where
        Self: 'a,
        K: 'a,
        V: 'a,
        Shared: 'a;

    fn on_remove<'a>(
        &'a mut self,
        shared: &'a mut Shared,
        key: K,
        map: &'a BTreeMap<K, V>,
        removed: V,
    ) -> Self::OnRemoveFut<'a> {
        self.0.on_remove(shared, key, map, removed)
    }
}

impl<F, K, V> OnRemove<K, V> for BlockingHandler<F>
where
    F: FnMut(K, &BTreeMap<K, V>, V) + Send,
{
    type OnRemoveFut<'a>
        = Ready<()>
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    fn on_remove<'a>(
        &'a mut self,
        key: K,
        map: &'a BTreeMap<K, V>,
        removed: V,
    ) -> Self::OnRemoveFut<'a> {
        let BlockingHandler(f) = self;
        f(key, map, removed);
        ready(())
    }
}

impl<F, K, V, Shared> OnRemoveShared<K, V, Shared> for BlockingHandler<F>
where
    F: FnMut(&mut Shared, K, &BTreeMap<K, V>, V) + Send,
{
    type OnRemoveFut<'a>
        = Ready<()>
    where
        Self: 'a,
        K: 'a,
        V: 'a,
        Shared: 'a;

    fn on_remove<'a>(
        &'a mut self,
        shared: &'a mut Shared,
        key: K,
        map: &'a BTreeMap<K, V>,
        removed: V,
    ) -> Self::OnRemoveFut<'a> {
        let BlockingHandler(f) = self;
        f(shared, key, map, removed);
        ready(())
    }
}
