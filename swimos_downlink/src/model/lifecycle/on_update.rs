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

use crate::model::lifecycle::{MapUpdateFn, SharedMapUpdateFn};
use futures::future::{ready, Ready};
use std::collections::BTreeMap;
use std::future::Future;
use swimos_api::handlers::{BlockingHandler, FnMutHandler, NoHandler, WithShared};

/// Trait for event handlers to be called when a downlink updates a value.
pub trait OnUpdate<K, V>: Send {
    type OnUpdateFut<'a>: Future<Output = ()> + Send + 'a
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    fn on_update<'a>(
        &'a mut self,
        key: K,
        map: &'a BTreeMap<K, V>,
        previous: Option<V>,
        new_value: &'a V,
    ) -> Self::OnUpdateFut<'a>;
}

/// Trait for event handlers, that share state with other handlers, called when a downlink
/// updates a value.
pub trait OnUpdateShared<K, V, Shared>: Send {
    type OnUpdateFut<'a>: Future<Output = ()> + Send + 'a
    where
        Self: 'a,
        K: 'a,
        V: 'a,
        Shared: 'a;

    fn on_update<'a>(
        &'a mut self,
        shared: &'a mut Shared,
        key: K,
        map: &'a BTreeMap<K, V>,
        previous: Option<V>,
        new_value: &'a V,
    ) -> Self::OnUpdateFut<'a>;
}

impl<K, V> OnUpdate<K, V> for NoHandler {
    type OnUpdateFut<'a> = Ready<()>
    where
        Self: 'a,
        K: 'a,
        V:'a;

    fn on_update<'a>(
        &'a mut self,
        _key: K,
        _map: &'a BTreeMap<K, V>,
        _previous: Option<V>,
        _new_value: &'a V,
    ) -> Self::OnUpdateFut<'a> {
        ready(())
    }
}

impl<K, V, F> OnUpdate<K, V> for FnMutHandler<F>
where
    F: for<'a> MapUpdateFn<'a, K, V> + Send,
{
    type OnUpdateFut<'a> = <F as MapUpdateFn<'a, K, V>>::Fut
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    fn on_update<'a>(
        &'a mut self,
        key: K,
        map: &'a BTreeMap<K, V>,
        previous: Option<V>,
        new_value: &'a V,
    ) -> Self::OnUpdateFut<'a> {
        let FnMutHandler(f) = self;
        f.apply(key, map, previous, new_value)
    }
}

impl<K, V, Shared> OnUpdateShared<K, V, Shared> for NoHandler {
    type OnUpdateFut<'a> = Ready<()>
    where
        Self: 'a,
        K: 'a,
        V:'a,
        Shared: 'a;

    fn on_update<'a>(
        &'a mut self,
        _shared: &'a mut Shared,
        _key: K,
        _map: &'a BTreeMap<K, V>,
        _previous: Option<V>,
        _new_value: &'a V,
    ) -> Self::OnUpdateFut<'a> {
        ready(())
    }
}

impl<K, V, Shared, F> OnUpdateShared<K, V, Shared> for FnMutHandler<F>
where
    F: for<'a> SharedMapUpdateFn<'a, Shared, K, V> + Send,
{
    type OnUpdateFut<'a> = <F as SharedMapUpdateFn<'a, Shared, K,V>>::Fut
    where
        Self: 'a,
        K: 'a,
        V: 'a,
        Shared: 'a;

    fn on_update<'a>(
        &'a mut self,
        shared: &'a mut Shared,
        key: K,
        map: &'a BTreeMap<K, V>,
        previous: Option<V>,
        new_value: &'a V,
    ) -> Self::OnUpdateFut<'a> {
        let FnMutHandler(f) = self;
        f.apply(shared, key, map, previous, new_value)
    }
}

impl<K, V, H, Shared> OnUpdateShared<K, V, Shared> for WithShared<H>
where
    H: OnUpdateShared<K, V, Shared>,
{
    type OnUpdateFut<'a> = H::OnUpdateFut<'a>
    where
        Self: 'a,
        K: 'a,
        V: 'a,
        Shared: 'a;

    fn on_update<'a>(
        &'a mut self,
        shared: &'a mut Shared,
        key: K,
        map: &'a BTreeMap<K, V>,
        previous: Option<V>,
        new_value: &'a V,
    ) -> Self::OnUpdateFut<'a> {
        self.0.on_update(shared, key, map, previous, new_value)
    }
}

impl<F, K, V> OnUpdate<K, V> for BlockingHandler<F>
where
    F: FnMut(K, &BTreeMap<K, V>, Option<V>, &V) + Send,
{
    type OnUpdateFut<'a> = Ready<()>
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    fn on_update<'a>(
        &'a mut self,
        key: K,
        map: &'a BTreeMap<K, V>,
        previous: Option<V>,
        new_value: &'a V,
    ) -> Self::OnUpdateFut<'a> {
        let BlockingHandler(f) = self;
        f(key, map, previous, new_value);
        ready(())
    }
}

impl<F, K, V, Shared> OnUpdateShared<K, V, Shared> for BlockingHandler<F>
where
    F: FnMut(&mut Shared, K, &BTreeMap<K, V>, Option<V>, &V) + Send,
{
    type OnUpdateFut<'a> = Ready<()>
    where
        Self: 'a,
        K: 'a,
        V: 'a,
        Shared: 'a;

    fn on_update<'a>(
        &'a mut self,
        shared: &'a mut Shared,
        key: K,
        map: &'a BTreeMap<K, V>,
        previous: Option<V>,
        new_value: &'a V,
    ) -> Self::OnUpdateFut<'a> {
        let BlockingHandler(f) = self;
        f(shared, key, map, previous, new_value);
        ready(())
    }
}
