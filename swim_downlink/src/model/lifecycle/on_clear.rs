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

use crate::model::lifecycle::{MapClearFn, SharedMapClearFn};
use futures::future::{ready, Ready};
use std::collections::BTreeMap;
use std::future::Future;
use swim_api::handlers::{BlockingHandler, FnMutHandler, NoHandler, WithShared};

/// Trait for event handlers to be called when a map downlink clears.
pub trait OnClear<K, V>: Send {
    type OnClearFut<'a>: Future<Output = ()> + Send + 'a
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    fn on_clear<'a>(&'a mut self, map: BTreeMap<K, V>) -> Self::OnClearFut<'a>
    where
        K: 'a,
        V: 'a;
}

/// Trait for event handlers, that share state with other handlers, called when a downlink
/// clears.
pub trait OnClearShared<K, V, Shared>: Send {
    type OnClearFut<'a>: Future<Output = ()> + Send + 'a
    where
        Self: 'a,
        K: 'a,
        V: 'a,
        Shared: 'a;

    fn on_clear<'a>(
        &'a mut self,
        shared: &'a mut Shared,
        map: BTreeMap<K, V>,
    ) -> Self::OnClearFut<'a>
    where
        K: 'a,
        V: 'a;
}

impl<K, V> OnClear<K, V> for NoHandler {
    type OnClearFut<'a> = Ready<()>
    where
        Self: 'a,
        K: 'a,
        V:'a;

    fn on_clear<'a>(&'a mut self, _map: BTreeMap<K, V>) -> Self::OnClearFut<'a>
    where
        K: 'a,
        V: 'a,
    {
        ready(())
    }
}

impl<K, V, F> OnClear<K, V> for FnMutHandler<F>
where
    F: for<'a> MapClearFn<'a, K, V> + Send,
{
    type OnClearFut<'a> = <F as MapClearFn<'a, K, V>>::Fut
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    fn on_clear<'a>(&'a mut self, map: BTreeMap<K, V>) -> Self::OnClearFut<'a>
    where
        K: 'a,
        V: 'a,
    {
        let FnMutHandler(f) = self;
        f.apply(map)
    }
}

impl<K, V, Shared> OnClearShared<K, V, Shared> for NoHandler {
    type OnClearFut<'a> = Ready<()>
    where
        Self: 'a,
        K: 'a,
        V:'a,
        Shared: 'a;

    fn on_clear<'a>(
        &'a mut self,
        _shared: &'a mut Shared,
        _map: BTreeMap<K, V>,
    ) -> Self::OnClearFut<'a>
    where
        K: 'a,
        V: 'a,
    {
        ready(())
    }
}

impl<K, V, Shared, F> OnClearShared<K, V, Shared> for FnMutHandler<F>
where
    F: for<'a> SharedMapClearFn<'a, Shared, K, V> + Send,
{
    type OnClearFut<'a> = <F as SharedMapClearFn<'a, Shared, K,V>>::Fut
    where
        Self: 'a,
        K: 'a,
        V: 'a,
        Shared: 'a;

    fn on_clear<'a>(
        &'a mut self,
        shared: &'a mut Shared,
        map: BTreeMap<K, V>,
    ) -> Self::OnClearFut<'a>
    where
        K: 'a,
        V: 'a,
    {
        let FnMutHandler(f) = self;
        f.apply(shared, map)
    }
}

impl<K, V, H, Shared> OnClearShared<K, V, Shared> for WithShared<H>
where
    H: OnClearShared<K, V, Shared>,
{
    type OnClearFut<'a> = H::OnClearFut<'a>
    where
        Self: 'a,
        K: 'a,
        V: 'a,
        Shared: 'a;

    fn on_clear<'a>(
        &'a mut self,
        shared: &'a mut Shared,
        map: BTreeMap<K, V>,
    ) -> Self::OnClearFut<'a>
    where
        K: 'a,
        V: 'a,
    {
        self.0.on_clear(shared, map)
    }
}

impl<F, K, V> OnClear<K, V> for BlockingHandler<F>
where
    F: FnMut(BTreeMap<K, V>) + Send,
{
    type OnClearFut<'a> = Ready<()>
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    fn on_clear<'a>(&'a mut self, map: BTreeMap<K, V>) -> Self::OnClearFut<'a>
    where
        K: 'a,
        V: 'a,
    {
        let BlockingHandler(f) = self;
        f(map);
        ready(())
    }
}

impl<F, K, V, Shared> OnClearShared<K, V, Shared> for BlockingHandler<F>
where
    F: FnMut(&mut Shared, BTreeMap<K, V>) + Send,
{
    type OnClearFut<'a> = Ready<()>
    where
        Self: 'a,
        K: 'a,
        V: 'a,
        Shared: 'a;

    fn on_clear<'a>(
        &'a mut self,
        shared: &'a mut Shared,
        map: BTreeMap<K, V>,
    ) -> Self::OnClearFut<'a>
    where
        K: 'a,
        V: 'a,
    {
        let BlockingHandler(f) = self;
        f(shared, map);
        ready(())
    }
}
