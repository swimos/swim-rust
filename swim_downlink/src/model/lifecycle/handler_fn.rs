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

use std::collections::BTreeMap;

use futures::Future;

pub trait SharedHandlerFn0<'a, Shared> {
    type Fut: Future<Output=()> + Send + 'a;

    fn apply(&'a mut self, shared: &'a mut Shared) -> Self::Fut;
}

impl<'a, Shared, F, Fut> SharedHandlerFn0<'a, Shared> for F
    where
        Shared: 'a,
        F: FnMut(&'a mut Shared) -> Fut,
        Fut: Future<Output=()> + Send + 'a,
{
    type Fut = Fut;

    fn apply(&'a mut self, shared: &'a mut Shared) -> Self::Fut {
        self(shared)
    }
}

pub trait EventFn<'a, T: ?Sized> {
    type Fut: Future<Output=()> + Send + 'a;

    fn apply(&'a mut self, value: &'a T) -> Self::Fut;
}

impl<'a, T, F, Fut> EventFn<'a, T> for F
    where
        T: ?Sized + 'static,
        F: FnMut(&'a T) -> Fut,
        Fut: Future<Output=()> + Send + 'a,
{
    type Fut = Fut;

    fn apply(&'a mut self, value: &'a T) -> Self::Fut {
        self(value)
    }
}

pub trait SharedEventFn<'a, Shared, T: ?Sized> {
    type Fut: Future<Output=()> + Send + 'a;

    fn apply(&'a mut self, shared: &'a mut Shared, value: &'a T) -> Self::Fut;
}

impl<'a, Shared, T, F, Fut> SharedEventFn<'a, Shared, T> for F
    where
        T: ?Sized + 'static,
        Shared: 'a,
        F: FnMut(&'a mut Shared, &'a T) -> Fut,
        Fut: Future<Output=()> + Send + 'a,
{
    type Fut = Fut;

    fn apply(&'a mut self, shared: &'a mut Shared, value: &'a T) -> Self::Fut {
        self(shared, value)
    }
}

pub trait MapUpdateFn<'a, K, V> {
    type Fut: Future<Output=()> + Send + 'a;

    fn apply(
        &'a mut self,
        key: K,
        map: &'a BTreeMap<K, V>,
        previous: Option<V>,
        new_value: &'a V,
    ) -> Self::Fut
        where
            K: 'a,
            V: 'a;
}

impl<'a, K, V, F, Fut> MapUpdateFn<'a, K, V> for F
    where
        K: 'static,
        V: 'static,
        F: FnMut(K, &'a BTreeMap<K, V>, Option<V>, &'a V) -> Fut,
        Fut: Future<Output=()> + Send + 'a,
{
    type Fut = Fut;

    fn apply(
        &'a mut self,
        key: K,
        map: &'a BTreeMap<K, V>,
        previous: Option<V>,
        new_value: &'a V,
    ) -> Self::Fut
        where
            K: 'a,
            V: 'a,
    {
        self(key, map, previous, new_value)
    }
}

pub trait SharedMapUpdateFn<'a, Shared, K, V> {
    type Fut: Future<Output=()> + Send + 'a;

    fn apply(
        &'a mut self,
        shared: &'a mut Shared,
        key: K,
        map: &'a BTreeMap<K, V>,
        previous: Option<V>,
        new_value: &'a V,
    ) -> Self::Fut
        where
            K: 'a,
            V: 'a;
}

impl<'a, Shared, K, V, F, Fut> SharedMapUpdateFn<'a, Shared, K, V> for F
    where
        K: 'static,
        V: 'static,
        Shared: 'a,
        F: FnMut(&'a mut Shared, K, &'a BTreeMap<K, V>, Option<V>, &'a V) -> Fut,
        Fut: Future<Output=()> + Send + 'a,
{
    type Fut = Fut;

    fn apply(
        &'a mut self,
        shared: &'a mut Shared,
        key: K,
        map: &'a BTreeMap<K, V>,
        previous: Option<V>,
        new_value: &'a V,
    ) -> Self::Fut
        where
            K: 'a,
            V: 'a,
    {
        self(shared, key, map, previous, new_value)
    }
}

pub trait MapRemoveFn<'a, K, V> {
    type Fut: Future<Output=()> + Send + 'a;

    fn apply(&'a mut self, key: K, map: &'a BTreeMap<K, V>, removed: V) -> Self::Fut
        where
            K: 'a,
            V: 'a;
}

impl<'a, K, V, F, Fut> MapRemoveFn<'a, K, V> for F
    where
        K: 'static,
        V: 'static,
        F: FnMut(K, &'a BTreeMap<K, V>, V) -> Fut,
        Fut: Future<Output=()> + Send + 'a,
{
    type Fut = Fut;

    fn apply(&'a mut self, key: K, map: &'a BTreeMap<K, V>, removed: V) -> Self::Fut
        where
            K: 'a,
            V: 'a,
    {
        self(key, map, removed)
    }
}

pub trait SharedMapRemoveFn<'a, Shared, K, V> {
    type Fut: Future<Output=()> + Send + 'a;

    fn apply(
        &'a mut self,
        shared: &'a mut Shared,
        key: K,
        map: &'a BTreeMap<K, V>,
        removed: V,
    ) -> Self::Fut
        where
            K: 'a,
            V: 'a;
}

impl<'a, Shared, K, V, F, Fut> SharedMapRemoveFn<'a, Shared, K, V> for F
    where
        K: 'static,
        V: 'static,
        Shared: 'a,
        F: FnMut(&'a mut Shared, K, &'a BTreeMap<K, V>, V) -> Fut,
        Fut: Future<Output=()> + Send + 'a,
{
    type Fut = Fut;

    fn apply(
        &'a mut self,
        shared: &'a mut Shared,
        key: K,
        map: &'a BTreeMap<K, V>,
        removed: V,
    ) -> Self::Fut
        where
            K: 'a,
            V: 'a,
    {
        self(shared, key, map, removed)
    }
}

pub trait MapClearFn<'a, K, V> {
    type Fut: Future<Output=()> + Send + 'a;

    fn apply(&'a mut self, map: BTreeMap<K, V>) -> Self::Fut;
}

impl<'a, K, V, F, Fut> MapClearFn<'a, K, V> for F
    where
        K: 'static,
        V: 'static,
        F: FnMut(BTreeMap<K, V>) -> Fut,
        Fut: Future<Output=()> + Send + 'a,
{
    type Fut = Fut;

    fn apply(&'a mut self, map: BTreeMap<K, V>) -> Self::Fut {
        self(map)
    }
}

pub trait SharedMapClearFn<'a, Shared, K, V> {
    type Fut: Future<Output=()> + Send + 'a;

    fn apply(&'a mut self, shared: &'a mut Shared, map: BTreeMap<K, V>) -> Self::Fut;
}

impl<'a, Shared, K, V, F, Fut> SharedMapClearFn<'a, Shared, K, V> for F
    where
        K: 'static,
        V: 'static,
        Shared: 'a,
        F: FnMut(&'a mut Shared, BTreeMap<K, V>) -> Fut,
        Fut: Future<Output=()> + Send + 'a,
{
    type Fut = Fut;

    fn apply(&'a mut self, shared: &'a mut Shared, map: BTreeMap<K, V>) -> Self::Fut {
        self(shared, map)
    }
}

pub trait SetFn<'a, T: ?Sized> {
    type Fut: Future<Output=()> + Send + 'a;

    fn apply(&'a mut self, previous: Option<&'a T>, value: &'a T) -> Self::Fut;
}

impl<'a, T, F, Fut> SetFn<'a, T> for F
    where
        T: ?Sized + 'static,
        F: FnMut(Option<&'a T>, &'a T) -> Fut,
        Fut: Future<Output=()> + Send + 'a,
{
    type Fut = Fut;

    fn apply(&'a mut self, previous: Option<&'a T>, value: &'a T) -> Self::Fut {
        self(previous, value)
    }
}

pub trait SharedSetFn<'a, Shared, T: ?Sized> {
    type Fut: Future<Output=()> + Send + 'a;

    fn apply(&'a mut self, shared: &'a Shared, previous: Option<&'a T>, value: &'a T) -> Self::Fut;
}

impl<'a, Shared, T, F, Fut> SharedSetFn<'a, Shared, T> for F
    where
        T: ?Sized + 'a,
        Shared: 'a,
        F: FnMut(&'a Shared, Option<&'a T>, &'a T) -> Fut,
        Fut: Future<Output=()> + Send + 'a,
{
    type Fut = Fut;

    fn apply(&'a mut self, shared: &'a Shared, previous: Option<&'a T>, value: &'a T) -> Self::Fut {
        self(shared, previous, value)
    }
}
