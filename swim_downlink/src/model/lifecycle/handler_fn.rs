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

use futures::Future;

pub trait SharedHandlerFn0<'a, Shared> {

    type Fut: Future<Output = ()> + Send + 'a;

    fn apply(&'a mut self, shared: &'a mut Shared) -> Self::Fut;
}

impl<'a, Shared, F, Fut> SharedHandlerFn0<'a, Shared> for F
where
    Shared: 'a,
    F: FnMut(&'a mut Shared) -> Fut,
    Fut: Future<Output = ()> + Send + 'a,
{
    type Fut = Fut;

    fn apply(&'a mut self, shared: &'a mut Shared) -> Self::Fut {
        self(shared)
    }
}

pub trait EventFn<'a, T> {

    type Fut: Future<Output = ()> + Send + 'a;

    fn apply(&'a mut self, value: &'a T) -> Self::Fut;

}

impl<'a, T, F, Fut> EventFn<'a, T> for F
where
    T: 'static,
    F: FnMut(&'a T) -> Fut,
    Fut: Future<Output = ()> + Send + 'a,
{
    type Fut = Fut;

    fn apply(&'a mut self, value: &'a T) -> Self::Fut {
        self(value)
    }
}

pub trait SharedEventFn<'a, Shared, T> {

    type Fut: Future<Output = ()> + Send + 'a;

    fn apply(&'a mut self, shared: &'a mut Shared, value: &'a T) -> Self::Fut;

}

impl<'a, Shared, T, F, Fut> SharedEventFn<'a, Shared, T> for F
where
    T: 'static,
    Shared: 'a,
    F: FnMut(&'a mut Shared, &'a T) -> Fut,
    Fut: Future<Output = ()> + Send + 'a,
{
    type Fut = Fut;

    fn apply(&'a mut self, shared: &'a mut Shared, value: &'a T) -> Self::Fut {
        self(shared, value)
    }
}

pub trait SetFn<'a, T> {

    type Fut: Future<Output = ()> + Send + 'a;

    fn apply(&'a mut self, previous: Option<&'a T>, value: &'a T) -> Self::Fut;

}

impl<'a, T, F, Fut> SetFn<'a, T> for F
where
    T: 'static,
    F: FnMut(Option<&'a T>, &'a T) -> Fut,
    Fut: Future<Output = ()> + Send + 'a,
{
    type Fut = Fut;

    fn apply(&'a mut self, previous: Option<&'a T>, value: &'a T) -> Self::Fut {
        self(previous, value)
    }
}

pub trait SharedSetFn<'a, Shared, T> {

    type Fut: Future<Output = ()> + Send + 'a;

    fn apply(&'a mut self, shared: &'a Shared, previous: Option<&'a T>, value: &'a T) -> Self::Fut;

}

impl<'a, Shared, T, F, Fut> SharedSetFn<'a, Shared, T> for F
where
    T: 'a,
    Shared: 'a,
    F: FnMut(&'a Shared, Option<&'a T>, &'a T) -> Fut,
    Fut: Future<Output = ()> + Send + 'a,
{
    type Fut = Fut;

    fn apply(&'a mut self, shared: &'a Shared, previous: Option<&'a T>, value: &'a T) -> Self::Fut {
        self(shared, previous, value)
    }
}