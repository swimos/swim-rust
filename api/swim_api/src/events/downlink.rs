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

use super::{FnMutHandler, NoHandler};

pub trait OnLinked: Clone {
    type Fut: Future<Output = ()> + Send + 'static;

    fn on_linked(&mut self) -> Self::Fut;
}

impl OnLinked for NoHandler {
    type Fut = Ready<()>;

    fn on_linked(&mut self) -> Self::Fut {
        ready(())
    }
}

impl<F, Fut> OnLinked for FnMutHandler<F>
where
    F: FnMut() -> Fut + Clone,
    Fut: Future<Output = ()> + Send + 'static,
{
    type Fut = Fut;

    fn on_linked(&mut self) -> Self::Fut {
        let FnMutHandler(f) = self;
        f()
    }
}

pub trait OnSynced<'a, T>: Clone {
    type Fut: Future<Output = ()> + Send + 'a;

    fn on_synced(&'a mut self, value: &'a T) -> Self::Fut;
}

impl<'a, T> OnSynced<'a, T> for NoHandler {
    type Fut = Ready<()>;

    fn on_synced(&'a mut self, _value: &'a T) -> Self::Fut {
        ready(())
    }
}

impl<'a, T, F, Fut> OnSynced<'a, T> for FnMutHandler<F>
where
    T: 'static,
    F: FnMut(&'a T) -> Fut + Clone,
    Fut: Future<Output = ()> + Send + 'a,
{
    type Fut = Fut;

    fn on_synced(&'a mut self, value: &'a T) -> Self::Fut {
        let FnMutHandler(f) = self;
        f(value)
    }
}

pub trait OnEvent<'a, T>: Clone {
    type Fut: Future<Output = ()> + Send + 'a;

    fn on_event(&'a mut self, value: &'a T) -> Self::Fut;
}

impl<'a, T> OnEvent<'a, T> for NoHandler {
    type Fut = Ready<()>;

    fn on_event(&'a mut self, _value: &'a T) -> Self::Fut {
        ready(())
    }
}

impl<'a, T, F, Fut> OnEvent<'a, T> for FnMutHandler<F>
where
    T: 'static,
    F: FnMut(&'a T) -> Fut + Clone,
    Fut: Future<Output = ()> + Send + 'a,
{
    type Fut = Fut;

    fn on_event(&'a mut self, value: &'a T) -> Self::Fut {
        let FnMutHandler(f) = self;
        f(value)
    }
}

pub trait OnSet<'a, T> {
    type Fut: Future<Output = ()> + Send + 'a;

    fn on_set(&'a mut self, existing: &'a T, new_value: &'a T) -> Self::Fut;
}

impl<'a, T> OnSet<'a, T> for NoHandler {
    type Fut = Ready<()>;

    fn on_set(&'a mut self, _existing: &'a T, _new_value: &'a T) -> Self::Fut {
        ready(())
    }
}

impl<'a, T, F, Fut> OnSet<'a, T> for FnMutHandler<F>
where
    T: 'static,
    F: FnMut(&'a T, &'a T) -> Fut,
    Fut: Future<Output = ()> + Send + 'a,
{
    type Fut = Fut;

    fn on_set(&'a mut self, existing: &'a T, new_value: &'a T) -> Self::Fut {
        let FnMutHandler(f) = self;
        f(existing, new_value)
    }
}
