// Copyright 2015-2020 SWIM.AI inc.
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

use crate::var::Contents;
use futures::future;
use futures_util::future::ready;
use std::any::Any;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use utilities::future::{SwimFutureExt, Unit};

/// An observer to watch for changes to the value of a [`TVar`].
pub trait Observer<'a, T> {
    type RecFuture: Future<Output = ()> + Send + 'a;

    /// Called by the [`TVar`] each time the value changes.
    fn notify(&'a mut self, value: T) -> Self::RecFuture;
}

/// An observer that can be applied over any lifetime.
pub trait StaticObserver<T>: for<'a> Observer<'a, T> {}

impl<T, O> StaticObserver<T> for O where O: for<'a> Observer<'a, T> {}

/// Type erased observer to be passed into [`TVarInner`].
pub(super) trait RawObserver {
    fn notify_raw<'a>(
        &'a mut self,
        value: Contents,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

pub(super) type DynObserver = Box<dyn RawObserver + Send + Sync + 'static>;

/// Wraps an observer to erase its type.
pub(super) struct RawWrapper<T, Obs>(Obs, PhantomData<fn(T)>);

impl<T, Obs> RawWrapper<T, Obs> {
    pub(super) fn new(observer: Obs) -> Self {
        RawWrapper(observer, PhantomData)
    }
}

impl<Obs, T> RawObserver for RawWrapper<T, Obs>
where
    T: Any + Send + Sync,
    Obs: StaticObserver<Arc<T>>,
{
    fn notify_raw<'a>(
        &'a mut self,
        value: Contents,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        let RawWrapper(observer, _) = self;
        match value.downcast::<T>() {
            Ok(t) => Box::pin(observer.notify(t)),
            Err(_) => Box::pin(ready(())),
        }
    }
}

/// An observer that forwards to two other observers.
pub struct JoinObserver<Obs1, Obs2>(Obs1, Obs2);

/// Where the observed type is cloneable, create an observer that will forward copies to two other
/// observers.
pub fn join<T, Obs1, Obs2>(obs1: Obs1, obs2: Obs2) -> JoinObserver<Obs1, Obs2>
where
    T: Clone,
    Obs1: for<'a> Observer<'a, T>,
    Obs2: for<'a> Observer<'a, T>,
{
    JoinObserver(obs1, obs2)
}

impl<'a, T, Obs1, Obs2> Observer<'a, T> for JoinObserver<Obs1, Obs2>
where
    T: Clone,
    Obs1: Observer<'a, T>,
    Obs2: Observer<'a, T>,
{
    type RecFuture = Unit<future::Join<Obs1::RecFuture, Obs2::RecFuture>>;

    fn notify(&'a mut self, value: T) -> Self::RecFuture {
        let JoinObserver(first, second) = self;
        future::join(first.notify(value.clone()), second.notify(value)).unit()
    }
}
