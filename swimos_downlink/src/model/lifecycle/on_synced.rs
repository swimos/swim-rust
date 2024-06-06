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

use futures::future::{ready, Ready};
use std::future::Future;
use swimos_utilities::handlers::{BlockingHandler, FnMutHandler, NoHandler, WithShared};

use super::{EventFn, SharedEventFn};

/// Trait for event handlers to be called when a downlink synchronizes.
pub trait OnSynced<T>: Send {
    type OnSyncedFut<'a>: Future<Output = ()> + Send + 'a
    where
        Self: 'a,
        T: 'a;

    fn on_synced<'a>(&'a mut self, value: &'a T) -> Self::OnSyncedFut<'a>;
}

/// Trait for event handlers, that share state with other handlers, called when a downlink
/// synchronizes.
pub trait OnSyncedShared<T, Shared>: Send {
    type OnSyncedFut<'a>: Future<Output = ()> + Send + 'a
    where
        Self: 'a,
        T: 'a,
        Shared: 'a;

    fn on_synced<'a>(&'a mut self, shared: &'a mut Shared, value: &'a T) -> Self::OnSyncedFut<'a>;
}

impl<T> OnSynced<T> for NoHandler {
    type OnSyncedFut<'a> = Ready<()>
    where
        Self: 'a,
        T: 'a;

    fn on_synced<'a>(&'a mut self, _value: &'a T) -> Self::OnSyncedFut<'a> {
        ready(())
    }
}

impl<T, F> OnSynced<T> for FnMutHandler<F>
where
    F: for<'a> EventFn<'a, T> + Send,
{
    type OnSyncedFut<'a> = <F as EventFn<'a, T>>::Fut
    where
        Self: 'a,
        T: 'a;

    fn on_synced<'a>(&'a mut self, value: &'a T) -> Self::OnSyncedFut<'a> {
        let FnMutHandler(f) = self;
        f.apply(value)
    }
}

impl<T, Shared> OnSyncedShared<T, Shared> for NoHandler {
    type OnSyncedFut<'a> = Ready<()>
    where
        Self: 'a,
        T: 'a,
        Shared: 'a;

    fn on_synced<'a>(
        &'a mut self,
        _shared: &'a mut Shared,
        _value: &'a T,
    ) -> Self::OnSyncedFut<'a> {
        ready(())
    }
}

impl<T, Shared, F> OnSyncedShared<T, Shared> for FnMutHandler<F>
where
    F: for<'a> SharedEventFn<'a, Shared, T> + Send,
{
    type OnSyncedFut<'a> = <F as SharedEventFn<'a, Shared, T>>::Fut
    where
        Self: 'a,
        T: 'a,
        Shared: 'a;

    fn on_synced<'a>(&'a mut self, shared: &'a mut Shared, value: &'a T) -> Self::OnSyncedFut<'a> {
        let FnMutHandler(f) = self;
        f.apply(shared, value)
    }
}

impl<T, H, Shared> OnSyncedShared<T, Shared> for WithShared<H>
where
    H: OnSynced<T>,
{
    type OnSyncedFut<'a> = H::OnSyncedFut<'a>
    where
        Self: 'a,
        T: 'a,
        Shared: 'a;

    fn on_synced<'a>(&'a mut self, _shared: &'a mut Shared, value: &'a T) -> Self::OnSyncedFut<'a> {
        self.0.on_synced(value)
    }
}

impl<F, T> OnSynced<T> for BlockingHandler<F>
where
    F: FnMut(&T) + Send,
{
    type OnSyncedFut<'a> = Ready<()>
    where
        Self: 'a,
        T: 'a;

    fn on_synced<'a>(&'a mut self, value: &'a T) -> Self::OnSyncedFut<'a> {
        let BlockingHandler(f) = self;
        f(value);
        ready(())
    }
}

impl<F, T, Shared> OnSyncedShared<T, Shared> for BlockingHandler<F>
where
    F: FnMut(&mut Shared, &T) + Send,
{
    type OnSyncedFut<'a> = Ready<()>
    where
        Self: 'a,
        T: 'a,
        Shared: 'a;

    fn on_synced<'a>(&'a mut self, shared: &'a mut Shared, value: &'a T) -> Self::OnSyncedFut<'a> {
        let BlockingHandler(f) = self;
        f(shared, value);
        ready(())
    }
}

#[macro_export]
macro_rules! on_synced_handler {
    ($t:ty, |$param:ident| $body:expr) => {{
        async fn handler($param: &$t) {
            $body
        }
        handler
    }};
    ($t:ty, $s:ty, |$shared:ident, $param:ident| $body:expr) => {{
        async fn handler($shared: &mut $s, $param: &$t) {
            $body
        }
        handler
    }};
}
