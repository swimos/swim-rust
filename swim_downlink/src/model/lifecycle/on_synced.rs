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

pub trait OnSynced<'a, T>: Send {
    type OnSyncedFut: Future<Output = ()> + Send + 'a;

    fn on_synced(&'a mut self, value: &'a T) -> Self::OnSyncedFut;
}

impl<'a, T> OnSynced<'a, T> for NoHandler {
    type OnSyncedFut = Ready<()>;

    fn on_synced(&'a mut self, _value: &'a T) -> Self::OnSyncedFut {
        ready(())
    }
}

impl<'a, T, F, Fut> OnSynced<'a, T> for FnMutHandler<F>
where
    T: 'static,
    F: FnMut(&'a T) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
{
    type OnSyncedFut = Fut;

    fn on_synced(&'a mut self, value: &'a T) -> Self::OnSyncedFut {
        let FnMutHandler(f) = self;
        f(value)
    }
}

pub trait OnSyncedShared<'a, T, Shared>: Send {
    type OnSyncedFut: Future<Output = ()> + Send + 'a;

    fn on_synced(&'a mut self, shared: &'a mut Shared, value: &'a T) -> Self::OnSyncedFut;
}

impl<'a, T, Shared> OnSyncedShared<'a, T, Shared> for NoHandler {
    type OnSyncedFut = Ready<()>;

    fn on_synced(&'a mut self, _shared: &'a mut Shared, _value: &'a T) -> Self::OnSyncedFut {
        ready(())
    }
}

impl<'a, T, Shared, F, Fut> OnSyncedShared<'a, T, Shared> for FnMutHandler<F>
where
    T: 'static,
    Shared: 'static,
    F: FnMut(&'a mut Shared, &'a T) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
{
    type OnSyncedFut = Fut;

    fn on_synced(&'a mut self, shared: &'a mut Shared, value: &'a T) -> Self::OnSyncedFut {
        let FnMutHandler(f) = self;
        f(shared, value)
    }
}

impl<'a, T, H, Shared> OnSyncedShared<'a, T, Shared> for WithShared<H>
where
    H: OnSynced<'a, T>,
{
    type OnSyncedFut = H::OnSyncedFut;

    fn on_synced(&'a mut self, _shared: &'a mut Shared, value: &'a T) -> Self::OnSyncedFut {
        self.0.on_synced(value)
    }
}

impl<'a, F, T> OnSynced<'a, T> for BlockingHandler<F>
where
    T: 'static,
    F: FnMut(&'a T) + Send,
{
    type OnSyncedFut = Ready<()>;

    fn on_synced(&'a mut self, value: &'a T) -> Self::OnSyncedFut {
        let BlockingHandler(f) = self;
        f(value);
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
