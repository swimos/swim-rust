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

use futures::future::{ready, Ready};
use std::future::Future;
use swimos_utilities::handlers::{BlockingHandler, FnMutHandler, NoHandler, WithShared};

use super::SharedHandlerFn0;

/// Trait for event handlers to be called when a downlink disconnects.
pub trait OnUnlinked: Send {
    type OnUnlinkedFut<'a>: Future<Output = ()> + Send + 'a
    where
        Self: 'a;

    fn on_unlinked(&mut self) -> Self::OnUnlinkedFut<'_>;
}

/// Trait for event handlers, that share state with other handlers, called when a downlink
/// disconnects.
pub trait OnUnlinkedShared<Shared>: Send {
    type OnUnlinkedFut<'a>: Future<Output = ()> + Send + 'a
    where
        Self: 'a,
        Shared: 'a;

    fn on_unlinked<'a>(&'a mut self, shared: &'a mut Shared) -> Self::OnUnlinkedFut<'a>;
}

impl OnUnlinked for NoHandler {
    type OnUnlinkedFut<'a> = Ready<()>
    where
        Self: 'a;

    fn on_unlinked(&mut self) -> Self::OnUnlinkedFut<'_> {
        ready(())
    }
}

impl<F, Fut> OnUnlinked for FnMutHandler<F>
where
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'static,
{
    type OnUnlinkedFut<'a> = Fut
    where
        Self: 'a;

    fn on_unlinked(&mut self) -> Self::OnUnlinkedFut<'_> {
        let FnMutHandler(f) = self;
        f()
    }
}

impl<Shared> OnUnlinkedShared<Shared> for NoHandler {
    type OnUnlinkedFut<'a> = Ready<()>
    where
        Self: 'a,
        Shared: 'a;

    fn on_unlinked<'a>(&'a mut self, _shared: &'a mut Shared) -> Self::OnUnlinkedFut<'a> {
        ready(())
    }
}

impl<Shared, F> OnUnlinkedShared<Shared> for FnMutHandler<F>
where
    F: for<'a> SharedHandlerFn0<'a, Shared> + Send,
{
    type OnUnlinkedFut<'a> = <F as SharedHandlerFn0<'a, Shared>>::Fut
    where
        Self: 'a,
        Shared: 'a;

    fn on_unlinked<'a>(&'a mut self, shared: &'a mut Shared) -> Self::OnUnlinkedFut<'a> {
        let FnMutHandler(f) = self;
        f.apply(shared)
    }
}

impl<H, Shared> OnUnlinkedShared<Shared> for WithShared<H>
where
    H: OnUnlinked,
{
    type OnUnlinkedFut<'a> = H::OnUnlinkedFut<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_unlinked<'a>(&'a mut self, _state: &'a mut Shared) -> Self::OnUnlinkedFut<'a> {
        self.0.on_unlinked()
    }
}

impl<F> OnUnlinked for BlockingHandler<F>
where
    F: FnMut() + Send,
{
    type OnUnlinkedFut<'a> = Ready<()>
    where
        Self: 'a;

    fn on_unlinked(&mut self) -> Self::OnUnlinkedFut<'_> {
        let BlockingHandler(f) = self;
        f();
        ready(())
    }
}

impl<Shared, F> OnUnlinkedShared<Shared> for BlockingHandler<F>
where
    Shared: 'static,
    F: FnMut(&mut Shared) + Send,
{
    type OnUnlinkedFut<'a> = Ready<()>
    where
        Self: 'a,
        Shared: 'a;

    fn on_unlinked<'a>(&'a mut self, shared: &'a mut Shared) -> Self::OnUnlinkedFut<'a> {
        let BlockingHandler(f) = self;
        f(shared);
        ready(())
    }
}

#[macro_export]
macro_rules! on_unlinked_handler {
    ($s:ty, |$shared:ident| $body:expr) => {{
        async fn handler($shared: &mut $s) {
            $body
        }
        handler
    }};
}
