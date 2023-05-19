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
use swim_api::handlers::{BlockingHandler, FnMutHandler, NoHandler, WithShared};

use super::handler_fn::SharedHandlerFn0;

/// Trait for event handlers to be called when a downlink connects.
pub trait OnLinked: Send {
    type OnLinkedFut<'a>: Future<Output = ()> + Send + 'a
    where
        Self: 'a;

    fn on_linked(&mut self) -> Self::OnLinkedFut<'_>;
}

/// Trait for event handlers, that share state with other handlers, called when a downlink
/// connects.
pub trait OnLinkedShared<Shared>: Send {
    type OnLinkedFut<'a>: Future<Output = ()> + Send + 'a
    where
        Self: 'a,
        Shared: 'a;

    fn on_linked<'a>(&'a mut self, shared: &'a mut Shared) -> Self::OnLinkedFut<'a>;
}

impl OnLinked for NoHandler {
    type OnLinkedFut<'a> = Ready<()>
    where
        Self: 'a;

    fn on_linked(&mut self) -> Self::OnLinkedFut<'_> {
        ready(())
    }
}

impl<F, Fut> OnLinked for FnMutHandler<F>
where
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'static,
{
    type OnLinkedFut<'a> = Fut
    where
        Self: 'a;

    fn on_linked(&mut self) -> Self::OnLinkedFut<'_> {
        let FnMutHandler(f) = self;
        f()
    }
}

impl<Shared> OnLinkedShared<Shared> for NoHandler {
    type OnLinkedFut<'a> = Ready<()>
    where
        Self: 'a,
        Shared: 'a;

    fn on_linked<'a>(&'a mut self, _shared: &'a mut Shared) -> Self::OnLinkedFut<'a> {
        ready(())
    }
}

impl<F, Shared> OnLinkedShared<Shared> for FnMutHandler<F>
where
    F: for<'a> SharedHandlerFn0<'a, Shared> + Send,
{
    type OnLinkedFut<'a> = <F as SharedHandlerFn0<'a, Shared>>::Fut
    where
        Self: 'a,
        Shared: 'a;

    fn on_linked<'a>(&'a mut self, shared: &'a mut Shared) -> Self::OnLinkedFut<'a> {
        let FnMutHandler(f) = self;
        f.apply(shared)
    }
}

impl<H, Shared> OnLinkedShared<Shared> for WithShared<H>
where
    H: OnLinked,
{
    type OnLinkedFut<'a> = H::OnLinkedFut<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_linked<'a>(&'a mut self, _state: &'a mut Shared) -> Self::OnLinkedFut<'a> {
        self.0.on_linked()
    }
}

impl<F> OnLinked for BlockingHandler<F>
where
    F: FnMut() + Send,
{
    type OnLinkedFut<'a> = Ready<()>
    where
        Self: 'a;

    fn on_linked(&mut self) -> Self::OnLinkedFut<'_> {
        let BlockingHandler(f) = self;
        f();
        ready(())
    }
}

impl<Shared, F> OnLinkedShared<Shared> for BlockingHandler<F>
where
    F: for<'a> FnMut(&'a mut Shared) + Send,
{
    type OnLinkedFut<'a> = Ready<()>
    where
        Self: 'a,
        Shared: 'a;

    fn on_linked<'a>(&'a mut self, shared: &'a mut Shared) -> Self::OnLinkedFut<'a> {
        let BlockingHandler(f) = self;
        f(shared);
        ready(())
    }
}

#[macro_export]
macro_rules! on_linked_handler {
    ($s:ty, |$shared:ident| $body:expr) => {{
        async fn handler($shared: &mut $s) {
            $body
        }
        handler
    }};
}
