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

pub trait OnUnlinked<'a>: Send {
    type OnUnlinkedFut: Future<Output = ()> + Send + 'a;

    fn on_unlinked(&'a mut self) -> Self::OnUnlinkedFut;
}

impl<'a> OnUnlinked<'a> for NoHandler {
    type OnUnlinkedFut = Ready<()>;

    fn on_unlinked(&'a mut self) -> Self::OnUnlinkedFut {
        ready(())
    }
}

impl<'a, F, Fut> OnUnlinked<'a> for FnMutHandler<F>
where
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
{
    type OnUnlinkedFut = Fut;

    fn on_unlinked(&'a mut self) -> Self::OnUnlinkedFut {
        let FnMutHandler(f) = self;
        f()
    }
}

pub trait OnUnlinkedShared<'a, Shared>: Send {
    type OnUnlinkedFut: Future<Output = ()> + Send + 'a;

    fn on_unlinked(&'a mut self, shared: &'a mut Shared) -> Self::OnUnlinkedFut;
}

impl<'a, Shared> OnUnlinkedShared<'a, Shared> for NoHandler {
    type OnUnlinkedFut = Ready<()>;

    fn on_unlinked(&'a mut self, _shared: &'a mut Shared) -> Self::OnUnlinkedFut {
        ready(())
    }
}

impl<'a, Shared, F, Fut> OnUnlinkedShared<'a, Shared> for FnMutHandler<F>
where
    Shared: 'static,
    F: FnMut(&'a mut Shared) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
{
    type OnUnlinkedFut = Fut;

    fn on_unlinked(&'a mut self, shared: &'a mut Shared) -> Self::OnUnlinkedFut {
        let FnMutHandler(f) = self;
        f(shared)
    }
}

impl<'a, H, Shared> OnUnlinkedShared<'a, Shared> for WithShared<H>
where
    H: OnUnlinked<'a>,
{
    type OnUnlinkedFut = H::OnUnlinkedFut;

    fn on_unlinked(&'a mut self, _state: &'a mut Shared) -> Self::OnUnlinkedFut {
        self.0.on_unlinked()
    }
}

impl<'a, F> OnUnlinked<'a> for BlockingHandler<F>
where
    F: FnMut() + Send,
{
    type OnUnlinkedFut = Ready<()>;

    fn on_unlinked(&'a mut self) -> Self::OnUnlinkedFut {
        let BlockingHandler(f) = self;
        f();
        ready(())
    }
}

impl<'a, Shared, F> OnUnlinkedShared<'a, Shared> for BlockingHandler<F>
where
    Shared: 'static,
    F: FnMut(&'a mut Shared) + Send,
{
    type OnUnlinkedFut = Ready<()>;

    fn on_unlinked(&'a mut self, shared: &'a mut Shared) -> Self::OnUnlinkedFut {
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
