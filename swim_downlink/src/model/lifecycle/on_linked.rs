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
use swim_api::handlers::{BlockingHandler, ClosureHandler, FnMutHandler, NoHandler, WithShared};

pub trait OnLinked<'a>: Send {
    type OnLinkedFut: Future<Output = ()> + Send + 'a;

    fn on_linked(&'a mut self) -> Self::OnLinkedFut;
}

impl<'a> OnLinked<'a> for NoHandler {
    type OnLinkedFut = Ready<()>;

    fn on_linked(&'a mut self) -> Self::OnLinkedFut {
        ready(())
    }
}

impl<'a, F, Fut> OnLinked<'a> for FnMutHandler<F>
where
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
{
    type OnLinkedFut = Fut;

    fn on_linked(&'a mut self) -> Self::OnLinkedFut {
        let FnMutHandler(f) = self;
        f()
    }
}

pub trait OnLinkedShared<'a, Shared>: Send {
    type OnLinkedFut: Future<Output = ()> + Send + 'a;

    fn on_linked(&'a mut self, state: &'a mut Shared) -> Self::OnLinkedFut;
}

impl<'a, Shared> OnLinkedShared<'a, Shared> for NoHandler {
    type OnLinkedFut = Ready<()>;

    fn on_linked(&'a mut self, _shared: &'a mut Shared) -> Self::OnLinkedFut {
        ready(())
    }
}

impl<'a, F, Fut, Shared> OnLinkedShared<'a, Shared> for FnMutHandler<F>
where
    Shared: 'static,
    F: FnMut(&'a mut Shared) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
{
    type OnLinkedFut = Fut;

    fn on_linked(&'a mut self, shared: &'a mut Shared) -> Self::OnLinkedFut {
        let FnMutHandler(f) = self;
        f(shared)
    }
}

impl<'a, H, Shared> OnLinkedShared<'a, Shared> for WithShared<H>
where
    H: OnLinked<'a>,
{
    type OnLinkedFut = H::OnLinkedFut;

    fn on_linked(&'a mut self, _state: &'a mut Shared) -> Self::OnLinkedFut {
        self.0.on_linked()
    }
}

impl<'a, State, F, Fut> OnLinked<'a> for ClosureHandler<State, F>
where
    State: Send + 'static,
    F: FnMut(&'a mut State) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
{
    type OnLinkedFut = Fut;

    fn on_linked(&'a mut self) -> Self::OnLinkedFut {
        let ClosureHandler { state, f } = self;
        f(state)
    }
}

impl<'a, F> OnLinked<'a> for BlockingHandler<F>
where
    F: FnMut() + Send,
{
    type OnLinkedFut = Ready<()>;

    fn on_linked(&'a mut self) -> Self::OnLinkedFut {
        let BlockingHandler(f) = self;
        f();
        ready(())
    }
}
