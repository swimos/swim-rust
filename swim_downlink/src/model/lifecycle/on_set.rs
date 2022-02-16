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

pub trait OnSet<'a, T>: Send {
    type OnSetFut: Future<Output = ()> + Send + 'a;

    fn on_set(&'a mut self, existing: Option<&'a T>, new_value: &'a T) -> Self::OnSetFut;
}

impl<'a, T> OnSet<'a, T> for NoHandler {
    type OnSetFut = Ready<()>;

    fn on_set(&'a mut self, _existing: Option<&'a T>, _new_value: &'a T) -> Self::OnSetFut {
        ready(())
    }
}

impl<'a, T, F, Fut> OnSet<'a, T> for FnMutHandler<F>
where
    T: 'static,
    F: FnMut(Option<&'a T>, &'a T) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
{
    type OnSetFut = Fut;

    fn on_set(&'a mut self, existing: Option<&'a T>, new_value: &'a T) -> Self::OnSetFut {
        let FnMutHandler(f) = self;
        f(existing, new_value)
    }
}

pub trait OnSetShared<'a, T, Shared>: Send {
    type OnSetFut: Future<Output = ()> + Send + 'a;

    fn on_set(
        &'a mut self,
        shared: &'a mut Shared,
        existing: Option<&'a T>,
        new_value: &'a T,
    ) -> Self::OnSetFut;
}

impl<'a, T, Shared> OnSetShared<'a, T, Shared> for NoHandler {
    type OnSetFut = Ready<()>;

    fn on_set(
        &'a mut self,
        _shared: &'a mut Shared,
        _existing: Option<&'a T>,
        _new_value: &'a T,
    ) -> Self::OnSetFut {
        ready(())
    }
}

impl<'a, T, Shared, F, Fut> OnSetShared<'a, T, Shared> for FnMutHandler<F>
where
    T: 'static,
    Shared: 'static,
    F: FnMut(&'a mut Shared, Option<&'a T>, &'a T) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
{
    type OnSetFut = Fut;

    fn on_set(
        &'a mut self,
        shared: &'a mut Shared,
        existing: Option<&'a T>,
        new_value: &'a T,
    ) -> Self::OnSetFut {
        let FnMutHandler(f) = self;
        f(shared, existing, new_value)
    }
}

impl<'a, T, H, Shared> OnSetShared<'a, T, Shared> for WithShared<H>
where
    H: OnSet<'a, T>,
{
    type OnSetFut = H::OnSetFut;

    fn on_set(
        &'a mut self,
        _shared: &'a mut Shared,
        existing: Option<&'a T>,
        new_value: &'a T,
    ) -> Self::OnSetFut {
        self.0.on_set(existing, new_value)
    }
}

impl<'a, F, T> OnSet<'a, T> for BlockingHandler<F>
where
    T: 'static,
    F: FnMut(Option<&'a T>, &'a T) + Send,
{
    type OnSetFut = Ready<()>;

    fn on_set(&'a mut self, existing: Option<&'a T>, new_value: &'a T) -> Self::OnSetFut {
        let BlockingHandler(f) = self;
        f(existing, new_value);
        ready(())
    }
}

impl<'a, T, F, Shared> OnSetShared<'a, T, Shared> for BlockingHandler<F>
where
    Shared: 'static,
    T: 'static,
    F: FnMut(&'a mut Shared, Option<&'a T>, &'a T) + Send,
{
    type OnSetFut = Ready<()>;

    fn on_set(
        &'a mut self,
        shared: &'a mut Shared,
        existing: Option<&'a T>,
        new_value: &'a T,
    ) -> Self::OnSetFut {
        let BlockingHandler(f) = self;
        f(shared, existing, new_value);
        ready(())
    }
}

#[macro_export]
macro_rules! on_set_handler {
    ($t:ty, |$before:ident, $after:ident| $body:expr) => {{
        async fn handler($before: core::option::Option<&$t>, $after: &$t) {
            $body
        }
        handler
    }};
    ($t:ty, $s:ty, |$shared:ident, $before:ident, $after:ident| $body:expr) => {{
        async fn handler($shared: &mut $s, $before: core::option::Option<&$t>, $after: &$t) {
            $body
        }
        handler
    }};
}
