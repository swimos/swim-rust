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

pub trait OnEvent<'a, T>: Send {
    type OnEventFut: Future<Output = ()> + Send + 'a;

    fn on_event(&'a mut self, value: &'a T) -> Self::OnEventFut;
}

impl<'a, T> OnEvent<'a, T> for NoHandler {
    type OnEventFut = Ready<()>;

    fn on_event(&'a mut self, _value: &'a T) -> Self::OnEventFut {
        ready(())
    }
}

impl<'a, T, F, Fut> OnEvent<'a, T> for FnMutHandler<F>
where
    T: 'static,
    F: FnMut(&'a T) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
{
    type OnEventFut = Fut;

    fn on_event(&'a mut self, value: &'a T) -> Self::OnEventFut {
        let FnMutHandler(f) = self;
        f(value)
    }
}

pub trait OnEventShared<'a, T, Shared>: Send {
    type OnEventFut: Future<Output = ()> + Send + 'a;

    fn on_event(&'a mut self, shared: &'a mut Shared, value: &'a T) -> Self::OnEventFut;
}

impl<'a, T, Shared> OnEventShared<'a, T, Shared> for NoHandler {
    type OnEventFut = Ready<()>;

    fn on_event(&'a mut self, _shared: &'a mut Shared, _value: &'a T) -> Self::OnEventFut {
        ready(())
    }
}

impl<'a, T, Shared, F, Fut> OnEventShared<'a, T, Shared> for FnMutHandler<F>
where
    T: 'static,
    Shared: 'static,
    F: FnMut(&'a mut Shared, &'a T) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
{
    type OnEventFut = Fut;

    fn on_event(&'a mut self, shared: &'a mut Shared, value: &'a T) -> Self::OnEventFut {
        let FnMutHandler(f) = self;
        f(shared, value)
    }
}

impl<'a, T, H, Shared> OnEventShared<'a, T, Shared> for WithShared<H>
where
    H: OnEvent<'a, T>,
{
    type OnEventFut = H::OnEventFut;

    fn on_event(&'a mut self, _shared: &'a mut Shared, value: &'a T) -> Self::OnEventFut {
        self.0.on_event(value)
    }
}

impl<'a, F, T> OnEvent<'a, T> for BlockingHandler<F>
where
    T: 'static,
    F: FnMut(&'a T) + Send,
{
    type OnEventFut = Ready<()>;

    fn on_event(&'a mut self, value: &'a T) -> Self::OnEventFut {
        let BlockingHandler(f) = self;
        f(value);
        ready(())
    }
}

impl<'a, F, T, Shared> OnEventShared<'a, T, Shared> for BlockingHandler<F>
where
    T: 'static,
    Shared: 'static,
    F: FnMut(&'a mut Shared, &'a T) + Send,
{
    type OnEventFut = Ready<()>;

    fn on_event(&'a mut self, shared: &'a mut Shared, value: &'a T) -> Self::OnEventFut {
        let BlockingHandler(f) = self;
        f(shared, value);
        ready(())
    }
}

#[macro_export]
macro_rules! on_event_handler {
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
