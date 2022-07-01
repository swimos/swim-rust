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

use swim_api::handlers::{FnHandler, NoHandler};

use crate::event_handler::{EventHandler, UnitHandler};

use super::utility::HandlerContext;

pub trait OnStart<'a, Context>: Send {
    type OnStartHandler: EventHandler<Context, Completion = ()> + Send + 'a;

    fn on_start(&'a self) -> Self::OnStartHandler;
}

pub trait OnStartShared<'a, Context, Shared>: Send {
    type OnStartHandler: EventHandler<Context, Completion = ()> + Send + 'a;

    fn on_start(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::OnStartHandler;
}

impl<'a, Context> OnStart<'a, Context> for NoHandler {
    type OnStartHandler = UnitHandler;

    fn on_start(&'a self) -> Self::OnStartHandler {
        Default::default()
    }
}

impl<'a, Context, Shared> OnStartShared<'a, Context, Shared> for NoHandler {
    type OnStartHandler = UnitHandler;

    fn on_start(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
    ) -> Self::OnStartHandler {
        Default::default()
    }
}

impl<'a, Context, F, H> OnStart<'a, Context> for FnHandler<F>
where
    F: Fn() -> H + Send,
    H: EventHandler<Context, Completion = ()> + Send + 'static,
{
    type OnStartHandler = H;

    fn on_start(&'a self) -> Self::OnStartHandler {
        let FnHandler(f) = self;
        f()
    }
}

impl<'a, Context, F, H, Shared> OnStartShared<'a, Context, Shared> for FnHandler<F>
where
    Shared: 'static,
    F: Fn(&'a Shared, HandlerContext<Context>) -> H + Send,
    H: EventHandler<Context, Completion = ()> + Send + 'static,
{
    type OnStartHandler = H;

    fn on_start(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::OnStartHandler {
        let FnHandler(f) = self;
        f(shared, handler_context)
    }
}