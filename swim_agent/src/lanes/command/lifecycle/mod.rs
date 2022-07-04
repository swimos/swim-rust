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

use std::marker::PhantomData;

use swim_api::handlers::{FnHandler, NoHandler};

use self::on_command::{OnCommand, OnCommandShared};

pub mod on_command;

pub trait CommandLaneHandlers<'a, T, Context>: OnCommand<'a, T, Context> {}
pub trait CommandLaneLifecycle<T, Context>: for<'a> OnCommand<'a, T, Context> {}

impl<'a, T, Context, L> CommandLaneHandlers<'a, T, Context> for L where L: OnCommand<'a, T, Context> {}

impl<T, Context, L> CommandLaneLifecycle<T, Context> for L where
    L: for<'a> CommandLaneHandlers<'a, T, Context>
{
}

pub trait CommandLaneHandlersShared<'a, T, Context, Shared>:
    OnCommandShared<'a, T, Context, Shared>
{
}
pub trait CommandLaneLifecycleShared<T, Context, Shared>:
    for<'a> OnCommandShared<'a, T, Context, Shared>
{
}

impl<'a, T, Context, Shared, L> CommandLaneHandlersShared<'a, T, Context, Shared> for L where
    L: OnCommandShared<'a, T, Context, Shared>
{
}

impl<T, Context, Shared, L> CommandLaneLifecycleShared<T, Context, Shared> for L where
    L: for<'a> CommandLaneHandlersShared<'a, T, Context, Shared>
{
}

pub struct StatefulCommandLaneLifecycle<Context, Shared, T, OnCmd = NoHandler> {
    _value_type: PhantomData<fn(Context, Shared, T)>,
    on_command: OnCmd,
}

impl<Context, Shared, T> Default for StatefulCommandLaneLifecycle<Context, Shared, T> {
    fn default() -> Self {
        Self {
            _value_type: Default::default(),
            on_command: Default::default(),
        }
    }
}

impl<'a, T, Context, Shared, OnCmd> OnCommandShared<'a, T, Context, Shared>
    for StatefulCommandLaneLifecycle<Context, Shared, T, OnCmd>
where
    OnCmd: OnCommandShared<'a, T, Context, Shared>,
{
    type OnCommandHandler = OnCmd::OnCommandHandler;

    fn on_command(
        &'a self,
        shared: &'a Shared,
        handler_context: crate::lifecycle::utility::HandlerContext<Context>,
        value: &T,
    ) -> Self::OnCommandHandler {
        self.on_command.on_command(shared, handler_context, value)
    }
}

impl<Context, Shared, T, OnCmd> StatefulCommandLaneLifecycle<Context, Shared, T, OnCmd> {
    pub fn on_command<F, H>(
        self,
        f: F,
    ) -> StatefulCommandLaneLifecycle<Context, Shared, T, FnHandler<F>>
    where
        FnHandler<F>: for<'a> OnCommandShared<'a, T, Context, Shared>,
    {
        StatefulCommandLaneLifecycle {
            _value_type: Default::default(),
            on_command: FnHandler(f),
        }
    }
}
