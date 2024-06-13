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

use std::{borrow::Borrow, marker::PhantomData};

use swimos_utilities::handlers::{BorrowHandler, NoHandler};

use self::on_command::{OnCommand, OnCommandShared};

pub mod on_command;

/// Trait for the lifecycle of a command lane.
///
/// # Type Parameters
/// * `T` - The type of the commands.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
pub trait CommandLaneLifecycle<T, Context>: OnCommand<T, Context> {}

/// Trait for the lifecycle of a command lane where the lifecycle has access to some shared state (shared
/// with all other lifecycles in the agent).
///
/// # Type Parameters
/// * `T` - The type of the commands.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `Shared` - The shared state to which the lifecycle has access.
pub trait CommandLaneLifecycleShared<T, Context, Shared>:
    OnCommandShared<T, Context, Shared>
{
}

impl<T, Context, L> CommandLaneLifecycle<T, Context> for L where L: OnCommand<T, Context> {}

impl<T, Context, Shared, L> CommandLaneLifecycleShared<T, Context, Shared> for L where
    L: OnCommandShared<T, Context, Shared>
{
}

/// A lifecycle for a command lane with some shared state (shard with other lifecycles in the same agent).
///
/// # Type Parameters
/// * `Context` - The context for the event handlers (providing access to the agent lanes).
/// * `Shared` - The shared state to which the lifecycle has access.
/// * `T` - The type of the commands.
/// * `OnCmd` - The type of the `on_command` event.
pub struct StatefulCommandLaneLifecycle<Context, Shared, T, OnCmd = NoHandler> {
    _value_type: PhantomData<fn(Context, Shared, T)>,
    on_command: OnCmd,
}

impl<Context, Shared, T, OnCmd: Clone> Clone
    for StatefulCommandLaneLifecycle<Context, Shared, T, OnCmd>
{
    fn clone(&self) -> Self {
        Self {
            _value_type: PhantomData,
            on_command: self.on_command.clone(),
        }
    }
}

impl<Context, Shared, T> Default for StatefulCommandLaneLifecycle<Context, Shared, T> {
    fn default() -> Self {
        Self {
            _value_type: Default::default(),
            on_command: Default::default(),
        }
    }
}

impl<T, Context, Shared, OnCmd> OnCommandShared<T, Context, Shared>
    for StatefulCommandLaneLifecycle<Context, Shared, T, OnCmd>
where
    OnCmd: OnCommandShared<T, Context, Shared>,
{
    type OnCommandHandler<'a> = OnCmd::OnCommandHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_command<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: crate::agent_lifecycle::HandlerContext<Context>,
        value: &T,
    ) -> Self::OnCommandHandler<'a> {
        self.on_command.on_command(shared, handler_context, value)
    }
}

impl<Context, Shared, T, OnCmd> StatefulCommandLaneLifecycle<Context, Shared, T, OnCmd> {
    /// Replace the `on_command` handler with another derived from a closure.
    pub fn on_command<F, B>(
        self,
        f: F,
    ) -> StatefulCommandLaneLifecycle<Context, Shared, T, BorrowHandler<F, B>>
    where
        B: ?Sized,
        T: Borrow<B>,
        BorrowHandler<F, B>: OnCommandShared<T, Context, Shared>,
    {
        StatefulCommandLaneLifecycle {
            _value_type: Default::default(),
            on_command: BorrowHandler::new(f),
        }
    }
}
