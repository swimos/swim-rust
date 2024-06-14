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

use self::{item_event::ItemEvent, on_init::OnInit, on_start::OnStart, on_stop::OnStop};

#[doc(hidden)]
pub mod item_event;
/// The `on_init` event is called when an agent starts. It is a simple function (rather than an event
/// handler) and is guaranteed to run before any event handlers execute. The signature of the event is
/// described by the [`on_init::OnInit`] trait.
pub mod on_init;
/// The `on_start` event handler is executed when an agent starts. The signature of the event is
/// described by the [`on_start::OnStart`] trait.
pub mod on_start;
/// The `on_stop` event handler is executed when an agent stops. No more event handlers will be executed
/// after execution of this handler stops. The signature of the event is described by the
/// [`on_stop::OnStop`] trait.
pub mod on_stop;
mod stateful;
mod utility;

/// This includes all of the required event handler traits to implement an agent lifecycle. It should not
/// be implemented directly as it it has a blanket implementation for any type that can implement it.
/// # Type Parameters
/// * `Context` - The context in which the lifecycle events run (provides access to the lanes of the agent).
pub trait AgentLifecycle<Context>:
    OnInit<Context> + OnStart<Context> + OnStop<Context> + ItemEvent<Context>
{
}

impl<L, Context> AgentLifecycle<Context> for L where
    L: OnInit<Context> + OnStart<Context> + OnStop<Context> + ItemEvent<Context>
{
}

pub use stateful::StatefulAgentLifecycle;
pub use utility::*;
