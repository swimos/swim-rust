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

use self::{item_event::ItemEvent, on_init::OnInit, on_start::OnStart, on_stop::OnStop};

pub mod item_event;
pub mod on_init;
pub mod on_start;
pub mod on_stop;
pub mod stateful;
pub mod stateless;
pub mod utility;

/// Trait for agent lifecycles.
/// #Type Parameters
/// * `Context` - The context in which the lifecycle events run (provides access to the lanes of the agent).
pub trait AgentLifecycle<Context>:
    OnInit<Context> + OnStart<Context> + OnStop<Context> + ItemEvent<Context>
{
}

impl<L, Context> AgentLifecycle<Context> for L where
    L: OnInit<Context> + OnStart<Context> + OnStop<Context> + ItemEvent<Context>
{
}
