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

use self::{lane_event::LaneEvent, on_start::OnStart, on_stop::OnStop};

pub mod lane_event;
pub mod on_start;
pub mod on_stop;
pub mod stateful;
pub mod stateless;
pub mod utility;

pub trait AgentHandlers<'a, Context>:
    OnStart<'a, Context> + OnStop<'a, Context> + LaneEvent<'a, Context>
{
}

pub trait AgentLifecycle<Context>: for<'a> AgentHandlers<'a, Context> {}

impl<L, Context> AgentLifecycle<Context> for L where L: for<'a> AgentHandlers<'a, Context> {}

impl<'a, L, Context> AgentHandlers<'a, Context> for L where
    L: OnStart<'a, Context> + OnStop<'a, Context> + LaneEvent<'a, Context>
{
}
