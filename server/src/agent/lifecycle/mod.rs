// Copyright 2015-2020 SWIM.AI inc.
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

use crate::agent::AgentContext;
use futures::future::{BoxFuture, ready};
use futures::FutureExt;

pub trait AgentLifecycle<Agent> {
    fn on_start<'a, C: AgentContext<Agent>>(&'a self, context: &'a C) -> BoxFuture<'a, ()>;
}

impl<Agent> AgentLifecycle<Agent> for () {
    fn on_start<'a, C: AgentContext<Agent>>(&'a self, _context: &'a C) -> BoxFuture<'a, ()> {
        ready(()).boxed()
    }
}
