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

use swim_api::agent::{Agent, BoxAgent};

/// Adds additional methods for manipulating [`Agent`]s.
pub trait AgentExt: Agent {
    /// Boxes an agent (allowing it to be instantiated by dynamic dispatch).
    fn boxed(self) -> BoxAgent
    where
        Self: Sized + Send + 'static,
    {
        Box::new(self)
    }
}

impl<A> AgentExt for A where A: Agent {}
