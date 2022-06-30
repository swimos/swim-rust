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

use swim_utilities::routing::uri::RelativeUri;

use crate::{
    event_handler::{EventHandler, GetAgentUri, SideEffect},
    lanes::{ValueLane, ValueLaneGet, ValueLaneSet},
};

pub struct HandlerContext<Agent> {
    _agent_type: PhantomData<fn(Agent)>,
}

pub struct Action;

impl<Agent> Default for HandlerContext<Agent> {
    fn default() -> Self {
        Self {
            _agent_type: Default::default(),
        }
    }
}

impl<Agent> Clone for HandlerContext<Agent> {
    fn clone(&self) -> Self {
        Self {
            _agent_type: self._agent_type.clone(),
        }
    }
}

impl<Agent> Copy for HandlerContext<Agent> {}

impl<Agent: 'static> HandlerContext<Agent> {
    pub fn effect<F, T>(&self, f: F) -> impl EventHandler<Agent, Completion = T> + Send
    where
        F: FnOnce() -> T + Send,
    {
        SideEffect::from(f)
    }

    pub fn get_agent_uri(
        &self,
    ) -> impl EventHandler<Agent, Completion = RelativeUri> + Send + 'static {
        GetAgentUri::default()
    }

    pub fn get_value<T: Clone + 'static>(
        &self,
        lane: fn(&Agent) -> &ValueLane<T>,
    ) -> impl EventHandler<Agent, Completion = T> + Send + 'static {
        ValueLaneGet::new(lane)
    }

    pub fn set_value<T: Send + 'static>(
        &self,
        lane: fn(&Agent) -> &ValueLane<T>,
        value: T,
    ) -> impl EventHandler<Agent, Completion = ()> + Send + 'static {
        ValueLaneSet::new(lane, value)
    }
}
