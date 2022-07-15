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

use std::fmt::Debug;
use std::hash::Hash;
use std::{collections::HashMap, marker::PhantomData};

use swim_utilities::routing::uri::RelativeUri;

use crate::lanes::command::{CommandLane, DoCommand};
use crate::lanes::map::MapLaneGetMap;
use crate::{
    event_handler::{EventHandler, GetAgentUri, SideEffect},
    lanes::{
        map::{MapLane, MapLaneClear, MapLaneGet, MapLaneRemove, MapLaneUpdate},
        value::{ValueLane, ValueLaneGet, ValueLaneSet},
    },
};

/// A utility class to aid in the creation of event handlers for an agent. This has no data
/// and is used to provide easy access to the agent type parameter to avoid the need for
/// explcit type ascriptios when creating handlers.
pub struct HandlerContext<Agent> {
    _agent_type: PhantomData<fn(Agent)>,
}

impl<Agent> Debug for HandlerContext<Agent> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandlerContext").finish()
    }
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
            _agent_type: self._agent_type,
        }
    }
}

impl<Agent> Copy for HandlerContext<Agent> {}

impl<Agent: 'static> HandlerContext<Agent> {
    /// Create an event handler that executes a side effect.
    pub fn effect<F, T>(&self, f: F) -> impl EventHandler<Agent, Completion = T> + Send
    where
        F: FnOnce() -> T + Send,
    {
        SideEffect::from(f)
    }

    /// Create an event handler that will fetch the metadata of the agent instance.
    pub fn get_agent_uri(
        &self,
    ) -> impl EventHandler<Agent, Completion = RelativeUri> + Send + 'static {
        GetAgentUri::default()
    }

    /// Create an event handler that will get the value of a value lane of the agent.
    ///
    /// #Arguments
    /// * `lane` - Projection to the value lane.
    pub fn get_value<T>(
        &self,
        lane: fn(&Agent) -> &ValueLane<T>,
    ) -> impl EventHandler<Agent, Completion = T> + Send + 'static
    where
        T: Clone + Send + 'static,
    {
        ValueLaneGet::new(lane)
    }

    /// Create an event handler that will set a new value into value lane of the agent.
    ///
    /// #Arguments
    /// * `lane` - Projection to the value lane.
    /// * `value` - The value to set.
    pub fn set_value<T>(
        &self,
        lane: fn(&Agent) -> &ValueLane<T>,
        value: T,
    ) -> impl EventHandler<Agent, Completion = ()> + Send + 'static
    where
        T: Send + 'static,
    {
        ValueLaneSet::new(lane, value)
    }

    /// Create an event handler that will update an entry in a map lane of the agent.
    ///
    /// #Arguments
    /// * `lane` - Projection to the map lane.
    /// * `key - The key to update.
    /// * `value` - The new value.
    pub fn update<K, V>(
        &self,
        lane: fn(&Agent) -> &MapLane<K, V>,
        key: K,
        value: V,
    ) -> impl EventHandler<Agent, Completion = ()> + Send + 'static
    where
        K: Send + Clone + Eq + Hash + 'static,
        V: Send + 'static,
    {
        MapLaneUpdate::new(lane, key, value)
    }

    /// Create an event handler that will remove an entry from a map lane of the agent.
    ///
    /// #Arguments
    /// * `lane` - Projection to the map lane.
    /// * `key - The key to remove.
    pub fn remove<K, V>(
        &self,
        lane: fn(&Agent) -> &MapLane<K, V>,
        key: K,
    ) -> impl EventHandler<Agent, Completion = ()> + Send + 'static
    where
        K: Send + Clone + Eq + Hash + 'static,
        V: Send + 'static,
    {
        MapLaneRemove::new(lane, key)
    }

    /// Create an event handler that will clear a map lane of the agent.
    ///
    /// #Arguments
    /// * `lane` - Projection to the map lane.
    pub fn clear<K, V>(
        &self,
        lane: fn(&Agent) -> &MapLane<K, V>,
    ) -> impl EventHandler<Agent, Completion = ()> + Send + 'static
    where
        K: Send + Clone + Eq + Hash + 'static,
        V: Send + 'static,
    {
        MapLaneClear::new(lane)
    }

    /// Create an event handler that will attempt to get an entry from a map lane of the agent.
    ///
    /// #Arguments
    /// * `lane` - Projection to the map lane.
    /// * `key - The key to fetch.
    pub fn get_entry<K, V>(
        &self,
        lane: fn(&Agent) -> &MapLane<K, V>,
        key: K,
    ) -> impl EventHandler<Agent, Completion = Option<V>> + Send + 'static
    where
        K: Send + Clone + Eq + Hash + 'static,
        V: Send + Clone + 'static,
    {
        MapLaneGet::new(lane, key)
    }

    /// Create an event handler that will attempt to get the entire contents of a map lane of
    /// the agent.
    ///
    /// #Arguments
    /// * `lane` - Projection to the map lane.
    pub fn get_map<K, V>(
        &self,
        lane: fn(&Agent) -> &MapLane<K, V>,
    ) -> impl EventHandler<Agent, Completion = HashMap<K, V>> + Send + 'static
    where
        K: Send + Clone + Eq + Hash + 'static,
        V: Send + Clone + 'static,
    {
        MapLaneGetMap::new(lane)
    }

    /// Create an event handler that will send a command to a commabd lane of the agent.
    ///
    /// #Arguments
    /// * `lane` - Projection to the value lane.
    /// * `value` - The value of the command.
    pub fn command<T: Send + 'static>(
        &self,
        lane: fn(&Agent) -> &CommandLane<T>,
        value: T,
    ) -> impl EventHandler<Agent, Completion = ()> + Send + 'static
    where
        T: Send + 'static,
    {
        DoCommand::new(lane, value)
    }
}