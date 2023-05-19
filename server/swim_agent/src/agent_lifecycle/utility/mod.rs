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

use std::any::Any;
use std::fmt::Debug;
use std::hash::Hash;
use std::{collections::HashMap, marker::PhantomData};

use futures::{Future, FutureExt};
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_form::Form;
use swim_model::address::Address;
use swim_utilities::routing::route_uri::RouteUri;

use crate::agent_model::downlink::hosted::{MapDownlinkHandle, ValueDownlinkHandle};
use crate::agent_model::downlink::{
    OpenEventDownlinkAction, OpenMapDownlinkAction, OpenValueDownlinkAction,
};
use crate::config::{MapDownlinkConfig, SimpleDownlinkConfig};
use crate::downlink_lifecycle::event::EventDownlinkLifecycle;
use crate::downlink_lifecycle::map::MapDownlinkLifecycle;
use crate::downlink_lifecycle::value::ValueDownlinkLifecycle;
use crate::event_handler::{EventHandler, GetParameter, Suspend, UnitHandler};
use crate::lanes::command::{CommandLane, DoCommand};
use crate::lanes::join_value::{JoinValueAddDownlink, JoinValueLane};
use crate::lanes::map::MapLaneGetMap;
use crate::stores::map::{
    MapStoreClear, MapStoreGet, MapStoreGetMap, MapStoreRemove, MapStoreUpdate,
};
use crate::stores::value::{ValueStore, ValueStoreGet, ValueStoreSet};
use crate::stores::MapStore;
use crate::{
    event_handler::{GetAgentUri, HandlerAction, SideEffect},
    lanes::{
        map::{MapLane, MapLaneClear, MapLaneGet, MapLaneRemove, MapLaneUpdate},
        value::{ValueLane, ValueLaneGet, ValueLaneSet},
    },
};

pub use self::downlink_builder::event::{
    StatefulEventDownlinkBuilder, StatelessEventDownlinkBuilder,
};
pub use self::downlink_builder::map::{StatefulMapDownlinkBuilder, StatelessMapDownlinkBuilder};
pub use self::downlink_builder::value::{
    StatefulValueDownlinkBuilder, StatelessValueDownlinkBuilder,
};
pub use self::join_value_builder::{StatefulJoinValueLaneBuilder, StatelessJoinValueLaneBuilder};

mod downlink_builder;
mod join_value_builder;

/// A utility class to aid in the creation of event handlers for an agent. This has no data
/// and is used to provide easy access to the agent type parameter to avoid the need for
/// explicit type ascriptions when creating handlers.
pub struct HandlerContext<Agent> {
    _agent_type: PhantomData<fn(Agent)>,
}

impl<Agent> Debug for HandlerContext<Agent> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandlerContext").finish()
    }
}

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
    pub fn effect<F, T>(&self, f: F) -> impl HandlerAction<Agent, Completion = T>
    where
        F: FnOnce() -> T,
    {
        SideEffect::from(f)
    }

    /// Create an event handler that will fetch the metadata of the agent instance.
    pub fn get_agent_uri(
        &self,
    ) -> impl HandlerAction<Agent, Completion = RouteUri> + Send + 'static {
        GetAgentUri::default()
    }

    /// Get the value of a parameter extracted from the route URI of the agent instance.
    /// #Arguments
    /// * `name` - The name of the parameter.
    pub fn get_parameter<'a>(
        &self,
        name: &'a str,
    ) -> impl HandlerAction<Agent, Completion = Option<String>> + Send + 'a {
        GetParameter::new(name)
    }

    /// Create an event handler that will get the value of a value lane of the agent.
    ///
    /// #Arguments
    /// * `lane` - Projection to the value lane.
    pub fn get_value<T>(
        &self,
        lane: fn(&Agent) -> &ValueLane<T>,
    ) -> impl HandlerAction<Agent, Completion = T> + Send + 'static
    where
        T: Clone + Send + 'static,
    {
        ValueLaneGet::new(lane)
    }

    /// Create an event handler that will get the value of a value store of the agent.
    ///
    /// #Arguments
    /// * `store` - Projection to the value store.
    pub fn get_store_value<T>(
        &self,
        store: fn(&Agent) -> &ValueStore<T>,
    ) -> impl HandlerAction<Agent, Completion = T> + Send + 'static
    where
        T: Clone + Send + 'static,
    {
        ValueStoreGet::new(store)
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
    ) -> impl HandlerAction<Agent, Completion = ()> + Send + 'static
    where
        T: Send + 'static,
    {
        ValueLaneSet::new(lane, value)
    }

    /// Create an event handler that will set a new value into value store of the agent.
    ///
    /// #Arguments
    /// * `store` - Projection to the value store.
    /// * `value` - The value to set.
    pub fn set_store_value<T>(
        &self,
        store: fn(&Agent) -> &ValueStore<T>,
        value: T,
    ) -> impl HandlerAction<Agent, Completion = ()> + Send + 'static
    where
        T: Send + 'static,
    {
        ValueStoreSet::new(store, value)
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
    ) -> impl HandlerAction<Agent, Completion = ()> + Send + 'static
    where
        K: Send + Clone + Eq + Hash + 'static,
        V: Send + 'static,
    {
        MapLaneUpdate::new(lane, key, value)
    }

    /// Create an event handler that will update an entry in a map store of the agent.
    ///
    /// #Arguments
    /// * `store` - Projection to the map store.
    /// * `key - The key to update.
    /// * `value` - The new value.
    pub fn update_store<K, V>(
        &self,
        store: fn(&Agent) -> &MapStore<K, V>,
        key: K,
        value: V,
    ) -> impl HandlerAction<Agent, Completion = ()> + Send + 'static
    where
        K: Send + Clone + Eq + Hash + 'static,
        V: Send + 'static,
    {
        MapStoreUpdate::new(store, key, value)
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
    ) -> impl HandlerAction<Agent, Completion = ()> + Send + 'static
    where
        K: Send + Clone + Eq + Hash + 'static,
        V: Send + 'static,
    {
        MapLaneRemove::new(lane, key)
    }

    /// Create an event handler that will remove an entry from a map store of the agent.
    ///
    /// #Arguments
    /// * `store` - Projection to the map store.
    /// * `key - The key to remove.
    pub fn remove_store<K, V>(
        &self,
        store: fn(&Agent) -> &MapStore<K, V>,
        key: K,
    ) -> impl HandlerAction<Agent, Completion = ()> + Send + 'static
    where
        K: Send + Clone + Eq + Hash + 'static,
        V: Send + 'static,
    {
        MapStoreRemove::new(store, key)
    }

    /// Create an event handler that will clear a map lane of the agent.
    ///
    /// #Arguments
    /// * `lane` - Projection to the map lane.
    pub fn clear<K, V>(
        &self,
        lane: fn(&Agent) -> &MapLane<K, V>,
    ) -> impl HandlerAction<Agent, Completion = ()> + Send + 'static
    where
        K: Send + Clone + Eq + Hash + 'static,
        V: Send + 'static,
    {
        MapLaneClear::new(lane)
    }

    /// Create an event handler that will clear a map store of the agent.
    ///
    /// #Arguments
    /// * `store` - Projection to the map store.
    pub fn clear_store<K, V>(
        &self,
        store: fn(&Agent) -> &MapStore<K, V>,
    ) -> impl HandlerAction<Agent, Completion = ()> + Send + 'static
    where
        K: Send + Clone + Eq + Hash + 'static,
        V: Send + 'static,
    {
        MapStoreClear::new(store)
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
    ) -> impl HandlerAction<Agent, Completion = Option<V>> + Send + 'static
    where
        K: Send + Clone + Eq + Hash + 'static,
        V: Send + Clone + 'static,
    {
        MapLaneGet::new(lane, key)
    }

    /// Create an event handler that will attempt to get an entry from a map store of the agent.
    ///
    /// #Arguments
    /// * `store` - Projection to the map lane.
    /// * `key - The key to fetch.
    pub fn get_entry_store<K, V>(
        &self,
        store: fn(&Agent) -> &MapStore<K, V>,
        key: K,
    ) -> impl HandlerAction<Agent, Completion = Option<V>> + Send + 'static
    where
        K: Send + Clone + Eq + Hash + 'static,
        V: Send + Clone + 'static,
    {
        MapStoreGet::new(store, key)
    }

    /// Create an event handler that will attempt to get the entire contents of a map lane of
    /// the agent.
    ///
    /// #Arguments
    /// * `lane` - Projection to the map lane.
    pub fn get_map<K, V>(
        &self,
        lane: fn(&Agent) -> &MapLane<K, V>,
    ) -> impl HandlerAction<Agent, Completion = HashMap<K, V>> + Send + 'static
    where
        K: Send + Clone + Eq + Hash + 'static,
        V: Send + Clone + 'static,
    {
        MapLaneGetMap::new(lane)
    }

    /// Create an event handler that will attempt to get the entire contents of a map store of
    /// the agent.
    ///
    /// #Arguments
    /// * `store` - Projection to the map lane.
    pub fn get_map_store<K, V>(
        &self,
        store: fn(&Agent) -> &MapStore<K, V>,
    ) -> impl HandlerAction<Agent, Completion = HashMap<K, V>> + Send + 'static
    where
        K: Send + Clone + Eq + Hash + 'static,
        V: Send + Clone + 'static,
    {
        MapStoreGetMap::new(store)
    }

    /// Create an event handler that will send a command to a command lane of the agent.
    ///
    /// #Arguments
    /// * `lane` - Projection to the value lane.
    /// * `value` - The value of the command.
    pub fn command<T: Send + 'static>(
        &self,
        lane: fn(&Agent) -> &CommandLane<T>,
        value: T,
    ) -> impl HandlerAction<Agent, Completion = ()> + Send + 'static
    where
        T: Send + 'static,
    {
        DoCommand::new(lane, value)
    }

    /// Suspend a future to be executed by the agent task. The future must result in another
    /// event handler that will be executed by the agent upon completion.
    pub fn suspend<Fut, H>(&self, future: Fut) -> impl EventHandler<Agent> + Send + 'static
    where
        Fut: Future<Output = H> + Send + 'static,
        H: EventHandler<Agent> + 'static,
    {
        Suspend::new(future)
    }

    /// Suspend a future to be executed by the agent task.
    pub fn suspend_effect<Fut>(&self, future: Fut) -> impl EventHandler<Agent> + Send + 'static
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.suspend(future.map(|_| UnitHandler::default()))
    }

    /// Open a value downlink to a lane on another agent.
    ///
    /// #Arguments
    /// * `host` - The remote host at which the agent resides (a local agent if not specified).
    /// * `node` - The node URI of the agent.
    /// * `lane` - The lane to downlink from.
    /// * `lifecycle` - Lifecycle events for the downlink.
    /// * `config` - Configuration parameters for the downlink.
    pub fn open_value_downlink<T, LC>(
        &self,
        host: Option<&str>,
        node: &str,
        lane: &str,
        lifecycle: LC,
        config: SimpleDownlinkConfig,
    ) -> impl HandlerAction<Agent, Completion = ValueDownlinkHandle<T>> + Send + 'static
    where
        T: Form + Send + Sync + 'static,
        LC: ValueDownlinkLifecycle<T, Agent> + Send + 'static,
        T::Rec: Send,
    {
        OpenValueDownlinkAction::new(Address::text(host, node, lane), lifecycle, config)
    }

    /// Open an event downlink to a lane on another agent.
    ///
    /// #Arguments
    /// * `host` - The remote host at which the agent resides (a local agent if not specified).
    /// * `node` - The node URI of the agent.
    /// * `lane` - The lane to downlink from.
    /// * `lifecycle` - Lifecycle events for the downlink.
    /// * `config` - Configuration parameters for the downlink.
    pub fn open_event_downlink<T, LC>(
        &self,
        host: Option<&str>,
        node: &str,
        lane: &str,
        lifecycle: LC,
        config: SimpleDownlinkConfig,
    ) -> impl EventHandler<Agent> + Send + 'static
    where
        T: Form + Send + Sync + 'static,
        LC: EventDownlinkLifecycle<T, Agent> + Send + 'static,
        T::Rec: Send,
    {
        OpenEventDownlinkAction::new(Address::text(host, node, lane), lifecycle, config)
    }

    /// Open a map downlink to a lane on another agent.
    ///
    /// #Arguments
    /// * `host` - The remote host at which the agent resides (a local agent if not specified).
    /// * `node` - The node URI of the agent.
    /// * `lane` - The lane to downlink from.
    /// * `config` - Configuration parameters for the downlink.
    /// * `lifecycle` - Lifecycle events for the downlink.
    pub fn open_map_downlink<K, V, LC>(
        &self,
        host: Option<&str>,
        node: &str,
        lane: &str,
        lifecycle: LC,
        config: MapDownlinkConfig,
    ) -> impl HandlerAction<Agent, Completion = MapDownlinkHandle<K, V>> + Send + 'static
    where
        K: Form + Hash + Eq + Ord + Clone + Send + Sync + 'static,
        V: Form + Send + Sync + 'static,
        LC: MapDownlinkLifecycle<K, V, Agent> + Send + 'static,
        K::Rec: Send,
        V::Rec: Send,
    {
        OpenMapDownlinkAction::new(Address::text(host, node, lane), lifecycle, config)
    }

    /// Create a builder to construct a request to open an event downlink.
    /// #Arguments
    /// * `host` - The remote host at which the agent resides (a local agent if not specified).
    /// * `node` - The node URI of the agent.
    /// * `lane` - The lane to downlink from.
    /// * `config` - Configuration parameters for the downlink.
    pub fn event_downlink_builder<T>(
        &self,
        host: Option<&str>,
        node: &str,
        lane: &str,
        config: SimpleDownlinkConfig,
    ) -> StatelessEventDownlinkBuilder<Agent, T>
    where
        T: RecognizerReadable + Send + Sync + 'static,
        T::Rec: Send,
    {
        StatelessEventDownlinkBuilder::new(Address::text(host, node, lane), config)
    }

    /// Create a builder to construct a request to open a value downlink.
    /// #Arguments
    /// * `host` - The remote host at which the agent resides (a local agent if not specified).
    /// * `node` - The node URI of the agent.
    /// * `lane` - The lane to downlink from.
    /// * `config` - Configuration parameters for the downlink.
    pub fn value_downlink_builder<T>(
        &self,
        host: Option<&str>,
        node: &str,
        lane: &str,
        config: SimpleDownlinkConfig,
    ) -> StatelessValueDownlinkBuilder<Agent, T>
    where
        T: Form + Send + Sync + 'static,
        T::Rec: Send,
    {
        StatelessValueDownlinkBuilder::new(Address::text(host, node, lane), config)
    }

    /// Create a builder to construct a request to open a map downlink.
    /// #Arguments
    /// * `host` - The remote host at which the agent resides (a local agent if not specified).
    /// * `node` - The node URI of the agent.
    /// * `lane` - The lane to downlink from.
    /// * `config` - Configuration parameters for the downlink.
    pub fn map_downlink_builder<K, V>(
        &self,
        host: Option<&str>,
        node: &str,
        lane: &str,
        config: MapDownlinkConfig,
    ) -> StatelessMapDownlinkBuilder<Agent, K, V>
    where
        K: Form + Hash + Eq + Ord + Clone + Send + Sync + 'static,
        K::Rec: Send,
        V: Form + Send + Sync + 'static,
        V::Rec: Send,
    {
        StatelessMapDownlinkBuilder::new(Address::text(host, node, lane), config)
    }

    /// Add a downlink to a Join Value lane. All values received on the downlink will be set into the map
    /// state of the lane, using the provided key.
    ///
    /// #Arguments
    /// * `lane` - Projection to the lane.
    /// * `key - The key for the downlink.
    /// * `host` - The remote host at which the agent resides (a local agent if not specified).
    /// * `node` - The node URI of the agent.
    /// * `lane_uri` - The lane to downlink from.
    pub fn add_downlink<K, V>(
        &self,
        lane: fn(&Agent) -> &JoinValueLane<K, V>,
        key: K,
        host: Option<&str>,
        node: &str,
        lane_uri: &str,
    ) -> impl HandlerAction<Agent, Completion = ()> + Send + 'static
    where
        K: Any + Clone + Eq + Hash + Send + 'static,
        V: Form + Send + 'static,
        V::Rec: Send,
    {
        let address = Address::text(host, node, lane_uri);
        JoinValueAddDownlink::new(lane, key, address)
    }
}

pub struct JoinValueContext<Agent, K, V> {
    _type: PhantomData<fn(&Agent, K, V)>,
}

impl<Agent, K, V> Default for JoinValueContext<Agent, K, V> {
    fn default() -> Self {
        Self {
            _type: Default::default(),
        }
    }
}

impl<Agent, K, V> JoinValueContext<Agent, K, V>
where
    Agent: 'static,
    K: Any + Clone + Eq + Hash + Send + 'static,
    V: Form + Send + 'static,
    V::Rec: Send,
{
    /// Creates a builder to construct a lifecycle for the downlinks of a [`JoinValueLane`].
    pub fn builder(&self) -> StatelessJoinValueLaneBuilder<Agent, K, V> {
        StatelessJoinValueLaneBuilder::default()
    }
}
