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
use std::borrow::Borrow;
use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;
use std::{collections::HashMap, marker::PhantomData};

use futures::stream::unfold;
use futures::{Future, FutureExt, Stream, StreamExt};
use swimos_form::structural::read::recognizer::RecognizerReadable;
use swimos_form::structural::write::StructuralWritable;
use swimos_form::Form;
use swimos_model::address::Address;
use swimos_utilities::routing::route_uri::RouteUri;
use tokio::time::Instant;

use crate::agent_model::downlink::hosted::{
    EventDownlinkHandle, MapDownlinkHandle, ValueDownlinkHandle,
};
use crate::agent_model::downlink::{
    OpenEventDownlinkAction, OpenMapDownlinkAction, OpenValueDownlinkAction,
};
use crate::config::{MapDownlinkConfig, SimpleDownlinkConfig};
use crate::downlink_lifecycle::event::EventDownlinkLifecycle;
use crate::downlink_lifecycle::map::MapDownlinkLifecycle;
use crate::downlink_lifecycle::value::ValueDownlinkLifecycle;
use crate::event_handler::{
    run_after, run_schedule, run_schedule_async, ConstHandler, EventHandler, GetParameter,
    HandlerActionExt, SendCommand, Sequentially, Stop, Suspend, UnitHandler,
};
use crate::event_handler::{GetAgentUri, HandlerAction, SideEffect};
use crate::item::{
    InspectableMapLikeItem, MapLikeItem, MutableMapLikeItem, MutableValueLikeItem, ValueLikeItem
};
use crate::lanes::command::{CommandLane, DoCommand};
use crate::lanes::demand::{Cue, DemandLane};
use crate::lanes::demand_map::CueKey;
use crate::lanes::join_map::JoinMapAddDownlink;
use crate::lanes::join_value::{JoinValueAddDownlink, JoinValueLane};
use crate::lanes::supply::{Supply, SupplyLane};
use crate::lanes::{DemandMapLane, JoinMapLane};

pub use self::downlink_builder::event::{
    StatefulEventDownlinkBuilder, StatelessEventDownlinkBuilder,
};
pub use self::downlink_builder::map::{StatefulMapDownlinkBuilder, StatelessMapDownlinkBuilder};
pub use self::downlink_builder::value::{
    StatefulValueDownlinkBuilder, StatelessValueDownlinkBuilder,
};
use self::join_map_builder::StatelessJoinMapLaneBuilder;
pub use self::join_value_builder::{StatefulJoinValueLaneBuilder, StatelessJoinValueLaneBuilder};

mod downlink_builder;
mod join_map_builder;
mod join_value_builder;

#[cfg(test)]
mod tests;

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
        *self
    }
}

impl<Agent> Copy for HandlerContext<Agent> {}

impl<Agent: 'static> HandlerContext<Agent> {
    /// Create an event handler that resolves to a specific value.
    pub fn value<T>(&self, val: T) -> impl HandlerAction<Agent, Completion = T> {
        ConstHandler::from(val)
    }

    /// Create an event handler that executes a side effect.
    pub fn effect<F, T>(&self, f: F) -> impl HandlerAction<Agent, Completion = T>
    where
        F: FnOnce() -> T,
    {
        SideEffect::from(f)
    }

    /// Send a command to a lane (either on a remote host or locally to an agent on the same plane).
    ///
    /// #Arguments
    /// * `host` - The target remote host or [`None`] for an agent in the same plane.
    /// * `node` - The target node hosting the lane.
    /// * `lane` - The name of the target lane.
    /// * `command` - The value to send.
    pub fn send_command<'a, S, T>(
        &self,
        host: Option<S>,
        node: S,
        lane: S,
        command: T,
    ) -> impl EventHandler<Agent> + 'a
    where
        S: AsRef<str> + 'a,
        T: StructuralWritable + 'a,
    {
        let addr = Address::new(host, node, lane);
        SendCommand::new(addr, command, true)
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

    /// Create an event handler that will get the value of a value lane store of the agent.
    ///
    /// #Arguments
    /// * `item` - Projection to the value lane or store.
    pub fn get_value<Item, T>(
        &self,
        item: fn(&Agent) -> &Item,
    ) -> impl HandlerAction<Agent, Completion = T> + Send + 'static
    where
        Item: ValueLikeItem<T>,
        T: Clone + Send + 'static,
    {
        Item::get_handler::<Agent>(item)
    }

    /// Create an event handler that will set a new value into a value lane or store of the agent.
    ///
    /// #Arguments
    /// * `item` - Projection to the value lane or store.
    /// * `value` - The value to set.
    pub fn set_value<Item, T>(
        &self,
        item: fn(&Agent) -> &Item,
        value: T,
    ) -> impl HandlerAction<Agent, Completion = ()> + Send + 'static
    where
        Item: MutableValueLikeItem<T>,
        T: Send + 'static,
    {
        Item::set_handler::<Agent>(item, value)
    }

    /// Create an event handler that will transform the value of a value lane or store of the agent.
    /// 
    /// #Arguments
    /// * `item` - Projection to the value lane or store.
    /// * `f` - A closure that produces a new value from a reference to the existing value.
    pub fn transform_value<'a, Item, T, F>(
        &self,
        item: fn(&Agent) -> &Item,
        f: F,
    ) -> impl HandlerAction<Agent, Completion = ()> + Send + 'a
    where
        Agent: 'static,
        Item: ValueLikeItem<T> + MutableValueLikeItem<T> + 'static,
        T: 'static,
        F: FnOnce(&T) -> T + Send + 'a,
    {
        Item::with_value_handler::<Item, Agent, F, T, T>(item, f)
            .and_then(move |v| Item::set_handler(item, v))
    }

    /// Create an event handler that will inspect the value of a value lane or store and generate a result from it.
    /// This differs from using [`Self::get_value`] in that it does not require a clone to be made of the existing value.
    /// 
    /// #Arguments
    /// * `item` - Projection to the value lane or store.
    /// * `f` - A closure that produces a value from a reference to the current value of the item.
    pub fn with_value<'a, Item, T, F, B, U>(
        &self,
        item: fn(&Agent) -> &Item,
        f: F,
    ) -> impl HandlerAction<Agent, Completion = U> + Send + 'a
    where
        Agent: 'static,
        Item: ValueLikeItem<T> + 'static,
        T: Borrow<B>,
        B: 'static,
        F: FnOnce(&B) -> U + Send + 'a,
    {
        Item::with_value_handler::<Item, Agent, F, B, U>(item, f)
    }

    /// Create an event handler that will update an entry in a map lane or store of the agent.
    ///
    /// #Arguments
    /// * `item` - Projection to the map lane or store.
    /// * `key - The key to update.
    /// * `value` - The new value.
    pub fn update<Item, K, V>(
        &self,
        item: fn(&Agent) -> &Item,
        key: K,
        value: V,
    ) -> impl HandlerAction<Agent, Completion = ()> + Send + 'static
    where
        Item: MutableMapLikeItem<K, V>,
        K: Send + Clone + Eq + Hash + 'static,
        V: Send + 'static,
    {
        Item::update_handler::<Agent>(item, key, value)
    }

    /// Create an event handler that will inspect an entry in the map and produce a new value from it.
    /// This differs from using [`Self::get_entry`] in that it does not require that a clone be made of the existing value.
    ///
    /// #Arguments
    /// * `item` - Projection to the map lane or store.
    /// * `key - The key to update.
    /// * `f` - A function to apple to the entry in the map.
    pub fn with_entry<'a, Item, K, V, F, B, U>(
        &self,
        item: fn(&Agent) -> &Item,
        key: K,
        f: F
    ) -> impl HandlerAction<Agent, Completion = U> + Send + 'a
    where
        Agent: 'static, 
        Item: InspectableMapLikeItem<K, V> + 'static,
        K: Send + Clone + Eq + Hash + 'static,
        V: Borrow<B> + 'static,
        B: ?Sized + 'static,
        F: FnOnce(Option<&B>) -> U + Send + 'a,
    {
        Item::with_entry_handler::<Agent, F, B, U>(item, key, f)
    }


    /// Create an event handler that will transform the value in an entry of a map lane or store of the agent.
    /// If map contains an entry with that key, it will be updated (or removed) based on the result of the calling
    /// the closure on it. If the map does not contain an entry with that key, the closure will be called with [`None`]
    /// and an entry will be inserted if it returns a value.
    ///
    /// #Arguments
    /// * `item` - Projection to the map lane.
    /// * `key - The key to update.
    /// * `f` - A closure to apply to the entry in the map to produce the replacement.
    pub fn transform_entry<'a, Item, K, V, F>(
        &self,
        item: fn(&Agent) -> &Item,
        key: K,
        f: F,
    ) -> impl HandlerAction<Agent, Completion = ()> + Send + 'a
    where
        Agent: 'static,
        Item: MutableMapLikeItem<K, V> + 'static,
        K: Send + Clone + Eq + Hash + 'static,
        V: 'static,
        F: FnOnce(Option<&V>) -> Option<V> + Send + 'a,
    {
        Item::transform_entry_handler::<Agent, F>(item, key, f)
    }

    /// Create an event handler that will remove an entry from a map lane or store of the agent.
    ///
    /// #Arguments
    /// * `item` - Projection to the map lane or store.
    /// * `key - The key to remove.
    pub fn remove<Item, K, V>(
        &self,
        item: fn(&Agent) -> &Item,
        key: K,
    ) -> impl HandlerAction<Agent, Completion = ()> + Send + 'static
    where
        Item: MutableMapLikeItem<K, V>,
        K: Send + Clone + Eq + Hash + 'static,
        V: Send + 'static,
    {
        Item::remove_handler::<Agent>(item, key)
    }

    /// Create an event handler that will clear a map lane or store of the agent.
    ///
    /// #Arguments
    /// * `item` - Projection to the map lane or store.
    pub fn clear<Item, K, V>(
        &self,
        item: fn(&Agent) -> &Item,
    ) -> impl HandlerAction<Agent, Completion = ()> + Send + 'static
    where
        Item: MutableMapLikeItem<K, V>,
        K: Send + Clone + Eq + Hash + 'static,
        V: Send + 'static,
    {
        Item::clear_handler::<Agent>(item)
    }

    /// Create an event handler that replaces the entire contents of a map lane or store.
    ///
    /// #Arguments
    /// * `item` - Projection to the map lane or store.
    /// * `entries` - The new entries for the lane.
    pub fn replace_map<Item, K, V, I>(
        &self,
        item: fn(&Agent) -> &Item,
        entries: I,
    ) -> impl HandlerAction<Agent, Completion = ()> + Send + 'static
    where
        Item: MutableMapLikeItem<K, V> + 'static,
        K: Send + Clone + Eq + Hash + 'static,
        V: Send + 'static,
        I: IntoIterator<Item = (K, V)>,
        I::IntoIter: Send + 'static,
    {
        let context = *self;
        let insertions = entries
            .into_iter()
            .map(move |(k, v)| context.update(item, k, v));
        self.clear(item).followed_by(Sequentially::new(insertions))
    }

    /// Create an event handler that will attempt to get an entry from a map-like item of the agent.
    /// This includes map lanes and stores and join lanes.
    ///
    /// #Arguments
    /// * `item` - Projection to the map-like item.
    /// * `key - The key to fetch.
    pub fn get_entry<Item, K, V>(
        &self,
        item: fn(&Agent) -> &Item,
        key: K,
    ) -> impl HandlerAction<Agent, Completion = Option<V>> + Send + 'static
    where
        Item: MapLikeItem<K, V>,
        K: Send + Clone + Eq + Hash + 'static,
        V: Send + Clone + 'static,
    {
        Item::get_handler::<Agent>(item, key)
    }

    /// Create an event handler that will attempt to get the entire contents of a map-like item of the
    /// agent. This includes map lanes and stores and join lanes.
    ///
    /// #Arguments
    /// * `item` - Projection to the map-like item.
    pub fn get_map<Item, K, V>(
        &self,
        item: fn(&Agent) -> &Item,
    ) -> impl HandlerAction<Agent, Completion = HashMap<K, V>> + Send + 'static
    where
        Item: MapLikeItem<K, V>,
        K: Send + Clone + Eq + Hash + 'static,
        V: Send + Clone + 'static,
    {
        Item::get_map_handler::<Agent>(item)
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

    /// Create an event handler that will cue a demand lane to produce a value.
    ///
    /// #Arguments
    /// * `lane` - Projection to the demand lane.
    pub fn cue<T>(
        &self,
        lane: fn(&Agent) -> &DemandLane<T>,
    ) -> impl EventHandler<Agent> + Send + 'static
    where
        T: Send + 'static,
    {
        Cue::new(lane)
    }

    /// Create an event handler that will cue a key on a demand-map lane to produce a value.
    ///
    /// #Arguments
    /// * `lane` - Projection to the demand-map lane.
    /// * `key` - The key to cue.
    pub fn cue_key<K, V>(
        &self,
        lane: fn(&Agent) -> &DemandMapLane<K, V>,
        key: K,
    ) -> impl EventHandler<Agent> + Send + 'static
    where
        K: Send + Eq + Clone + Hash + 'static,
        V: 'static,
    {
        CueKey::new(lane, key)
    }

    /// Create an event handler that will supply an event to a supply lane.
    ///
    /// #Arguments
    /// * `lane` - Projection to the supply lane.
    /// * `value` - The value to supply.
    pub fn supply<V>(
        &self,
        lane: fn(&Agent) -> &SupplyLane<V>,
        value: V,
    ) -> impl EventHandler<Agent> + Send + 'static
    where
        V: Send + 'static,
    {
        Supply::new(lane, value)
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

    /// Schedule a handler to run after a fixed delay.
    pub fn run_after<H>(
        &self,
        delay: Duration,
        handler: H,
    ) -> impl EventHandler<Agent> + Send + 'static
    where
        H: EventHandler<Agent> + Send + 'static,
    {
        run_after(delay, handler)
    }

    /// Run a (potentially infinite) sequence of handlers on a schedule. For each pair of a duration
    /// and handler produced by the iterator the handler will be scheduled to run after the delay.
    /// This is the most general scheduling handler and it will often be possible to achieve simpler
    /// schedule with [run_handlers_with_delay](HandlerAction::run_handlers_with_delay) and
    /// [schedule_repeatedly](HandlerAction::schedule_repeatedly).
    pub fn run_schedule<I, H>(&self, schedule: I) -> impl EventHandler<Agent> + Send + 'static
    where
        I: IntoIterator<Item = (Duration, H)> + 'static,
        I::IntoIter: Send + 'static,
        H: EventHandler<Agent> + Send + 'static,
    {
        run_schedule(schedule)
    }

    /// Run a (potentially infinite) sequence of futures (each resulting in an event handler).
    pub fn suspend_schedule<S, H>(&self, schedule: S) -> impl EventHandler<Agent> + Send + 'static
    where
        S: Stream<Item = H> + Send + Unpin + 'static,
        H: EventHandler<Agent> + Send + 'static,
    {
        run_schedule_async(schedule)
    }

    /// Schedule a (potentially infinite) sequence of handlers to run with a fixed delay between them.
    pub fn run_handlers_with_delay<I, H>(
        &self,
        delay: Duration,
        handlers: I,
    ) -> impl EventHandler<Agent> + Send + 'static
    where
        I: IntoIterator<Item = H> + 'static,
        I::IntoIter: Send + 'static,
        H: EventHandler<Agent> + Send + 'static,
    {
        self.run_schedule(handlers.into_iter().map(move |h| (delay, h)))
    }

    /// Schedule a (potentially infinite) sequence of futures (each resulting in an event handler) to
    /// run with a fixed delay between them. The delay is computed when the future starts executing so if
    /// a futures takes longer than the delay to complete, the next will start immediately.
    pub fn suspend_handlers_with_delay<I, F, H>(
        &self,
        delay: Duration,
        futures: I,
    ) -> impl EventHandler<Agent> + Send + 'static
    where
        I: IntoIterator<Item = F> + 'static,
        I::IntoIter: Send + 'static,
        F: Future<Output = Option<H>> + Send + 'static,
        H: EventHandler<Agent> + Send + 'static,
    {
        let stream = unfold(
            (futures.into_iter(), Instant::now()),
            move |(mut it, next_at)| async move {
                if let Some(f) = it.next() {
                    tokio::time::sleep_until(next_at).await;
                    let next_time = Instant::now() + delay;
                    f.await.map(|h| (h, (it, next_time)))
                } else {
                    None
                }
            },
        );
        self.suspend_schedule(stream.boxed())
    }

    /// Schedule a sequence of handlers, generated by a closure, to execute with a fixed delay between them.
    pub fn schedule_repeatedly<H, F>(
        &self,
        delay: Duration,
        f: F,
    ) -> impl EventHandler<Agent> + Send + 'static
    where
        F: FnMut() -> Option<H> + Send + 'static,
        H: EventHandler<Agent> + Send + 'static,
    {
        self.run_handlers_with_delay(delay, std::iter::from_fn(f))
    }

    /// Schedule a sequences of futures (each resulting in an event handler) to execute with a fixed delay
    /// between them. The delay is computed when the future starts executing so if a futures takes longer
    /// than the delay to complete, the next will start immediately.
    pub fn suspend_repeatedly<H, Fut, F>(
        &self,
        delay: Duration,
        f: F,
    ) -> impl EventHandler<Agent> + Send + 'static
    where
        F: FnMut() -> Option<Fut> + Send + 'static,
        Fut: Future<Output = Option<H>> + Send + 'static,
        H: EventHandler<Agent> + Send + 'static,
    {
        self.suspend_handlers_with_delay(delay, std::iter::from_fn(f))
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
    ) -> impl HandlerAction<Agent, Completion = EventDownlinkHandle> + Send + 'static
    where
        T: Form + Send + Sync + 'static,
        LC: EventDownlinkLifecycle<T, Agent> + Send + 'static,
        T::Rec: Send,
    {
        OpenEventDownlinkAction::new(Address::text(host, node, lane), lifecycle, config, false)
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

    /// Add a downlink to a Join Map lane. All key-value pairs received on the downlink will be set into the
    /// map state of the lane.
    ///
    /// #Arguments
    /// * `lane` - Projection to the lane.
    /// * `link_key - A key to identify the link.
    /// * `host` - The remote host at which the agent resides (a local agent if not specified).
    /// * `node` - The node URI of the agent.
    /// * `lane_uri` - The lane to downlink from.
    pub fn add_map_downlink<L, K, V>(
        &self,
        lane: fn(&Agent) -> &JoinMapLane<L, K, V>,
        link_key: L,
        host: Option<&str>,
        node: &str,
        lane_uri: &str,
    ) -> impl HandlerAction<Agent, Completion = ()> + Send + 'static
    where
        L: Any + Clone + Eq + Hash + Send + 'static,
        K: Any + Form + Clone + Eq + Hash + Send + Ord + 'static,
        V: Form + Send + 'static,
        K::Rec: Send,
        V::BodyRec: Send,
    {
        let address = Address::text(host, node, lane_uri);
        JoinMapAddDownlink::new(lane, link_key, address)
    }

    /// Causes the agent to stop. If this is encountered during the `on_start` event of an agent it will
    /// fail to start at all. Otherwise, execution of the event handler will terminate and the agent will
    /// begin to shutdown. The 'on_stop' handler will still be run. If a stop is requested in
    /// the 'on_stop' handler, the agent will stop immediately.
    pub fn stop(&self) -> impl EventHandler<Agent> + Send + 'static {
        HandlerActionExt::<Agent>::discard(Stop)
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

pub struct JoinMapContext<Agent, L, K, V> {
    _type: PhantomData<fn(&Agent, L, K, V)>,
}

impl<Agent, L, K, V> Default for JoinMapContext<Agent, L, K, V> {
    fn default() -> Self {
        Self {
            _type: Default::default(),
        }
    }
}

impl<Agent, L, K, V> JoinMapContext<Agent, L, K, V>
where
    Agent: 'static,
    L: Any + Clone + Eq + Hash + Send + 'static,
    K: Any + Form + Clone + Eq + Hash + Ord + Send + 'static,
    V: Form + Send + 'static,
    K::Rec: Send,
    V::BodyRec: Send,
{
    /// Creates a builder to construct a lifecycle for the downlinks of a [`JoinMapLane`].
    pub fn builder(&self) -> StatelessJoinMapLaneBuilder<Agent, L, K, V> {
        StatelessJoinMapLaneBuilder::default()
    }
}
