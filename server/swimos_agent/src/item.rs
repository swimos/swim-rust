// Copyright 2015-2024 Swim Inc.
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

use std::collections::HashMap;

use crate::{
    event_handler::{EventHandler, HandlerAction},
    lanes::map::MapLaneEvent,
};

/// Base trait for all agent items (lanes and stores).
pub trait AgentItem {
    /// Each item has a (unique within an agent instance) ID. This is used by the task that drives the
    /// agent to avoid keying internal hash-maps on the name.
    fn id(&self) -> u64;
}

pub trait ValueItem<T>: AgentItem {
    fn read_with_prev<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<T>, &T) -> R;

    fn init(&self, value: T);
}

pub trait MapItem<K, V>: AgentItem {
    fn init(&self, map: HashMap<K, V>);

    fn read_with_prev<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<MapLaneEvent<K, V>>, &HashMap<K, V>) -> R;
}

pub trait MapLikeItem<K, V> {
    type GetHandler<C>: HandlerAction<C, Completion = Option<V>> + Send + 'static
    where
        C: 'static;
    type GetMapHandler<C>: HandlerAction<C, Completion = HashMap<K, V>> + Send + 'static
    where
        C: 'static;

    fn get_handler<C: 'static>(projection: fn(&C) -> &Self, key: K) -> Self::GetHandler<C>;
    fn get_map_handler<C: 'static>(projection: fn(&C) -> &Self) -> Self::GetMapHandler<C>;
}

pub trait MutableMapLikeItem<K, V> {
    type UpdateHandler<C>: EventHandler<C> + Send + 'static
    where
        C: 'static;
    type RemoveHandler<C>: EventHandler<C> + Send + 'static
    where
        C: 'static;
    type ClearHandler<C>: EventHandler<C> + Send + 'static
    where
        C: 'static;

    fn update_handler<C: 'static>(
        projection: fn(&C) -> &Self,
        key: K,
        value: V,
    ) -> Self::UpdateHandler<C>;
    fn remove_handler<C: 'static>(projection: fn(&C) -> &Self, key: K) -> Self::RemoveHandler<C>;
    fn clear_handler<C: 'static>(projection: fn(&C) -> &Self) -> Self::ClearHandler<C>;
}

pub trait TransformableMapLikeItem<K, V> {
    type WithEntryHandler<'a, C, F>: EventHandler<C> + Send + 'a
    where
        Self: 'static,
        C: 'a,
        F: FnOnce(Option<V>) -> Option<V> + Send + 'a;

    fn with_handler<'a, C, F>(
        projection: fn(&C) -> &Self,
        key: K,
        f: F,
    ) -> Self::WithEntryHandler<'a, C, F>
    where
        Self: 'static,
        C: 'a,
        F: FnOnce(Option<V>) -> Option<V> + Send + 'a;
}

pub trait ValueLikeItem<T> {
    type GetHandler<C>: HandlerAction<C, Completion = T> + Send + 'static
    where
        C: 'static;

    fn get_handler<C: 'static>(projection: fn(&C) -> &Self) -> Self::GetHandler<C>;
}

pub trait MutableValueLikeItem<T> {
    type SetHandler<C>: EventHandler<C> + Send + 'static
    where
        C: 'static;

    fn set_handler<C: 'static>(projection: fn(&C) -> &Self, value: T) -> Self::SetHandler<C>;
}

/// Trait for abstracting over common functionality between Join Map and Join Value lanes.
pub trait JoinLikeItem<L> {
    /// Handler action for removing a downlink from a join lane.
    type RemoveDownlinkHandler<C>: HandlerAction<C, Completion = ()> + Send + 'static
    where
        C: 'static;

    /// Create a handler that will remove a downlink from the lane and clear any entries in the
    /// underlying map.
    ///
    /// # Arguments
    /// * `projection`: a projection to the join lane.
    /// * `link_key`: a key that signifies the downlink to remove.
    fn remove_downlink_handler<C: 'static>(
        projection: fn(&C) -> &Self,
        link_key: L,
    ) -> Self::RemoveDownlinkHandler<C>;
}
