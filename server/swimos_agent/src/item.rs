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

use std::{borrow::Borrow, collections::HashMap};

use crate::{
    agent_model::AgentDescription,
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

pub trait MapItem<K, V, M = HashMap<K, V>>: AgentItem {
    fn init(&self, map: M);

    fn read_with_prev<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<MapLaneEvent<K, V, M>>, &M) -> R;
}

pub trait MapLikeItem<K, V, M = HashMap<K, V>> {
    type GetHandler<C>: HandlerAction<C, Completion = Option<V>> + Send + 'static
    where
        C: AgentDescription + 'static;
    type GetMapHandler<C>: HandlerAction<C, Completion = M> + Send + 'static
    where
        C: AgentDescription + 'static;

    fn get_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
        key: K,
    ) -> Self::GetHandler<C>;

    fn get_map_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
    ) -> Self::GetMapHandler<C>;
}

pub trait InspectableMapLikeItem<K, V: Borrow<B>, B: ?Sized + 'static> {
    type WithEntryHandler<'a, C, F, U>: HandlerAction<C, Completion = U> + Send + 'a
    where
        Self: 'static,
        C: AgentDescription + 'a,
        F: FnOnce(Option<&B>) -> U + Send + 'a;

    fn with_entry_handler<'a, C, F, U>(
        projection: fn(&C) -> &Self,
        key: K,
        f: F,
    ) -> Self::WithEntryHandler<'a, C, F, U>
    where
        Self: 'static,
        C: AgentDescription + 'a,
        F: FnOnce(Option<&B>) -> U + Send + 'a;
}

pub trait MutableMapLikeItem<K, V> {
    type UpdateHandler<C>: EventHandler<C> + Send + 'static
    where
        C: AgentDescription + 'static;
    type RemoveHandler<C>: EventHandler<C> + Send + 'static
    where
        C: AgentDescription + 'static;
    type ClearHandler<C>: EventHandler<C> + Send + 'static
    where
        C: AgentDescription + 'static;

    fn update_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
        key: K,
        value: V,
    ) -> Self::UpdateHandler<C>;
    fn remove_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
        key: K,
    ) -> Self::RemoveHandler<C>;
    fn clear_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
    ) -> Self::ClearHandler<C>;

    type TransformEntryHandler<'a, C, F>: EventHandler<C> + Send + 'a
    where
        Self: 'static,
        C: AgentDescription + 'a,
        F: FnOnce(Option<&V>) -> Option<V> + Send + 'a;

    fn transform_entry_handler<'a, C, F>(
        projection: fn(&C) -> &Self,
        key: K,
        f: F,
    ) -> Self::TransformEntryHandler<'a, C, F>
    where
        Self: 'static,
        C: AgentDescription + 'a,
        F: FnOnce(Option<&V>) -> Option<V> + Send + 'a;
}

pub trait ValueLikeItem<T> {
    type GetHandler<C>: HandlerAction<C, Completion = T> + Send + 'static
    where
        C: AgentDescription + 'static;

    type WithValueHandler<'a, C, F, B, U>: HandlerAction<C, Completion = U> + Send + 'a
    where
        Self: 'static,
        C: AgentDescription + 'a,
        T: Borrow<B>,
        B: ?Sized + 'static,
        F: FnOnce(&B) -> U + Send + 'a;

    fn get_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
    ) -> Self::GetHandler<C>;

    fn with_value_handler<'a, Item, C, F, B, U>(
        projection: fn(&C) -> &Self,
        f: F,
    ) -> Self::WithValueHandler<'a, C, F, B, U>
    where
        C: AgentDescription + 'a,
        T: Borrow<B>,
        B: ?Sized + 'static,
        F: FnOnce(&B) -> U + Send + 'a;
}

pub trait MutableValueLikeItem<T> {
    type SetHandler<C>: EventHandler<C> + Send + 'static
    where
        C: AgentDescription + 'static;

    fn set_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
        value: T,
    ) -> Self::SetHandler<C>;
}

/// Trait for abstracting over common functionality between Join Map and Join Value lanes.
pub trait JoinLikeItem<L> {
    /// Handler action for removing a downlink from a join lane.
    type RemoveDownlinkHandler<C>: HandlerAction<C, Completion = ()> + Send + 'static
    where
        C: AgentDescription + 'static;

    /// Create a handler that will remove a downlink from the lane and clear any entries in the
    /// underlying map.
    ///
    /// # Arguments
    /// * `projection`: a projection to the join lane.
    /// * `link_key`: a key that signifies the downlink to remove.
    fn remove_downlink_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
        link_key: L,
    ) -> Self::RemoveDownlinkHandler<C>;
}
