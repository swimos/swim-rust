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

use std::{borrow::Borrow, collections::HashMap};

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

pub trait InspectableMapLikeItem<K, V> {
    type WithEntryHandler<'a, C, F, B, U>: HandlerAction<C, Completion = U> + Send + 'a
    where
        Self: 'static,
        C: 'a,
        B: ?Sized + 'static,
        V: Borrow<B>,
        F: FnOnce(Option<&B>) -> U + Send + 'a;

    fn with_entry_handler<'a, C, F, B, U>(
        projection: fn(&C) -> &Self,
        key: K,
        f: F,
    ) -> Self::WithEntryHandler<'a, C, F, B, U>
    where
        Self: 'static,
        C: 'a,
        B: ?Sized + 'static,
        V: Borrow<B>,
        F: FnOnce(Option<&B>) -> U + Send + 'a;
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

    type TransformEntryHandler<'a, C, F>: EventHandler<C> + Send + 'a
    where
        Self: 'static,
        C: 'a,
        F: FnOnce(Option<&V>) -> Option<V> + Send + 'a;

    fn transform_entry_handler<'a, C, F>(
        projection: fn(&C) -> &Self,
        key: K,
        f: F,
    ) -> Self::TransformEntryHandler<'a, C, F>
    where
        Self: 'static,
        C: 'a,
        F: FnOnce(Option<&V>) -> Option<V> + Send + 'a;
}

pub trait ValueLikeItem<T> {
    type GetHandler<C>: HandlerAction<C, Completion = T> + Send + 'static
    where
        C: 'static;

    type WithValueHandler<'a, C, F, B, U>: HandlerAction<C, Completion = U> + Send + 'a
    where
        Self: 'static,
        C: 'a,
        T: Borrow<B>,
        B: ?Sized + 'static,
        F: FnOnce(&B) -> U + Send + 'a;

    fn get_handler<C: 'static>(projection: fn(&C) -> &Self) -> Self::GetHandler<C>;

    fn with_value_handler<'a, Item, C, F, B, U>(
        projection: fn(&C) -> &Self,
        f: F,
    ) -> Self::WithValueHandler<'a, C, F, B, U>
    where
        C: 'a,
        T: Borrow<B>,
        B: ?Sized + 'static,
        F: FnOnce(&B) -> U + Send + 'a;
}

pub trait MutableValueLikeItem<T> {
    type SetHandler<C>: EventHandler<C> + Send + 'static
    where
        C: 'static;

    fn set_handler<C: 'static>(projection: fn(&C) -> &Self, value: T) -> Self::SetHandler<C>;

}
