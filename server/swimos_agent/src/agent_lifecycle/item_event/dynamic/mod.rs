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

use frunk::Coprod;
use swimos_model::Value;

use crate::{
    event_handler::EventHandler,
    item::{MapItem, ValueItem},
    lanes::{map::MapLaneEvent, MapLane, ValueLane},
};

/// A trait for agent models where the items exposed by the agent can be modified by its lifecycle.
pub trait DynamicAgent {
    /// The type of a borrow of an item of the agent.
    type Borrowed<'a>: BorrowItem + 'a
    where
        Self: 'a;

    /// Borrow an item of the agent, if it exists.
    /// # Arguments
    /// * `name` - The name of the item.
    fn item<'a>(&'a self, name: &'a str) -> Option<Self::Borrowed<'a>>;
}

/// A [`ValueLane] with arbitrary content.
pub type DynamicValueLane = ValueLane<Value>;
/// A [`MapLane`] with arbitrary content.
pub type DynamicMapLane = MapLane<Value, Value>;

/// A trait for borrows of an agent item.
pub trait BorrowItem {
    /// Get a reference to the item.
    fn borrow_item(&self) -> DynamicItem<'_>;
}

/// Enumeration of possible item kinds. (Currently only value lanes and map lanes are supported
/// but more can be added in future).
#[non_exhaustive]
pub enum DynamicItem<'a> {
    ValueLane(&'a DynamicValueLane),
    MapLane(&'a DynamicMapLane),
}

/// A specialized lifecycle trait to aid in the implementation of agent lifecycles for [dynamic agents](`DynamicAgent`).
pub trait DynamicLifecycle<Context> {
    /// The type of [event handler](`EventHandler`) produced in response to a value lane event.
    type ValueHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    /// The type of [event handler](`EventHandler`) produced in response to a map lane event.
    type MapHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    /// Handle an event on a value lane of the agent.
    ///
    /// # Arguments
    /// * `lane_name` - The name of the lane.
    /// * `previous` - The previous value of the lane, before the change.
    /// * `value` - The new value of the lane, after the change.
    fn value_lane<'a>(
        &'a self,
        lane_name: &'a str,
        previous: Value,
        value: &Value,
    ) -> Self::ValueHandler<'a>;

    /// Handle an event on a map lane of the agent.
    ///
    /// # Arguments
    /// * `lane_name` - The name of the lane.
    /// * `event` - The map lane event.
    /// * `contents` - The contents of the map lane, after the change.
    fn map_lane<'a>(
        &'a self,
        lane_name: &'a str,
        event: MapLaneEvent<Value, Value>,
        contents: &HashMap<Value, Value>,
    ) -> Self::MapHandler<'a>;
}

#[doc(hidden)]
pub type DynamicHandler<'a, Context, LC> = Coprod!(
    <LC as DynamicLifecycle<Context>>::ValueHandler<'a>,
    <LC as DynamicLifecycle<Context>>::MapHandler<'a>
);

/// Get an [event handler](`EventHandler`) for a pending update to an item of a [dynamic agents](`DynamicAgent`).
///
/// # Arguments
/// * `context` - The agent instance.
/// * `lifecycle` - The dynamic agent lifecycle.
/// * `item_name` - The name of the item.
pub fn dynamic_handler<'a, Context, LC>(
    context: &Context,
    lifecycle: &'a LC,
    item_name: &'a str,
) -> Option<DynamicHandler<'a, Context, LC>>
where
    Context: DynamicAgent,
    LC: DynamicLifecycle<Context>,
{
    context
        .item(item_name)
        .and_then(|lane| match lane.borrow_item() {
            DynamicItem::ValueLane(value_lane) => {
                value_lane.read_with_prev(|maybe_prev, current| {
                    maybe_prev.map(|prev| {
                        <DynamicHandler<'a, Context, LC>>::inject(
                            lifecycle.value_lane(item_name, prev, current),
                        )
                    })
                })
            }
            DynamicItem::MapLane(map_lane) => map_lane.read_with_prev(|maybe_event, current| {
                maybe_event.map(|event| {
                    <DynamicHandler<'a, Context, LC>>::inject(
                        lifecycle.map_lane(item_name, event, current),
                    )
                })
            }),
        })
}
