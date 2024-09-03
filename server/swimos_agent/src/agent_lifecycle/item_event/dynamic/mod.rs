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

use super::ItemEvent;

pub trait DynamicAgent {
    type Borrowed<'a>: BorrowItem + 'a
    where
        Self: 'a;

    fn lane<'a>(&'a self, name: &'a str) -> Option<Self::Borrowed<'a>>;
}

pub type DynamicValueLane = ValueLane<Value>;
pub type DynamicMapLane = MapLane<Value, Value>;

pub trait BorrowItem {
    fn borrow_item(&self) -> DynamicItem<'_>;
}

pub enum DynamicItem<'a> {
    ValueLane(&'a DynamicValueLane),
    MapLane(&'a DynamicMapLane),
}

pub trait DynamicLifecycle<Context> {
    type ValueHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    type MapHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    fn value_lane<'a>(
        &'a self,
        lane_name: &'a str,
        previous: Value,
        value: &Value,
    ) -> Self::ValueHandler<'a>;

    fn map_lane<'a>(
        &'a self,
        lane_name: &'a str,
        event: MapLaneEvent<Value, Value>,
        contents: &HashMap<Value, Value>,
    ) -> Self::MapHandler<'a>;
}

pub struct DynamicLifecycleWrapper<LC>(LC);

impl<Context, LC> ItemEvent<Context> for DynamicLifecycleWrapper<LC>
where
    Context: DynamicAgent,
    LC: DynamicLifecycle<Context>,
{
    type ItemEventHandler<'a> = Coprod!(LC::ValueHandler<'a>, LC::MapHandler<'a>)
    where
        Self: 'a;

    fn item_event<'a>(
        &'a self,
        context: &Context,
        item_name: &'a str,
    ) -> Option<Self::ItemEventHandler<'a>> {
        let DynamicLifecycleWrapper(lc) = self;

        context
            .lane(item_name)
            .and_then(|lane| match lane.borrow_item() {
                DynamicItem::ValueLane(value_lane) => {
                    value_lane.read_with_prev(|maybe_prev, current| {
                        maybe_prev.map(|prev| {
                            <Self::ItemEventHandler<'a>>::inject(
                                lc.value_lane(item_name, prev, current),
                            )
                        })
                    })
                }
                DynamicItem::MapLane(map_lane) => {
                    map_lane.read_with_prev(|maybe_event, current| {
                        maybe_event.map(|event| {
                            <Self::ItemEventHandler<'a>>::inject(
                                lc.map_lane(item_name, event, current),
                            )
                        })
                    })
                }
            })
    }
}
