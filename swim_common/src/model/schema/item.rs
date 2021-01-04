// Copyright 2015-2020 SWIM.AI inc.
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

use crate::model::schema::slot::SlotSchema;
use crate::model::schema::{Schema, StandardSchema};
use crate::model::{Item, ToValue, Value};
use std::cmp::Ordering;

/// Schema for Recon [`Item`]s.
#[derive(Clone, Debug, PartialEq)]
pub enum ItemSchema {
    Field(SlotSchema),
    ValueItem(StandardSchema),
}

impl PartialOrd for ItemSchema {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.eq(other) {
            Some(Ordering::Equal)
        } else {
            match (self, other) {
                (ItemSchema::Field(this_schema), ItemSchema::Field(other_schema)) => {
                    this_schema.partial_cmp(other_schema)
                }
                (ItemSchema::ValueItem(this_schema), ItemSchema::ValueItem(other_schema)) => {
                    this_schema.partial_cmp(other_schema)
                }
                _ => None,
            }
        }
    }
}

impl ToValue for ItemSchema {
    fn to_value(&self) -> Value {
        match self {
            ItemSchema::Field(slot) => slot.to_value(),
            ItemSchema::ValueItem(s) => s.to_value(),
        }
    }
}

impl Schema<Item> for ItemSchema {
    fn matches(&self, item: &Item) -> bool {
        match self {
            ItemSchema::Field(slot_schema) => slot_schema.matches(item),
            ItemSchema::ValueItem(schema) => match item {
                Item::ValueItem(value) => schema.matches(value),
                Item::Slot(_, _) => false,
            },
        }
    }
}
