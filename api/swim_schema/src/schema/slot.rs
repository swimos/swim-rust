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

use crate::schema::{combine_orderings, FieldMatchResult, FieldSchema, Schema, StandardSchema};
use std::cmp::Ordering;
use swim_model::{Item, ToValue, Value};

/// Schema for Recon slots.
#[derive(Clone, Debug, PartialEq)]
pub struct SlotSchema {
    key_schema: StandardSchema,
    value_schema: StandardSchema,
}

impl SlotSchema {
    /// Create an slot schema.
    /// # Arguments
    ///
    /// * `key` - Schema for the key of the attribute.
    /// * `value` - Schema for the value of the attribute.
    ///
    pub fn new(key: StandardSchema, value: StandardSchema) -> Self {
        SlotSchema {
            key_schema: key,
            value_schema: value,
        }
    }
}

impl PartialOrd for SlotSchema {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.eq(other) {
            Some(Ordering::Equal)
        } else {
            let key = self.key_schema.partial_cmp(&other.key_schema)?;
            let val = self.value_schema.partial_cmp(&other.value_schema)?;

            combine_orderings(key, val)
        }
    }
}

impl ToValue for SlotSchema {
    fn to_value(&self) -> Value {
        Value::of_attr((
            "slot",
            Value::from_vec(vec![
                Item::slot("key", self.key_schema.to_value()),
                Item::slot("value", self.value_schema.to_value()),
            ]),
        ))
    }
}

impl FieldSchema<Item> for SlotSchema {
    fn matches_field(&self, field: &Item) -> FieldMatchResult {
        match field {
            Item::ValueItem(_) => FieldMatchResult::KeyFailed,
            Item::Slot(key, value) => {
                if self.key_schema.matches(key) {
                    if self.value_schema.matches(value) {
                        FieldMatchResult::Both
                    } else {
                        FieldMatchResult::KeyOnly
                    }
                } else {
                    FieldMatchResult::KeyFailed
                }
            }
        }
    }
}
