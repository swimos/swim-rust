// Copyright 2015-2021 SWIM.AI inc.
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

use crate::model::schema::text::TextSchema;
use crate::model::schema::{
    combine_orderings, FieldMatchResult, FieldSchema, Schema, StandardSchema,
};
use crate::model::{Attr, Item, ToValue, Value, ValueKind};
use std::borrow::Borrow;
use std::cmp::Ordering;

/// Schema for Recon [`Attr`]s.
#[derive(Clone, Debug, PartialEq)]
pub struct AttrSchema {
    name_schema: TextSchema,
    value_schema: StandardSchema,
}

impl AttrSchema {
    /// Create an attribute schema.
    /// # Arguments
    ///
    /// * `name` - Schema for the name of the attribute.
    /// * `value` - Schema for the value of the attribute.
    ///
    pub fn new(name: TextSchema, value: StandardSchema) -> Self {
        AttrSchema {
            name_schema: name,
            value_schema: value,
        }
    }

    /// Create an attribute schema with a fixed name.
    /// # Arguments
    ///
    /// * `name` - The name of the attribute.
    /// * `value` - Schema for the value of the attribute.
    ///
    pub fn named(name: &str, value: StandardSchema) -> Self {
        AttrSchema {
            name_schema: TextSchema::exact(name),
            value_schema: value,
        }
    }

    /// Create a schema that matches attributes without bodies.
    /// # Arguments
    ///
    /// * `name` - The name of the attribute.
    ///
    pub fn tag(name: &str) -> Self {
        AttrSchema {
            name_schema: TextSchema::exact(name),
            value_schema: StandardSchema::OfKind(ValueKind::Extant),
        }
    }

    /// Creates a schema that checks the first attribute of a record against this schema
    /// the the remainder of the record against another.
    pub fn and_then(self, schema: StandardSchema) -> StandardSchema {
        StandardSchema::HeadAttribute {
            schema: Box::new(self),
            required: true,
            remainder: Box::new(schema),
        }
    }

    /// Creates a schema that checks that the value is a record containing only an attribute
    /// matching this schema.
    pub fn only(self) -> StandardSchema {
        self.and_then(StandardSchema::is_empty_record())
    }

    /// Creates a schema that optionally checks the first attribute of a record against this schema
    /// the the remainder of the record against another.
    pub fn optionally_and_then(self, schema: StandardSchema) -> StandardSchema {
        StandardSchema::HeadAttribute {
            schema: Box::new(self),
            required: false,
            remainder: Box::new(schema),
        }
    }

    pub fn to_attr(&self) -> Attr {
        Attr::of((
            "attr",
            Value::from_vec(vec![
                Item::slot("name", self.name_schema.to_value()),
                Item::slot("value", self.value_schema.to_value()),
            ]),
        ))
    }
}

impl PartialOrd for AttrSchema {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.eq(other) {
            Some(Ordering::Equal)
        } else {
            let name = self.name_schema.partial_cmp(&other.name_schema)?;
            let val = self.value_schema.partial_cmp(&other.value_schema)?;

            combine_orderings(name, val)
        }
    }
}

impl ToValue for AttrSchema {
    fn to_value(&self) -> Value {
        Value::of_attr(self.to_attr())
    }
}

impl FieldSchema<Attr> for AttrSchema {
    fn matches_field(&self, field: &Attr) -> FieldMatchResult {
        if self.name_schema.matches_str(field.name.borrow()) {
            if self.value_schema.matches(&field.value) {
                FieldMatchResult::Both
            } else {
                FieldMatchResult::KeyOnly
            }
        } else {
            FieldMatchResult::KeyFailed
        }
    }
}
