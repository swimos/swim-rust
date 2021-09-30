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

use crate::Value;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::hash::Hash;

/// An item that may occur in the body of record ['Value'].
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Item {
    /// An item consisting of a single ['Value'].
    ValueItem(Value),

    /// An item that is a key value pair where both are ['Value']s.
    Slot(Value, Value),
}

impl Item {
    /// Create an ['Item'] from anything that can be converted to one.
    ///
    /// #Examples
    ///
    /// ```
    /// use swim_model::{Attr, Item, Value};
    ///
    /// assert_eq!(Item::of("name"), Item::ValueItem(Value::text("name")));
    /// assert_eq!(Item::of(("key", 1)), Item::Slot(Value::text("key"), Value::Int32Value(1)));
    /// assert_eq!(Item::of((true, -1i64)), Item::Slot(Value::BooleanValue(true), Value::Int64Value(-1)));
    /// ```
    pub fn of<I: Into<Item>>(item: I) -> Item {
        item.into()
    }

    /// Create a slot ['Item'] from a pair of things that can be converted to ['Value']s.
    ///
    /// #Examples
    ///
    /// ```
    /// use swim_model::{Value, Item};
    ///
    /// assert_eq!(Item::slot("key", 1), Item::Slot(Value::text("key"), Value::Int32Value(1)));
    /// ```
    pub fn slot<K: Into<Value>, V: Into<Value>>(key: K, value: V) -> Item {
        Item::Slot(key.into(), value.into())
    }

    fn compare(&self, other: &Item) -> Ordering {
        match self {
            Item::ValueItem(v1) => match other {
                Item::ValueItem(v2) => v1.cmp(v2),
                Item::Slot(_, _) => Ordering::Greater,
            },
            Item::Slot(key1, value1) => match other {
                Item::ValueItem(_) => Ordering::Less,
                Item::Slot(key2, value2) => match key1.cmp(key2) {
                    Ordering::Equal => value1.cmp(value2),
                    ow => ow,
                },
            },
        }
    }
}

impl PartialOrd for Item {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.compare(other))
    }
}

impl Ord for Item {
    fn cmp(&self, other: &Self) -> Ordering {
        self.compare(other)
    }
}

impl<V: Into<Value>> From<V> for Item {
    fn from(v: V) -> Self {
        Item::ValueItem(v.into())
    }
}

impl<K: Into<Value>, V: Into<Value>> From<(K, V)> for Item {
    fn from(pair: (K, V)) -> Self {
        let (key, value) = pair;
        Item::Slot(key.into(), value.into())
    }
}

impl Display for Item {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Item::ValueItem(value) => write!(f, "{}", value),
            Item::Slot(key, value) => write!(f, "{}:{}", key, value),
        }
    }
}
