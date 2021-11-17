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

use crate::text::Text;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::hash::Hash;

use super::write_string_literal;
use crate::{Item, Value};

/// An attribute that can be applied to a record ['Value']. A key value pair where the key is
/// a ['String'] and the value can be any ['Value'].
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct Attr {
    pub name: Text,
    pub value: Value,
}

impl Attr {
    /// Create an ['Attr'] from anything that can be converted to one.
    ///
    /// #Examples
    ///
    /// ```
    /// use swim_model::{Attr, Value};
    /// use swim_model::Text;
    ///
    /// assert_eq!(Attr::of("name"), Attr { name: Text::from("name"), value: Value::Extant, });
    /// assert_eq!(Attr::of(("key", 1)), Attr { name: Text::from("key"), value: Value::Int32Value(1), });
    /// ```
    pub fn of<T: Into<Attr>>(rep: T) -> Attr {
        rep.into()
    }

    /// Create an [`Attr`] with a specified named and value.
    ///
    /// #Examples
    ///
    /// ```
    /// use swim_model::{Attr, Value};
    /// use swim_model::Text;
    ///
    /// assert_eq!(Attr::with_value("name", 1), Attr { name: Text::from("name"), value: Value::Int32Value(1), });
    ///
    /// ```
    pub fn with_value<V: Into<Value>>(name: &str, value: V) -> Attr {
        Attr::of((name, value))
    }

    /// Create an [`Attr`] containing a record with a single slot.
    ///
    /// #Examples
    ///
    /// ```
    /// use swim_model::{Attr, Value, Item};
    /// use swim_model::Text;
    ///
    /// assert_eq!(
    ///     Attr::with_field("name", "inner", 1),
    ///     Attr {
    ///         name: Text::from("name"),
    ///         value: Value::Record(vec![], vec![Item::Slot(Value::Text(Text::from("inner")), Value::Int32Value(1))]),
    ///     }
    /// );
    ///
    /// ```
    pub fn with_field<V: Into<Value>>(name: &str, field_name: &str, value: V) -> Attr {
        Attr::of((name, Value::from_vec(vec![(field_name, value)])))
    }

    /// Create an [`Attr`] containing a record with a single item.
    ///
    /// #Examples
    ///
    /// ```
    /// use swim_model::{Attr, Value, Item};
    /// use swim_model::Text;
    ///
    /// assert_eq!(
    ///     Attr::with_item("name", 1),
    ///     Attr {
    ///         name: Text::from("name"),
    ///         value: Value::Record(vec![], vec![Item::ValueItem(Value::Int32Value(1))]),
    ///     }
    /// );
    ///
    /// ```
    pub fn with_item<I: Into<Item>>(name: &str, item: I) -> Attr {
        Attr::of((name, Value::from_vec(vec![item])))
    }

    /// Create an [`Attr`] containing a record with a multiple items.
    ///
    /// #Examples
    ///
    /// ```
    /// use swim_model::{Attr, Value, Item};
    /// use swim_model::Text;
    ///
    /// assert_eq!(
    ///     Attr::with_items("name", vec![0, 1]),
    ///     Attr {
    ///         name: Text::from("name"),
    ///         value: Value::Record(vec![], vec![Item::ValueItem(Value::Int32Value(0)), Item::ValueItem(Value::Int32Value(1))]),
    ///     }
    /// );
    ///
    /// ```
    pub fn with_items<I: Into<Item>>(name: &str, items: Vec<I>) -> Attr {
        Attr::of((name, Value::from_vec(items)))
    }

    fn compare(&self, other: &Attr) -> Ordering {
        match self.name.cmp(&other.name) {
            Ordering::Equal => self.value.cmp(&other.value),
            ow => ow,
        }
    }
}

impl From<Attr> for Value {
    fn from(attr: Attr) -> Self {
        Value::Record(vec![attr], vec![])
    }
}

impl PartialOrd for Attr {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.compare(other))
    }
}

impl Ord for Attr {
    fn cmp(&self, other: &Self) -> Ordering {
        self.compare(other)
    }
}

impl From<&str> for Attr {
    fn from(s: &str) -> Self {
        Attr {
            name: s.into(),
            value: Value::Extant,
        }
    }
}

impl From<String> for Attr {
    fn from(name: String) -> Self {
        Attr {
            name: name.into(),
            value: Value::Extant,
        }
    }
}

impl From<Text> for Attr {
    fn from(name: Text) -> Self {
        Attr {
            name,
            value: Value::Extant,
        }
    }
}

impl<V: Into<Value>> From<(&str, V)> for Attr {
    fn from(pair: (&str, V)) -> Self {
        let (name_str, v) = pair;
        Attr {
            name: name_str.into(),
            value: v.into(),
        }
    }
}

impl<V: Into<Value>> From<(String, V)> for Attr {
    fn from(pair: (String, V)) -> Self {
        let (name, v) = pair;
        Attr {
            name: name.into(),
            value: v.into(),
        }
    }
}

impl<V: Into<Value>> From<(Text, V)> for Attr {
    fn from(pair: (Text, V)) -> Self {
        let (name, v) = pair;
        Attr {
            name,
            value: v.into(),
        }
    }
}

impl Display for Attr {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match &self.value {
            Value::Record(attrs, body) if attrs.is_empty() && body.len() > 1 => {
                write!(f, "@{}(", self.name)?;
                let mut first = true;
                for elem in body.iter() {
                    if !first {
                        f.write_str(",")?;
                    }
                    write!(f, "{}", elem)?;
                    first = false;
                }
                f.write_str(")")
            }
            Value::Record(attrs, body) if attrs.is_empty() && body.len() == 1 => {
                f.write_str("@")?;
                write_string_literal(self.name.as_str(), f)?;
                match body.first() {
                    Some(slot @ Item::Slot(_, _)) => write!(f, "({})", slot),
                    _ => write!(f, "({})", &self.value),
                }
            }
            Value::Extant => {
                f.write_str("@")?;
                write_string_literal(self.name.as_str(), f)
            }
            ow => {
                f.write_str("@")?;
                write_string_literal(self.name.as_str(), f)?;
                write!(f, "({})", ow)
            }
        }
    }
}
