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

use bytes::*;
use either::Either;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt::Write;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use tokio_util::codec::Encoder;

use crate::model::blob::Blob;
use crate::model::parser::is_identifier;
use num_bigint::{BigInt, BigUint, ToBigInt};
use num_traits::sign::Signed;
use num_traits::ToPrimitive;
use std::str::FromStr;

pub mod blob;
pub mod parser;
pub mod schema;

#[cfg(test)]
mod tests;
pub mod text;

/// The core Swim model type. A recursive data type that can be represented in text as a Recon
/// document.
#[derive(Clone, Debug)]
pub enum Value {
    /// A defined but empty value.
    Extant,

    /// A 32-bit integer wrapped as a [`Value`].
    Int32Value(i32),

    /// A 64-bit integer wrapped as a [`Value`].
    Int64Value(i64),

    /// A 64-bit floating point number wrapped as a [`Value`].
    Float64Value(f64),

    /// A boolean wrapped as a [`Value`].
    BooleanValue(bool),

    /// A textual value. A text can either be an identifier or a string literal. A literal
    /// consists of underscores, digits and most characters from the basic multilingual plane and
    /// may not start with a digit.
    ///
    /// Literals will be printed "as is" whereas string literals will be quoted and escaped using
    /// Java conventions.
    ///
    /// Additionally, the strings `true` and `false` are not identifiers.
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// use common::model::Value;
    ///
    /// assert_eq!(Value::text("an_identifier").to_string(), "an_identifier");
    /// assert_eq!(Value::text("2morrow").to_string(), r#""2morrow""#);
    /// assert_eq!(Value::text("\t\r\n").to_string(), r#""\t\r\n""#);
    /// assert_eq!(Value::text("true").to_string(), r#""true""#);
    /// ```
    ///
    Text(String),

    ///
    /// A compound [`Value`] consisting of any number of [`Attr`]s and [`Item`]s.
    ///
    Record(Vec<Attr>, Vec<Item>),

    /// A big signed integer type wrapped as a [`Value`].
    BigInt(BigInt),

    /// A big unsigned integer type wrapped as a [`Value`].
    BigUint(BigUint),

    /// A Binary Large OBject (BLOB)
    Data(Blob),
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum ValueKind {
    Extant,
    Int32,
    Int64,
    Float64,
    Boolean,
    Text,
    Record,
    BigInt,
    BigUint,
    Data,
}

impl PartialOrd for ValueKind {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.eq(other) {
            Some(Ordering::Equal)
        } else {
            match (self, other) {
                (ValueKind::Int32, ValueKind::Int64) => Some(Ordering::Less),
                (ValueKind::Int64, ValueKind::Int32) => Some(Ordering::Greater),
                _ => None,
            }
        }
    }
}

/// Trait for types that can be converted to [`Value`]s.
pub trait ToValue {
    fn to_value(&self) -> Value;
}

impl ToValue for Value {
    fn to_value(&self) -> Value {
        self.clone()
    }
}

/// Trait for types that can be reconstructed from [`Value`]s.
pub trait ReconstructFromValue: Sized {
    type Error;

    fn try_reconstruct(value: &Value) -> Result<Self, Self::Error>;
}

impl ReconstructFromValue for Value {
    type Error = ();

    fn try_reconstruct(value: &Value) -> Result<Self, Self::Error> {
        Ok(value.clone())
    }
}

impl Display for ValueKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueKind::Extant => write!(f, "Extant"),
            ValueKind::Int32 => write!(f, "Int32"),
            ValueKind::Int64 => write!(f, "Int64"),
            ValueKind::Float64 => write!(f, "Float64"),
            ValueKind::Boolean => write!(f, "Boolean"),
            ValueKind::Text => write!(f, "Text"),
            ValueKind::Record => write!(f, "Record"),
            ValueKind::BigInt => write!(f, "BigInt"),
            ValueKind::BigUint => write!(f, "BigUint"),
            ValueKind::Data => write!(f, "data"),
        }
    }
}

#[allow(clippy::float_cmp, clippy::cognitive_complexity)]
impl Value {
    /// Create a text value from anything that can be converted to a ['String'].
    pub fn text<T: ToString>(x: T) -> Value {
        Value::Text(x.to_string())
    }

    /// Create a record from a vector of ['Item']s.
    pub fn record(items: Vec<Item>) -> Value {
        Value::Record(vec![], items)
    }

    /// Create a singleton record from anything that can be converted to an ['Item'].
    pub fn singleton<I: Into<Item>>(value: I) -> Value {
        Value::record(vec![value.into()])
    }

    /// Create an empty record.
    pub fn empty_record() -> Value {
        Value::Record(vec![], vec![])
    }

    /// Create a record from a vector of anything that can be converted to ['Item']s.
    pub fn from_vec<I: Into<Item>>(items: Vec<I>) -> Value {
        Value::Record(vec![], items.into_iter().map(Item::of).collect())
    }

    /// Create a record consisting of only a single ['Attr'].
    pub fn of_attr<A: Into<Attr>>(attr: A) -> Value {
        Value::Record(vec![attr.into()], vec![])
    }

    /// Create a record from a vector of ['Attr']s.
    pub fn of_attrs(attrs: Vec<Attr>) -> Value {
        Value::Record(attrs, vec![])
    }

    fn compare(&self, other: &Self) -> Ordering {
        match self {
            Value::Data(left_len) => match other {
                Value::Data(right_len) => left_len.cmp(right_len),
                _ => Ordering::Less,
            },
            Value::Extant => match other {
                Value::Extant => Ordering::Equal,
                _ => Ordering::Greater,
            },
            Value::Int32Value(n) => match other {
                Value::Extant | Value::BooleanValue(_) => Ordering::Less,
                Value::Int32Value(m) => n.cmp(m),
                Value::Int64Value(m) => (*n as i64).cmp(m),
                Value::Float64Value(y) => {
                    if y.is_nan() {
                        Ordering::Greater
                    } else {
                        match PartialOrd::partial_cmp(&(*n as f64), y) {
                            Some(Ordering::Less) => Ordering::Less,
                            Some(Ordering::Greater) => Ordering::Greater,
                            _ => Ordering::Equal,
                        }
                    }
                }
                Value::BigInt(bi) => BigInt::from(*n).cmp(&bi),
                Value::BigUint(bi) => match BigUint::try_from(*n) {
                    Ok(n) => n.cmp(bi),
                    Err(_) => Ordering::Greater,
                },
                _ => Ordering::Greater,
            },
            Value::Int64Value(n) => match other {
                Value::Extant | Value::BooleanValue(_) => Ordering::Less,
                Value::Int32Value(m) => n.cmp(&(*m as i64)),
                Value::Int64Value(m) => n.cmp(m),
                Value::Float64Value(y) => {
                    if y.is_nan() {
                        Ordering::Greater
                    } else {
                        match PartialOrd::partial_cmp(&(*n as f64), y) {
                            Some(Ordering::Less) => Ordering::Less,
                            Some(Ordering::Greater) => Ordering::Greater,
                            _ => Ordering::Equal,
                        }
                    }
                }
                Value::BigInt(bi) => BigInt::from(*n).cmp(&bi),
                Value::BigUint(bi) => match BigUint::try_from(*n) {
                    Ok(n) => n.cmp(bi),
                    Err(_) => Ordering::Greater,
                },
                _ => Ordering::Greater,
            },
            Value::Float64Value(x) => match other {
                Value::BigInt(bi) => {
                    if x.is_nan() {
                        Ordering::Less
                    } else {
                        match bi.to_f64() {
                            Some(bi) => match x.partial_cmp(&bi) {
                                Some(Ordering::Less) => Ordering::Less,
                                Some(Ordering::Greater) => Ordering::Greater,
                                _ => Ordering::Equal,
                            },
                            None => {
                                if x.is_sign_negative() && bi.is_negative() {
                                    Ordering::Less
                                } else {
                                    Ordering::Greater
                                }
                            }
                        }
                    }
                }
                Value::BigUint(bi) => {
                    if x.is_nan() {
                        Ordering::Less
                    } else {
                        match f64::from_str(&bi.to_string()) {
                            Ok(bi) => match x.partial_cmp(&bi) {
                                Some(Ordering::Less) => Ordering::Less,
                                Some(Ordering::Greater) => Ordering::Greater,
                                _ => Ordering::Equal,
                            },
                            Err(_) => {
                                if x.is_sign_negative() {
                                    Ordering::Greater
                                } else {
                                    Ordering::Less
                                }
                            }
                        }
                    }
                }
                Value::Extant | Value::BooleanValue(_) => Ordering::Less,
                Value::Int32Value(m) => {
                    if x.is_nan() {
                        Ordering::Less
                    } else {
                        match PartialOrd::partial_cmp(x, &(*m as f64)) {
                            Some(Ordering::Less) => Ordering::Less,
                            Some(Ordering::Greater) => Ordering::Greater,
                            _ => Ordering::Equal,
                        }
                    }
                }
                Value::Int64Value(m) => {
                    if x.is_nan() {
                        Ordering::Less
                    } else {
                        match PartialOrd::partial_cmp(x, &(*m as f64)) {
                            Some(Ordering::Less) => Ordering::Less,
                            Some(Ordering::Greater) => Ordering::Greater,
                            _ => Ordering::Equal,
                        }
                    }
                }
                Value::Float64Value(y) => {
                    if x.is_nan() {
                        if y.is_nan() {
                            Ordering::Equal
                        } else {
                            Ordering::Less
                        }
                    } else if y.is_nan() {
                        Ordering::Greater
                    } else if *x == *y {
                        Ordering::Equal
                    } else if *x < *y {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                _ => Ordering::Greater,
            },
            Value::BooleanValue(p) => match other {
                Value::Extant => Ordering::Less,
                Value::BooleanValue(q) => p.cmp(q),
                _ => Ordering::Greater,
            },
            Value::Text(s) => match other {
                Value::Record(_, _) => Ordering::Greater,
                Value::Text(t) => s.cmp(t),
                _ => Ordering::Less,
            },
            Value::Record(attrs1, items1) => match other {
                Value::Record(attrs2, items2) => {
                    let first = attrs1
                        .iter()
                        .map(Either::Left)
                        .chain(items1.iter().map(Either::Right));
                    let second = attrs2
                        .iter()
                        .map(Either::Left)
                        .chain(items2.iter().map(Either::Right));
                    first.cmp(second)
                }
                _ => Ordering::Less,
            },
            Value::BigInt(bi) => match other {
                Value::Extant | Value::BooleanValue(_) => Ordering::Less,
                Value::Int32Value(m) => bi.cmp(&BigInt::from(*m)),
                Value::Int64Value(m) => bi.cmp(&BigInt::from(*m)),
                Value::Float64Value(y) => bi.cmp(&BigInt::from(*y as i64)),
                Value::BigInt(other_bi) => bi.cmp(&other_bi),
                Value::BigUint(other_bi) => match other_bi.to_bigint() {
                    Some(other_bi) => bi.cmp(&other_bi),
                    None => Ordering::Less,
                },
                _ => Ordering::Greater,
            },
            Value::BigUint(bi) => match other {
                Value::Extant | Value::BooleanValue(_) => Ordering::Less,
                Value::Int32Value(m) => match u32::try_from(*m) {
                    Ok(m) => bi.cmp(&BigUint::from(m)),
                    Err(_) => Ordering::Greater,
                },
                Value::Int64Value(m) => match u64::try_from(*m) {
                    Ok(m) => bi.cmp(&BigUint::from(m)),
                    Err(_) => Ordering::Greater,
                },
                Value::Float64Value(m) => match u64::try_from(*m as i64) {
                    Ok(m) => bi.cmp(&BigUint::from(m)),
                    Err(_) => Ordering::Greater,
                },
                Value::BigInt(other_bi) => match other_bi.to_biguint() {
                    Some(other_bi) => bi.cmp(&other_bi),
                    None => Ordering::Less,
                },
                Value::BigUint(other_bi) => bi.cmp(&other_bi),
                _ => Ordering::Greater,
            },
        }
    }

    pub fn kind(&self) -> ValueKind {
        match self {
            Value::Extant => ValueKind::Extant,
            Value::Int32Value(_) => ValueKind::Int32,
            Value::Int64Value(_) => ValueKind::Int64,
            Value::Float64Value(_) => ValueKind::Float64,
            Value::BooleanValue(_) => ValueKind::Boolean,
            Value::Text(_) => ValueKind::Text,
            Value::Record(_, _) => ValueKind::Record,
            Value::BigInt(_) => ValueKind::BigInt,
            Value::BigUint(_) => ValueKind::BigUint,
            Value::Data(_) => ValueKind::Data,
        }
    }

    pub fn prepend(self, attr: Attr) -> Value {
        match self {
            Value::Record(mut attrs, items) => {
                attrs.insert(0, attr);
                Value::Record(attrs, items)
            }
            ow => Value::Record(vec![attr], vec![Item::ValueItem(ow)]),
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Extant
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Value::Data(mb) => match other {
                Value::Data(tb) => mb.eq(tb),
                _ => false,
            },
            Value::Extant => match other {
                Value::Extant => true,
                _ => false,
            },
            Value::Int32Value(n) => match other {
                Value::Int32Value(m) => n == m,
                _ => false,
            },
            Value::Int64Value(n) => match other {
                Value::Int64Value(m) => n == m,
                _ => false,
            },
            Value::Float64Value(x) => match other {
                Value::Float64Value(y) => {
                    if x.is_nan() {
                        y.is_nan()
                    } else {
                        x == y
                    }
                }
                _ => false,
            },
            Value::BooleanValue(p) => match other {
                Value::BooleanValue(q) => p == q,
                _ => false,
            },
            Value::Text(s) => match other {
                Value::Text(t) => s == t,
                _ => false,
            },
            Value::Record(attrs1, items1) => match other {
                Value::Record(attrs2, items2) => attrs1 == attrs2 && items1 == items2,
                _ => false,
            },
            Value::BigInt(left) => match other {
                Value::BigInt(right) => left == right,
                _ => false,
            },
            Value::BigUint(left) => match other {
                Value::BigUint(right) => left == right,
                _ => false,
            },
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.compare(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        self.compare(other)
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        const EXTANT_HASH: u8 = 0;
        const INT32_HASH: u8 = 1;
        const INT64_HASH: u8 = 2;
        const FLOAT64_HASH: u8 = 3;
        const BOOLEAN_HASH: u8 = 4;
        const TEXT_HASH: u8 = 5;
        const RECORD_HASH: u8 = 6;
        const BIGINT_HASH: u8 = 7;
        const BIGUINT_HASH: u8 = 8;
        const DATA_HASH: u8 = 9;

        match self {
            Value::Extant => {
                state.write_u8(EXTANT_HASH);
            }
            Value::Int32Value(n) => {
                state.write_u8(INT32_HASH);
                state.write_i32(*n);
            }
            Value::Int64Value(n) => {
                state.write_u8(INT64_HASH);
                state.write_i64(*n);
            }
            Value::Float64Value(x) => {
                state.write_u8(FLOAT64_HASH);
                if x.is_nan() {
                    state.write_u64(0);
                } else {
                    state.write_u64(x.to_bits());
                }
            }
            Value::BooleanValue(p) => {
                state.write_u8(BOOLEAN_HASH);
                state.write_u8(if *p { 1 } else { 0 })
            }
            Value::Text(s) => {
                state.write_u8(TEXT_HASH);
                s.hash(state);
            }
            Value::Record(attrs, items) => {
                state.write_u8(RECORD_HASH);
                attrs.hash(state);
                items.hash(state);
            }
            Value::BigInt(bi) => {
                state.write_u8(BIGINT_HASH);
                bi.hash(state);
            }
            Value::BigUint(bi) => {
                state.write_u8(BIGUINT_HASH);
                bi.hash(state);
            }
            Value::Data(b) => {
                state.write_u8(DATA_HASH);
                b.hash(state);
            }
        }
    }
}

impl From<i32> for Value {
    fn from(n: i32) -> Self {
        Value::Int32Value(n)
    }
}

impl From<i64> for Value {
    fn from(n: i64) -> Self {
        Value::Int64Value(n)
    }
}

impl From<f64> for Value {
    fn from(x: f64) -> Self {
        Value::Float64Value(x)
    }
}

impl From<bool> for Value {
    fn from(p: bool) -> Self {
        Value::BooleanValue(p)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Text(s)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Text(s.to_owned())
    }
}

impl From<BigInt> for Value {
    fn from(bi: BigInt) -> Self {
        Value::BigInt(bi)
    }
}

impl From<BigUint> for Value {
    fn from(bi: BigUint) -> Self {
        Value::BigUint(bi)
    }
}

/// An attribute that can be applied to a record ['Value']. A key value pair where the key is
/// a ['String'] and the value can be any ['Value'].
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Attr {
    pub name: String,
    pub value: Value,
}

impl Attr {
    /// Create an ['Attr'] from anything that can be converted to one.
    ///
    /// #Examples
    ///
    /// ```
    /// use common::model::{Attr, Value};
    ///
    /// assert_eq!(Attr::of("name"), Attr { name: String::from("name"), value: Value::Extant, });
    /// assert_eq!(Attr::of(("key", 1)), Attr { name: String::from("key"), value: Value::Int32Value(1), });
    /// ```
    pub fn of<T: Into<Attr>>(rep: T) -> Attr {
        rep.into()
    }

    /// Create an [`Attr`] with a specified named and value.
    ///
    /// #Examples
    ///
    /// ```
    /// use common::model::{Attr, Value};
    ///
    /// assert_eq!(Attr::with_value("name", 1), Attr { name: String::from("name"), value: Value::Int32Value(1), });
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
    /// use common::model::{Attr, Value, Item};
    ///
    /// assert_eq!(
    ///     Attr::with_field("name", "inner", 1),
    ///     Attr {
    ///         name: String::from("name"),
    ///         value: Value::Record(vec![], vec![Item::Slot(Value::Text(String::from("inner")), Value::Int32Value(1))]),
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
    /// use common::model::{Attr, Value, Item};
    ///
    /// assert_eq!(
    ///     Attr::with_item("name", 1),
    ///     Attr {
    ///         name: String::from("name"),
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
    /// use common::model::{Attr, Value, Item};
    ///
    /// assert_eq!(
    ///     Attr::with_items("name", vec![0, 1]),
    ///     Attr {
    ///         name: String::from("name"),
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

impl Into<Value> for Attr {
    fn into(self) -> Value {
        Value::Record(vec![self], vec![])
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
            name: s.to_owned(),
            value: Value::Extant,
        }
    }
}

impl From<String> for Attr {
    fn from(name: String) -> Self {
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
            name: name_str.to_owned(),
            value: v.into(),
        }
    }
}

impl<V: Into<Value>> From<(String, V)> for Attr {
    fn from(pair: (String, V)) -> Self {
        let (name, v) = pair;
        Attr {
            name,
            value: v.into(),
        }
    }
}

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
    /// use common::model::{Attr, Item, Value};
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
    /// use common::model::{Value, Item};
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

fn write_string_literal(literal: &str, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
    if parser::is_identifier(literal) {
        f.write_str(literal)
    } else if needs_escape(literal) {
        write!(f, "\"{}\"", escape_text(literal))
    } else {
        write!(f, "\"{}\"", literal)
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Value::Data(b) => write!(f, "{}", b),
            Value::Extant => f.write_str(""),
            Value::Int32Value(n) => write!(f, "{}", n),
            Value::Int64Value(n) => write!(f, "{}", n),
            Value::Float64Value(x) => write!(f, "{:e}", x),
            Value::BooleanValue(p) => write!(f, "{}", p),
            Value::Text(s) => write_string_literal(s, f),
            Value::Record(attrs, body) => {
                if attrs.is_empty() && body.is_empty() {
                    f.write_str("{}")
                } else {
                    for attr in attrs {
                        write!(f, "{}", attr)?;
                    }
                    if !body.is_empty() {
                        f.write_str("{")?;
                        let mut first = true;
                        for elem in body.iter() {
                            if !first {
                                f.write_str(",")?;
                            }
                            write!(f, "{}", elem)?;
                            first = false;
                        }
                        f.write_str("}")
                    } else {
                        Result::Ok(())
                    }
                }
            }
            Value::BigInt(bi) => write!(f, "{}", bi),
            Value::BigUint(bi) => write!(f, "{}", bi),
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
                write_string_literal(&self.name, f)?;
                match body.first() {
                    Some(slot @ Item::Slot(_, _)) => write!(f, "({})", slot),
                    _ => write!(f, "({})", &self.value),
                }
            }
            Value::Extant => {
                f.write_str("@")?;
                write_string_literal(&self.name, f)
            }
            ow => {
                f.write_str("@")?;
                write_string_literal(&self.name, f)?;
                write!(f, "({})", ow)
            }
        }
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

fn needs_escape(text: &str) -> bool {
    text.chars().any(|c| c < '\u{20}' || c == '"' || c == '\\')
}

static DIGITS: [char; 16] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
];

fn escape_text(text: &str) -> String {
    let mut output = Vec::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '"' => {
                output.push('\\');
                output.push('\"');
            }
            '\\' => {
                output.push('\\');
                output.push('\\');
            }
            '\r' => {
                output.push('\\');
                output.push('r');
            }
            '\n' => {
                output.push('\\');
                output.push('n');
            }
            '\t' => {
                output.push('\\');
                output.push('t');
            }
            '\u{08}' => {
                output.push('\\');
                output.push('b');
            }
            '\u{0c}' => {
                output.push('\\');
                output.push('f');
            }
            cp if cp < '\u{20}' => {
                let n = cp as usize;
                output.push('\\');
                output.push('u');
                output.push(DIGITS[(n >> 12) & 0xf]);
                output.push(DIGITS[(n >> 8) & 0xf]);
                output.push(DIGITS[(n >> 4) & 0xf]);
                output.push(DIGITS[n & 0xf]);
            }
            _ => output.push(c),
        }
    }
    output.iter().collect()
}

fn encode_escaped(s: &str, dst: &mut BytesMut) -> Result<(), std::io::Error> {
    let mut from = 0;
    let bytes = s.as_bytes();
    let mut put_acc = |dst: &mut BytesMut, off: usize| {
        if off > from {
            dst.put(&bytes[from..off]);
            from = off + 1;
        }
    };
    s.char_indices().for_each(|(off, c)| {
        match c {
            '"' => {
                put_acc(dst, off);
                dst.put(b"\\\"".as_ref());
            }
            '\\' => {
                put_acc(dst, off);
                dst.put(b"\\\\".as_ref())
            }
            '\r' => {
                put_acc(dst, off);
                dst.put(b"\\r".as_ref())
            }
            '\n' => {
                put_acc(dst, off);
                dst.put(b"\\n".as_ref())
            }
            '\t' => {
                put_acc(dst, off);
                dst.put(b"\\t".as_ref())
            }
            '\u{08}' => {
                put_acc(dst, off);
                dst.put(b"\\b".as_ref())
            }
            '\u{0c}' => {
                put_acc(dst, off);
                dst.put(b"\\f".as_ref())
            }
            cp if cp < '\u{20}' => {
                put_acc(dst, off);
                let n = cp as usize;
                dst.put(b"\\u".as_ref());
                dst.put_u8(DIGITS[(n >> 12) & 0xf] as u8);
                dst.put_u8(DIGITS[(n >> 8) & 0xf] as u8);
                dst.put_u8(DIGITS[(n >> 4) & 0xf] as u8);
                dst.put_u8(DIGITS[n & 0xf] as u8);
            }
            _ => {}
        };
    });
    put_acc(dst, bytes.len());
    Ok(())
}

///
/// Encodes [`Value`]s as bytes using a compact UTF-8 recon formatting.
///
pub struct ValueEncoder {}

const TRUE: &[u8] = b"true";
const FALSE: &[u8] = b"false";

fn unpack_attr_body(attrs: &[Attr], items: &[Item]) -> bool {
    if !attrs.is_empty() {
        false
    } else if items.len() > 1 {
        true
    } else {
        match items.first() {
            Some(item) => match item {
                Item::Slot(_, _) => true,
                _ => false,
            },
            _ => false,
        }
    }
}

fn encode_attr(
    encoder: &mut ValueEncoder,
    attr: Attr,
    dst: &mut BytesMut,
) -> Result<(), ValueEncodeErr> {
    dst.put_u8(b'@');
    ValueEncoder::encode_text(dst, &attr.name)?;
    if attr.value != Value::Extant {
        dst.put_u8(b'(');
        match attr.value {
            Value::Record(attrs, items) if unpack_attr_body(&attrs, &items) => {
                encoder.encode_items(dst, items)?
            }
            ow => encoder.encode(ow, dst)?,
        }
        dst.put_u8(b')');
    };
    Ok(())
}

pub enum ValueEncodeErr {
    IoErr(std::io::Error),
    FormatErr(std::fmt::Error),
}

impl From<std::io::Error> for ValueEncodeErr {
    fn from(e: std::io::Error) -> Self {
        ValueEncodeErr::IoErr(e)
    }
}

impl From<std::fmt::Error> for ValueEncodeErr {
    fn from(e: std::fmt::Error) -> Self {
        ValueEncodeErr::FormatErr(e)
    }
}

impl Encoder<Value> for ValueEncoder {
    type Error = ValueEncodeErr;

    fn encode(&mut self, item: Value, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.reserve(ValueEncoder::estimate_size(&item));
        self.encode_value(item, dst)
    }
}

fn len_str_literal(s: &str) -> usize {
    s.chars()
        .map(|c| match c {
            '\\' | '\"' | '\r' | '\n' | '\t' | '\u{08}' | '\u{0c}' => 2,
            cp if cp < '\u{20}' => 6,
            _ => c.len_utf8(),
        })
        .sum::<usize>()
        + 2
}

impl Default for ValueEncoder {
    fn default() -> Self {
        Self {}
    }
}

impl ValueEncoder {
    pub fn new() -> ValueEncoder {
        ValueEncoder {}
    }

    fn encode_value(&mut self, item: Value, dst: &mut BytesMut) -> Result<(), ValueEncodeErr> {
        match item {
            Value::Data(b) => write!(dst, "{:?}", b.as_ref()).map_err(Into::into),
            Value::Extant => Ok(()),
            Value::Int32Value(n) => write!(dst, "{}", n).map_err(|e| e.into()),
            Value::Int64Value(n) => write!(dst, "{}", n).map_err(|e| e.into()),
            Value::Float64Value(x) => write!(dst, "{}", x).map_err(|e| e.into()),
            Value::BooleanValue(p) => {
                if p {
                    dst.put(TRUE);
                } else {
                    dst.put(FALSE);
                }
                Ok(())
            }
            Value::Text(s) => ValueEncoder::encode_text(dst, &s),
            Value::Record(attrs, items) => {
                if attrs.is_empty() && items.is_empty() {
                    dst.put_u8(b'{');
                    dst.put_u8(b'}');
                }
                for attr in attrs {
                    encode_attr(self, attr, dst)?;
                }

                if !items.is_empty() {
                    dst.put_u8(b'{');
                    self.encode_items(dst, items)?;
                    dst.put_u8(b'}');
                }
                Ok(())
            }
            Value::BigInt(bi) => write!(dst, "{}", bi).map_err(|e| e.into()),
            Value::BigUint(bi) => write!(dst, "{}", bi).map_err(|e| e.into()),
        }
    }

    fn encode_text(dst: &mut BytesMut, s: &str) -> Result<(), ValueEncodeErr> {
        if parser::is_identifier(s.borrow()) {
            dst.put(s.as_bytes());
            Ok(())
        } else if needs_escape(s.borrow()) {
            dst.put_u8(b'\"');
            encode_escaped(s.borrow(), dst)?;
            dst.put_u8(b'\"');
            Ok(())
        } else {
            dst.put_u8(b'\"');
            dst.put(s.as_bytes());
            dst.put_u8(b'\"');
            Ok(())
        }
    }

    fn encode_items(&mut self, dst: &mut BytesMut, items: Vec<Item>) -> Result<(), ValueEncodeErr> {
        let mut first: bool = true;
        for item in items.into_iter() {
            if !first {
                dst.put_u8(b',');
            } else {
                first = false;
            }
            match item {
                Item::ValueItem(v) => self.encode(v, dst)?,
                Item::Slot(k, v) => {
                    self.encode(k, dst)?;
                    dst.put_u8(b':');
                    self.encode(v, dst)?
                }
            };
        }
        Ok(())
    }

    fn estimate_attr_size(attr: &Attr) -> usize {
        let mut sum: usize = 1;
        sum += if is_identifier(attr.name.borrow()) {
            attr.name.len()
        } else {
            len_str_literal(attr.name.borrow())
        };
        match &attr.value {
            Value::Extant => {}
            Value::Record(attrs, items) if unpack_attr_body(attrs, items) => {
                sum += items.len() + 1;
                for item in items.iter() {
                    match item {
                        Item::ValueItem(v) => sum += ValueEncoder::estimate_size(v),
                        Item::Slot(k, v) => {
                            sum +=
                                ValueEncoder::estimate_size(k) + ValueEncoder::estimate_size(v) + 1
                        }
                    };
                }
            }
            ow => {
                sum += 2 + ValueEncoder::estimate_size(ow);
            }
        };
        sum
    }

    fn estimate_size(value: &Value) -> usize {
        match value {
            Value::Data(b) => b.as_ref().len(),
            Value::Extant => 0,
            Value::Int32Value(n) => {
                let mut a = (*n).abs();
                let mut i = 0;
                while a > 0 {
                    a /= 10;
                    i += 1;
                }
                if *n < 0 {
                    i + 1
                } else {
                    i
                }
            }
            Value::Int64Value(n) => {
                let mut a = (*n).abs();
                let mut i = 0;
                while a > 0 {
                    a /= 10;
                    i += 1;
                }
                if *n < 0 {
                    i + 1
                } else {
                    i
                }
            }
            Value::Float64Value(_) => 5,
            Value::BooleanValue(_) => 10,
            Value::Text(s) => {
                if is_identifier(s.borrow()) {
                    s.len()
                } else {
                    len_str_literal(s.borrow())
                }
            }
            Value::Record(attrs, items) => {
                if attrs.is_empty() && items.is_empty() {
                    2
                } else {
                    let mut sum: usize = 0;
                    for attr in attrs.iter() {
                        sum += ValueEncoder::estimate_attr_size(attr);
                    }
                    if sum == 0 || !items.is_empty() {
                        sum += 1 + items.len();
                        for item in items.iter() {
                            match item {
                                Item::ValueItem(v) => sum += ValueEncoder::estimate_size(v),
                                Item::Slot(k, v) => {
                                    sum += ValueEncoder::estimate_size(k)
                                        + ValueEncoder::estimate_size(v)
                                        + 1
                                }
                            };
                        }
                    }
                    sum
                }
            }
            Value::BigInt(bi) => {
                let req = if bi.is_negative() {
                    bi.bits() + 1
                } else {
                    bi.bits()
                };

                req as usize
            }
            Value::BigUint(bi) => {
                let req = bi.bits();
                if req > usize::max_value() as u64 {
                    panic!("Buffer overflow")
                }
                req as usize
            }
        }
    }
}
