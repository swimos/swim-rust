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

use crate::bigint::{BigInt, BigUint, ToBigInt};
use crate::Blob;
use crate::Text;
use either::Either;
use num_traits::Signed;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use super::write_string_literal;
use crate::{num, Attr, Item};

/// The core Swim model type. A recursive data type that can be represented in text as a Recon
/// document.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    /// A defined but empty value.
    Extant,
    /// A 32-bit integer wrapped as a [`Value`].
    Int32Value(i32),

    /// A 64-bit integer wrapped as a [`Value`].
    Int64Value(i64),

    /// A 32-bit unsigned integer wrapped as a [`Value`].
    UInt32Value(u32),

    /// A 64-bit unsigned integer wrapped as a [`Value`].
    UInt64Value(u64),

    /// A 64-bit floating point number wrapped as a [`Value`].
    Float64Value(f64),

    /// A boolean wrapped as a [`Value`].
    BooleanValue(bool),

    /// A big signed integer type wrapped as a [`Value`].
    BigInt(BigInt),

    /// A big unsigned integer type wrapped as a [`Value`].
    BigUint(BigUint),

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
    /// use swim_model::Value;
    ///
    /// assert_eq!(Value::text("an_identifier").to_string(), "an_identifier");
    /// assert_eq!(Value::text("2morrow").to_string(), r#""2morrow""#);
    /// assert_eq!(Value::text("\t\r\n").to_string(), r#""\t\r\n""#);
    /// assert_eq!(Value::text("true").to_string(), r#""true""#);
    /// ```
    ///
    Text(Text),

    ///
    /// A compound [`Value`] consisting of any number of [`Attr`]s and [`Item`]s.
    ///
    Record(Vec<Attr>, Vec<Item>),

    /// A Binary Large OBject (BLOB)
    Data(Blob),
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum ValueKind {
    Extant,
    Int32,
    Int64,
    UInt32,
    UInt64,
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
                (ValueKind::Int32, ValueKind::BigInt) => Some(Ordering::Less),

                (ValueKind::Int64, ValueKind::Int32) => Some(Ordering::Greater),
                (ValueKind::Int64, ValueKind::UInt32) => Some(Ordering::Greater),
                (ValueKind::Int64, ValueKind::BigInt) => Some(Ordering::Less),

                (ValueKind::UInt32, ValueKind::Int64) => Some(Ordering::Less),
                (ValueKind::UInt32, ValueKind::UInt64) => Some(Ordering::Less),
                (ValueKind::UInt32, ValueKind::BigInt) => Some(Ordering::Less),
                (ValueKind::UInt32, ValueKind::BigUint) => Some(Ordering::Less),

                (ValueKind::UInt64, ValueKind::UInt32) => Some(Ordering::Greater),
                (ValueKind::UInt64, ValueKind::BigInt) => Some(Ordering::Less),
                (ValueKind::UInt64, ValueKind::BigUint) => Some(Ordering::Less),

                (ValueKind::BigInt, ValueKind::Int32) => Some(Ordering::Greater),
                (ValueKind::BigInt, ValueKind::Int64) => Some(Ordering::Greater),
                (ValueKind::BigInt, ValueKind::UInt32) => Some(Ordering::Greater),
                (ValueKind::BigInt, ValueKind::UInt64) => Some(Ordering::Greater),
                (ValueKind::BigInt, ValueKind::BigUint) => Some(Ordering::Greater),

                (ValueKind::BigUint, ValueKind::UInt32) => Some(Ordering::Greater),
                (ValueKind::BigUint, ValueKind::UInt64) => Some(Ordering::Greater),
                (ValueKind::BigUint, ValueKind::BigInt) => Some(Ordering::Less),

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
            ValueKind::UInt32 => write!(f, "UInt32"),
            ValueKind::UInt64 => write!(f, "UInt64"),
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

impl Value {
    /// Checks if the a [`Value`] is coercible into the [`ValueKind`] provided.
    pub fn is_coercible_to(&self, kind: ValueKind) -> bool {
        match &self {
            Value::Int32Value(n) => match &kind {
                ValueKind::Int32 => true,
                ValueKind::Int64 => true,
                ValueKind::UInt32 => u32::try_from(*n).is_ok(),
                ValueKind::UInt64 => u64::try_from(*n).is_ok(),
                ValueKind::BigUint => BigUint::try_from(*n).is_ok(),
                ValueKind::BigInt => true,
                _ => false,
            },
            Value::Int64Value(n) => match &kind {
                ValueKind::Int32 => i32::try_from(*n).is_ok(),
                ValueKind::Int64 => true,
                ValueKind::UInt32 => u32::try_from(*n).is_ok(),
                ValueKind::UInt64 => u64::try_from(*n).is_ok(),
                ValueKind::BigUint => BigUint::try_from(*n).is_ok(),
                ValueKind::BigInt => true,
                _ => false,
            },
            Value::UInt32Value(n) => match &kind {
                ValueKind::Int32 => i32::try_from(*n).is_ok(),
                ValueKind::Int64 => i64::try_from(*n).is_ok(),
                ValueKind::BigInt => BigInt::try_from(*n).is_ok(),
                ValueKind::BigUint => true,
                ValueKind::UInt32 => true,
                ValueKind::UInt64 => true,
                _ => false,
            },
            Value::UInt64Value(n) => match &kind {
                ValueKind::Int32 => i32::try_from(*n).is_ok(),
                ValueKind::Int64 => i64::try_from(*n).is_ok(),
                ValueKind::UInt32 => u32::try_from(*n).is_ok(),
                ValueKind::BigInt => BigInt::try_from(*n).is_ok(),
                ValueKind::BigUint => true,
                ValueKind::UInt64 => true,
                _ => false,
            },
            _ => self.kind() == kind,
        }
    }

    /// Create a text value from anything that can be converted to a ['String'].
    pub fn text<T: Into<Text>>(x: T) -> Value {
        Value::Text(x.into())
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
                Value::UInt32Value(x) => num::cmp_i32_u32(*n, *x),
                Value::UInt64Value(x) => num::cmp_i64_u64(*n as i64, *x),
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
                Value::BigInt(bi) => BigInt::from(*n).cmp(bi),
                Value::BigUint(bi) => match BigUint::try_from(*n) {
                    Ok(n) => n.cmp(bi),
                    Err(_) => Ordering::Less,
                },
                _ => Ordering::Greater,
            },
            Value::Int64Value(n) => match other {
                Value::Extant | Value::BooleanValue(_) => Ordering::Less,
                Value::Int32Value(m) => n.cmp(&(*m as i64)),
                Value::Int64Value(m) => n.cmp(m),
                Value::UInt32Value(x) => num::cmp_i64_u64(*n, *x as u64),
                Value::UInt64Value(x) => num::cmp_i64_u64(*n, *x),
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
                Value::BigInt(bi) => BigInt::from(*n).cmp(bi),
                Value::BigUint(bi) => match BigUint::try_from(*n) {
                    Ok(n) => n.cmp(bi),
                    Err(_) => Ordering::Less,
                },
                _ => Ordering::Greater,
            },
            Value::UInt32Value(n) => match other {
                Value::Extant | Value::BooleanValue(_) => Ordering::Less,
                Value::UInt32Value(u) => n.cmp(u),
                Value::UInt64Value(u) => (*n as u64).cmp(u),
                Value::Int32Value(x) => num::cmp_u32_i32(*n, *x),
                Value::Int64Value(x) => num::cmp_u64_i64(*n as u64, *x),
                Value::Float64Value(f) => {
                    if f.is_nan() {
                        Ordering::Greater
                    } else {
                        match PartialOrd::partial_cmp(&(*n as f64), f) {
                            Some(Ordering::Less) => Ordering::Less,
                            Some(Ordering::Greater) => Ordering::Greater,
                            _ => Ordering::Equal,
                        }
                    }
                }
                Value::BigInt(bi) => BigInt::from(*n).cmp(bi),
                Value::BigUint(bi) => match BigUint::try_from(*n) {
                    Ok(n) => n.cmp(bi),
                    Err(_) => unreachable!(),
                },
                _ => Ordering::Greater,
            },
            Value::UInt64Value(n) => match other {
                Value::Extant | Value::BooleanValue(_) => Ordering::Less,
                Value::Int32Value(m) => num::cmp_u64_i64(*n, *m as i64),
                Value::Int64Value(m) => num::cmp_u64_i64(*n, *m),
                Value::UInt32Value(x) => n.cmp(&(*x as u64)),
                Value::UInt64Value(x) => n.cmp(x),
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
                Value::BigInt(bi) => BigInt::from(*n).cmp(bi),
                Value::BigUint(bi) => match BigUint::try_from(*n) {
                    Ok(n) => n.cmp(bi),
                    Err(_) => unreachable!(),
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
                Value::UInt32Value(m) => {
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
                Value::UInt64Value(m) => {
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
                    } else if (*x - *y).abs() < f64::EPSILON {
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
                Value::UInt32Value(m) => bi.cmp(&BigInt::from(*m)),
                Value::UInt64Value(m) => bi.cmp(&BigInt::from(*m)),
                Value::Float64Value(y) => bi.cmp(&BigInt::from(*y as i64)),
                Value::BigInt(other_bi) => bi.cmp(other_bi),
                Value::BigUint(other_bi) => match other_bi.to_bigint() {
                    Some(other_bi) => bi.cmp(&other_bi),
                    None => unreachable!(),
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
                Value::UInt32Value(u) => bi.cmp(&BigUint::from(*u)),
                Value::UInt64Value(u) => bi.cmp(&BigUint::from(*u)),
                Value::Float64Value(m) => match u64::try_from(*m as i64) {
                    Ok(m) => bi.cmp(&BigUint::from(m)),
                    Err(_) => Ordering::Greater,
                },
                Value::BigInt(other_bi) => match other_bi.to_biguint() {
                    Some(other_bi) => bi.cmp(&other_bi),
                    None => Ordering::Greater,
                },
                Value::BigUint(other_bi) => bi.cmp(other_bi),
                _ => Ordering::Greater,
            },
        }
    }

    pub fn kind(&self) -> ValueKind {
        match self {
            Value::Extant => ValueKind::Extant,
            Value::Int32Value(_) => ValueKind::Int32,
            Value::Int64Value(_) => ValueKind::Int64,
            Value::UInt32Value(_) => ValueKind::UInt32,
            Value::UInt64Value(_) => ValueKind::UInt64,
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
            Value::Extant => matches!(other, Value::Extant),
            Value::Int32Value(n) => match other {
                Value::Int32Value(m) => n == m,
                Value::Int64Value(m) => i32::try_from(*m).map(|ref m| n == m).unwrap_or(false),
                Value::UInt32Value(m) => i32::try_from(*m).map(|ref m| n == m).unwrap_or(false),
                Value::UInt64Value(m) => i32::try_from(*m).map(|ref m| n == m).unwrap_or(false),
                Value::BigInt(big_m) => big_m.to_i32().map(|ref m| n == m).unwrap_or(false),
                Value::BigUint(big_m) => big_m.to_i32().map(|ref m| n == m).unwrap_or(false),
                _ => false,
            },
            Value::Int64Value(n) => match other {
                Value::Int32Value(m) => i64::try_from(*m).map(|ref m| n == m).unwrap_or(false),
                Value::Int64Value(m) => n == m,
                Value::UInt32Value(m) => i64::try_from(*m).map(|ref m| n == m).unwrap_or(false),
                Value::UInt64Value(m) => i64::try_from(*m).map(|ref m| n == m).unwrap_or(false),
                Value::BigInt(big_m) => big_m.to_i64().map(|ref m| n == m).unwrap_or(false),
                Value::BigUint(big_m) => big_m.to_i64().map(|ref m| n == m).unwrap_or(false),
                _ => false,
            },
            Value::UInt32Value(n) => match other {
                Value::Int32Value(m) => u32::try_from(*m).map(|ref m| n == m).unwrap_or(false),
                Value::Int64Value(m) => u32::try_from(*m).map(|ref m| n == m).unwrap_or(false),
                Value::UInt32Value(m) => n == m,
                Value::UInt64Value(m) => u32::try_from(*m).map(|ref m| n == m).unwrap_or(false),
                Value::BigInt(big_m) => big_m.to_u32().map(|ref m| n == m).unwrap_or(false),
                Value::BigUint(big_m) => big_m.to_u32().map(|ref m| n == m).unwrap_or(false),
                _ => false,
            },
            Value::UInt64Value(n) => match other {
                Value::Int32Value(m) => u64::try_from(*m).map(|ref m| n == m).unwrap_or(false),
                Value::Int64Value(m) => u64::try_from(*m).map(|ref m| n == m).unwrap_or(false),
                Value::UInt32Value(m) => u64::try_from(*m).map(|ref m| n == m).unwrap_or(false),
                Value::UInt64Value(m) => n == m,
                Value::BigInt(big_m) => big_m.to_u64().map(|ref m| n == m).unwrap_or(false),
                Value::BigUint(big_m) => big_m.to_u64().map(|ref m| n == m).unwrap_or(false),
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
                Value::Int32Value(right) => {
                    left.to_i32().map(|ref left| left == right).unwrap_or(false)
                }
                Value::Int64Value(right) => {
                    left.to_i64().map(|ref left| left == right).unwrap_or(false)
                }
                Value::UInt32Value(right) => {
                    left.to_u32().map(|ref left| left == right).unwrap_or(false)
                }
                Value::UInt64Value(right) => {
                    left.to_u64().map(|ref left| left == right).unwrap_or(false)
                }
                Value::BigInt(right) => left == right,
                Value::BigUint(right) => right
                    .to_bigint()
                    .map(|ref right| left == right)
                    .unwrap_or(false),
                _ => false,
            },
            Value::BigUint(left) => match other {
                Value::Int32Value(right) => {
                    left.to_i32().map(|ref left| left == right).unwrap_or(false)
                }
                Value::Int64Value(right) => {
                    left.to_i64().map(|ref left| left == right).unwrap_or(false)
                }
                Value::UInt32Value(right) => {
                    left.to_u32().map(|ref left| left == right).unwrap_or(false)
                }
                Value::UInt64Value(right) => {
                    left.to_u64().map(|ref left| left == right).unwrap_or(false)
                }
                Value::BigInt(right) => left
                    .to_bigint()
                    .map(|ref left| left == right)
                    .unwrap_or(false),
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
        const INT_HASH: u8 = 1;
        const FLOAT64_HASH: u8 = 2;
        const BOOLEAN_HASH: u8 = 3;
        const TEXT_HASH: u8 = 4;
        const RECORD_HASH: u8 = 5;
        const BIGINT_HASH: u8 = 6;
        const DATA_HASH: u8 = 7;

        match self {
            Value::Extant => {
                state.write_u8(EXTANT_HASH);
            }
            Value::Int32Value(n) => {
                state.write_u8(INT_HASH);
                state.write_i128(*n as i128);
            }
            Value::Int64Value(n) => {
                state.write_u8(INT_HASH);
                state.write_i128(*n as i128);
            }
            Value::UInt32Value(n) => {
                state.write_u8(INT_HASH);
                state.write_i128(*n as i128);
            }
            Value::UInt64Value(n) => {
                state.write_u8(INT_HASH);
                state.write_i128(*n as i128);
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
                if let Some(n) = bi.to_i128() {
                    state.write_u8(INT_HASH);
                    state.write_i128(n as i128);
                } else {
                    state.write_u8(BIGINT_HASH);
                    bi.hash(state);
                }
            }
            Value::BigUint(bi) => {
                if let Some(n) = bi.to_i128() {
                    state.write_u8(INT_HASH);
                    state.write_i128(n as i128);
                } else if let Some(n) = bi.to_bigint() {
                    state.write_u8(BIGINT_HASH);
                    n.hash(state);
                } else {
                    unreachable!();
                }
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

impl From<u32> for Value {
    fn from(n: u32) -> Self {
        Value::UInt32Value(n)
    }
}

impl From<u64> for Value {
    fn from(n: u64) -> Self {
        Value::UInt64Value(n)
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
        Value::Text(s.into())
    }
}

impl From<Text> for Value {
    fn from(t: Text) -> Self {
        Value::Text(t)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Text(s.into())
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

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Value::Data(b) => write!(f, "{}", b),
            Value::Extant => f.write_str(""),
            Value::Int32Value(n) => write!(f, "{}", n),
            Value::Int64Value(n) => write!(f, "{}", n),
            Value::UInt32Value(n) => write!(f, "{}", n),
            Value::UInt64Value(n) => write!(f, "{}", n),
            Value::Float64Value(x) => write!(f, "{:e}", x),
            Value::BooleanValue(p) => write!(f, "{}", p),
            Value::Text(s) => write_string_literal(s.as_str(), f),
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
