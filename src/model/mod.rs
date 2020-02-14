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

use crate::model::parser::is_identifier;
use bytes::*;
use std::borrow::Borrow;
use std::convert::TryFrom;
use std::fmt::Write;
use std::fmt::{Display, Formatter};
use std::iter;
use tokio_util::codec::Encoder;
use std::hash::{Hash, Hasher};
use std::cmp::Ordering;
use either::Either;

pub mod parser;

#[cfg(test)]
mod tests;

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
    /// use swim_rust::model::Value;
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
}

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
        Value::Record(
            vec![],
            items.into_iter().map(|item| Item::of(item)).collect(),
        )
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
            Value::Extant => {
                match other {
                    Value::Extant => Ordering::Equal,
                    _ => Ordering::Greater
                }
            },
            Value::Int32Value(n) => {
                match other {
                    Value::Extant | Value::BooleanValue(_) => Ordering::Less,
                    Value::Int32Value(m) => n.cmp(m),
                    Value::Int64Value(m) => (*n as i64).cmp(m),
                    Value::Float64Value(y) => {
                        if y.is_nan() {
                            Ordering::Greater
                        } else {
                            let x = *n as f64;
                            if x == *y {
                                Ordering::Equal
                            } else if x < *y {
                                Ordering::Less
                            } else {
                                Ordering::Greater
                            }
                        }
                    },
                    _ => Ordering::Greater
                }
            },
            Value::Int64Value(n) => {
                match other {
                    Value::Extant | Value::BooleanValue(_) => Ordering::Less,
                    Value::Int32Value(m) => n.cmp(&(*m as i64)),
                    Value::Int64Value(m) => n.cmp(m),
                    Value::Float64Value(y) => {
                        if y.is_nan() {
                            Ordering::Greater
                        } else {
                            let x = *n as f64;
                            if x == *y {
                                Ordering::Equal
                            } else if x < *y {
                                Ordering::Less
                            } else {
                                Ordering::Greater
                            }
                        }
                    },
                    _ => Ordering::Greater
                }
            },
            Value::Float64Value(x) => {
                match other {
                    Value::Extant | Value::BooleanValue(_) => Ordering::Less,
                    Value::Int32Value(m) => {
                        if x.is_nan() {
                            Ordering::Less
                        } else {
                            let y = *m as f64;
                            if *x == y {
                                Ordering::Equal
                            } else if *x < y {
                                Ordering::Less
                            } else {
                                Ordering::Greater
                            }
                        }
                    },
                    Value::Int64Value(m) => {
                        if x.is_nan() {
                            Ordering::Less
                        } else {
                            let y = *m as f64;
                            if *x == y {
                                Ordering::Equal
                            } else if *x < y {
                                Ordering::Less
                            } else {
                                Ordering::Greater
                            }
                        }
                    },
                    Value::Float64Value(y) => {
                        if x.is_nan() {
                            if y.is_nan() {
                                Ordering::Equal
                            } else {
                                Ordering::Less
                            }
                        } else {
                            if y.is_nan() {
                                Ordering::Greater
                            } else {
                                if *x == *y {
                                    Ordering::Equal
                                } else if *x < *y {
                                    Ordering::Less
                                } else {
                                    Ordering::Greater
                                }
                            }
                        }
                    },
                    _ => Ordering::Greater
                }
            },
            Value::BooleanValue(p) => {
                match other {
                    Value::Extant => Ordering::Less,
                    Value::BooleanValue(q) => p.cmp(q),
                    _ => Ordering::Greater
                }
            },
            Value::Text(s) => {
                match other {
                    Value::Record(_, _) => Ordering::Greater,
                    Value::Text(t) => s.cmp(t),
                    _ => Ordering::Less
                }
            },
            Value::Record(attrs1, items1) => {
                match other {
                    Value::Record(attrs2, items2) => {
                        let first = attrs1.iter().map(|a| Either::Left(a))
                            .chain(items1.iter().map(|i| Either::Right(i)));
                        let second = attrs2.iter().map(|a| Either::Left(a))
                            .chain(items2.iter().map(|i| Either::Right(i)));
                        first.cmp(second)
                    },
                    _ => Ordering::Less
                }
            },
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Value::Extant => {
                match other {
                    Value::Extant => true,
                    _ => false,
                }
            },
            Value::Int32Value(n) => {
                match other {
                    Value::Int32Value(m) => n == m,
                    _ => false
                }
            },
            Value::Int64Value(n) => {
                match other {
                    Value::Int64Value(m) => n == m,
                    _ => false
                }
            },
            Value::Float64Value(x) => {
                match other {
                    Value::Float64Value(y) => {
                        if x.is_nan() {
                            y.is_nan()
                        } else {
                            x == y
                        }
                    },
                    _ => false
                }
            },
            Value::BooleanValue(p) => {
                match other {
                    Value::BooleanValue(q) => p == q,
                    _ => false
                }
            },
            Value::Text(s) => {
                match other {
                    Value::Text(t) => s == t,
                    _ => false
                }
            },
            Value::Record(attrs1, items1) => {
                match other {
                    Value::Record(attrs2, items2) => {
                        attrs1 == attrs2 && items1 == items2
                    },
                    _ => false
                }
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
        match self {
            Value::Extant => {
                state.write_u8(0);
            },
            Value::Int32Value(n) => {
                state.write_u8(1);
                state.write_i32(*n);
            },
            Value::Int64Value(n) => {
                state.write_u8(2);
                state.write_i64(*n);
            },
            Value::Float64Value(x) => {
                state.write_u8(3);
                if x.is_nan() {
                    state.write_u64(0);
                } else {
                    state.write_u64(x.to_bits());
                }
            },
            Value::BooleanValue(p) => {
                state.write_u8(4);
                state.write_u8(if *p { 1 } else { 0 })
            },
            Value::Text(s) => {
                state.write_u8(5);
                s.hash(state);
            },
            Value::Record(attrs, items) => {
                state.write_u8(6);
                attrs.hash(state);
                items.hash(state);
            },
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
    /// use swim_rust::model::*;
    ///
    /// assert_eq!(Attr::of("name"), Attr { name: String::from("name"), value: Value::Extant });
    /// assert_eq!(Attr::of(("key", 1)), Attr { name: String::from("key"), value: Value::Int32Value(1) });
    /// ```
    pub fn of<T: Into<Attr>>(rep: T) -> Attr {
        rep.into()
    }

    fn compare(&self, other: &Attr) -> Ordering {
        match self.name.cmp(&other.name) {
            Ordering::Equal => self.value.cmp(&other.value),
            ow => ow,
        }
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
    /// use swim_rust::model::*;
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
    /// use swim_rust::model::*;
    /// assert_eq!(Item::slot("key", 1), Item::Slot(Value::text("key"), Value::Int32Value(1)));
    /// ```
    pub fn slot<K: Into<Value>, V: Into<Value>>(key: K, value: V) -> Item {
        Item::Slot(key.into(), value.into())
    }

    fn compare(&self, other: &Item) -> Ordering {
        match self {
            Item::ValueItem(v1) => {
                match other {
                    Item::ValueItem(v2) => v1.cmp(v2),
                    Item::Slot(_, _) => Ordering::Greater,
                }
            },
            Item::Slot(key1, value1) => {
                match other {
                    Item::ValueItem(_) => Ordering::Less,
                    Item::Slot(key2, value2) => {
                        match key1.cmp(key2) {
                            Ordering::Equal => value1.cmp(value2),
                            ow => ow,
                        }
                    },
                }
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

const TRUE: &'static [u8] = b"true";
const FALSE: &'static [u8] = b"false";

fn unpack_attr_body(attrs: &Vec<Attr>, items: &Vec<Item>) -> bool {
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

impl Encoder for ValueEncoder {
    type Item = Value;
    type Error = ValueEncodeErr;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
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

impl ValueEncoder {
    pub fn new() -> ValueEncoder {
        ValueEncoder {}
    }

    fn encode_value(&mut self, item: Value, dst: &mut BytesMut) -> Result<(), ValueEncodeErr> {
        match item {
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
        }
    }

    fn encode_text(dst: &mut BytesMut, s: &String) -> Result<(), ValueEncodeErr> {
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
        }
    }
}
