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

use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque};
use std::hash::BuildHasher;
use std::hash::Hash;

use im::{HashMap as ImHashMap, HashSet as ImHashSet, OrdSet};
use num_bigint::{BigInt, BigUint};

use common::model::schema::StandardSchema;
use common::model::ValueKind;
use common::model::{Item, Value};

use crate::reader::ValueReadError;
use crate::{Form, ValidatedForm, WriteValueError};
use common::model::blob::Blob;
use num_traits::FromPrimitive;
use std::convert::TryFrom;
use std::str::FromStr;

impl<'a, F> Form for &'a F
where
    F: Form,
{
    fn as_value(&self) -> Value {
        (**self).as_value()
    }

    fn try_from_value<'f>(value: &Value) -> Result<Self, ValueReadError> {
        Form::try_from_value(value)
    }
}

impl<'a, F> Form for &'a mut F
where
    F: Form,
{
    fn as_value(&self) -> Value {
        (**self).as_value()
    }

    fn try_from_value<'f>(value: &Value) -> Result<Self, ValueReadError> {
        Form::try_from_value(value)
    }
}

impl Form for f64 {
    fn as_value(&self) -> Value {
        Value::Float64Value(*self)
    }

    fn try_from_value<'f>(value: &Value) -> Result<Self, ValueReadError> {
        match value {
            Value::Float64Value(i) => Ok(*i),
            v => de_incorrect_type("Value::Float64Value", v),
        }
    }
}

impl ValidatedForm for f64 {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Float64)
    }
}

pub fn de_incorrect_type<V>(expected: &str, actual: &Value) -> Result<V, ValueReadError> {
    Err(ValueReadError::IncorrectType(format!(
        "Expected: {}, found: {}",
        expected,
        actual.kind()
    )))
}

impl Form for i32 {
    fn as_value(&self) -> Value {
        Value::Int32Value(*self)
    }

    fn try_from_value<'f>(value: &Value) -> Result<Self, ValueReadError> {
        match value {
            Value::Int32Value(i) => Ok(*i),
            v => de_incorrect_type("Value::Int32Value", v),
        }
    }
}

impl ValidatedForm for i32 {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Int32)
    }
}

impl Form for i64 {
    fn as_value(&self) -> Value {
        Value::Int64Value(*self)
    }

    fn try_from_value<'f>(value: &Value) -> Result<Self, ValueReadError> {
        match value {
            Value::Int64Value(i) => Ok(*i),
            v => de_incorrect_type("Value::Int64Value", v),
        }
    }
}

impl ValidatedForm for i64 {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Int64)
    }
}

impl Form for bool {
    fn as_value(&self) -> Value {
        Value::BooleanValue(*self)
    }

    fn try_from_value<'f>(value: &Value) -> Result<Self, ValueReadError> {
        match value {
            Value::BooleanValue(i) => Ok(*i),
            v => de_incorrect_type("Value::BooleanValue", v),
        }
    }
}

impl ValidatedForm for bool {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Boolean)
    }
}

impl Form for String {
    fn as_value(&self) -> Value {
        Value::Text(String::from(self))
    }

    fn try_from_value<'f>(value: &Value) -> Result<Self, ValueReadError> {
        match value {
            Value::Text(i) => Ok(i.to_owned()),
            v => de_incorrect_type("Value::Text", v),
        }
    }
}

impl<V> Form for Option<V>
where
    V: Form,
{
    fn as_value(&self) -> Value {
        match &self {
            Some(o) => o.as_value(),
            None => Value::Extant,
        }
    }

    fn try_from_value(value: &Value) -> Result<Self, ValueReadError> {
        match value {
            Value::Extant => Ok(None),
            _ => match V::try_from_value(value) {
                Ok(r) => Ok(Some(r)),
                Err(e) => Err(e),
            },
        }
    }
}

impl<V> ValidatedForm for Option<V>
where
    V: ValidatedForm,
{
    fn schema() -> StandardSchema {
        StandardSchema::Or(vec![V::schema(), StandardSchema::OfKind(ValueKind::Extant)])
    }
}

impl ValidatedForm for String {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Text)
    }
}

fn seq_to_record<V>(it: V) -> Value
where
    V: Iterator,
    <V as Iterator>::Item: Form,
{
    let vec = match it.size_hint() {
        (u, Some(r)) if u == r => Vec::with_capacity(r),
        _ => Vec::new(),
    };

    let items = it.fold(vec, |mut items, i| {
        items.push(Item::of(i.as_value()));
        items
    });

    Value::Record(Vec::new(), items)
}

macro_rules! impl_seq_form {
    ($ty:ident < V $(: $tbound1:ident $(+ $tbound2:ident)*)* $(, $typaram:ident : $bound:ident)* >) => {
        impl<V $(, $typaram)*> Form for $ty<V $(, $typaram)*>
        where
            V: Form $(+ $tbound1 $(+ $tbound2)*)*,
            $($typaram: Form + $bound,)*
        {
            fn as_value(&self) -> Value {
                seq_to_record(self.iter())
            }

            fn try_from_value(_value: &Value) -> Result<Self, ValueReadError> {
                unimplemented!()
            }
        }
    }
}

impl_seq_form!(Vec<V>);
impl_seq_form!(ImHashSet<V, L: BuildHasher>);
impl_seq_form!(ImHashSet<V>);
impl_seq_form!(OrdSet<V: Ord>);
impl_seq_form!(VecDeque<V>);
impl_seq_form!(BinaryHeap<V: Ord>);
impl_seq_form!(BTreeSet<V>);
impl_seq_form!(HashSet<V: Eq + Hash, L: BuildHasher>);
impl_seq_form!(HashSet<V: Eq + Hash>);
impl_seq_form!(LinkedList<V>);

fn map_to_record<Iter, K, V>(it: Iter) -> Value
where
    Iter: Iterator<Item = (K, V)>,
    K: Form,
    V: Form,
{
    let vec = match it.size_hint() {
        (u, Some(r)) if u == r => Vec::with_capacity(r),
        _ => Vec::new(),
    };

    it.fold(Value::Record(vec![], vec), |mut v, (key, value)| {
        if let Value::Record(_, items) = &mut v {
            items.push(Item::slot(key.as_value(), value.as_value()))
        } else {
            unreachable!()
        }
        v
    })
}

macro_rules! impl_map_form {
    ($ty:ident < K $(: $kbound1:ident $(+ $kbound2:ident)*)*, V $(, $typaram:ident : $bound:ident)* >) => {
        impl<K, V $(, $typaram)*> Form for $ty<K, V $(, $typaram)*>
        where
            K: Form $(+ $kbound1 $(+ $kbound2)*)*,
            V: Form,
            $($typaram: $bound,)*
        {
            fn as_value(&self) -> Value {
                map_to_record(self.iter())
            }

            fn try_from_value(_value: &Value) -> Result<Self, ValueReadError> {
                unimplemented!()
            }
        }
    }
}

impl_map_form!(BTreeMap<K: Ord, V>);
impl_map_form!(HashMap<K: Eq + Hash, V, H: BuildHasher>);

impl<K, V> Form for ImHashMap<K, V>
where
    K: Form,
    V: Form,
{
    fn as_value(&self) -> Value {
        self.iter().fold(
            Value::Record(vec![], Vec::with_capacity(self.len())),
            |mut v, (key, value)| {
                if let Value::Record(_, items) = &mut v {
                    items.push(Item::slot(key.as_value(), value.as_value()))
                } else {
                    unreachable!()
                }
                v
            },
        )
    }

    fn try_from_value(value: &Value) -> Result<Self, ValueReadError> {
        unimplemented!()
    }
}

impl Form for Blob {
    fn as_value(&self) -> Value {
        Value::Data(self.clone())
    }

    fn into_value(self) -> Value {
        Value::Data(self)
    }

    fn try_from_value(value: &Value) -> Result<Self, ValueReadError> {
        match value {
            Value::Data(blob) => Ok(blob.clone()),
            Value::Text(s) => Ok(Blob::from_encoded(Vec::from(s.as_bytes()))),
            v => de_incorrect_type("Value::Data", v),
        }
    }

    fn try_convert(value: Value) -> Result<Self, ValueReadError> {
        match value {
            Value::Data(blob) => Ok(blob),
            Value::Text(s) => Ok(Blob::from_encoded(Vec::from(s.as_bytes()))),
            v => de_incorrect_type("Value::Data", &v),
        }
    }
}

impl ValidatedForm for Blob {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Data)
    }
}

impl Form for BigInt {
    fn as_value(&self) -> Value {
        Value::BigInt(self.clone())
    }

    fn try_from_value(value: &Value) -> Result<Self, ValueReadError> {
        match value {
            Value::BigInt(bi) => Ok(bi.clone()),
            Value::Int32Value(v) => Ok(BigInt::from(*v)),
            Value::Int64Value(v) => Ok(BigInt::from(*v)),
            Value::Float64Value(v) => BigInt::from_f64(*v).ok_or_else(|| {
                ValueReadError::Message(String::from(
                    "Failed to parse float into big unsigned integer",
                ))
            }),
            Value::Text(t) => BigInt::from_str(&t).map_err(|_| {
                ValueReadError::Message(String::from(
                    "Failed to parse text into big unsigned integer",
                ))
            }),
            Value::BigUint(uint) => Ok(BigInt::from(uint.clone())),
            v => de_incorrect_type("Value::Float64Value", v),
        }
    }
}

impl ValidatedForm for BigInt {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::BigInt)
    }
}

impl Form for BigUint {
    fn as_value(&self) -> Value {
        Value::BigUint(self.clone())
    }

    fn try_from_value(value: &Value) -> Result<Self, ValueReadError> {
        match value {
            Value::BigInt(bi) => BigUint::try_from(bi).map_err(|_| {
                ValueReadError::Message(String::from(
                    "Failed to parse big integer into big unsigned integer",
                ))
            }),
            Value::Int32Value(v) => BigUint::from_i32(*v).ok_or_else(|| {
                ValueReadError::Message(String::from(
                    "Failed to parse int32 into big unsigned integer",
                ))
            }),
            Value::Int64Value(v) => BigUint::from_i64(*v).ok_or_else(|| {
                ValueReadError::Message(String::from(
                    "Failed to parse int64 into big unsigned integer",
                ))
            }),
            Value::Float64Value(v) => BigUint::from_f64(*v).ok_or_else(|| {
                ValueReadError::Message(String::from(
                    "Failed to parse float64 into big unsigned integer",
                ))
            }),
            Value::Text(t) => BigUint::from_str(&t).map_err(|_| {
                ValueReadError::Message(String::from(
                    "Failed to parse text into big unsigned integer",
                ))
            }),
            Value::BigUint(uint) => Ok(uint.clone()),
            v => de_incorrect_type("Value::Float64Value", v),
        }
    }
}

impl ValidatedForm for BigUint {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::BigUint)
    }
}
