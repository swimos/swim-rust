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

use core::iter::FromIterator;
use std::borrow::Cow;
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque};
use std::convert::TryFrom;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::str::FromStr;
use std::sync::Arc;

use im::{HashMap as ImHashMap, HashSet as ImHashSet, OrdSet};
use num_bigint::{BigInt, BigUint};
use num_traits::FromPrimitive;

use crate::form::{Form, FormErr, ValidatedForm};
use crate::model::blob::Blob;
use crate::model::schema::StandardSchema;
use crate::model::{Item, Value, ValueKind};
use std::marker::PhantomData;

impl<T: ?Sized> Form for PhantomData<T> {
    fn as_value(&self) -> Value {
        Value::Extant
    }

    fn try_from_value<'l>(value: &Value) -> Result<Self, FormErr> {
        match value {
            Value::Extant => Ok(Default::default()),
            v => Err(FormErr::incorrect_type("Value::Extant", v)),
        }
    }
}

impl Form for Blob {
    fn as_value(&self) -> Value {
        Value::Data(self.clone())
    }

    fn into_value(self) -> Value {
        Value::Data(self)
    }

    fn try_from_value(value: &Value) -> Result<Self, FormErr> {
        match value {
            Value::Data(blob) => Ok(blob.clone()),
            Value::Text(s) => Ok(Blob::from_encoded(Vec::from(s.as_bytes()))),
            v => Err(FormErr::incorrect_type("Value::Data", v)),
        }
    }

    fn try_convert(value: Value) -> Result<Self, FormErr> {
        match value {
            Value::Data(blob) => Ok(blob),
            Value::Text(s) => Ok(Blob::from_encoded(Vec::from(s.as_bytes()))),
            v => Err(FormErr::incorrect_type("Value::Data", &v)),
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

    fn try_from_value(value: &Value) -> Result<Self, FormErr> {
        match value {
            Value::BigInt(bi) => Ok(bi.clone()),
            Value::Int32Value(v) => Ok(BigInt::from(*v)),
            Value::Int64Value(v) => Ok(BigInt::from(*v)),
            Value::UInt32Value(v) => BigInt::from_u32(*v).ok_or_else(|| {
                FormErr::Message(String::from(
                    "Failed to parse u32 into big unsigned integer",
                ))
            }),
            Value::UInt64Value(v) => BigInt::from_u64(*v).ok_or_else(|| {
                FormErr::Message(String::from(
                    "Failed to parse u64 into big unsigned integer",
                ))
            }),
            Value::Float64Value(v) => BigInt::from_f64(*v).ok_or_else(|| {
                FormErr::Message(String::from(
                    "Failed to parse float into big unsigned integer",
                ))
            }),
            Value::Text(t) => BigInt::from_str(&t).map_err(|_| {
                FormErr::Message(String::from(
                    "Failed to parse text into big unsigned integer",
                ))
            }),
            Value::BigUint(uint) => Ok(BigInt::from(uint.clone())),
            v => Err(FormErr::incorrect_type("Value::Float64Value", v)),
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

    fn try_from_value(value: &Value) -> Result<Self, FormErr> {
        match value {
            Value::BigInt(bi) => BigUint::try_from(bi).map_err(|_| {
                FormErr::Message(String::from(
                    "Failed to parse big integer into big unsigned integer",
                ))
            }),
            Value::Int32Value(v) => BigUint::from_i32(*v).ok_or_else(|| {
                FormErr::Message(String::from(
                    "Failed to parse int32 into big unsigned integer",
                ))
            }),
            Value::Int64Value(v) => BigUint::from_i64(*v).ok_or_else(|| {
                FormErr::Message(String::from(
                    "Failed to parse int64 into big unsigned integer",
                ))
            }),
            Value::UInt32Value(v) => BigUint::from_u32(*v).ok_or_else(|| {
                FormErr::Message(String::from(
                    "Failed to parse int32 into big unsigned integer",
                ))
            }),
            Value::UInt64Value(v) => BigUint::from_u64(*v).ok_or_else(|| {
                FormErr::Message(String::from(
                    "Failed to parse int64 into big unsigned integer",
                ))
            }),
            Value::Float64Value(v) => BigUint::from_f64(*v).ok_or_else(|| {
                FormErr::Message(String::from(
                    "Failed to parse float64 into big unsigned integer",
                ))
            }),
            Value::Text(t) => BigUint::from_str(&t).map_err(|_| {
                FormErr::Message(String::from(
                    "Failed to parse text into big unsigned integer",
                ))
            }),
            Value::BigUint(uint) => Ok(uint.clone()),
            v => Err(FormErr::incorrect_type("Value::Float64Value", v)),
        }
    }
}

impl ValidatedForm for BigUint {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::BigUint)
    }
}

impl Form for f64 {
    fn as_value(&self) -> Value {
        Value::Float64Value(*self)
    }

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormErr> {
        match value {
            Value::Float64Value(i) => Ok(*i),
            v => Err(FormErr::incorrect_type("Value::Float64Value", v)),
        }
    }
}

impl ValidatedForm for f64 {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Float64)
    }
}

impl Form for i32 {
    fn as_value(&self) -> Value {
        Value::Int32Value(*self)
    }

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormErr> {
        match value {
            Value::Int32Value(i) => Ok(*i),
            Value::Int64Value(i) => i32::try_from(*i).map_err(|_| {
                FormErr::IncorrectType("Expected Value::Int32Value, found Value::Int64Value".into())
            }),
            Value::UInt32Value(i) => i32::try_from(*i).map_err(|_| {
                FormErr::IncorrectType(
                    "Expected Value::Int32Value, found Value::UInt32Value".into(),
                )
            }),
            Value::UInt64Value(i) => i32::try_from(*i).map_err(|_| {
                FormErr::IncorrectType(
                    "Expected Value::Int32Value, found Value::UInt64Value".into(),
                )
            }),
            Value::BigInt(i) => i32::try_from(i).map_err(|_| {
                FormErr::IncorrectType("Expected Value::Int32Value, found Value::BigInt".into())
            }),
            Value::BigUint(i) => i32::try_from(i).map_err(|_| {
                FormErr::IncorrectType("Expected Value::Int32Value, found Value::BigUint".into())
            }),
            v => Err(FormErr::incorrect_type("Value::Int32Value", v)),
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

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormErr> {
        match value {
            Value::Int32Value(i) => Ok(*i as i64),
            Value::Int64Value(i) => Ok(*i),
            Value::UInt32Value(i) => i64::try_from(*i).map_err(|_| {
                FormErr::IncorrectType(
                    "Expected Value::Int64Value, found Value::UInt32Value".into(),
                )
            }),
            Value::UInt64Value(i) => i64::try_from(*i).map_err(|_| {
                FormErr::IncorrectType(
                    "Expected Value::Int64Value, found Value::UInt64Value".into(),
                )
            }),
            Value::BigInt(i) => i64::try_from(i).map_err(|_| {
                FormErr::IncorrectType("Expected Value::Int64Value, found Value::BigInt".into())
            }),
            Value::BigUint(i) => i64::try_from(i).map_err(|_| {
                FormErr::IncorrectType("Expected Value::Int64Value, found Value::BigUint".into())
            }),
            v => Err(FormErr::incorrect_type("Value::Int64Value", v)),
        }
    }
}

impl ValidatedForm for i64 {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Int64)
    }
}

impl Form for u32 {
    fn as_value(&self) -> Value {
        Value::UInt32Value(*self)
    }

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormErr> {
        match value {
            Value::Int32Value(i) => u32::try_from(*i).map_err(|_| {
                FormErr::IncorrectType(
                    "Expected Value::UInt32Value, found Value::Int32Value".into(),
                )
            }),
            Value::Int64Value(i) => u32::try_from(*i).map_err(|_| {
                FormErr::IncorrectType(
                    "Expected Value::UInt32Value, found Value::Int64Value".into(),
                )
            }),
            Value::UInt32Value(i) => Ok(*i),
            Value::UInt64Value(i) => u32::try_from(*i).map_err(|_| {
                FormErr::IncorrectType(
                    "Expected Value::UInt32Value, found Value::UInt64Value".into(),
                )
            }),
            Value::BigInt(i) => u32::try_from(i).map_err(|_| {
                FormErr::IncorrectType("Expected Value::UInt32Value, found Value::BigInt".into())
            }),
            Value::BigUint(i) => u32::try_from(i).map_err(|_| {
                FormErr::IncorrectType("Expected Value::UInt32Value, found Value::BigUint".into())
            }),
            v => Err(FormErr::incorrect_type("Value::UInt32Value", v)),
        }
    }
}

impl ValidatedForm for u32 {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::UInt32)
    }
}

impl Form for u64 {
    fn as_value(&self) -> Value {
        Value::UInt64Value(*self)
    }

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormErr> {
        match value {
            Value::Int32Value(i) => u64::try_from(*i).map_err(|_| {
                FormErr::IncorrectType(
                    "Expected Value::UInt64Value, found Value::Int32Value".into(),
                )
            }),
            Value::Int64Value(i) => u64::try_from(*i).map_err(|_| {
                FormErr::IncorrectType(
                    "Expected Value::UInt64Value, found Value::Int32Value".into(),
                )
            }),
            Value::UInt32Value(i) => Ok(*i as u64),
            Value::UInt64Value(i) => Ok(*i),
            Value::BigInt(i) => u64::try_from(i).map_err(|_| {
                FormErr::IncorrectType("Expected Value::UInt64Value, found Value::BigInt".into())
            }),
            Value::BigUint(i) => u64::try_from(i).map_err(|_| {
                FormErr::IncorrectType("Expected Value::UInt64Value, found Value::BigUint".into())
            }),
            v => Err(FormErr::incorrect_type("Value::UInt64Value", v)),
        }
    }
}

impl ValidatedForm for u64 {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::UInt64)
    }
}

impl Form for bool {
    fn as_value(&self) -> Value {
        Value::BooleanValue(*self)
    }

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormErr> {
        match value {
            Value::BooleanValue(i) => Ok(*i),
            v => Err(FormErr::incorrect_type("Value::BooleanValue", v)),
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

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormErr> {
        match value {
            Value::Text(i) => Ok(i.to_owned()),
            v => Err(FormErr::incorrect_type("Value::Text", v)),
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

    fn try_from_value(value: &Value) -> Result<Self, FormErr> {
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

macro_rules! impl_seq_form {
    ($ty:ident < V $(: $tbound1:ident $(+ $tbound2:ident)*)* $(, $typaram:ident : $bound:ident $(+ $bound2:ident)*)* >) => {
        impl<V $(, $typaram)*> Form for $ty<V $(, $typaram)*>
        where
            V: Form $(+ $tbound1 $(+ $tbound2)*)*,
            $($typaram: Form + $bound $(+ $bound2)*,)*
        {
            fn as_value(&self) -> Value {
                let it = self.iter();

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

            fn try_from_value(value: &Value) -> Result<Self, FormErr> {
                match value {
                    Value::Record(_attrs, items) => {
                        let elems =
                                items
                                    .iter()
                                    .try_fold(Vec::with_capacity(items.len()), |mut items, i| {
                                        let value = match i {
                                            Item::Slot(_key, value) => value,
                                            Item::ValueItem(value) => value,
                                        };

                                        let v = V::try_from_value(value)?;
                                        items.push(v);

                                        Ok(items)
                                    })?;

                        Ok($ty::from_iter(elems))
                    }
                    v => Err(FormErr::incorrect_type("Value::Record", v)),
                }
            }
        }
    }
}

impl_seq_form!(Vec<V>);
impl_seq_form!(VecDeque<V>);
impl_seq_form!(LinkedList<V>);
impl_seq_form!(BinaryHeap<V: Ord>);
impl_seq_form!(ImHashSet<V: Hash + Eq + Clone, L: BuildHasher + Default>);
impl_seq_form!(ImHashSet<V: Hash + Eq + Clone>);
impl_seq_form!(OrdSet<V: Ord + Clone>);
impl_seq_form!(BTreeSet<V: Ord>);
impl_seq_form!(HashSet<V: Eq + Hash, L: BuildHasher + Default>);
impl_seq_form!(HashSet<V: Eq + Hash>);

macro_rules! impl_map_form {
    ($ty:ident < K $(: $kbound1:ident $(+ $kbound2:ident)*)*, V $(: $vbound1:ident $(+ $vbound2:ident)*)* $(, $typaram:ident : $bound:ident $(+ $bound2:ident)*)* >) => {
        impl<K, V $(, $typaram)*> Form for $ty<K, V $(, $typaram)*>
        where
            K: Form $(+ $kbound1 $(+ $kbound2)*)*,
            V: Form $(+ $vbound1 $(+ $vbound2)*)*,
            $($typaram: Form + $bound $(+ $bound2)*,)*
        {
            fn as_value(&self) -> Value {
                let it = self.iter();

                let vec = match it.size_hint() {
                    (l, Some(u)) if l == u => Vec::with_capacity(u),
                    _ => Vec::new(),
                };

                let items = it.fold(vec, |mut items, (key, value)| {
                    items.push(Item::slot(key.as_value(), value.as_value()));
                    items
                });

                Value::Record(Vec::new(), items)
            }

            fn try_from_value(value: &Value) -> Result<Self, FormErr> {
                match value {
                    Value::Record(_attrs, items) => {
                        let elems =
                                items
                                    .iter()
                                    .try_fold(Vec::with_capacity(items.len()), |mut items, i| {
                                        match i {
                                            Item::Slot(key, value) => {
                                                let k = K::try_from_value(key)?;
                                                let v = V::try_from_value(value)?;
                                                items.push((k, v));

                                                Ok(items)
                                            },
                                            Item::ValueItem(_) => {
                                                Err(FormErr::IncorrectType(String::from("Expected a key-value pair")))
                                            },
                                        }
                                    })?;

                        Ok($ty::from_iter(elems))
                    }
                    v => Err(FormErr::incorrect_type("Value::Record", v)),
                }
            }
        }
    }
}

impl_map_form!(BTreeMap<K: Ord, V>);
impl_map_form!(HashMap<K: Eq + Hash, V, H: BuildHasher + Default>);
impl_map_form!(ImHashMap<K: Eq + Hash + Clone, V: Clone>);

impl Form for () {
    fn as_value(&self) -> Value {
        Value::Extant
    }

    fn try_from_value(value: &Value) -> Result<Self, FormErr> {
        match value {
            Value::Extant => Ok(()),
            v => Err(FormErr::incorrect_type("()", v)),
        }
    }
}

impl ValidatedForm for () {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Extant)
    }
}

impl<F> Form for Cell<F>
where
    F: Form + Copy,
{
    fn as_value(&self) -> Value {
        self.get().as_value()
    }

    fn try_from_value(value: &Value) -> Result<Self, FormErr> {
        F::try_from_value(value).map(Cell::new)
    }
}

impl<V> ValidatedForm for Cell<V>
where
    V: ValidatedForm + Copy,
{
    fn schema() -> StandardSchema {
        StandardSchema::Or(vec![V::schema(), StandardSchema::OfKind(ValueKind::Extant)])
    }
}

impl<F> Form for Box<F>
where
    F: Form,
{
    fn as_value(&self) -> Value {
        (**self).as_value()
    }

    fn try_from_value(value: &Value) -> Result<Self, FormErr> {
        F::try_from_value(value).map(Box::new)
    }
}

impl<V> ValidatedForm for Box<V>
where
    V: ValidatedForm,
{
    fn schema() -> StandardSchema {
        V::schema()
    }
}

impl<F> Form for Arc<F>
where
    F: Form,
{
    fn as_value(&self) -> Value {
        (**self).as_value()
    }

    fn try_from_value(value: &Value) -> Result<Self, FormErr> {
        F::try_from_value(value).map(Arc::new)
    }
}

impl<V> ValidatedForm for Arc<V>
where
    V: ValidatedForm,
{
    fn schema() -> StandardSchema {
        V::schema()
    }
}

impl<'l, F> Form for Cow<'l, F>
where
    F: Form + Clone,
{
    fn as_value(&self) -> Value {
        (**self).as_value()
    }

    fn try_from_value(value: &Value) -> Result<Self, FormErr> {
        F::try_from_value(value).map(Cow::Owned)
    }
}

impl<'l, F> ValidatedForm for Cow<'l, F>
where
    F: ValidatedForm + Clone,
{
    fn schema() -> StandardSchema {
        F::schema()
    }
}
