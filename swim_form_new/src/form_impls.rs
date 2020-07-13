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

use std::collections::{BTreeSet, BinaryHeap, HashSet, LinkedList, VecDeque};
use std::hash::BuildHasher;

use im::{HashSet as ImHashSet, OrdSet};
use num_bigint::{BigInt, BigUint};

use common::model::schema::StandardSchema;
use common::model::ValueKind;
use common::model::{Item, Value};

use crate::deserialize::FormDeserializeErr;
use crate::{Form, ValidatedForm};

fn iter_to_record<V>(it: V) -> Value
where
    V: Iterator,
    <V as Iterator>::Item: Form,
{
    let vec = match it.size_hint() {
        (u, Some(r)) if u == r => Vec::with_capacity(r),
        _ => Vec::new(),
    };

    it.fold(Value::Record(vec![], vec), |mut v, i| {
        if let Value::Record(_, items) = &mut v {
            items.push(Item::of(i.as_value()))
        } else {
            unreachable!()
        }
        v
    })
}

impl<'a, F> Form for &'a F
where
    F: Form,
{
    fn as_value(&self) -> Value {
        (**self).as_value()
    }

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormDeserializeErr> {
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

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormDeserializeErr> {
        Form::try_from_value(value)
    }
}

impl Form for f64 {
    fn as_value(&self) -> Value {
        Value::Float64Value(*self)
    }

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormDeserializeErr> {
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

pub fn de_incorrect_type<V>(expected: &str, actual: &Value) -> Result<V, FormDeserializeErr> {
    Err(FormDeserializeErr::IncorrectType(format!(
        "Expected: {}, found: {}",
        expected,
        actual.kind()
    )))
}

impl Form for i32 {
    fn as_value(&self) -> Value {
        Value::Int32Value(*self)
    }

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormDeserializeErr> {
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

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormDeserializeErr> {
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

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormDeserializeErr> {
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

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormDeserializeErr> {
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

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
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

impl Form for BigInt {
    fn as_value(&self) -> Value {
        // todo update once bigint pr is in
        Value::Text(self.to_string())
    }

    fn try_from_value(_value: &Value) -> Result<Self, FormDeserializeErr> {
        unimplemented!()
    }
}

impl Form for BigUint {
    fn as_value(&self) -> Value {
        // todo update once bigint pr is in
        Value::Text(self.to_string())
    }

    fn try_from_value(_value: &Value) -> Result<Self, FormDeserializeErr> {
        unimplemented!()
    }
}

impl<V> Form for Vec<V>
where
    V: Form,
{
    fn as_value(&self) -> Value {
        iter_to_record(self.iter())
    }

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        unimplemented!()
    }
}

impl<K, V> Form for ImHashSet<K, V>
where
    K: Form,
    V: Form,
{
    fn as_value(&self) -> Value {
        unimplemented!()
    }

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        unimplemented!()
    }
}

impl<V> Form for OrdSet<V>
where
    V: Form + Ord,
{
    fn as_value(&self) -> Value {
        iter_to_record(self.iter())
    }

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        unimplemented!()
    }
}

impl<V> Form for VecDeque<V>
where
    V: Form,
{
    fn as_value(&self) -> Value {
        iter_to_record(self.iter())
    }

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        unimplemented!()
    }
}

impl<V> Form for BinaryHeap<V>
where
    V: Form + Ord,
{
    fn as_value(&self) -> Value {
        iter_to_record(self.iter())
    }

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        unimplemented!()
    }
}

impl<V> Form for BTreeSet<V>
where
    V: Form,
{
    fn as_value(&self) -> Value {
        iter_to_record(self.iter())
    }

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        unimplemented!()
    }
}

impl<K, H> Form for HashSet<K, H>
where
    K: Form,
    H: Form + BuildHasher,
{
    fn as_value(&self) -> Value {
        unimplemented!()
    }

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        unimplemented!()
    }
}

impl<V> Form for LinkedList<V>
where
    V: Form,
{
    fn as_value(&self) -> Value {
        iter_to_record(self.iter())
    }

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        unimplemented!()
    }
}
