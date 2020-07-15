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

use common::model::blob::Blob;
use common::model::schema::StandardSchema;
use common::model::{Value, ValueKind};
use deserialize::FormDeserializeErr;

use crate::{Form, ValidatedForm};
use num_bigint::{BigInt, BigUint};
use num_traits::FromPrimitive;
use std::convert::TryFrom;
use std::str::FromStr;

impl Form for Blob {
    fn as_value(&self) -> Value {
        Value::Data(self.clone())
    }

    fn into_value(self) -> Value {
        Value::Data(self)
    }

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::Data(blob) => Ok(blob.clone()),
            Value::Text(s) => Ok(Blob::from_encoded(Vec::from(s.as_bytes()))),
            v => de_incorrect_type("Value::Data", v),
        }
    }

    fn try_convert(value: Value) -> Result<Self, FormDeserializeErr> {
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

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::BigInt(bi) => Ok(bi.clone()),
            Value::Int32Value(v) => Ok(BigInt::from(*v)),
            Value::Int64Value(v) => Ok(BigInt::from(*v)),
            Value::Float64Value(v) => BigInt::from_f64(*v).ok_or_else(|| {
                FormDeserializeErr::Message(String::from(
                    "Failed to parse float into big unsigned integer",
                ))
            }),
            Value::Text(t) => BigInt::from_str(&t).map_err(|_| {
                FormDeserializeErr::Message(String::from(
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

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::BigInt(bi) => BigUint::try_from(bi).map_err(|_| {
                FormDeserializeErr::Message(String::from(
                    "Failed to parse big integer into big unsigned integer",
                ))
            }),
            Value::Int32Value(v) => BigUint::from_i32(*v).ok_or_else(|| {
                FormDeserializeErr::Message(String::from(
                    "Failed to parse int32 into big unsigned integer",
                ))
            }),
            Value::Int64Value(v) => BigUint::from_i64(*v).ok_or_else(|| {
                FormDeserializeErr::Message(String::from(
                    "Failed to parse int64 into big unsigned integer",
                ))
            }),
            Value::Float64Value(v) => BigUint::from_f64(*v).ok_or_else(|| {
                FormDeserializeErr::Message(String::from(
                    "Failed to parse float64 into big unsigned integer",
                ))
            }),
            Value::Text(t) => BigUint::from_str(&t).map_err(|_| {
                FormDeserializeErr::Message(String::from(
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

impl<'s> Form for &'s str {
    fn as_value(&self) -> Value {
        Value::Text(String::from(self.clone()))
    }

    fn try_from_value(_value: &Value) -> Result<Self, FormDeserializeErr> {
        unimplemented!()
    }
}
