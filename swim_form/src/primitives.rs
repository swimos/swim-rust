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

use common::model::{Value, ValueKind};
use deserialize::FormDeserializeErr;

use crate::{Form, ValidatedForm};
use common::model::schema::StandardSchema;
use std::convert::TryFrom;

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
            Value::Int64Value(i) => i32::try_from(*i).map_err(|_| {
                FormDeserializeErr::IncorrectType(
                    "Expected Value::Int32Value, found Value::Int64Value".into(),
                )
            }),
            Value::UInt32Value(i) => i32::try_from(*i).map_err(|_| {
                FormDeserializeErr::IncorrectType(
                    "Expected Value::Int32Value, found Value::UInt32Value".into(),
                )
            }),
            Value::UInt64Value(i) => i32::try_from(*i).map_err(|_| {
                FormDeserializeErr::IncorrectType(
                    "Expected Value::Int32Value, found Value::UInt64Value".into(),
                )
            }),
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
            Value::Int32Value(i) => Ok(*i as i64),
            Value::Int64Value(i) => Ok(*i),
            Value::UInt32Value(i) => i64::try_from(*i).map_err(|_| {
                FormDeserializeErr::IncorrectType(
                    "Expected Value::Int64Value, found Value::UInt32Value".into(),
                )
            }),
            Value::UInt64Value(i) => i64::try_from(*i).map_err(|_| {
                FormDeserializeErr::IncorrectType(
                    "Expected Value::Int64Value, found Value::UInt64Value".into(),
                )
            }),
            v => de_incorrect_type("Value::Int64Value", v),
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

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::Int32Value(i) => u32::try_from(*i).map_err(|_| {
                FormDeserializeErr::IncorrectType(
                    "Expected Value::UInt32Value, found Value::Int32Value".into(),
                )
            }),
            Value::Int64Value(i) => u32::try_from(*i).map_err(|_| {
                FormDeserializeErr::IncorrectType(
                    "Expected Value::UInt32Value, found Value::Int64Value".into(),
                )
            }),
            Value::UInt32Value(i) => Ok(*i),
            Value::UInt64Value(i) => u32::try_from(*i).map_err(|_| {
                FormDeserializeErr::IncorrectType(
                    "Expected Value::UInt32Value, found Value::UInt64Value".into(),
                )
            }),
            v => de_incorrect_type("Value::UInt32Value", v),
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

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::Int32Value(i) => u64::try_from(*i).map_err(|_| {
                FormDeserializeErr::IncorrectType(
                    "Expected Value::UInt64Value, found Value::Int32Value".into(),
                )
            }),
            Value::Int64Value(i) => u64::try_from(*i).map_err(|_| {
                FormDeserializeErr::IncorrectType(
                    "Expected Value::UInt64Value, found Value::Int32Value".into(),
                )
            }),
            Value::UInt32Value(i) => Ok(*i as u64),
            Value::UInt64Value(i) => Ok(*i),
            v => de_incorrect_type("Value::UInt64Value", v),
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
