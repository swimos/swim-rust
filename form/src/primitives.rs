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

use common::model::Value;
use deserialize::FormDeserializeErr;

use crate::_common::model::schema::StandardSchema;
use crate::_common::model::ValueKind;
use crate::{Form, ValidatedForm};

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

impl ValidatedForm for String {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Text)
    }
}
