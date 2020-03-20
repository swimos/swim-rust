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

use crate::Form;

impl Form for f64 {
    fn as_value(&self) -> Value {
        Value::Float64Value(*self)
    }

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::Float64Value(i) => Ok(*i),
            v => de_incorrect_type("f64", v),
        }
    }
}

pub fn de_incorrect_type<V>(expected: &str, actual: &Value) -> Result<V, FormDeserializeErr> {
    Err(FormDeserializeErr::IncorrectType(format!(
        "Expected: {}, found: {}",
        expected,
        actual.to_string()
    )))
}

impl Form for i32 {
    fn as_value(&self) -> Value {
        Value::Int32Value(*self)
    }

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::Int32Value(i) => Ok(*i),
            v => de_incorrect_type("i32", v),
        }
    }
}

impl Form for i64 {
    fn as_value(&self) -> Value {
        Value::Int64Value(*self)
    }

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::Int64Value(i) => Ok(*i),
            v => de_incorrect_type("i64", v),
        }
    }
}

impl Form for bool {
    fn as_value(&self) -> Value {
        Value::BooleanValue(*self)
    }

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::BooleanValue(i) => Ok(*i),
            v => de_incorrect_type("bool", v),
        }
    }
}

impl Form for String {
    fn as_value(&self) -> Value {
        Value::Text(String::from(self))
    }

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::Text(i) => Ok(i.to_owned()),
            v => de_incorrect_type("String", v),
        }
    }
}
