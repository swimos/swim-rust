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

use core::fmt;
use std::convert::TryFrom;
use std::fmt::{Debug, Display};

use serde::de::StdError;
use serde::export::Formatter;

use crate::model::{Item, Value};

mod form;


#[cfg(test)]
mod tests;

#[derive(Debug, PartialEq)]
pub enum FormParseErr {
    None,
    IncorrectType(Value),
    Malformatted,
    InvalidString(String),
    IllegalItem(Item),
    NotABoolean,
}

impl Display for FormParseErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.to_string())
    }
}

impl StdError for FormParseErr {}

impl serde::ser::Error for FormParseErr {
    fn custom<T>(_msg: T) -> Self where
        T: Display {
        FormParseErr::InvalidString(String::from("ser::Error"))
    }
}

impl TryFrom<Value> for f64 {
    type Error = FormParseErr;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Int32Value(i) => {
                Ok(i.into())
            }
            Value::Int64Value(i) => {
                Ok(i as f64)
            }
            Value::Float64Value(f) => Ok(f),
            v @ _ => Err(FormParseErr::IncorrectType(v))
        }
    }
}

impl TryFrom<Value> for i32 {
    type Error = FormParseErr;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Int32Value(i) => Ok(i),
            v @ _ => Err(FormParseErr::IncorrectType(v))
        }
    }
}

impl TryFrom<Value> for i64 {
    type Error = FormParseErr;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Int32Value(i) => Ok(i as i64),
            Value::Int64Value(i) => Ok(i),
            v @ _ => Err(FormParseErr::IncorrectType(v))
        }
    }
}

impl TryFrom<Value> for bool {
    type Error = FormParseErr;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::BooleanValue(b) => Ok(b),
            v @ _ => Err(FormParseErr::IncorrectType(v))
        }
    }
}

impl TryFrom<Value> for String {
    type Error = FormParseErr;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Text(t) => {
                Ok(t)
            }
            v @ _ => Err(FormParseErr::IncorrectType(v))
        }
    }
}

impl<T: TryFrom<Value, Error=FormParseErr>> TryFrom<Value> for Vec<T> {
    type Error = FormParseErr;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Record(attr, items) if attr.is_empty() => {
                let length = items.len();
                items.into_iter().try_fold(Vec::with_capacity(length), |mut results: Vec<T>, item| {
                    match item {
                        Item::ValueItem(v) => {
                            let result = T::try_from(v)?;
                            results.push(result);
                            Ok(results)
                        }
                        i @ _ => {
                            Err(FormParseErr::IllegalItem(i))
                        }
                    }
                })
            }
            v @ _ => {
                Err(FormParseErr::IncorrectType(v))
            }
        }
    }
}