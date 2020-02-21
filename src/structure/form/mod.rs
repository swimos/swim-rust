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

use std::convert::TryFrom;
use std::fmt::Debug;

use crate::model::{Item, Value};

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

impl TryFrom<Value> for f64 {
    type Error = FormParseErr;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Int32Value(i) => {
                Ok(i as f64)
            }
            Value::Int64Value(i) => {
                Ok(i as f64)
            }
            Value::Text(t) => {
                let r = t.parse::<f64>();
                match r {
                    Ok(n) => Ok(n),
                    Err(e) => Err(FormParseErr::InvalidString(e.to_string()))
                }
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
            Value::Text(t) => {
                let r = t.parse::<i32>();
                match r {
                    Ok(n) => Ok(n),
                    Err(e) => Err(FormParseErr::InvalidString(e.to_string()))
                }
            }
            Value::Int32Value(i) => Ok(i),
            v @ _ => Err(FormParseErr::IncorrectType(v))
        }
    }
}

impl TryFrom<Value> for i64 {
    type Error = FormParseErr;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Text(t) => {
                let r = t.parse::<i64>();
                match r {
                    Ok(n) => Ok(n),
                    Err(e) => Err(FormParseErr::InvalidString(e.to_string()))
                }
            }
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
            Value::Text(t) => {
                if t.to_lowercase() == "true" {
                    Ok(true)
                } else if t.to_lowercase() == "false" {
                    Ok(false)
                } else {
                    Err(FormParseErr::NotABoolean)
                }
            }
            Value::Int64Value(n) => {
                if n == 0 || n == 1 {
                    Ok(n == 1)
                } else {
                    Err(FormParseErr::NotABoolean)
                }
            }
            Value::Int32Value(n) => {
                if n == 0 || n == 1 {
                    Ok(n == 1)
                } else {
                    Err(FormParseErr::NotABoolean)
                }
            }
            Value::Float64Value(n) => {
                if n == 0.0 || n == 1.0 {
                    Ok(n == 1.0)
                } else {
                    Err(FormParseErr::NotABoolean)
                }
            }
            Value::BooleanValue(b) => Ok(b),
            v @ _ => Err(FormParseErr::IncorrectType(v))
        }
    }
}

impl TryFrom<Value> for String {
    type Error = FormParseErr;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Int32Value(i) => {
                Ok(i.to_string())
            }
            Value::Int64Value(i) => {
                Ok(i.to_string())
            }
            Value::Float64Value(f) => {
                Ok(f.to_string())
            }
            Value::Text(t) => {
                Ok(t)
            }
            Value::BooleanValue(b) => {
                Ok(b.to_string())
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
                items.iter().try_fold(Vec::with_capacity(items.len()), |mut results: Vec<T>, item| {
                    match item {
                        Item::ValueItem(v) => {
                            match T::try_from(v.to_owned()) {
                                Ok(result) => {
                                    results.push(result);
                                    Ok(results)
                                }
                                Err(e) => {
                                    Err(e)
                                }
                            }
                        }
                        i @ _ => {
                            Err(FormParseErr::IllegalItem(i.to_owned()))
                        }
                    }
                })
            }
            v @ _ => {
                Err(FormParseErr::IncorrectType(v.to_owned()))
            }
        }
    }
}