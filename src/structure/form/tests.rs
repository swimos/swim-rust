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

use hamcrest2::assert_that;
use hamcrest2::prelude::*;

use crate::model::{Item, Value};
use crate::model::Value::Record;
use crate::structure::form::FormParseErr;

fn assert_success<T: PartialEq + Debug>(r: Result<T, FormParseErr>, expected: T) {
    match r {
        Ok(r) => {
            assert_eq!(r, expected);
        }
        Err(e) => {
            panic!("{:?}", e);
        }
    }
}

fn assert_err<T: PartialEq + Debug>(r: Result<T, FormParseErr>, expected: FormParseErr) {
    match r {
        Ok(_) => {
            panic!("Expected {:?} but parsed correctly", expected);
        }
        Err(e) => {
            assert_eq!(e, expected);
        }
    }
}

#[test]
fn from_bool() {
    let r = bool::try_from(Value::BooleanValue(true));
    assert_success(r, true);

    let r = bool::try_from(Value::BooleanValue(false));
    assert_success(r, false);

    let r = bool::try_from(Value::Int32Value(1));
    assert_err(r, FormParseErr::IncorrectType(Value::Int32Value(1)));

    let r = bool::try_from(Value::Int64Value(0));
    assert_err(r, FormParseErr::IncorrectType(Value::Int64Value(0)));

    let r = bool::try_from(Value::Float64Value(1.0));
    assert_err(r, FormParseErr::IncorrectType(Value::Float64Value(1.0)));

    let r = bool::try_from(Value::Float64Value(0.0));
    assert_err(r, FormParseErr::IncorrectType(Value::Float64Value(0.0)));

    let r = bool::try_from(Value::Text(String::from("true")));
    assert_err(r, FormParseErr::IncorrectType(Value::Text(String::from("true"))));

    let r = bool::try_from(Value::Text(String::from("false")));
    assert_err(r, FormParseErr::IncorrectType(Value::Text(String::from("false"))));

    let r = bool::try_from(Value::Float64Value(2.0));
    assert_err(r, FormParseErr::IncorrectType(Value::Float64Value(2.0)));
}

#[test]
fn from_f64() {
    let r = f64::try_from(Value::Float64Value(1.0));
    assert_success(r, 1.0);

    let r = f64::try_from(Value::Int32Value(1));
    assert_success(r, 1.0);

    let r = f64::try_from(Value::Int64Value(1));
    assert_success(r, 1.0);

    let r = f64::try_from(Value::Text(String::from("1.0")));
    assert_err(r, FormParseErr::IncorrectType(Value::Text(String::from("1.0"))));
}

#[test]
fn from_i64() {
    let r = i64::try_from(Value::Int64Value(1));
    assert_success(r, 1);

    let r = i64::try_from(Value::Int32Value(1));
    assert_success(r, 1);

    let r = i64::try_from(Value::Text(String::from("1")));
    assert_err(r, FormParseErr::IncorrectType(Value::Text(String::from("1"))));
}

#[test]
fn from_str() {
    let r = String::try_from(Value::Float64Value(1.0));
    assert_err(r, FormParseErr::IncorrectType(Value::Float64Value(1.0)));
}

#[test]
fn vector_mismatched_types() {
    let r = Vec::<i64>::try_from(Record(Vec::new(), vec![
        Item::ValueItem(Value::Int64Value(1)),
        Item::ValueItem(Value::Float64Value(1.0))
    ]));

    match r {
        Ok(_) => {
            panic!("Parsed correctly with mismatched types.")
        }
        Err(e) => {
            assert_that!(e, eq(FormParseErr::IncorrectType(Value::Float64Value(1.0))));
        }
    }
}

#[test]
fn vector() {
    let r = Vec::<i64>::try_from(Record(Vec::new(), vec![
        Item::ValueItem(Value::Int64Value(1)),
        Item::ValueItem(Value::Int64Value(2)),
        Item::ValueItem(Value::Int64Value(3)),
        Item::ValueItem(Value::Int64Value(4)),
        Item::ValueItem(Value::Int64Value(5)),
    ]));

    match r {
        Ok(r) => {
            let expected = vec![1, 2, 3, 4, 5];
            assert_eq!(r, expected);
        }
        Err(e) => {
            panic!("{:?}", e);
        }
    }
}
