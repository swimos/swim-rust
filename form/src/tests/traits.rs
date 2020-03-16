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

use std::fmt::Debug;

use hamcrest2::assert_that;
use hamcrest2::prelude::*;

use common::model::Value::Record;
use common::model::{Item, Value};
use deserialize::FormDeserializeErr;

use super::super::Form;
use crate::impls::de_incorrect_type;

fn assert_success<T: PartialEq + Debug>(r: Result<T, FormDeserializeErr>, expected: T) {
    match r {
        Ok(r) => {
            assert_eq!(r, expected);
        }
        Err(e) => {
            panic!("{:?}", e);
        }
    }
}

fn assert_err<T: PartialEq + Debug>(
    r: Result<T, FormDeserializeErr>,
    expected: FormDeserializeErr,
) {
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
    fn expect_err(from: Value) {
        let r = bool::try_from_value(&from);
        assert_err(r, de_incorrect_type::<bool>("bool", &from).err().unwrap());
    }

    let r = bool::try_from_value(&Value::from(true));
    assert_success(r, true);

    let r = bool::try_from_value(&Value::from(false));
    assert_success(r, false);

    expect_err(Value::from(1));
    expect_err(Value::Int64Value(0));

    expect_err(Value::from(1.0));
    expect_err(Value::from(0.0));
    expect_err(Value::from("true"));
    expect_err(Value::from("false"));
    expect_err(Value::from(2.0));
}

#[test]
fn from_f64() {
    let r = f64::try_from_value(&Value::from(1.0));
    assert_success(r, 1.0);
}

#[test]
fn from_i64() {
    let r = i64::try_from_value(&Value::Int64Value(1));
    assert_success(r, 1);
}

#[test]
fn from_str() {
    let r = String::try_from_value(&Value::from(1.0));
    assert_err(
        r,
        de_incorrect_type::<bool>("String", &Value::from(1.0))
            .err()
            .unwrap(),
    );
}

#[test]
fn vector_mismatched_types() {
    let r = Vec::<i64>::try_from_value(&Record(
        Vec::new(),
        vec![Item::ValueItem(Value::Int64Value(1)), Item::from(1.0)],
    ));

    match r {
        Ok(_) => panic!("Parsed correctly with mismatched types."),
        Err(e) => {
            assert_that!(
                e,
                eq(de_incorrect_type::<bool>("i64", &Value::from(1.0))
                    .err()
                    .unwrap())
            );
        }
    }
}

#[test]
fn vector() {
    let r = Vec::<i32>::try_from_value(&Record(
        Vec::new(),
        vec![
            Item::from(1),
            Item::from(2),
            Item::from(3),
            Item::from(4),
            Item::from(5),
        ],
    ));

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
