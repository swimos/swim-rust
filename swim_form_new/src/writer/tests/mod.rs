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

use crate::writer::as_value;
use crate::writer::TransmuteValue;
use common::model::{Item, Value};
mod bigint;
mod compound;
mod enumeration;

macro_rules! test_impl {
    ($test_name:ident, $typ:expr, $expected:expr) => {
        #[test]
        fn $test_name() {
            let value = as_value(&$typ);
            assert_eq!(value, $expected);
        }
    };
}

test_impl!(test_bool, true, Value::BooleanValue(true));
test_impl!(test_i32, 100i32, Value::Int32Value(100));
test_impl!(test_i64, 100i64, Value::Int64Value(100));
test_impl!(test_f64, 100.0f64, Value::Float64Value(100.0));
test_impl!(test_opt_some, Some(100i32), Value::Int32Value(100));
test_impl!(
    test_string,
    String::from("test"),
    Value::Text(String::from("test"))
);
test_impl!(
    test_vec,
    vec![1, 2, 3, 4, 5],
    Value::Record(
        Vec::new(),
        vec![
            Item::of(Value::Int32Value(1)),
            Item::of(Value::Int32Value(2)),
            Item::of(Value::Int32Value(3)),
            Item::of(Value::Int32Value(4)),
            Item::of(Value::Int32Value(5)),
        ]
    )
);

#[test]
fn test_opt_none() {
    let r: Option<i32> = None;
    let value = as_value(&r);
    assert_eq!(value, Value::Extant);
}
