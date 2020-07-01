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

use crate::model::{Item, Value, ValueKind};

#[test]
fn test_i32() {
    let value = Value::Int32Value(100);

    assert_eq!(Value::is_coercible_to(&value, ValueKind::Int32), true);
    assert_eq!(Value::is_coercible_to(&value, ValueKind::Int64), true);

    assert_eq!(
        Value::is_coercible_to(&Value::Int32Value(-100), ValueKind::UInt32),
        false
    );
    assert_eq!(
        Value::is_coercible_to(&Value::Int32Value(-100), ValueKind::UInt64),
        false
    );

    assert_eq!(Value::is_coercible_to(&value, ValueKind::UInt32), true);
    assert_eq!(Value::is_coercible_to(&value, ValueKind::UInt64), true);
    assert_eq!(
        Value::is_coercible_to(&Value::Int32Value(0), ValueKind::Extant),
        true
    );
    assert_eq!(Value::is_coercible_to(&value, ValueKind::Record), true);

    assert_eq!(Value::is_coercible_to(&value, ValueKind::Boolean), false);
    assert_eq!(
        Value::is_coercible_to(&Value::Int32Value(0), ValueKind::Boolean),
        true
    );

    assert_eq!(Value::is_coercible_to(&value, ValueKind::Text), true);
}

#[test]
fn test_u32() {
    let value = Value::UInt32Value(100);

    assert_eq!(Value::is_coercible_to(&value, ValueKind::Int32), true);
    assert_eq!(Value::is_coercible_to(&value, ValueKind::Int64), true);

    assert_eq!(
        Value::is_coercible_to(&Value::UInt32Value(u32::max_value()), ValueKind::Int32),
        false
    );
    assert_eq!(
        Value::is_coercible_to(&Value::UInt32Value(u32::max_value()), ValueKind::UInt64),
        true
    );

    assert_eq!(Value::is_coercible_to(&value, ValueKind::Int32), true);
    assert_eq!(Value::is_coercible_to(&value, ValueKind::Int64), true);
    assert_eq!(
        Value::is_coercible_to(&Value::UInt32Value(0), ValueKind::Extant),
        true
    );
    assert_eq!(Value::is_coercible_to(&value, ValueKind::Record), true);

    assert_eq!(Value::is_coercible_to(&value, ValueKind::Boolean), false);
    assert_eq!(
        Value::is_coercible_to(&Value::UInt32Value(0), ValueKind::Boolean),
        true
    );

    assert_eq!(Value::is_coercible_to(&value, ValueKind::Text), true);
}

#[test]
fn test_u64() {
    let value = Value::UInt64Value(100);

    assert_eq!(Value::is_coercible_to(&value, ValueKind::Int32), true);
    assert_eq!(Value::is_coercible_to(&value, ValueKind::Int64), true);

    assert_eq!(
        Value::is_coercible_to(&Value::UInt64Value(u64::max_value()), ValueKind::Int32),
        false
    );
    assert_eq!(
        Value::is_coercible_to(&Value::UInt64Value(u64::max_value()), ValueKind::Int64),
        false
    );

    assert_eq!(Value::is_coercible_to(&value, ValueKind::Int32), true);
    assert_eq!(Value::is_coercible_to(&value, ValueKind::Int64), true);
    assert_eq!(
        Value::is_coercible_to(&Value::UInt64Value(0), ValueKind::Extant),
        true
    );
    assert_eq!(Value::is_coercible_to(&value, ValueKind::Record), true);

    assert_eq!(Value::is_coercible_to(&value, ValueKind::Boolean), false);
    assert_eq!(
        Value::is_coercible_to(&Value::UInt64Value(0), ValueKind::Boolean),
        true
    );

    assert_eq!(Value::is_coercible_to(&value, ValueKind::Text), true);
}

#[test]
fn test_f64() {
    let value = Value::Float64Value(0.0);

    assert_eq!(Value::is_coercible_to(&value, ValueKind::Extant), true);
    assert_eq!(Value::is_coercible_to(&value, ValueKind::Boolean), true);
}

#[test]
fn test_text() {
    assert_eq!(
        Value::is_coercible_to(&Value::Text(String::from("swim.ai")), ValueKind::Int32),
        false
    );
    assert_eq!(
        Value::is_coercible_to(&Value::Text(String::from("10")), ValueKind::Int32),
        true
    );
    assert_eq!(
        Value::is_coercible_to(&Value::Text(String::from("10")), ValueKind::Int64),
        true
    );
    assert_eq!(
        Value::is_coercible_to(
            &Value::Text(u32::max_value().to_string()),
            ValueKind::UInt32
        ),
        true
    );
    assert_eq!(
        Value::is_coercible_to(
            &Value::Text(u64::max_value().to_string()),
            ValueKind::UInt64
        ),
        true
    );
    assert_eq!(
        Value::is_coercible_to(&Value::Text(String::from("1.0000")), ValueKind::Float64),
        true
    );
    assert_eq!(
        Value::is_coercible_to(&Value::Text(String::from("true")), ValueKind::Boolean),
        true
    );
}

#[test]
fn test_record() {
    assert_eq!(
        Value::is_coercible_to(
            &Value::record(vec![Item::slot("swim", 3)]),
            ValueKind::Boolean
        ),
        false
    );

    assert_eq!(
        Value::is_coercible_to(
            &Value::record(vec![Item::ValueItem(1.into())]),
            ValueKind::Boolean
        ),
        true
    );
}
