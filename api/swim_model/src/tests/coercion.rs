// Copyright 2015-2021 Swim Inc.
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

use crate::{Value, ValueKind};

#[test]
fn test_i32() {
    let value = Value::Int32Value(100);

    assert!(Value::is_coercible_to(&value, ValueKind::Int32));
    assert!(Value::is_coercible_to(&value, ValueKind::Int64));

    assert!(!Value::is_coercible_to(
        &Value::Int32Value(-100),
        ValueKind::UInt32
    ));
    assert!(!Value::is_coercible_to(
        &Value::Int32Value(-100),
        ValueKind::UInt64
    ));

    assert!(Value::is_coercible_to(&value, ValueKind::UInt32));
    assert!(Value::is_coercible_to(&value, ValueKind::UInt64));
    assert!(!Value::is_coercible_to(
        &Value::Int32Value(0),
        ValueKind::Extant
    ));
    assert!(!Value::is_coercible_to(&value, ValueKind::Record));

    assert!(!Value::is_coercible_to(&value, ValueKind::Boolean));
    assert!(!Value::is_coercible_to(
        &Value::Int32Value(0),
        ValueKind::Boolean
    ));

    assert!(!Value::is_coercible_to(&value, ValueKind::Text));
}

#[test]
fn test_u32() {
    let value = Value::UInt32Value(100);

    assert!(Value::is_coercible_to(&value, ValueKind::Int32));
    assert!(Value::is_coercible_to(&value, ValueKind::Int64));

    assert!(!Value::is_coercible_to(
        &Value::UInt32Value(u32::MAX),
        ValueKind::Int32
    ));
    assert!(Value::is_coercible_to(
        &Value::UInt32Value(u32::MAX),
        ValueKind::UInt64
    ));

    assert!(Value::is_coercible_to(&value, ValueKind::Int32));
    assert!(Value::is_coercible_to(&value, ValueKind::Int64));
    assert!(!Value::is_coercible_to(
        &Value::UInt32Value(0),
        ValueKind::Extant
    ));
    assert!(!Value::is_coercible_to(&value, ValueKind::Record));

    assert!(!Value::is_coercible_to(&value, ValueKind::Boolean));
    assert!(!Value::is_coercible_to(
        &Value::UInt32Value(0),
        ValueKind::Boolean
    ));

    assert!(!Value::is_coercible_to(&value, ValueKind::Text));
}

#[test]
fn test_u64() {
    let value = Value::UInt64Value(100);

    assert!(Value::is_coercible_to(&value, ValueKind::Int32));
    assert!(Value::is_coercible_to(&value, ValueKind::Int64));

    assert!(!Value::is_coercible_to(
        &Value::UInt64Value(u64::MAX),
        ValueKind::Int32
    ));
    assert!(!Value::is_coercible_to(
        &Value::UInt64Value(u64::MAX),
        ValueKind::Int64
    ));

    assert!(Value::is_coercible_to(&value, ValueKind::Int32));
    assert!(Value::is_coercible_to(&value, ValueKind::Int64));
    assert!(!Value::is_coercible_to(
        &Value::UInt64Value(0),
        ValueKind::Extant
    ));
    assert!(!Value::is_coercible_to(&value, ValueKind::Record));

    assert!(!Value::is_coercible_to(&value, ValueKind::Boolean));
    assert!(!Value::is_coercible_to(
        &Value::UInt64Value(0),
        ValueKind::Boolean
    ));

    assert!(!Value::is_coercible_to(&value, ValueKind::Text));
}
