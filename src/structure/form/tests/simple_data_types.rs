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



use crate::model::{Item, Value};
use crate::structure::form::compound::{to_string, SerializerError};
use crate::model::Item::ValueItem;

use serde::Serialize;

fn assert_err(parsed:Result<Value, SerializerError>, expected:SerializerError){
    match parsed {
        Ok(v)=> {
            eprintln!("Expected error: {:?}", v);
            panic!();
        }
        Err(e) => assert_eq!(e, expected)
    }
}

#[test]
fn test_u8() {
    #[derive(Serialize)]
    struct Test {
        a: u8,
    }

    let test = Test {
        a: 1,
    };

    let parsed_value = to_string(&test);
    assert_err(parsed_value, SerializerError::UnsupportedType(String::from("u8")));
}

#[test]
fn test_u16() {
    #[derive(Serialize)]
    struct Test {
        a: u16,
    }

    let test = Test {
        a: 1,
    };

    let parsed_value = to_string(&test);
    assert_err(parsed_value, SerializerError::UnsupportedType(String::from("u16")));
}

#[test]
fn test_32() {
    #[derive(Serialize)]
    struct Test {
        a: u32,
    }

    let test = Test {
        a: 1,
    };

    let parsed_value = to_string(&test);
    assert_err(parsed_value, SerializerError::UnsupportedType(String::from("u32")));
}

#[test]
fn test_u64() {
    #[derive(Serialize)]
    struct Test {
        a: u64,
    }

    let test = Test {
        a: 1,
    };

    let parsed_value = to_string(&test);
    assert_err(parsed_value, SerializerError::UnsupportedType(String::from("u64")));
}

#[test]
fn simple_struct() {
    #[derive(Serialize)]
    struct Test {
        a: i32,
        b: f32,
        c: i8,
        d: String,
    }

    let test = Test {
        a: 1,
        b: 2.0,
        c: 3,
        d: String::from("hello"),
    };

    let parsed_value = to_string(&test).unwrap();
    let expected = Value::Record(Vec::new(), vec![
        Item::Slot(Value::Text(String::from("a")), Value::Int32Value(1)),
        Item::Slot(Value::Text(String::from("b")), Value::Float64Value(2.0)),
        Item::Slot(Value::Text(String::from("c")), Value::Int32Value(3)),
        Item::Slot(Value::Text(String::from("d")), Value::Text(String::from("hello"))),
    ]);

    assert_eq!(parsed_value, expected);
}

#[test]
fn struct_with_vec() {
    #[derive(Serialize)]
    struct Test {
        seq: Vec<&'static str>,
    }

    let test = Test {
        seq: vec!["a", "b"],
    };

    let parsed_value = to_string(&test).unwrap();
    let expected = Value::Record(Vec::new(), vec![
        Item::Slot(Value::Text(String::from("seq")), Value::Record(Vec::new(), vec![
            ValueItem(Value::Text(String::from("a"))),
            ValueItem(Value::Text(String::from("b"))),
        ])),
    ]);

    assert_eq!(parsed_value, expected);
}

#[test]
fn struct_with_vec_and_members() {
    #[derive(Serialize)]
    struct Test {
        int: i32,
        seq: Vec<&'static str>,
    }

    let test = Test {
        int: 1,
        seq: vec!["a", "b"],
    };

    let parsed_value = to_string(&test).unwrap();
    let expected = Value::Record(Vec::new(), vec![
        Item::Slot(Value::Text(String::from("int")), Value::Int32Value(1)),
        Item::Slot(Value::Text(String::from("seq")), Value::Record(Vec::new(), vec![
            ValueItem(Value::Text(String::from("a"))),
            ValueItem(Value::Text(String::from("b"))),
        ])),
    ]);

    assert_eq!(parsed_value, expected);
}
