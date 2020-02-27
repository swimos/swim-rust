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
use crate::structure::form::compound::{to_string, Serializer};
use crate::model::Item::ValueItem;

use serde::{Serialize};


#[test]
fn vecs() {
    let mut serializer = Serializer::new();

    serializer.current_state.attr_name = Some(String::from("a"));
    serializer.enter_sequence();
    serializer.push_value(Value::Int32Value(1));
    serializer.push_value(Value::Int32Value(2));
    serializer.exit_sequence();

    serializer.current_state.attr_name = Some(String::from("b"));
    serializer.enter_sequence();
    serializer.push_value(Value::Int32Value(3));
    serializer.push_value(Value::Int32Value(4));
    serializer.exit_sequence();

    println!("{:?}", serializer.output());
}

#[test]
fn nested() {
    let mut serializer = Serializer::new();

    serializer.current_state.attr_name = Some(String::from("a"));
    serializer.enter_sequence();

    serializer.current_state.attr_name = Some(String::from("b"));
    serializer.enter_sequence();
    serializer.push_value(Value::Int32Value(1));
    serializer.push_value(Value::Int32Value(2));
    serializer.exit_sequence();

    serializer.current_state.attr_name = Some(String::from("c"));
    serializer.enter_sequence();
    serializer.push_value(Value::Int32Value(3));
    serializer.push_value(Value::Int32Value(4));
    serializer.exit_sequence();

    serializer.exit_sequence();

    println!("{:?}", serializer.output());
}

#[test]
fn simple_struct() {
    #[derive(Serialize)]
    struct Test {
        a: u32,
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
fn test_struct() {
    #[derive(Serialize)]
    struct Test {
        a:i32,
        b:f64
    }

    let test = Test {
        a:1,
        b:2.0
    };

    let parsed_value = to_string(&test).unwrap();
    let expected = Value::Record(Vec::new(), vec![
        Item::Slot(Value::Text(String::from("a")), Value::Int32Value(1)),
        Item::Slot(Value::Text(String::from("b")), Value::Float64Value(2.0)),
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
        int: u32,
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

#[test]
fn vec_of_vecs() {
    #[derive(Serialize)]
    struct Test {
        seq: Vec<Vec<&'static str>>,
    }

    let test = Test {
        seq: vec![
            vec!["a", "b"],
            vec!["c", "d"]
        ],
    };

    let parsed_value = to_string(&test).unwrap();
    let expected = Value::Record(Vec::new(), vec![
        Item::Slot(Value::Text(String::from("seq")), Value::Record(Vec::new(), vec![
            Item::ValueItem(Value::Record(Vec::new(), vec![
                Item::ValueItem(Value::Text(String::from("a"))),
                Item::ValueItem(Value::Text(String::from("b"))),
            ])),
            Item::ValueItem(Value::Record(Vec::new(), vec![
                Item::ValueItem(Value::Text(String::from("c"))),
                Item::ValueItem(Value::Text(String::from("d"))),
            ]))
        ]))]);

    assert_eq!(parsed_value, expected);
}