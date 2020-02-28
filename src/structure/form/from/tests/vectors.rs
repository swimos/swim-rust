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

use serde::Serialize;

use crate::model::{Attr, Item, Value};
use crate::structure::form::from::to_value;

#[test]
fn vector_of_tuples() {
    #[derive(Serialize)]
    struct Parent {
        seq: Vec<(i32, i32)>,
    }

    let test = Parent {
        seq: vec![(1, 2), (3, 4), (5, 6)],
    };

    let parsed_value = to_value(&test).unwrap();
    let expected = Value::Record(
        vec![Attr::of("Parent")],
        vec![Item::Slot(
            Value::Text(String::from("seq")),
            Value::Record(
                Vec::new(),
                vec![
                    Item::ValueItem(Value::Record(
                        Vec::new(),
                        vec![
                            Item::ValueItem(Value::Int32Value(1)),
                            Item::ValueItem(Value::Int32Value(2)),
                        ],
                    )),
                    Item::ValueItem(Value::Record(
                        Vec::new(),
                        vec![
                            Item::ValueItem(Value::Int32Value(3)),
                            Item::ValueItem(Value::Int32Value(4)),
                        ],
                    )),
                    Item::ValueItem(Value::Record(
                        Vec::new(),
                        vec![
                            Item::ValueItem(Value::Int32Value(5)),
                            Item::ValueItem(Value::Int32Value(6)),
                        ],
                    )),
                ],
            ),
        )],
    );

    assert_eq!(parsed_value, expected);
}

#[test]
fn vector_of_structs() {
    #[derive(Serialize)]
    struct Parent {
        seq: Vec<Child>,
    }

    #[derive(Serialize)]
    struct Child {
        id: i32,
    }

    let test = Parent {
        seq: vec![Child { id: 1 }, Child { id: 2 }, Child { id: 3 }],
    };

    let parsed_value = to_value(&test).unwrap();
    let expected = Value::Record(
        vec![Attr::of("Parent")],
        vec![Item::Slot(
            Value::Text(String::from("seq")),
            Value::Record(
                Vec::new(),
                vec![
                    Item::ValueItem(Value::Record(
                        vec![Attr::of("Child")],
                        vec![Item::Slot(
                            Value::Text(String::from("id")),
                            Value::Int32Value(1),
                        )],
                    )),
                    Item::ValueItem(Value::Record(
                        vec![Attr::of("Child")],
                        vec![Item::Slot(
                            Value::Text(String::from("id")),
                            Value::Int32Value(2),
                        )],
                    )),
                    Item::ValueItem(Value::Record(
                        vec![Attr::of("Child")],
                        vec![Item::Slot(
                            Value::Text(String::from("id")),
                            Value::Int32Value(3),
                        )],
                    )),
                ],
            ),
        )],
    );

    assert_eq!(parsed_value, expected);
}

#[test]
fn simple_vector() {
    #[derive(Serialize)]
    struct Test {
        seq: Vec<i32>,
    }

    let test = Test {
        seq: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    };

    let parsed_value = to_value(&test).unwrap();
    let expected = Value::Record(
        vec![Attr::of("Test")],
        vec![Item::Slot(
            Value::Text(String::from("seq")),
            Value::Record(
                Vec::new(),
                vec![
                    Item::ValueItem(Value::Int32Value(1)),
                    Item::ValueItem(Value::Int32Value(2)),
                    Item::ValueItem(Value::Int32Value(3)),
                    Item::ValueItem(Value::Int32Value(4)),
                    Item::ValueItem(Value::Int32Value(5)),
                    Item::ValueItem(Value::Int32Value(6)),
                    Item::ValueItem(Value::Int32Value(7)),
                    Item::ValueItem(Value::Int32Value(8)),
                    Item::ValueItem(Value::Int32Value(9)),
                    Item::ValueItem(Value::Int32Value(10)),
                ],
            ),
        )],
    );

    assert_eq!(parsed_value, expected);
}

#[test]
fn nested_vectors() {
    #[derive(Serialize)]
    struct Test {
        seq: Vec<Vec<&'static str>>,
    }

    let test = Test {
        seq: vec![vec!["a", "b"], vec!["c", "d"]],
    };

    let parsed_value = to_value(&test).unwrap();
    let expected = Value::Record(
        vec![Attr::of("Test")],
        vec![Item::Slot(
            Value::Text(String::from("seq")),
            Value::Record(
                Vec::new(),
                vec![
                    Item::ValueItem(Value::Record(
                        Vec::new(),
                        vec![
                            Item::ValueItem(Value::Text(String::from("a"))),
                            Item::ValueItem(Value::Text(String::from("b"))),
                        ],
                    )),
                    Item::ValueItem(Value::Record(
                        Vec::new(),
                        vec![
                            Item::ValueItem(Value::Text(String::from("c"))),
                            Item::ValueItem(Value::Text(String::from("d"))),
                        ],
                    )),
                ],
            ),
        )],
    );

    assert_eq!(parsed_value, expected);
}
