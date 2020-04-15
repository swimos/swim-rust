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

use serde::Deserialize;

use common::model::{Attr, Item, Value};

use crate::tests::from_value;

#[cfg(test)]
mod valid {
    use super::*;

    #[test]
    fn generic() {
        type T = String;

        #[derive(Deserialize, PartialEq, Debug)]
        struct Test<T> {
            v: T,
        }

        let expected = Test {
            v: String::from("hello"),
        };

        let record = Value::Record(vec![Attr::of("Test")], vec![Item::slot("v", "hello")]);
        let parsed_value = from_value::<Test<T>>(&record).unwrap();

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn boxed_field() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct TestStruct {
            a: i32,
            b: Box<Option<TestStruct>>,
        }

        let expected = TestStruct {
            a: 1,
            b: Box::new(None),
        };

        let record = Value::Record(
            vec![Attr::from("TestStruct")],
            vec![
                Item::from(("a", Value::Int32Value(1))),
                Item::from(("b", Value::Extant)),
            ],
        );
        let parsed_value = from_value::<TestStruct>(&record).unwrap();

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn nested_boxes() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct TestStruct {
            a: i32,
            b: Box<Option<TestStruct>>,
        }

        let expected = TestStruct {
            a: 1,
            b: Box::new(Some(TestStruct {
                a: 2,
                b: Box::new(None),
            })),
        };

        let record = Value::Record(
            vec![Attr::from("TestStruct")],
            vec![
                Item::from(("a", Value::Int32Value(1))),
                Item::from((
                    "b",
                    Value::Record(
                        vec![Attr::from("TestStruct")],
                        vec![
                            Item::from(("a", Value::Int32Value(2))),
                            Item::from(("b", Value::Extant)),
                        ],
                    ),
                )),
            ],
        );
        let parsed_value = from_value::<TestStruct>(&record).unwrap();

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn deep_struct() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct TestStruct {
            a: i32,
            b: Box<Option<TestStruct>>,
        }

        let expected = TestStruct {
            a: 0,
            b: Box::new(Some(TestStruct {
                a: 1,
                b: Box::new(Some(TestStruct {
                    a: 2,
                    b: Box::new(Some(TestStruct {
                        a: 3,
                        b: Box::new(Some(TestStruct {
                            a: 4,
                            b: Box::new(None),
                        })),
                    })),
                })),
            })),
        };

        let record = Value::Record(
            vec![Attr::of("TestStruct")],
            vec![
                Item::slot("a", 0),
                Item::slot(
                    "b",
                    Value::Record(
                        vec![Attr::of("TestStruct")],
                        vec![
                            Item::slot("a", 1),
                            Item::slot(
                                "b",
                                Value::Record(
                                    vec![Attr::of("TestStruct")],
                                    vec![
                                        Item::slot("a", 2),
                                        Item::slot(
                                            "b",
                                            Value::Record(
                                                vec![Attr::of("TestStruct")],
                                                vec![
                                                    Item::slot("a", 3),
                                                    Item::slot(
                                                        Value::from("b"),
                                                        Value::Record(
                                                            vec![Attr::of("TestStruct")],
                                                            vec![
                                                                Item::slot("a", 4),
                                                                Item::slot("b", Value::Extant),
                                                            ],
                                                        ),
                                                    ),
                                                ],
                                            ),
                                        ),
                                    ],
                                ),
                            ),
                        ],
                    ),
                ),
            ],
        );

        let parsed_value = from_value::<TestStruct>(&record).unwrap();

        assert_eq!(parsed_value, expected);
    }
}
