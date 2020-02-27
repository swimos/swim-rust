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

use crate::structure::form::form::{SerializerError, to_value};
use crate::structure::form::tests::assert_err;

#[cfg(test)]
mod valid {
    use crate::model::{Item, Value};

    use super::*;

    #[test]
    fn generic() {
        #[derive(Serialize)]
        struct Test<T> {
            v: T,
        }

        let test = Test {
            v: String::from("hello")
        };

        let parsed_value = to_value(&test).unwrap();

        let expected = Value::Record(Vec::new(), vec![
            Item::Slot(Value::Text(String::from("v")), Value::Text(String::from("hello"))),
        ]);

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn illegal_struct() {
        #[derive(Serialize)]
        struct Parent {
            a: i32,
            b: Child,
        }

        #[derive(Serialize)]
        struct Child {
            a: u64,
        }

        let test = Parent {
            a: 0,
            b: Child {
                a: 0
            },
        };

        let parsed_value = to_value(&test);
        assert_err(parsed_value, SerializerError::UnsupportedType(String::from("u64")));
    }

    #[test]
    fn valid_struct() {
        #[derive(Serialize)]
        struct Parent {
            a: i32,
            b: Child,
            c: Child,
        }

        #[derive(Serialize)]
        struct Child {
            a: i64,
            b: String,
        }

        let test = Parent {
            a: 0,
            b: Child {
                a: 1,
                b: String::from("child1"),
            },
            c: Child {
                a: 2,
                b: String::from("child2"),
            },
        };

        let parsed_value = to_value(&test).unwrap();

        let expected = Value::Record(Vec::new(), vec![
            Item::Slot(Value::Text(String::from("a")), Value::Int32Value(0)),
            Item::Slot(Value::Text(String::from("b")), Value::Record(Vec::new(), vec![
                Item::Slot(Value::Text(String::from("a")), Value::Int64Value(1)),
                Item::Slot(Value::Text(String::from("b")), Value::Text(String::from("child1"))),
            ])),
            Item::Slot(Value::Text(String::from("c")), Value::Record(Vec::new(), vec![
                Item::Slot(Value::Text(String::from("a")), Value::Int64Value(2)),
                Item::Slot(Value::Text(String::from("b")), Value::Text(String::from("child2"))),
            ]))
        ]);

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn deep_struct() {
        #[derive(Serialize)]
        struct TestStruct {
            a: i32,
            b: Box<Option<TestStruct>>,
        }

        let test = TestStruct {
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

        let parsed_value = to_value(&test).unwrap();

        let expected = Value::Record(Vec::new(), vec![
            Item::Slot(Value::Text(String::from("a")), Value::Int32Value(0)),
            Item::Slot(Value::Text(String::from("b")), Value::Record(Vec::new(), vec![
                Item::Slot(Value::Text(String::from("a")), Value::Int32Value(1)),
                Item::Slot(Value::Text(String::from("b")), Value::Record(Vec::new(), vec![
                    Item::Slot(Value::Text(String::from("a")), Value::Int32Value(2)),
                    Item::Slot(Value::Text(String::from("b")), Value::Record(Vec::new(), vec![
                        Item::Slot(Value::Text(String::from("a")), Value::Int32Value(3)),
                        Item::Slot(Value::Text(String::from("b")), Value::Record(Vec::new(), vec![
                            Item::Slot(Value::Text(String::from("a")), Value::Int32Value(4)),
                            Item::Slot(Value::Text(String::from("b")), Value::Extant),
                        ])),
                    ])),
                ])),
            ])),
        ]);

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn multiple_deep_structs() {
        #[derive(Serialize)]
        struct TestStruct {
            a: Box<Option<TestStruct>>,
            b: Box<Option<TestStruct>>,
        }

        let test = TestStruct {
            a: Box::new(Some(TestStruct {
                a: Box::new(Some(TestStruct {
                    a: Box::new(None),
                    b: Box::new(None),
                })),
                b: Box::new(Some(TestStruct {
                    a: Box::new(None),
                    b: Box::new(None),
                })),
            })),
            b: Box::new(Some(TestStruct {
                a: Box::new(Some(TestStruct {
                    a: Box::new(None),
                    b: Box::new(None),
                })),
                b: Box::new(Some(TestStruct {
                    a: Box::new(None),
                    b: Box::new(None),
                })),
            })),
        };

        let parsed_value = to_value(&test).unwrap();

        let expected = Value::Record(Vec::new(), vec![
            Item::Slot(Value::Text(String::from("a")), Value::Record(Vec::new(), vec![
                Item::Slot(Value::Text(String::from("a")), Value::Record(Vec::new(), vec![
                    Item::Slot(Value::Text(String::from("a")), Value::Extant),
                    Item::Slot(Value::Text(String::from("b")), Value::Extant)],
                )),
                Item::Slot(Value::Text(String::from("b")), Value::Record(Vec::new(), vec![
                    Item::Slot(Value::Text(String::from("a")), Value::Extant),
                    Item::Slot(Value::Text(String::from("b")), Value::Extant)],
                ))],
            )),
            Item::Slot(Value::Text(String::from("b")), Value::Record(Vec::new(), vec![
                Item::Slot(Value::Text(String::from("a")), Value::Record(Vec::new(), vec![
                    Item::Slot(Value::Text(String::from("a")), Value::Extant),
                    Item::Slot(Value::Text(String::from("b")), Value::Extant)]),
                ),
                Item::Slot(Value::Text(String::from("b")), Value::Record(Vec::new(), vec![
                    Item::Slot(Value::Text(String::from("a")), Value::Extant),
                    Item::Slot(Value::Text(String::from("b")), Value::Extant)]),
                )]),
            )],
        );

        assert_eq!(parsed_value, expected);
    }
}
