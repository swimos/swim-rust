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

#[cfg(test)]
mod valid {
    use crate::model::{Attr, Item, Value};
    use crate::structure::form::to::tests::assert_err;
    use crate::structure::form::{Form, FormParseErr};

    use super::*;

    #[test]
    fn generic() {
        #[derive(Serialize)]
        struct Test<T> {
            v: T,
        }

        let test = Test {
            v: String::from("hello"),
        };

        let parsed_value = Form::default().to_value(&test).unwrap();

        let expected = Value::Record(vec![Attr::of("Test")], vec![Item::slot("v", "hello")]);

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
            b: Child { a: 0 },
        };

        let parsed_value = Form::default().to_value(&test);
        assert_err(
            parsed_value,
            FormParseErr::UnsupportedType(String::from("u64")),
        );
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

        let parsed_value = Form::default().to_value(&test).unwrap();

        let expected = Value::Record(
            vec![Attr::of("Parent")],
            vec![
                Item::slot("a", 0),
                Item::slot(
                    "b",
                    Value::Record(
                        vec![Attr::of("Child")],
                        vec![
                            Item::slot("a", Value::Int64Value(1)),
                            Item::slot("b", "child1"),
                        ],
                    ),
                ),
                Item::slot(
                    "c",
                    Value::Record(
                        vec![Attr::of("Child")],
                        vec![
                            Item::slot("a", Value::Int64Value(2)),
                            Item::slot("b", "child2"),
                        ],
                    ),
                ),
            ],
        );

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

        let parsed_value = Form::default().to_value(&test).unwrap();

        let expected = Value::Record(
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

        let parsed_value = Form::default().to_value(&test).unwrap();

        let expected = Value::Record(
            vec![Attr::of("TestStruct")],
            vec![
                Item::slot(
                    "a",
                    Value::Record(
                        vec![Attr::of("TestStruct")],
                        vec![
                            Item::slot(
                                "a",
                                Value::Record(
                                    vec![Attr::of("TestStruct")],
                                    vec![
                                        Item::slot("a", Value::Extant),
                                        Item::slot("b", Value::Extant),
                                    ],
                                ),
                            ),
                            Item::slot(
                                Value::from("b"),
                                Value::Record(
                                    vec![Attr::of("TestStruct")],
                                    vec![
                                        Item::slot("a", Value::Extant),
                                        Item::slot("b", Value::Extant),
                                    ],
                                ),
                            ),
                        ],
                    ),
                ),
                Item::slot(
                    Value::from("b"),
                    Value::Record(
                        vec![Attr::of("TestStruct")],
                        vec![
                            Item::slot(
                                Value::from("a"),
                                Value::Record(
                                    vec![Attr::of("TestStruct")],
                                    vec![
                                        Item::slot("a", Value::Extant),
                                        Item::slot("b", Value::Extant),
                                    ],
                                ),
                            ),
                            Item::slot(
                                Value::from("b"),
                                Value::Record(
                                    vec![Attr::of("TestStruct")],
                                    vec![
                                        Item::slot("a", Value::Extant),
                                        Item::slot("b", Value::Extant),
                                    ],
                                ),
                            ),
                        ],
                    ),
                ),
            ],
        );

        assert_eq!(parsed_value, expected);
    }
}
