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

use crate::model::Value;
use crate::model::{Attr, Item};
use crate::structure::form::Form;

#[cfg(test)]
mod illegal {
    use crate::structure::form::FormParseErr;

    use super::*;

    #[test]
    fn mismatched_tag() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct Test {
            a: i32,
            b: i64,
        }

        let mut record = Value::Record(
            vec![Attr::from("Incorrect")],
            vec![
                Item::from(("a", 1)),
                Item::from(("b", Value::Int64Value(2))),
            ],
        );

        let parsed_value = Form::default().from_value::<Test>(&mut record);

        assert_eq!(parsed_value.unwrap_err(), FormParseErr::Malformatted);
    }
}

#[cfg(test)]
mod tuples {
    use super::*;

    #[test]
    fn tuple_struct() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct Test {
            a: i32,
            b: (i64, i64),
        }

        let expected = Test { a: 1, b: (2, 3) };

        let mut record = Value::Record(
            vec![Attr::of("Test")],
            vec![
                Item::from(("a", 1)),
                Item::slot(
                    "b",
                    Value::record(vec![
                        Item::of(Value::Int64Value(2)),
                        Item::of(Value::Int64Value(3)),
                    ]),
                ),
            ],
        );

        let parsed_value = Form::default().from_value::<Test>(&mut record).unwrap();

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn tuple_struct_with_tuple() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct Test(i32, (i64, i64));

        let expected = Test(1, (2, 3));

        let mut record = Value::record(vec![
            Item::from(1),
            Item::from(Value::record(vec![
                Item::from(Value::Int64Value(2)),
                Item::from(Value::Int64Value(3)),
            ])),
        ]);

        let parsed_value = Form::default().from_value::<Test>(&mut record).unwrap();

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn simple_tuple() {
        let expected = (1, 2);
        let mut record = Value::record(vec![Item::from(1), Item::from(2)]);
        let parsed_value = Form::default()
            .from_value::<(i32, i32)>(&mut record)
            .unwrap();

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn struct_with_tuple() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct Test {
            a: i32,
            b: (i64, i64),
        }

        let expected = Test { a: 1, b: (2, 3) };

        let mut record = Value::Record(
            vec![Attr::of("Test")],
            vec![
                Item::of(("a", 1)),
                Item::slot(
                    "b",
                    Value::record(vec![
                        Item::of(Value::Int64Value(2)),
                        Item::of(Value::Int64Value(3)),
                    ]),
                ),
            ],
        );

        let parsed_value = Form::default().from_value::<Test>(&mut record).unwrap();

        assert_eq!(parsed_value, expected);
    }
}

#[cfg(test)]
mod enumeration {
    use super::*;

    #[test]
    fn simple() {
        #[derive(Deserialize, PartialEq, Debug)]
        enum TestEnum {
            A,
        }

        #[derive(Deserialize, PartialEq, Debug)]
        struct Test {
            a: TestEnum,
        }

        let mut record = Value::Record(
            vec![Attr::of("Test")],
            vec![Item::slot("a", Value::of_attr("A"))],
        );
        let parsed_value = Form::default().from_value::<Test>(&mut record).unwrap();

        let expected = Test { a: TestEnum::A };

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn with_tuple() {
        #[derive(Deserialize, PartialEq, Debug)]
        enum TestEnum {
            A(i32, i32),
        }

        #[derive(Deserialize, PartialEq, Debug)]
        struct Test {
            a: TestEnum,
        }

        let mut record = Value::Record(
            vec![Attr::of("Test")],
            vec![Item::slot(
                "a",
                Value::Record(vec![Attr::of("A")], vec![Item::from(1), Item::from(2)]),
            )],
        );

        let parsed_value = Form::default().from_value::<Test>(&mut record).unwrap();
        let expected = Test {
            a: TestEnum::A(1, 2),
        };

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn with_struct() {
        #[derive(Deserialize, PartialEq, Debug)]
        enum TestEnum {
            A { a: i32, b: i64 },
        }

        #[derive(Deserialize, PartialEq, Debug)]
        struct Test {
            a: TestEnum,
        }

        let mut record = Value::Record(
            vec![Attr::of("Test")],
            vec![Item::slot(
                "a",
                Value::Record(
                    vec![Attr::of("A")],
                    vec![Item::slot("a", 1), Item::slot("b", Value::Int64Value(2))],
                ),
            )],
        );
        let parsed_value = Form::default().from_value::<Test>(&mut record).unwrap();
        let expected = Test {
            a: TestEnum::A { a: 1, b: 2 },
        };

        assert_eq!(parsed_value, expected);
    }
}

#[cfg(test)]
mod structs {
    use super::*;

    #[test]
    fn nested_struct() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct Parent {
            a: i32,
            b: i64,
            c: Child,
        }

        #[derive(Deserialize, PartialEq, Debug)]
        struct Child {
            a: i32,
            b: i64,
        }

        let test = Parent {
            a: 1,
            b: 2,
            c: Child { a: 3, b: 4 },
        };

        let mut record = Value::Record(
            vec![Attr::from("Parent")],
            vec![
                Item::from(("a", 1)),
                Item::from(("b", Value::Int64Value(2))),
                Item::from((
                    "c",
                    Value::Record(
                        vec![Attr::from("Child")],
                        vec![
                            Item::from(("a", 3)),
                            Item::from(("b", Value::Int64Value(4))),
                        ],
                    ),
                )),
            ],
        );
        let parsed_value = Form::default().from_value::<Parent>(&mut record).unwrap();
        println!("{:?}", parsed_value);
        assert_eq!(parsed_value, test);
    }

    #[test]
    fn simple_struct() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct Test {
            a: i32,
            b: i64,
        }

        let test = Test { a: 1, b: 2 };

        let mut record = Value::Record(
            vec![Attr::from("Test")],
            vec![
                Item::from(("a", 1)),
                Item::from(("b", Value::Int64Value(2))),
            ],
        );
        let parsed_value = Form::default().from_value::<Test>(&mut record).unwrap();

        assert_eq!(parsed_value, test);
    }
}

#[cfg(test)]
mod valid_types {
    use super::*;

    #[test]
    fn test_extant() {
        let parsed_value = Form::default()
            .from_value::<Option<String>>(&mut Value::Extant)
            .unwrap();
        let record = None;

        assert_eq!(parsed_value, record);
    }

    #[test]
    fn test_i32() {
        let parsed_value = Form::default()
            .from_value::<i32>(&mut Value::Int32Value(1))
            .unwrap();
        let expected = 1;

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_i64() {
        let parsed_value = Form::default()
            .from_value::<i64>(&mut Value::Int64Value(2))
            .unwrap();
        let expected = 2;

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_f64() {
        let parsed_value = Form::default()
            .from_value::<f64>(&mut Value::Float64Value(1.0))
            .unwrap();
        let expected = 1.0;

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_bool() {
        let parsed_value = Form::default()
            .from_value::<bool>(&mut Value::BooleanValue(true))
            .unwrap();
        let expected = true;

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_text() {
        let parsed_value = Form::default()
            .from_value::<String>(&mut Value::Text(String::from("swim.ai")))
            .unwrap();
        let expected = String::from("swim.ai");

        assert_eq!(parsed_value, expected);
    }
}
