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

use crate::model::Item::ValueItem;
use crate::model::{Item, Value};

#[cfg(test)]
mod tuples {
    use crate::model::Attr;
    use crate::structure::form::from::to_value;

    use super::*;

    #[test]
    fn struct_with_tuple() {
        #[derive(Serialize)]
        struct Test {
            a: i32,
            b: (i64, i64),
        }

        let test = Test { a: 1, b: (2, 3) };

        let parsed_value = to_value(&test).unwrap();
        println!("{:?}", parsed_value);

        let expected = Value::Record(
            vec![Attr::of("Test")],
            vec![
                Item::of(("a", 1)),
                Item::Slot(
                    Value::from("b"),
                    Value::Record(
                        Vec::new(),
                        vec![
                            Item::of(Value::Int64Value(2)),
                            Item::of(Value::Int64Value(3)),
                        ],
                    ),
                ),
            ],
        );

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn tuple_struct_with_tuple() {
        #[derive(Serialize)]
        struct Test(i32, (i64, i64));

        let test = Test(1, (2, 3));
        let parsed_value = to_value(&test).unwrap();

        let expected = Value::Record(
            Vec::new(),
            vec![
                Item::ValueItem(Value::Int32Value(1)),
                Item::ValueItem(Value::Record(
                    Vec::new(),
                    vec![
                        Item::ValueItem(Value::Int64Value(2)),
                        Item::ValueItem(Value::Int64Value(3)),
                    ],
                )),
            ],
        );

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn tuple_struct() {
        #[derive(Serialize)]
        struct Test(i32, i64);

        let test = Test(1, 2);
        let parsed_value = to_value(&test).unwrap();

        let expected = Value::Record(
            Vec::new(),
            vec![
                Item::ValueItem(Value::Int32Value(1)),
                Item::ValueItem(Value::Int64Value(2)),
            ],
        );

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn simple_tuple() {
        let test = (1, 2);
        let parsed_value = to_value(&test).unwrap();

        let expected = Value::Record(
            Vec::new(),
            vec![
                Item::ValueItem(Value::Int32Value(1)),
                Item::ValueItem(Value::Int32Value(2)),
            ],
        );

        assert_eq!(parsed_value, expected);
    }
}

#[cfg(test)]
mod valid_types {
    use crate::structure::form::from::to_value;

    use super::*;

    #[test]
    fn test_bool() {
        let parsed_value = to_value(&true).unwrap();
        let expected = Value::BooleanValue(true);

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_i8() {
        let test: i8 = 1;
        let parsed_value = to_value(&test).unwrap();
        let expected = Value::Int32Value(1);

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_i16() {
        let test: i16 = 1;
        let parsed_value = to_value(&test).unwrap();
        let expected = Value::Int32Value(1);

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_i32() {
        let test: i32 = 1;
        let parsed_value = to_value(&test).unwrap();
        let expected = Value::Int32Value(1);

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_i64() {
        let test: i64 = 1;
        let parsed_value = to_value(&test).unwrap();
        let expected = Value::Int64Value(1);

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_f32() {
        let test: f32 = 1.0;
        let parsed_value = to_value(&test).unwrap();
        let expected = Value::Float64Value(1.0);

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_f64() {
        let test: f64 = 1.0;
        let parsed_value = to_value(&test).unwrap();
        let expected = Value::Float64Value(1.0);

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_char() {
        let parsed_value = to_value(&'s').unwrap();
        let expected = Value::Text(String::from("s"));

        assert_eq!(parsed_value, expected);
    }
}

#[cfg(test)]
mod enumeration {
    use crate::model::Attr;
    use crate::structure::form::from::to_value;

    use super::*;

    #[test]
    fn simple() {
        #[derive(Serialize)]
        enum TestEnum {
            A,
        }

        #[derive(Serialize)]
        struct Test {
            a: TestEnum,
        }

        let parsed_value = to_value(&Test { a: TestEnum::A }).unwrap();

        let expected = Value::Record(
            vec![Attr::of("Test")],
            vec![Item::Slot(
                Value::from("a"),
                Value::Record(vec![Attr::of("A")], Vec::new()),
            )],
        );

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn with_tuple() {
        #[derive(Serialize)]
        enum TestEnum {
            A(i32, i32),
        }

        #[derive(Serialize)]
        struct Test {
            a: TestEnum,
        }

        let parsed_value = to_value(&Test {
            a: TestEnum::A(1, 2),
        })
        .unwrap();

        let expected = Value::Record(
            vec![Attr::of("Test")],
            vec![Item::Slot(
                Value::from("a"),
                Value::Record(
                    vec![Attr::of("A")],
                    vec![
                        Item::ValueItem(Value::Int32Value(1)),
                        Item::ValueItem(Value::Int32Value(2)),
                    ],
                ),
            )],
        );

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn with_struct() {
        #[derive(Serialize)]
        enum TestEnum {
            A { a: i32, b: i64 },
        }

        #[derive(Serialize)]
        struct Test {
            a: TestEnum,
        }

        let parsed_value = to_value(&Test {
            a: TestEnum::A { a: 1, b: 2 },
        })
        .unwrap();

        let expected = Value::Record(
            vec![Attr::of("Test")],
            vec![Item::Slot(
                Value::from("a"),
                Value::Record(
                    vec![Attr::of("A")],
                    vec![
                        Item::Slot(Value::from("a"), Value::Int32Value(1)),
                        Item::Slot(Value::from("b"), Value::Int64Value(2)),
                    ],
                ),
            )],
        );

        assert_eq!(parsed_value, expected);
    }
}

#[cfg(test)]
mod struct_valid_types {
    use crate::model::Attr;
    use crate::structure::form::from::to_value;

    use super::*;

    #[test]
    fn test_bool() {
        #[derive(Serialize)]
        struct Test {
            a: bool,
        }

        let test = Test { a: true };

        let parsed_value = to_value(&test).unwrap();
        let expected = Value::Record(
            vec![Attr::of("Test")],
            vec![Item::Slot(
                Value::Text(String::from("a")),
                Value::BooleanValue(true),
            )],
        );

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_i8() {
        #[derive(Serialize)]
        struct Test {
            a: i8,
        }

        let test = Test { a: 1 };

        let parsed_value = to_value(&test).unwrap();

        let expected = Value::Record(
            vec![Attr::of("Test")],
            vec![Item::Slot(
                Value::Text(String::from("a")),
                Value::Int32Value(1),
            )],
        );

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_i16() {
        #[derive(Serialize)]
        struct Test {
            a: i16,
        }

        let test = Test { a: 1 };

        let parsed_value = to_value(&test).unwrap();

        let expected = Value::Record(
            vec![Attr::of("Test")],
            vec![Item::Slot(
                Value::Text(String::from("a")),
                Value::Int32Value(1),
            )],
        );

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_i32() {
        #[derive(Serialize)]
        struct Test {
            a: i32,
        }

        let test = Test { a: 1 };

        let parsed_value = to_value(&test).unwrap();

        let expected = Value::Record(
            vec![Attr::of("Test")],
            vec![Item::Slot(
                Value::Text(String::from("a")),
                Value::Int32Value(1),
            )],
        );

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_i64() {
        #[derive(Serialize)]
        struct Test {
            a: i64,
        }

        let test = Test { a: 1 };

        let parsed_value = to_value(&test).unwrap();

        let expected = Value::Record(
            vec![Attr::of("Test")],
            vec![Item::Slot(
                Value::Text(String::from("a")),
                Value::Int64Value(1),
            )],
        );

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_f32() {
        #[derive(Serialize)]
        struct Test {
            a: f32,
        }

        let test = Test { a: 1.0 };

        let parsed_value = to_value(&test).unwrap();

        let expected = Value::Record(
            vec![Attr::of("Test")],
            vec![Item::Slot(
                Value::Text(String::from("a")),
                Value::Float64Value(1.0),
            )],
        );

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_f64() {
        #[derive(Serialize)]
        struct Test {
            a: f64,
        }

        let test = Test { a: 1.0 };

        let parsed_value = to_value(&test).unwrap();

        let expected = Value::Record(
            vec![Attr::of("Test")],
            vec![Item::Slot(
                Value::Text(String::from("a")),
                Value::Float64Value(1.0),
            )],
        );

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_char() {
        #[derive(Serialize)]
        struct Test {
            a: char,
        }

        let test = Test { a: 's' };

        let parsed_value = to_value(&test).unwrap();

        let expected = Value::Record(
            vec![Attr::of("Test")],
            vec![Item::Slot(
                Value::Text(String::from("a")),
                Value::Text(String::from("s")),
            )],
        );

        assert_eq!(parsed_value, expected);
    }
}

#[cfg(test)]
mod illegal_types {
    use crate::structure::form::from::tests::assert_err;
    use crate::structure::form::from::{to_value, SerializerError};

    use super::*;

    #[test]
    fn test_bytes() {
        #[derive(Serialize)]
        struct Test<'a> {
            a: &'a [u8],
        }

        let test = Test {
            a: "abcd".as_bytes(),
        };

        let parsed_value = to_value(&test);
        assert_err(
            parsed_value,
            SerializerError::UnsupportedType(String::from("u8")),
        );
    }

    #[test]
    fn test_u8() {
        #[derive(Serialize)]
        struct Test {
            a: u8,
        }

        let test = Test { a: 1 };

        let parsed_value = to_value(&test);
        assert_err(
            parsed_value,
            SerializerError::UnsupportedType(String::from("u8")),
        );
    }

    #[test]
    fn test_u16() {
        #[derive(Serialize)]
        struct Test {
            a: u16,
        }

        let test = Test { a: 1 };

        let parsed_value = to_value(&test);
        assert_err(
            parsed_value,
            SerializerError::UnsupportedType(String::from("u16")),
        );
    }

    #[test]
    fn test_32() {
        #[derive(Serialize)]
        struct Test {
            a: u32,
        }

        let test = Test { a: 1 };

        let parsed_value = to_value(&test);
        assert_err(
            parsed_value,
            SerializerError::UnsupportedType(String::from("u32")),
        );
    }

    #[test]
    fn test_u64() {
        #[derive(Serialize)]
        struct Test {
            a: u64,
        }

        let test = Test { a: 1 };

        let parsed_value = to_value(&test);
        assert_err(
            parsed_value,
            SerializerError::UnsupportedType(String::from("u64")),
        );
    }
}

#[cfg(test)]
mod compound_types {
    use crate::model::Attr;
    use crate::structure::form::from::tests::assert_err;
    use crate::structure::form::from::{to_value, SerializerError};

    use super::*;

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

        let parsed_value = to_value(&test).unwrap();
        let expected = Value::Record(
            vec![Attr::of("Test")],
            vec![
                Item::Slot(Value::Text(String::from("a")), Value::Int32Value(1)),
                Item::Slot(Value::Text(String::from("b")), Value::Float64Value(2.0)),
                Item::Slot(Value::Text(String::from("c")), Value::Int32Value(3)),
                Item::Slot(
                    Value::Text(String::from("d")),
                    Value::Text(String::from("hello")),
                ),
            ],
        );

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn illegal_struct() {
        #[derive(Serialize)]
        struct Test {
            a: i32,
            b: f32,
            c: i8,
            d: String,
            e: u64,
        }

        let test = Test {
            a: 1,
            b: 2.0,
            c: 3,
            d: String::from("hello"),
            e: 1,
        };

        let parsed_value = to_value(&test);
        assert_err(
            parsed_value,
            SerializerError::UnsupportedType(String::from("u64")),
        );
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

        let parsed_value = to_value(&test).unwrap();
        let expected = Value::Record(
            vec![Attr::of("Test")],
            vec![Item::Slot(
                Value::Text(String::from("seq")),
                Value::Record(
                    Vec::new(),
                    vec![
                        ValueItem(Value::Text(String::from("a"))),
                        ValueItem(Value::Text(String::from("b"))),
                    ],
                ),
            )],
        );

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

        let parsed_value = to_value(&test).unwrap();
        let expected = Value::Record(
            vec![Attr::of("Test")],
            vec![
                Item::Slot(Value::Text(String::from("int")), Value::Int32Value(1)),
                Item::Slot(
                    Value::Text(String::from("seq")),
                    Value::Record(
                        Vec::new(),
                        vec![
                            ValueItem(Value::Text(String::from("a"))),
                            ValueItem(Value::Text(String::from("b"))),
                        ],
                    ),
                ),
            ],
        );

        assert_eq!(parsed_value, expected);
    }
}
