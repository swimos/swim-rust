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

use std::fmt::Debug;
use std::sync::Arc;
use swim_model::bigint::{BigInt, BigUint};

use crate::Form;
use swim_model::{Attr, Blob, Item, Value};

mod swim_form {
    pub use crate::*;
}

#[test]
fn transmute_bigint() {
    #[derive(Form)]
    struct S {
        a: BigInt,
        b: BigUint,
    }

    let s = S {
        a: BigInt::from(100),
        b: BigUint::from(100u32),
    };

    assert_eq!(
        s.as_value(),
        Value::Record(
            vec![Attr::from("S")],
            vec![
                Item::from(("a", Value::BigInt(BigInt::from(100)))),
                Item::from(("b", Value::BigUint(BigUint::from(100u32)))),
            ],
        )
    )
}

#[test]
fn blob() {
    #[derive(Form)]
    struct S {
        b: Blob,
    }

    let s = S {
        b: Blob::encode("blobby"),
    };

    assert_eq!(
        s.as_value(),
        Value::Record(
            vec![Attr::of("S")],
            vec![Item::Slot(
                Value::text("b"),
                Value::Data(Blob::from_vec(vec![89, 109, 120, 118, 89, 109, 74, 53])),
            )],
        )
    )
}

mod primitive {
    use std::sync::Arc;

    use super::*;

    macro_rules! test_impl {
        ($test_name:ident, $id:ident, $typ:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let value = $typ.as_value();
                assert_eq!(value, $expected);
                assert_eq!($id::try_from_value(&value), Ok($typ))
            }
        };
    }

    test_impl!(test_bool, bool, true, Value::BooleanValue(true));
    test_impl!(test_i32, i32, 100i32, Value::Int32Value(100));
    test_impl!(test_i64, i64, 100i64, Value::Int64Value(100));
    test_impl!(test_u32, u32, 100u32, Value::UInt32Value(100));
    test_impl!(test_u64, u64, 100u64, Value::UInt64Value(100));
    test_impl!(test_f64, f64, 100.0f64, Value::Float64Value(100.0));
    test_impl!(test_opt_some, Option, Some(100i32), Value::Int32Value(100));
    test_impl!(
        test_string,
        String,
        String::from("test"),
        Value::text("test")
    );
    test_impl!(
        test_bigint,
        BigInt,
        BigInt::from(100),
        Value::BigInt(BigInt::from(100))
    );
    test_impl!(
        test_biguint,
        BigUint,
        BigUint::from(100u32),
        Value::BigUint(BigUint::from(100u32))
    );

    #[test]
    fn test_unit() {
        let value = ().as_value();
        assert_eq!(value, Value::Extant);
        assert_eq!(<()>::try_from_value(&value), Ok(()));
    }

    #[test]
    fn test_arc() {
        let value = Arc::new(100).as_value();
        assert_eq!(value, Value::Int32Value(100));
        assert_eq!(Arc::try_from_value(&value), Ok(Arc::new(100)));
    }
}

mod collections {
    use super::*;

    #[test]
    fn test_opt_none() {
        let r: Option<i32> = None;
        let value = r.as_value();
        assert_eq!(value, Value::Extant);
    }

    #[test]
    fn test_opt_some() {
        let r = Some(100);
        let value = r.as_value();
        assert_eq!(value, Value::Int32Value(100));
    }

    #[test]
    fn test_vec() {
        let vec = vec![1, 2, 3, 4, 5];
        let value = vec.as_value();

        assert_eq!(value, expected());
        assert_eq!(Vec::try_from_value(&value), Ok(vec))
    }

    fn expected() -> Value {
        Value::record(vec![
            Item::of(Value::Int32Value(1)),
            Item::of(Value::Int32Value(2)),
            Item::of(Value::Int32Value(3)),
            Item::of(Value::Int32Value(4)),
            Item::of(Value::Int32Value(5)),
        ])
    }
}

mod field_collections {
    #[allow(unused_imports)]
    use swim_form_derive::*;

    use super::*;

    fn expected() -> Value {
        Value::record(vec![
            Item::of(Value::Int32Value(1)),
            Item::of(Value::Int32Value(2)),
            Item::of(Value::Int32Value(3)),
            Item::of(Value::Int32Value(4)),
            Item::of(Value::Int32Value(5)),
        ])
    }

    macro_rules! test_impl {
        ($name:ident, $typ:ty, $initial:expr) => {
            #[test]
            fn $name() {
                #[derive(Form)]
                struct Test {
                    member: $typ,
                }

                let val = Test { member: $initial };
                let rec = Value::Record(
                    vec![Attr::of("Test")],
                    vec![Item::Slot(Value::text("member"), expected())],
                );

                assert_eq!(val.as_value(), rec);
            }
        };
    }

    test_impl!(vec, Vec::<i32>, vec![1, 2, 3, 4, 5]);
}

#[test]
fn test_map_modification() {
    #[derive(Clone, PartialEq, Form, Debug)]
    enum FormMapUpdate<K, V> {
        Update(#[form(header, name = "key")] K, #[form(body)] Arc<V>),
    }

    let body = Arc::new(Value::Record(
        vec![Attr::of(("complex", 0))],
        vec![Item::slot("a", true)],
    ));
    let attr = Attr::of(("Update", Value::record(vec![Item::slot("key", "hello")])));
    let expected = Value::Record(
        vec![attr, Attr::of(("complex", 0))],
        vec![Item::slot("a", true)],
    );
    assert_eq!(
        Form::into_value(FormMapUpdate::Update(Value::text("hello"), body.clone())),
        expected
    );
    assert_eq!(
        Form::as_value(&FormMapUpdate::Update(Value::text("hello"), body)),
        expected
    );

    let body = Arc::new(Value::Record(
        vec![Attr::of(("complex", 0))],
        vec![Item::slot("a", true)],
    ));
    let attr = Attr::of(("Update", Value::record(vec![Item::slot("key", "hello")])));
    let rep = Value::Record(
        vec![attr, Attr::of(("complex", 0))],
        vec![Item::slot("a", true)],
    );
    let result1 = FormMapUpdate::try_from_value(&rep);
    assert_eq!(
        result1,
        Ok(FormMapUpdate::Update(Value::text("hello"), body.clone()))
    );
    let result2 = FormMapUpdate::try_convert(rep);
    assert_eq!(
        result2,
        Ok(FormMapUpdate::Update(Value::text("hello"), body))
    );
}

mod tuples {
    use super::*;

    #[test]
    fn test_tuple_1() {
        let value = (0,).as_value();
        assert_eq!(value, expected_tuple(1));
        assert_eq!(<(i32,)>::try_from_value(&value), Ok((0,)));
    }

    #[test]
    fn test_tuple_2() {
        let value = (0, 1).as_value();
        assert_eq!(value, expected_tuple(2));
        assert_eq!(<(i32, i32)>::try_from_value(&value), Ok((0, 1)));
    }

    #[test]
    fn test_tuple_3() {
        let value = (0, 1, 2).as_value();
        assert_eq!(value, expected_tuple(3));
        assert_eq!(<(i32, i32, i32)>::try_from_value(&value), Ok((0, 1, 2)));
    }

    #[test]
    fn test_tuple_4() {
        let value = (0, 1, 2, 3).as_value();
        assert_eq!(value, expected_tuple(4));
        assert_eq!(
            <(i32, i32, i32, i32)>::try_from_value(&value),
            Ok((0, 1, 2, 3))
        );
    }

    #[test]
    fn test_tuple_5() {
        let value = (0, 1, 2, 3, 4).as_value();
        assert_eq!(value, expected_tuple(5));
        assert_eq!(
            <(i32, i32, i32, i32, i32)>::try_from_value(&value),
            Ok((0, 1, 2, 3, 4))
        );
    }

    #[test]
    fn test_tuple_6() {
        let value = (0, 1, 2, 3, 4, 5).as_value();
        assert_eq!(value, expected_tuple(6));
        assert_eq!(
            <(i32, i32, i32, i32, i32, i32)>::try_from_value(&value),
            Ok((0, 1, 2, 3, 4, 5))
        );
    }

    #[test]
    fn test_tuple_7() {
        let value = (0, 1, 2, 3, 4, 5, 6).as_value();
        assert_eq!(value, expected_tuple(7));
        assert_eq!(
            <(i32, i32, i32, i32, i32, i32, i32)>::try_from_value(&value),
            Ok((0, 1, 2, 3, 4, 5, 6))
        );
    }

    #[test]
    fn test_tuple_8() {
        let value = (0, 1, 2, 3, 4, 5, 6, 7).as_value();
        assert_eq!(value, expected_tuple(8));
        assert_eq!(
            <(i32, i32, i32, i32, i32, i32, i32, i32)>::try_from_value(&value),
            Ok((0, 1, 2, 3, 4, 5, 6, 7))
        );
    }

    #[test]
    fn test_tuple_9() {
        let value = (0, 1, 2, 3, 4, 5, 6, 7, 8).as_value();
        assert_eq!(value, expected_tuple(9));
        assert_eq!(
            <(i32, i32, i32, i32, i32, i32, i32, i32, i32)>::try_from_value(&value),
            Ok((0, 1, 2, 3, 4, 5, 6, 7, 8))
        );
    }

    #[test]
    fn test_tuple_10() {
        let value = (0, 1, 2, 3, 4, 5, 6, 7, 8, 9).as_value();
        assert_eq!(value, expected_tuple(10));
        assert_eq!(
            <(i32, i32, i32, i32, i32, i32, i32, i32, i32, i32)>::try_from_value(&value),
            Ok((0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
        );
    }

    #[test]
    fn test_tuple_11() {
        let value = (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10).as_value();
        assert_eq!(value, expected_tuple(11));
        assert_eq!(
            <(i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32)>::try_from_value(&value),
            Ok((0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
        );
    }

    #[test]
    fn test_tuple_12() {
        let value = (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11).as_value();
        assert_eq!(value, expected_tuple(12));
        assert_eq!(
            <(i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32)>::try_from_value(&value),
            Ok((0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
        );
    }

    #[derive(Form, Debug, Eq, PartialEq)]
    struct Foo {
        a: usize,
        b: usize,
    }

    #[test]
    fn tuple_with_struct() {
        let value = (1, Foo { a: 10, b: 20 }).as_value();
        assert_eq!(
            value,
            Value::Record(vec![], vec![Item::from(1), expected_struct()])
        );
        assert_eq!(
            <(i32, Foo)>::try_from_value(&value),
            Ok((1, Foo { a: 10, b: 20 }))
        );

        let value = (Foo { a: 10, b: 20 }, 2, 3).as_value();
        assert_eq!(
            value,
            Value::Record(
                vec![],
                vec![expected_struct(), Item::from(2), Item::from(3)],
            )
        );
        assert_eq!(
            <(Foo, i32, i32)>::try_from_value(&value),
            Ok((Foo { a: 10, b: 20 }, 2, 3))
        );

        let value = (
            Foo { a: 10, b: 20 },
            Foo { a: 10, b: 20 },
            Foo { a: 10, b: 20 },
        )
            .as_value();
        assert_eq!(
            value,
            Value::Record(
                vec![],
                vec![expected_struct(), expected_struct(), expected_struct()],
            )
        );
        assert_eq!(
            <(Foo, Foo, Foo)>::try_from_value(&value),
            Ok((
                Foo { a: 10, b: 20 },
                Foo { a: 10, b: 20 },
                Foo { a: 10, b: 20 }
            ))
        );
    }

    #[derive(Form, Debug, Eq, PartialEq)]
    enum Bar {
        A(usize),
        B { a: usize, b: usize },
        C,
    }

    #[test]
    fn tuple_with_enum() {
        let value = (1, Bar::A(10)).as_value();
        assert_eq!(
            value,
            Value::Record(vec![], vec![Item::from(1), expected_enum(1)])
        );
        assert_eq!(<(i32, Bar)>::try_from_value(&value), Ok((1, Bar::A(10))));

        let value = (1, Bar::B { a: 10, b: 20 }).as_value();
        assert_eq!(
            value,
            Value::Record(vec![], vec![Item::from(1), expected_enum(2)])
        );
        assert_eq!(
            <(i32, Bar)>::try_from_value(&value),
            Ok((1, Bar::B { a: 10, b: 20 }))
        );

        let value = (1, Bar::C).as_value();
        assert_eq!(
            value,
            Value::Record(vec![], vec![Item::from(1), expected_enum(3)])
        );
        assert_eq!(<(i32, Bar)>::try_from_value(&value), Ok((1, Bar::C)));

        let value = (Bar::A(10), Bar::B { a: 10, b: 20 }, Bar::C).as_value();
        assert_eq!(
            value,
            Value::Record(
                vec![],
                vec![expected_enum(1), expected_enum(2), expected_enum(3)]
            )
        );
        assert_eq!(
            <(Bar, Bar, Bar)>::try_from_value(&value),
            Ok((Bar::A(10), Bar::B { a: 10, b: 20 }, Bar::C))
        );
    }

    #[test]
    fn tuple_with_both() {
        let value = (1, Foo { a: 10, b: 20 }, 2, Bar::A(10), 3).as_value();
        assert_eq!(
            value,
            Value::Record(
                vec![],
                vec![
                    Item::from(1),
                    expected_struct(),
                    Item::from(2),
                    expected_enum(1),
                    Item::from(3)
                ]
            )
        );
        assert_eq!(
            <(i32, Foo, i32, Bar, i32)>::try_from_value(&value),
            Ok((1, Foo { a: 10, b: 20 }, 2, Bar::A(10), 3))
        );
    }

    fn expected_tuple(size: i32) -> Value {
        let mut items = vec![];
        for i in 0..size {
            items.push(Item::of(Value::Int32Value(i)));
        }
        Value::record(items)
    }

    fn expected_struct() -> Item {
        Item::from(Value::Record(
            vec![Attr::from("Foo")],
            vec![
                Item::from(("a", Value::UInt64Value(10))),
                Item::from(("b", Value::UInt64Value(20))),
            ],
        ))
    }

    fn expected_enum(variant: usize) -> Item {
        match variant {
            1 => Item::from(Value::Record(
                vec![Attr::from("A")],
                vec![Item::from(Value::UInt64Value(10))],
            )),
            2 => Item::from(Value::Record(
                vec![Attr::from("B")],
                vec![
                    Item::from(("a", Value::UInt64Value(10))),
                    Item::from(("b", Value::UInt64Value(20))),
                ],
            )),
            3 => Item::from(Value::Record(vec![Attr::from("C")], vec![])),
            _ => unreachable!(),
        }
    }
}
