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

use common::model::{Attr, Item, Value};

use crate::Form;
use common::model::Value::Int32Value;

mod swim_form {
    pub use crate::*;
}

#[test]
fn enum_unit() {
    #[form(Value)]
    enum E {
        A,
    }

    let fe = E::A;
    let value: Value = fe.as_value();
    let expected = Value::Record(vec![Attr::of("A")], Vec::new());

    assert_eq!(value, expected)
}

#[test]
fn enum_newtype() {
    #[form(Value)]
    enum E {
        A(i32),
    }

    let fe = E::A(100);
    let value: Value = fe.as_value();
    let expected = Value::Record(vec![Attr::of("A")], vec![Item::of(100)]);

    assert_eq!(value, expected)
}

#[test]
fn enum_tuple() {
    #[form(Value)]
    enum E {
        A(i32, i32, i32, i32),
    }

    let fe = E::A(1, 2, 3, 4);

    let value: Value = fe.as_value();
    let expected = Value::Record(
        vec![Attr::of("A")],
        vec![Item::of(1), Item::of(2), Item::of(3), Item::of(4)],
    );

    assert_eq!(value, expected)
}

#[test]
fn enum_struct() {
    #[form(Value)]
    enum E {
        A { name: String, age: i32 },
    }

    let fe = E::A {
        name: String::from("Name"),
        age: 30,
    };

    let value: Value = fe.as_value();
    let expected = Value::Record(
        vec![Attr::of("A")],
        vec![Item::slot("name", "Name"), Item::slot("age", 30)],
    );

    assert_eq!(value, expected)
}

#[test]
fn struct_with_unit() {
    #[form(Value)]
    enum TestEnum {
        A,
    }

    #[form(Value)]
    struct Test {
        a: TestEnum,
    }

    let fe = &Test { a: TestEnum::A };
    let value: Value = fe.as_value();

    let expected = Value::Record(
        vec![Attr::of("Test")],
        vec![Item::slot("a", Value::of_attr("A"))],
    );

    assert_eq!(value, expected);
}

#[test]
fn struct_with_tuple() {
    #[form(Value)]
    enum TestEnum {
        A(i32, i32),
    }

    #[form(Value)]
    struct Test {
        a: TestEnum,
    }

    let fe = &Test {
        a: TestEnum::A(1, 2),
    };
    let value: Value = fe.as_value();

    let expected = Value::Record(
        vec![Attr::of("Test")],
        vec![Item::slot(
            "a",
            Value::Record(vec![Attr::of("A")], vec![Item::from(1), Item::from(2)]),
        )],
    );

    assert_eq!(value, expected);
}

#[test]
fn struct_with_struct() {
    #[form(Value)]
    enum TestEnum {
        A { a: i32, b: i64 },
    }

    #[form(Value)]
    struct Test {
        a: TestEnum,
    }

    let fe = &Test {
        a: TestEnum::A { a: 1, b: 2 },
    };
    let value: Value = fe.as_value();

    let expected = Value::Record(
        vec![Attr::of("Test")],
        vec![Item::slot(
            "a",
            Value::Record(
                vec![Attr::of("A")],
                vec![Item::slot("a", 1), Item::slot("b", Value::Int64Value(2))],
            ),
        )],
    );

    assert_eq!(value, expected);
}

#[test]
fn nested() {
    #[form(Value)]
    enum A {
        B(B),
    }

    #[form(Value)]
    enum B {
        C,
    }

    let fe: Value = A::B(B::C).as_value();
    let expected = Value::Record(
        vec![Attr::of("B")],
        vec![Item::ValueItem(Value::Record(
            vec![Attr::of("C")],
            Vec::new(),
        ))],
    );

    assert_eq!(fe, expected)
}
