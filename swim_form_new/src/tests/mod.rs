// // Copyright 2015-2020 SWIM.AI inc.
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// //     http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.
//
use common::model::{Attr, Item, Value};

use crate::{Form, TransmuteValue};
use common::model::Value::Int32Value;
mod writer;

mod swim_form {
    pub use crate::*;
    pub use common::model::{Attr, Item, Value};
}

#[test]
fn struct_derive() {
    #[form(swim_form)]
    struct FormStruct {
        a: i32,
    }

    let fs = FormStruct { a: 1 };
    let v: Value = fs.as_value();
    let expected = Value::Record(
        vec![Attr::of("FormStruct")],
        vec![Item::slot("a", Value::Int32Value(1))],
    );

    assert_eq!(v, expected);
}

#[test]
fn tuple_struct() {
    #[form(swim_form)]
    struct Tuple(i32, i32, i32);

    let fs = Tuple(1, 2, 3);
    let v: Value = fs.as_value();
    let expected = Value::Record(
        vec![Attr::of("Tuple")],
        vec![
            Item::of(Value::Int32Value(1)),
            Item::of(Value::Int32Value(2)),
            Item::of(Value::Int32Value(3)),
        ],
    );

    assert_eq!(v, expected);
}

#[test]
fn tuple_complex() {
    #[form(swim_form)]
    struct Child {
        s: String,
    }

    #[form(swim_form)]
    struct FormNested {
        c: Child,
    }

    #[form(swim_form)]
    struct FormStruct {
        i: i32,
        stringy: String,
    }

    #[form(swim_form)]
    struct FormUnit;

    #[form(swim_form)]
    struct Tuple(i32, FormStruct, FormUnit, FormNested);

    let fs = Tuple(
        1,
        FormStruct {
            i: 2,
            stringy: "a string".to_string(),
        },
        FormUnit,
        FormNested {
            c: Child {
                s: "another string".to_string(),
            },
        },
    );

    let v: Value = fs.as_value();
    let expected = Value::Record(
        vec![Attr::of("Tuple")],
        vec![
            Item::of(Value::Int32Value(1)),
            Item::of(Value::Record(
                vec![Attr::of("FormStruct")],
                vec![
                    Item::slot("i", Value::Int32Value(2)),
                    Item::slot("stringy", Value::Text(String::from("a string"))),
                ],
            )),
            Item::of(Value::Record(vec![Attr::of("FormUnit")], vec![])),
            Item::of(Value::Record(
                vec![Attr::of("FormNested")],
                vec![Item::slot(
                    "c",
                    Value::Record(
                        vec![Attr::of("Child")],
                        vec![Item::slot("s", Value::Text(String::from("another string")))],
                    ),
                )],
            )),
        ],
    );

    assert_eq!(v, expected);
}

#[test]
fn unit_struct_derve() {
    #[form(swim_form)]
    struct Nothing;

    let fs = Nothing;
    let v: Value = fs.as_value();
    let expected = Value::Record(vec![Attr::of("Nothing")], Vec::new());

    assert_eq!(v, expected);
}

#[test]
fn nested_struct_types() {
    #[form(swim_form)]
    struct Unit;

    #[form(swim_form)]
    struct NewType(Unit);

    #[form(swim_form)]
    struct Outer {
        nt: NewType,
    }

    let fs = Outer { nt: NewType(Unit) };
    let v: Value = fs.as_value();
    let expected = Value::Record(
        vec![Attr::of("Outer")],
        vec![Item::slot(
            "nt",
            Value::Record(
                vec![Attr::of("NewType")],
                vec![Item::of(Value::Record(vec![Attr::of("Unit")], vec![]))],
            ),
        )],
    );

    assert_eq!(v, expected);
}

#[test]
fn newtype_unit_struct_derive() {
    #[form(swim_form)]
    struct Nothing;

    #[form(swim_form)]
    struct FormStruct(Nothing);

    let fs = FormStruct(Nothing);
    let v: Value = fs.as_value();
    let expected = Value::Record(
        vec![Attr::of("FormStruct")],
        vec![Item::of(Value::Record(vec![Attr::of("Nothing")], vec![]))],
    );

    assert_eq!(v, expected);
}

#[test]
fn newtype_struct_derive() {
    #[form(swim_form)]
    struct FormStruct(i32);

    let fs = FormStruct(100);
    let v: Value = fs.as_value();
    let expected = Value::Record(
        vec![Attr::of("FormStruct")],
        vec![Item::of(Value::Int32Value(100))],
    );

    assert_eq!(v, expected);
}

#[test]
fn nested_newtype_struct_derive() {
    #[form(swim_form)]
    struct FormStructInner(i32);
    #[form(swim_form)]
    struct FormStruct(FormStructInner);

    let fs = FormStruct(FormStructInner(100));
    let v: Value = fs.as_value();
    let expected = Value::Record(
        vec![Attr::of("FormStruct")],
        vec![Item::of(Value::Record(
            vec![Attr::of("FormStructInner")],
            vec![Item::of(Value::Int32Value(100))],
        ))],
    );

    assert_eq!(v, expected);
}

#[test]
fn nested_struct_newtype_derive() {
    #[form(swim_form)]
    struct FormStruct {
        a: FormStructInner,
    };
    #[form(swim_form)]
    struct FormStructInner(i32);

    let fs = FormStruct {
        a: FormStructInner(100),
    };

    let v: Value = fs.as_value();
    let expected = Value::Record(
        vec![Attr::of("FormStruct")],
        vec![Item::slot(
            "a",
            Value::Record(
                vec![Attr::of("FormStructInner")],
                vec![Item::of(Value::Int32Value(100))],
            ),
        )],
    );

    assert_eq!(v, expected);
}

#[test]
fn newtype_with_struct() {
    #[form(swim_form)]
    struct Inner {
        a: i32,
    }

    #[form(swim_form)]
    struct FormNewType(Inner);

    let fs = FormNewType(Inner { a: 100 });

    let v = fs.as_value();
    let expected = Value::Record(
        vec![Attr::of("FormNewType")],
        vec![Item::of(Value::Record(
            vec![Attr::of("Inner")],
            vec![Item::slot("a", Value::Int32Value(100))],
        ))],
    );

    assert_eq!(v, expected);
}

#[test]
fn single_derve_with_generics() {
    #[form(swim_form)]
    struct FormStruct<V>
    where
        V: TransmuteValue,
    {
        v: V,
    }

    let fs = FormStruct { v: 1 };
    let v: Value = fs.as_value();
    let expected = Value::Record(
        vec![Attr::of("FormStruct")],
        vec![Item::slot("v", Value::Int32Value(1))],
    );

    assert_eq!(v, expected);
}

#[test]
fn nested_derives() {
    #[form(swim_form)]
    struct Parent {
        a: i32,
        b: Child,
    }

    #[form(swim_form)]
    struct Child {
        c: i32,
    }

    let parent = Parent {
        a: 1,
        b: Child { c: 2 },
    };

    let v: Value = parent.as_value();
    let expected = Value::Record(
        vec![Attr::of("Parent")],
        vec![
            Item::slot("a", Value::Int32Value(1)),
            Item::slot(
                "b",
                Value::Record(
                    vec![Attr::of("Child")],
                    vec![Item::slot("c", Int32Value(2))],
                ),
            ),
        ],
    );

    assert_eq!(v, expected)
}

#[test]
fn vector_of_structs() {
    #[form(swim_form)]
    struct Parent {
        seq: Vec<Child>,
    }

    #[form(swim_form)]
    struct Child {
        id: i32,
    }

    let test = Parent {
        seq: vec![Child { id: 1 }, Child { id: 2 }, Child { id: 3 }],
    };

    let v: Value = test.as_value();
    let expected = Value::Record(
        vec![Attr::of("Parent")],
        vec![Item::slot(
            "seq",
            Value::from_vec(vec![
                Item::ValueItem(Value::Record(
                    vec![Attr::of("Child")],
                    vec![Item::slot("id", 1)],
                )),
                Item::ValueItem(Value::Record(
                    vec![Attr::of("Child")],
                    vec![Item::slot("id", 2)],
                )),
                Item::ValueItem(Value::Record(
                    vec![Attr::of("Child")],
                    vec![Item::slot("id", 3)],
                )),
            ]),
        )],
    );

    assert_eq!(v, expected);
}

#[test]
fn simple_vector() {
    #[form(swim_form)]
    struct Test {
        seq: Vec<i32>,
    }

    let test = Test {
        seq: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    };

    let parsed_value = test.as_value();
    let expected = Value::Record(
        vec![Attr::of("Test")],
        vec![Item::slot(
            "seq",
            Value::from_vec(vec![
                Item::from(1),
                Item::from(2),
                Item::from(3),
                Item::from(4),
                Item::from(5),
                Item::from(6),
                Item::from(7),
                Item::from(8),
                Item::from(9),
                Item::from(10),
            ]),
        )],
    );

    assert_eq!(parsed_value, expected);
}

#[test]
fn field_lifetime() {
    #[form(swim_form)]
    struct Inner {
        a: i32,
    }

    #[form(swim_form)]
    struct FormStruct<'l> {
        inner: &'l Inner,
    }

    let inner = Inner { a: 1 };
    let fs = FormStruct { inner: &inner };
    let v: Value = fs.as_value();

    let expected = Value::Record(
        vec![Attr::of("FormStruct")],
        vec![Item::slot(
            "inner",
            Value::Record(vec![Attr::of("Inner")], vec![Item::slot("a", 1)]),
        )],
    );

    assert_eq!(v, expected);
}

#[test]
fn generic_field_lifetime() {
    #[form(swim_form)]
    struct Inner {
        a: i32,
    }

    #[form(swim_form)]
    struct FormStruct<'l, S>
    where
        S: TransmuteValue,
    {
        inner: &'l S,
    }

    let inner = Inner { a: 1 };
    let fs = FormStruct { inner: &inner };
    let v: Value = fs.as_value();

    let expected = Value::Record(
        vec![Attr::of("FormStruct")],
        vec![Item::slot(
            "inner",
            Value::Record(vec![Attr::of("Inner")], vec![Item::slot("a", 1)]),
        )],
    );

    assert_eq!(v, expected);
}
