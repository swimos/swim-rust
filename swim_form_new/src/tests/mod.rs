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

use crate::{Form, SerializeToValue};
use common::model::Value::Int32Value;

mod swim_form {
    pub use crate::*;
}

#[test]
fn struct_derive() {
    #[form(Value)]
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
fn newtype_struct_derive() {
    #[form(Value)]
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
    #[form(Value)]
    struct FormStructInner(i32);
    #[form(Value)]
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
    #[form(Value)]
    struct FormStruct {
        a: FormStructInner,
    };
    #[form(Value)]
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
    #[form(Value)]
    struct Inner {
        a: i32,
    }

    #[form(Value)]
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
    #[form(Value)]
    struct FormStruct<V>
    where
        V: SerializeToValue,
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
    #[form(Value)]
    struct Parent {
        a: i32,
        b: Child,
    }

    #[form(Value)]
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
