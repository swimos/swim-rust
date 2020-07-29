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

// mod enumeration;
mod impls;
mod structure;

use form_derive::*;

use crate::form::Form;
use crate::model::{Attr, Item, Value};

#[test]
fn test_transmute_named() {
    #[derive(Form, Debug, PartialEq)]
    struct S {
        a: i32,
        b: i64,
    }

    let rec = Value::Record(
        vec![Attr::of("S"), Attr::of(("a", Value::Int32Value(1)))],
        vec![
            Item::Slot(Value::Text(String::from("a")), Value::Int32Value(1)),
            Item::Slot(Value::Text(String::from("b")), Value::Int64Value(2)),
        ],
    );

    let res = S::try_from_value(&rec);
    assert_eq!(Ok(S { a: 1, b: 2 }), res);
}

#[test]
fn test_transmute_tuple() {
    #[derive(Form, Debug, PartialEq)]
    struct S(i32, i64);

    let rec = Value::Record(
        vec![Attr::of("S")],
        vec![
            Item::ValueItem(Value::Int32Value(1)),
            Item::ValueItem(Value::Int64Value(2)),
        ],
    );

    let res = S::try_from_value(&rec);
    assert_eq!(Ok(S(1, 2)), res);
}

#[test]
fn test_tag() {
    #[derive(Form, Debug, PartialEq)]
    #[form(tag = "Structure")]
    struct S(i32, i64);

    let rec = Value::Record(
        vec![Attr::of("Structure")],
        vec![
            Item::ValueItem(Value::Int32Value(1)),
            Item::ValueItem(Value::Int64Value(2)),
        ],
    );

    let res = S::try_from_value(&rec);
    assert_eq!(Ok(S(1, 2)), res);
}

#[test]
fn test_renamed_field() {
    #[derive(Form, Debug, PartialEq)]
    struct S(#[form(rename = "an_int")] i32, i64);

    let rec = Value::Record(
        vec![Attr::of("S")],
        vec![
            Item::Slot(Value::text("an_int"), Value::Int32Value(1)),
            Item::ValueItem(Value::Int64Value(2)),
        ],
    );

    let res = S::try_from_value(&rec);
    assert_eq!(Ok(S(1, 2)), res);
}

#[test]
fn test_skip() {
    {
        #[derive(Form, Debug, PartialEq)]
        struct S(#[form(skip)] i32);

        let rec = Value::Record(vec![Attr::of("S")], vec![]);

        let res = S::try_from_value(&rec);
        assert_eq!(Ok(S(0)), res);
    }
    {
        #[derive(Form, Debug, PartialEq)]
        struct S(#[form(skip)] i32, i64);

        let rec = Value::Record(
            vec![Attr::of("S")],
            vec![Item::ValueItem(Value::Int64Value(1))],
        );

        let res = S::try_from_value(&rec);
        assert_eq!(Ok(S(0, 1)), res);
    }
    {
        #[derive(Form, Debug, PartialEq)]
        struct S {
            #[form(skip)]
            a: i32,
            b: i64,
        };

        let rec = Value::Record(
            vec![Attr::of("S")],
            vec![Item::Slot(Value::text("b"), Value::Int64Value(1))],
        );

        let res = S::try_from_value(&rec);
        assert_eq!(Ok(S { a: 0, b: 1 }), res);
    }
}
