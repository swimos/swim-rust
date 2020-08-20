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

use crate::form::Form;
use crate::model::{Attr, Item, Value};

mod swim_common {
    pub use crate::*;
}

#[test]
fn test_transmute_single_variant() {
    #[derive(Form, Debug, PartialEq, Clone)]
    enum S {
        A { a: i32, b: i64 },
    }

    let s = S::A { a: 1, b: 2 };
    let rec = Value::Record(
        vec![Attr::of("A")],
        vec![
            Item::Slot(Value::Text(String::from("a")), Value::Int32Value(1)),
            Item::Slot(Value::Text(String::from("b")), Value::Int64Value(2)),
        ],
    );

    assert_eq!(s.as_value(), rec);
    assert_eq!(S::try_from_value(&rec), Ok(s.clone()));
    assert_eq!(S::try_convert(rec), Ok(s));
}

#[test]
fn test_generic() {
    #[derive(Form, Debug, PartialEq, Clone)]
    enum S<F>
    where
        F: Form,
    {
        A { f: F },
    }

    let s = S::A { f: 1 };
    let rec = Value::Record(
        vec![Attr::of("A")],
        vec![Item::Slot(
            Value::Text(String::from("f")),
            Value::Int32Value(1),
        )],
    );

    assert_eq!(s.as_value(), rec);
    assert_eq!(S::try_from_value(&rec), Ok(s.clone()));
    assert_eq!(S::try_convert(rec), Ok(s));
}

#[test]
fn test_skip() {
    {
        #[derive(Form, Debug, PartialEq, Clone)]
        enum S {
            A(#[form(skip)] i32),
        }

        let s = S::A(2);
        let rec = Value::Record(vec![Attr::of("A")], vec![]);

        assert_eq!(s.as_value(), rec);
        assert_eq!(S::try_from_value(&rec), Ok(S::A(0)));
        assert_eq!(S::try_convert(rec), Ok(S::A(0)));
    }
    {
        #[derive(Form, Debug, PartialEq, Clone)]
        enum S {
            A(#[form(skip)] i32, i64),
        }

        let s = S::A(2, 3);
        let rec = Value::Record(
            vec![Attr::of("A")],
            vec![Item::ValueItem(Value::Int64Value(3))],
        );

        assert_eq!(s.as_value(), rec);
        assert_eq!(S::try_from_value(&rec), Ok(S::A(0, 3)));
        assert_eq!(S::try_convert(rec), Ok(S::A(0, 3)));
    }
    {
        #[derive(Form, Clone, Debug, PartialEq)]
        enum S {
            A {
                #[form(skip)]
                a: i32,
                b: i64,
            },
        }

        let s = S::A { a: 1, b: 2 };
        let rec = Value::Record(
            vec![Attr::of("A")],
            vec![Item::Slot(
                Value::Text(String::from("b")),
                Value::Int64Value(2),
            )],
        );

        assert_eq!(s.as_value(), rec.clone());
        assert_eq!(S::try_convert(rec), Ok(S::A { a: 0, b: 2 }));
    }
}

#[test]
fn test_transmute_multiple_variants() {
    #[derive(Form, Debug, PartialEq, Clone)]
    #[allow(dead_code)]
    enum S {
        A { a: i32, b: i64 },
        B { c: i32, d: i64 },
        C { e: i32, f: i64 },
        D { g: i32, h: i64 },
        E { i: i32, j: i64 },
    }

    let s = S::C { e: 1, f: 2 };
    let rec = Value::Record(
        vec![Attr::of("C")],
        vec![
            Item::Slot(Value::Text(String::from("e")), Value::Int32Value(1)),
            Item::Slot(Value::Text(String::from("f")), Value::Int64Value(2)),
        ],
    );

    assert_eq!(s.as_value(), rec);
    assert_eq!(S::try_from_value(&rec), Ok(s.clone()));
    assert_eq!(S::try_convert(rec), Ok(s));
}

#[test]
fn test_unit() {
    #[derive(Form, Debug, PartialEq, Clone)]
    enum S {
        A,
    }

    let s = S::A;
    let rec = Value::Record(vec![Attr::of("A")], vec![]);

    assert_eq!(s.as_value(), rec);
    assert_eq!(S::try_from_value(&rec), Ok(s.clone()));
    assert_eq!(S::try_convert(rec), Ok(s));
}

#[test]
fn test_tag() {
    #[derive(Form, Debug, PartialEq, Clone)]
    enum S {
        #[form(tag = "MyTagA")]
        A,
        #[form(tag = "MyTagB")]
        B,
        #[form(tag = "MyTagC")]
        C(i32, i64),
        #[form(tag = "MyTagD")]
        D { a: i32, b: i64 },
    }

    {
        let rec = Value::Record(vec![Attr::of("MyTagA")], vec![]);
        assert_eq!(S::A.as_value(), rec);
        assert_eq!(S::try_from_value(&rec), Ok(S::A));
        assert_eq!(S::try_convert(rec), Ok(S::A));
    }
    {
        let rec = Value::Record(vec![Attr::of("MyTagB")], vec![]);
        assert_eq!(S::B.as_value(), rec);
        assert_eq!(S::try_from_value(&rec), Ok(S::B));
    }
    {
        let s = S::C(1, 2);
        let rec = Value::Record(
            vec![Attr::of("MyTagC")],
            vec![
                Item::ValueItem(Value::Int32Value(1)),
                Item::ValueItem(Value::Int64Value(2)),
            ],
        );
        assert_eq!(s.as_value(), rec);
        assert_eq!(S::try_from_value(&rec), Ok(s.clone()));
        assert_eq!(S::try_convert(rec), Ok(s));
    }
    {
        let s = S::D { a: 1, b: 2 };
        let rec = Value::Record(
            vec![Attr::of("MyTagD")],
            vec![
                Item::Slot(Value::Text(String::from("a")), Value::Int32Value(1)),
                Item::Slot(Value::Text(String::from("b")), Value::Int64Value(2)),
            ],
        );
        assert_eq!(s.as_value(), rec);
        assert_eq!(S::try_from_value(&rec), Ok(s.clone()));
        assert_eq!(S::try_convert(rec), Ok(s));
    }
}

#[test]
fn test_tuple() {
    #[derive(Form, Debug, PartialEq, Clone)]
    enum S {
        A(i32, i64),
    }

    let s = S::A(2, 3);
    let rec = Value::Record(
        vec![Attr::of("A")],
        vec![
            Item::ValueItem(Value::Int32Value(2)),
            Item::ValueItem(Value::Int64Value(3)),
        ],
    );

    assert_eq!(s.as_value(), rec);
    assert_eq!(S::try_from_value(&rec), Ok(s.clone()));
    assert_eq!(S::try_convert(rec), Ok(s));
}

#[test]
fn test_rename() {
    #[derive(Form, Debug, PartialEq, Clone)]
    enum S {
        A(#[form(rename = "A::a")] i32, i64),
        B {
            #[form(rename = "B::a")]
            a: i32,
            b: i64,
        },
    }

    {
        let s = S::A(1, 2);
        let rec = Value::Record(
            vec![Attr::of("A")],
            vec![
                Item::Slot(Value::Text(String::from("A::a")), Value::Int32Value(1)),
                Item::ValueItem(Value::Int64Value(2)),
            ],
        );
        assert_eq!(s.as_value(), rec);
        assert_eq!(S::try_from_value(&rec), Ok(s.clone()));
        assert_eq!(S::try_convert(rec), Ok(s));
    }
    {
        let s = S::B { a: 1, b: 2 };
        let rec = Value::Record(
            vec![Attr::of("B")],
            vec![
                Item::Slot(Value::Text(String::from("B::a")), Value::Int32Value(1)),
                Item::Slot(Value::Text(String::from("b")), Value::Int64Value(2)),
            ],
        );
        assert_eq!(s.as_value(), rec);
        assert_eq!(S::try_from_value(&rec), Ok(s.clone()));
        assert_eq!(S::try_convert(rec), Ok(s));
    }
}

#[test]
fn body_replaces() {
    #[derive(Form, Debug, PartialEq, Clone)]
    enum BodyReplace {
        A(i32, #[form(body)] Value),
    }

    let body = vec![
        Item::ValueItem(Value::Int32Value(7)),
        Item::ValueItem(Value::BooleanValue(true)),
    ];

    let rec = Value::Record(
        vec![Attr::of((
            "A",
            Value::Record(Vec::new(), vec![Item::ValueItem(Value::Int32Value(1033))]),
        ))],
        body.clone(),
    );

    let br = BodyReplace::A(1033, Value::Record(Vec::new(), body));

    assert_eq!(br.as_value(), rec);
    assert_eq!(BodyReplace::try_from_value(&rec), Ok(br.clone()));
    assert_eq!(BodyReplace::try_convert(rec), Ok(br));
}

#[test]
fn complex_header() {
    #[derive(Form, Debug, PartialEq, Clone)]
    enum ComplexHeader {
        A {
            #[form(header_body)]
            n: i32,
            #[form(header)]
            name: String,
            other: i32,
        },
    }

    let header_body = Value::Record(
        Vec::new(),
        vec![
            Item::ValueItem(Value::Int32Value(17)),
            Item::Slot(
                Value::Text(String::from("name")),
                Value::Text(String::from("hello")),
            ),
        ],
    );

    let rec = Value::Record(
        vec![Attr::of(("A", header_body))],
        vec![Item::Slot(
            Value::Text(String::from("other")),
            Value::Int32Value(-4),
        )],
    );

    let ch = ComplexHeader::A {
        n: 17,
        name: "hello".to_string(),
        other: -4,
    };

    assert_eq!(ch.as_value(), rec);
    assert_eq!(ComplexHeader::try_from_value(&rec), Ok(ch.clone()));
    assert_eq!(ComplexHeader::try_convert(rec), Ok(ch));
}

#[test]
fn nested() {
    #[derive(Form, Debug, PartialEq, Clone)]
    enum Outer {
        A { inner: Inner, opt: Option<i32> },
    }

    #[derive(Form, Debug, PartialEq, Clone)]
    enum Inner {
        #[form(tag = "custom")]
        B { a: i32, b: String },
    }

    let outer = Outer::A {
        inner: Inner::B {
            a: 4,
            b: "s".to_string(),
        },
        opt: Some(1),
    };

    let expected = Value::Record(
        vec![Attr::of("A")],
        vec![
            Item::Slot(
                Value::Text(String::from("inner")),
                Value::Record(
                    vec![Attr::of("custom")],
                    vec![
                        Item::Slot(Value::Text(String::from("a")), Value::Int32Value(4)),
                        Item::Slot(
                            Value::Text(String::from("b")),
                            Value::Text(String::from("s")),
                        ),
                    ],
                ),
            ),
            Item::Slot(Value::Text(String::from("opt")), Value::Int32Value(1)),
        ],
    );

    assert_eq!(outer.as_value(), expected);
    assert_eq!(Outer::try_from_value(&expected), Ok(outer.clone()));
    assert_eq!(Outer::try_convert(expected), Ok(outer));
}

#[test]
fn header() {
    #[derive(Form, Debug, PartialEq, Clone)]
    enum Example {
        A {
            a: String,
            #[form(header)]
            b: Option<i64>,
        },
    }

    let struct_none = Example::A {
        a: "hello".to_string(),
        b: None,
    };

    let rec_none = Value::Record(
        vec![Attr::of((
            "A",
            Value::Record(
                Vec::new(),
                vec![Item::Slot(Value::Text(String::from("b")), Value::Extant)],
            ),
        ))],
        vec![Item::Slot(
            Value::Text(String::from("a")),
            Value::Text(String::from("hello")),
        )],
    );

    assert_eq!(struct_none.as_value(), rec_none);

    let struct_some = Example::A {
        a: "hello".to_string(),
        b: Some(7),
    };

    let rec_some = Value::Record(
        vec![Attr::of((
            "A",
            Value::Record(
                Vec::new(),
                vec![Item::Slot(
                    Value::Text(String::from("b")),
                    Value::Int64Value(7),
                )],
            ),
        ))],
        vec![Item::Slot(
            Value::Text(String::from("a")),
            Value::Text(String::from("hello")),
        )],
    );

    assert_eq!(struct_some.as_value(), rec_some);
    assert_eq!(Example::try_from_value(&rec_some), Ok(struct_some.clone()));
    assert_eq!(Example::try_convert(rec_some), Ok(struct_some));
}

#[test]
fn annotated() {
    #[derive(Form, Debug, PartialEq, Clone)]
    enum ExampleAnnotated {
        #[form(tag = "example")]
        A {
            #[form(header)]
            count: i64,
            #[form(attr)]
            name: String,
            #[form(skip)]
            age: i32,
        },
    }

    let ex = ExampleAnnotated::A {
        count: 1033,
        name: String::from("bob"),
        age: i32::max_value(),
    };

    let expected = Value::Record(
        vec![
            Attr::of((
                "example",
                Value::Record(
                    Vec::new(),
                    vec![Item::Slot(
                        Value::Text(String::from("count")),
                        Value::Int64Value(1033),
                    )],
                ),
            )),
            Attr::of(("name", Value::Text(String::from("bob")))),
        ],
        vec![],
    );

    assert_eq!(ex.as_value(), expected);

    let expected_struct = ExampleAnnotated::A {
        count: 1033,
        name: String::from("bob"),
        age: 0,
    };
    assert_eq!(
        ExampleAnnotated::try_from_value(&expected),
        Ok(expected_struct.clone())
    );
    assert_eq!(ExampleAnnotated::try_convert(expected), Ok(expected_struct));
}

#[test]
fn header_body_replace() {
    #[derive(Form, Debug, PartialEq, Clone)]
    enum HeaderBodyReplace {
        A {
            #[form(header_body)]
            n: i64,
        },
    }

    let ex = HeaderBodyReplace::A { n: 16 };
    let expected = Value::Record(vec![Attr::of(("A", Value::Int64Value(16)))], Vec::new());

    assert_eq!(ex.as_value(), expected);
    assert_eq!(HeaderBodyReplace::try_from_value(&expected), Ok(ex.clone()));
    assert_eq!(HeaderBodyReplace::try_convert(expected), Ok(ex));
}
