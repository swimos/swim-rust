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

use crate::structural::Tag;
use crate::Form;
use swim_model::time::Timestamp;
use swim_model::{Attr, Item, Text, Value};

mod swim_form {
    pub use crate::*;
}

#[test]
fn test_transmute() {
    #[derive(Form, Debug, PartialEq, Clone)]
    struct S {
        a: i32,
        b: i64,
    }

    let s = S { a: 1, b: 2 };
    let rec = Value::Record(
        vec![Attr::of("S")],
        vec![
            Item::Slot(Value::text("a"), Value::Int32Value(1)),
            Item::Slot(Value::text("b"), Value::Int64Value(2)),
        ],
    );
    assert_eq!(s.as_value(), rec);
    assert_eq!(S::try_from_value(&rec), Ok(s.clone()));
    assert_eq!(S::try_convert(rec.clone()), Ok(s.clone()));
    assert_eq!(s.into_value(), rec);
}

#[test]
fn test_transmute_generic() {
    #[derive(Form, Debug, PartialEq, Clone)]
    struct S<F> {
        f: F,
    }

    let s = S { f: 1 };
    let rec = Value::Record(
        vec![Attr::of("S")],
        vec![Item::Slot(Value::text("f"), Value::Int32Value(1))],
    );
    assert_eq!(s.as_value(), rec);
    assert_eq!(S::try_from_value(&rec), Ok(s.clone()));
    assert_eq!(S::try_convert(rec.clone()), Ok(s.clone()));
    assert_eq!(s.into_value(), rec);
}

#[test]
fn test_transmute_newtype() {
    #[derive(Form, Debug, PartialEq, Clone)]
    struct S(i32);

    let s = S(1);
    let rec = Value::Record(
        vec![Attr::of("S")],
        vec![Item::ValueItem(Value::Int32Value(1))],
    );
    assert_eq!(s.as_value(), rec);
    assert_eq!(S::try_from_value(&rec), Ok(s.clone()));
    assert_eq!(S::try_convert(rec.clone()), Ok(s.clone()));
    assert_eq!(s.into_value(), rec);
}

#[test]
fn test_transmute_tuple() {
    #[derive(Form, Debug, PartialEq, Clone)]
    struct S(i32, i64);

    let s = S(1, 2);
    let rec = Value::Record(
        vec![Attr::of("S")],
        vec![
            Item::ValueItem(Value::Int32Value(1)),
            Item::ValueItem(Value::Int32Value(2)),
        ],
    );
    assert_eq!(s.as_value(), rec);
    assert_eq!(S::try_from_value(&rec), Ok(s.clone()));
    assert_eq!(S::try_convert(rec.clone()), Ok(s.clone()));
    assert_eq!(s.into_value(), rec);
}

#[test]
fn test_transmute_unit() {
    #[derive(Form, Debug, PartialEq, Clone)]
    struct S;

    let s = S;
    let rec = Value::Record(vec![Attr::of("S")], vec![]);

    assert_eq!(s.as_value(), rec);
    assert_eq!(S::try_from_value(&rec), Ok(s.clone()));
    assert_eq!(S::try_convert(rec.clone()), Ok(s.clone()));
    assert_eq!(s.into_value(), rec);
}

#[test]
fn test_skip_field() {
    {
        #[derive(Form, Debug, PartialEq, Clone)]
        struct S {
            a: i32,
            #[form(skip)]
            b: i64,
        }

        let s = S { a: 1, b: 2 };
        let rec = Value::Record(
            vec![Attr::of("S")],
            vec![Item::Slot(Value::text("a"), Value::Int32Value(1))],
        );
        assert_eq!(s.as_value(), rec);
        assert_eq!(S::try_from_value(&rec), Ok(S { a: 1, b: 0 }));
        assert_eq!(S::try_convert(rec.clone()), Ok(S { a: 1, b: 0 }));
        assert_eq!(s.into_value(), rec);
    }
    {
        #[derive(Form, Debug, PartialEq, Clone)]
        struct S(#[form(skip)] i32);

        let s = S(1);
        let rec = Value::Record(vec![Attr::of("S")], vec![]);
        assert_eq!(s.as_value(), rec);
        assert_eq!(S::try_from_value(&rec), Ok(S(0)));
        assert_eq!(S::try_convert(rec.clone()), Ok(S(0)));
        assert_eq!(s.into_value(), rec);
    }
    {
        #[derive(Form, Debug, PartialEq, Clone)]
        struct S(#[form(skip)] i32, i64);

        let s = S(1, 2);
        let rec = Value::Record(
            vec![Attr::of("S")],
            vec![Item::ValueItem(Value::Int64Value(2))],
        );
        assert_eq!(s.as_value(), rec);
        assert_eq!(S::try_from_value(&rec), Ok(S(0, 2)));
        assert_eq!(S::try_convert(rec.clone()), Ok(S(0, 2)));
        assert_eq!(s.into_value(), rec);
    }
}

#[test]
fn test_tag() {
    #[derive(Form, Debug, PartialEq, Clone)]
    #[form(tag = "Structure")]
    struct S {
        a: i32,
        b: i64,
    }

    let s = S { a: 1, b: 2 };
    let rec = Value::Record(
        vec![Attr::of("Structure")],
        vec![
            Item::Slot(Value::text("a"), Value::Int32Value(1)),
            Item::Slot(Value::text("b"), Value::Int64Value(2)),
        ],
    );
    assert_eq!(s.as_value(), rec);
    assert_eq!(S::try_from_value(&rec), Ok(s.clone()));
    assert_eq!(S::try_convert(rec.clone()), Ok(s.clone()));
    assert_eq!(s.into_value(), rec);
}

#[test]
fn test_rename() {
    #[derive(Form, Debug, PartialEq, Clone)]
    #[form(tag = "Structure")]
    struct S {
        #[form(name = "field_a")]
        a: i32,
        b: i64,
    }

    let s = S { a: 1, b: 2 };
    let rec = Value::Record(
        vec![Attr::of("Structure")],
        vec![
            Item::Slot(Value::text("field_a"), Value::Int32Value(1)),
            Item::Slot(Value::text("b"), Value::Int64Value(2)),
        ],
    );
    assert_eq!(s.as_value(), rec);
    assert_eq!(S::try_from_value(&rec), Ok(s.clone()));
    assert_eq!(S::try_convert(rec.clone()), Ok(s.clone()));
    assert_eq!(s.into_value(), rec);
}

#[test]
fn body_replaces() {
    #[derive(Form, Debug, PartialEq, Clone)]
    struct BodyReplace {
        n: i32,
        #[form(body)]
        body: Value,
    }

    let body = Value::Record(
        vec![Attr::of("attr2")],
        vec![
            Item::Slot(Value::text("a"), Value::Int32Value(7)),
            Item::Slot(Value::text("b"), Value::BooleanValue(true)),
        ],
    );

    let rec = Value::Record(
        vec![
            Attr::of((
                "BodyReplace",
                Value::Record(
                    Vec::new(),
                    vec![Item::Slot(Value::text("n"), Value::Int32Value(1033))],
                ),
            )),
            Attr::of("attr2"),
        ],
        vec![
            Item::Slot(Value::text("a"), Value::Int32Value(7)),
            Item::Slot(Value::text("b"), Value::BooleanValue(true)),
        ],
    );

    let br = BodyReplace { n: 1033, body };

    assert_eq!(br.as_value(), rec);
    assert_eq!(BodyReplace::try_from_value(&rec), Ok(br.clone()));
    assert_eq!(BodyReplace::try_convert(rec.clone()), Ok(br.clone()));
    assert_eq!(br.into_value(), rec);
}

#[test]
fn complex_header() {
    #[derive(Form, Debug, PartialEq, Clone)]
    struct ComplexHeader {
        #[form(header_body)]
        n: i32,
        #[form(header)]
        name: String,
        other: i32,
    }

    let header_body = Value::Record(
        Vec::new(),
        vec![
            Item::ValueItem(Value::Int32Value(17)),
            Item::Slot(Value::text("name"), Value::text("hello")),
        ],
    );

    let rec = Value::Record(
        vec![Attr::of(("ComplexHeader", header_body))],
        vec![Item::Slot(Value::text("other"), Value::Int32Value(-4))],
    );

    let ch = ComplexHeader {
        n: 17,
        name: "hello".to_string(),
        other: -4,
    };

    assert_eq!(ch.as_value(), rec);
    assert_eq!(ComplexHeader::try_from_value(&rec), Ok(ch.clone()));
    assert_eq!(ComplexHeader::try_convert(rec.clone()), Ok(ch.clone()));
    assert_eq!(ch.into_value(), rec);
}

#[test]
fn example1() {
    #[derive(Form, Debug, PartialEq, Clone)]
    struct Example1 {
        a: i32,
        b: String,
    }

    let e1 = Example1 {
        a: 4,
        b: String::from("s"),
    };
    let rec = Value::Record(
        vec![Attr::of("Example1")],
        vec![
            Item::Slot(Value::text("a"), Value::Int32Value(4)),
            Item::Slot(Value::text("b"), Value::text("s")),
        ],
    );

    assert_eq!(e1.as_value(), rec);
    assert_eq!(Example1::try_from_value(&rec), Ok(e1.clone()));
    assert_eq!(Example1::try_convert(rec.clone()), Ok(e1.clone()));
    assert_eq!(e1.into_value(), rec);
}

#[test]
fn nested() {
    #[derive(Form, Debug, PartialEq, Clone)]
    struct Outer {
        inner: Inner,
        opt: Option<i32>,
    }

    #[derive(Form, Debug, PartialEq, Clone)]
    #[form(tag = "custom")]
    struct Inner {
        a: i32,
        b: String,
    }

    let outer = Outer {
        inner: Inner {
            a: 4,
            b: "s".to_string(),
        },
        opt: Some(1),
    };

    let expected = Value::Record(
        vec![Attr::of("Outer")],
        vec![
            Item::Slot(
                Value::text("inner"),
                Value::Record(
                    vec![Attr::of("custom")],
                    vec![
                        Item::Slot(Value::text("a"), Value::Int32Value(4)),
                        Item::Slot(Value::text("b"), Value::text("s")),
                    ],
                ),
            ),
            Item::Slot(Value::text("opt"), Value::Int32Value(1)),
        ],
    );

    assert_eq!(outer.as_value(), expected);
    assert_eq!(Outer::try_from_value(&expected), Ok(outer.clone()));
    assert_eq!(Outer::try_convert(expected.clone()), Ok(outer.clone()));
    assert_eq!(outer.into_value(), expected);
}

#[test]
fn header() {
    #[derive(Form, Debug, PartialEq, Clone)]
    struct Example {
        a: String,
        #[form(header)]
        b: Option<i64>,
    }

    let struct_none = Example {
        a: "hello".to_string(),
        b: None,
    };

    let rec_none = Value::Record(
        vec![Attr::of(("Example", Value::empty_record()))],
        vec![Item::Slot(Value::text("a"), Value::text("hello"))],
    );

    assert_eq!(struct_none.as_value(), rec_none);

    let struct_some = Example {
        a: "hello".to_string(),
        b: Some(7),
    };

    let rec_some = Value::Record(
        vec![Attr::of((
            "Example",
            Value::Record(
                Vec::new(),
                vec![Item::Slot(Value::text("b"), Value::Int64Value(7))],
            ),
        ))],
        vec![Item::Slot(Value::text("a"), Value::text("hello"))],
    );

    assert_eq!(struct_some.as_value(), rec_some);
    assert_eq!(Example::try_from_value(&rec_some), Ok(struct_some.clone()));
    assert_eq!(
        Example::try_convert(rec_some.clone()),
        Ok(struct_some.clone())
    );
    assert_eq!(struct_some.into_value(), rec_some);
}

#[test]
fn annotated() {
    #[derive(Form, Debug, PartialEq, Clone)]
    #[form(tag = "example")]
    struct ExampleAnnotated {
        #[form(header)]
        count: i64,
        #[form(attr)]
        name: String,
    }

    let ex = ExampleAnnotated {
        count: 1033,
        name: String::from("bob"),
    };

    let expected = Value::Record(
        vec![
            Attr::of((
                "example",
                Value::Record(
                    Vec::new(),
                    vec![Item::Slot(Value::text("count"), Value::Int64Value(1033))],
                ),
            )),
            Attr::of(("name", Value::text("bob"))),
        ],
        vec![],
    );

    assert_eq!(ex.as_value(), expected);
    assert_eq!(ExampleAnnotated::try_from_value(&expected), Ok(ex.clone()));
    assert_eq!(
        ExampleAnnotated::try_convert(expected.clone()),
        Ok(ex.clone())
    );
    assert_eq!(ex.into_value(), expected);
}

#[test]
fn header_body_replace() {
    #[derive(Form, Debug, PartialEq, Clone)]
    struct HeaderBodyReplace {
        #[form(header_body)]
        n: i64,
    }

    let ex = HeaderBodyReplace { n: 16 };

    let expected = Value::Record(
        vec![Attr::of(("HeaderBodyReplace", Value::Int64Value(16)))],
        Vec::new(),
    );

    assert_eq!(ex.as_value(), expected);
    assert_eq!(HeaderBodyReplace::try_from_value(&expected), Ok(ex.clone()));
    assert_eq!(
        HeaderBodyReplace::try_convert(expected.clone()),
        Ok(ex.clone())
    );
    assert_eq!(ex.into_value(), expected);
}

#[test]
fn test_enum_tag() {
    #[derive(Tag, Clone, Copy, Debug, PartialEq, Eq)]
    enum Level {
        #[form(tag = "trace")]
        Trace,
        #[form(tag = "error")]
        Error,
    }

    #[derive(Form, Debug, PartialEq, Clone)]
    struct LogEntry<F: Form> {
        #[form(tag)]
        level: Level,
        #[form(header)]
        time: Timestamp,
        message: F,
    }

    let now = Timestamp::now();

    let entry = LogEntry {
        level: Level::Error,
        time: now,
        message: String::from("Not good"),
    };

    assert_eq!(
        entry.as_value(),
        Value::Record(
            vec![Attr::of((
                "error",
                Value::from_vec(vec![Item::Slot(Value::text("time"), now.as_value())])
            ))],
            vec![Item::Slot(Value::text("message"), Value::text("Not good"))]
        )
    )
}

#[test]
fn generic_duplicated_bound() {
    #[derive(Form)]
    struct Valid;

    #[derive(Form, Debug, PartialEq, Clone)]
    struct S<A: Form> {
        a: A,
    }

    let s = S { a: Valid };

    let _ = s.as_value();
}

#[test]
fn generic_duplicated_bound_2() {
    #[derive(Form)]
    struct Valid;

    #[derive(Form, Debug, PartialEq, Clone)]
    struct S<A>
    where
        A: Form,
    {
        a: A,
    }

    let s = S { a: Valid };

    let _ = s.as_value();
}

#[test]
fn test_newtype() {
    {
        #[derive(Form, Debug, PartialEq, Clone)]
        #[form(newtype)]
        struct First(i32);

        let first = First(13);
        let val = Value::Int32Value(13);
        assert_eq!(first.as_value(), val);
        assert_eq!(First::try_from_value(&val), Ok(First(13)));
        assert_eq!(First::try_convert(val.clone()), Ok(First(13)));
        assert_eq!(first.into_value(), val);
    }

    {
        #[derive(Form, Debug, PartialEq, Clone)]
        #[form(newtype)]
        struct Second(i32, #[form(skip)] String);

        let second = Second(42, "Test".to_string());
        let val = Value::Int32Value(42);
        assert_eq!(second.as_value(), val);
        assert_eq!(Second::try_from_value(&val), Ok(Second(42, "".to_string())));
        assert_eq!(
            Second::try_convert(val.clone()),
            Ok(Second(42, "".to_string()))
        );
        assert_eq!(second.into_value(), val);
    }

    {
        #[derive(Form, Debug, PartialEq, Clone)]
        #[form(newtype)]
        struct Third(#[form(skip)] Option<i32>, #[form(skip)] String, u32);

        let third = Third(Some(15), "Test".to_string(), 256);
        let val = Value::Int32Value(256);
        assert_eq!(third.as_value(), val);
        assert_eq!(
            Third::try_from_value(&val),
            Ok(Third(None, "".to_string(), 256))
        );
        assert_eq!(
            Third::try_convert(val.clone()),
            Ok(Third(None, "".to_string(), 256))
        );
        assert_eq!(third.into_value(), val);
    }

    {
        #[derive(Form, Debug, PartialEq, Clone)]
        #[form(newtype)]
        struct Fourth {
            a: String,
        }

        let fourth = Fourth {
            a: "Hello, world!".to_string(),
        };
        let val = Value::Text(Text::new("Hello, world!"));
        assert_eq!(fourth.as_value(), val);
        assert_eq!(
            Fourth::try_from_value(&val),
            Ok(Fourth {
                a: "Hello, world!".to_string()
            })
        );
        assert_eq!(
            Fourth::try_convert(val.clone()),
            Ok(Fourth {
                a: "Hello, world!".to_string()
            })
        );
        assert_eq!(fourth.into_value(), val);
    }

    {
        #[derive(Form, Debug, PartialEq, Clone)]
        #[form(newtype)]
        struct Fifth {
            a: String,
            #[form(skip)]
            b: i32,
            #[form(skip)]
            c: i32,
        }

        let fifth = Fifth {
            a: "O.o".to_string(),
            b: 11,
            c: 22,
        };
        let val = Value::Text(Text::new("O.o"));
        assert_eq!(fifth.as_value(), val);
        assert_eq!(
            Fifth::try_from_value(&val),
            Ok(Fifth {
                a: "O.o".to_string(),
                b: 0,
                c: 0,
            })
        );
        assert_eq!(
            Fifth::try_convert(val.clone()),
            Ok(Fifth {
                a: "O.o".to_string(),
                b: 0,
                c: 0,
            })
        );
        assert_eq!(fifth.into_value(), val);
    }

    {
        #[derive(Form, Debug, PartialEq, Clone)]
        #[form(newtype)]
        struct Sixth {
            #[form(skip)]
            a: String,
            b: i32,
            #[form(skip)]
            c: i32,
        }

        let sixth = Sixth {
            a: "@.@".to_string(),
            b: 11,
            c: 22,
        };
        let val = Value::Int32Value(11);
        assert_eq!(sixth.as_value(), val);
        assert_eq!(
            Sixth::try_from_value(&val),
            Ok(Sixth {
                a: "".to_string(),
                b: 11,
                c: 0,
            })
        );
        assert_eq!(
            Sixth::try_convert(val.clone()),
            Ok(Sixth {
                a: "".to_string(),
                b: 11,
                c: 0,
            })
        );
        assert_eq!(sixth.into_value(), val);
    }
}
