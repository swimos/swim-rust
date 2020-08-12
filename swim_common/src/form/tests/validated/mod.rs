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
use crate::form::ValidatedForm;
use crate::model::schema::attr::AttrSchema;
use crate::model::schema::slot::SlotSchema;
use crate::model::schema::ItemSchema;
use crate::model::schema::Schema;
use crate::model::schema::StandardSchema;
use crate::model::Item;
use crate::model::ValueKind;
use crate::model::{Attr, Value};

mod derive;

mod swim_common {
    pub use crate::*;
}

#[test]
fn test_tag() {
    #[derive(Form, ValidatedForm)]
    struct S;

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Anything),
    };

    let value = S {}.as_value();

    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&value));
}

#[test]
fn all_items() {
    #[derive(Form, ValidatedForm)]
    #[form(schema(all_items(of_kind(ValueKind::Int32))))]
    struct S {
        a: i32,
        b: i32,
    }

    let expected_schema = StandardSchema::And(vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "S",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Anything),
        },
        StandardSchema::AllItems(Box::new(ItemSchema::Field(SlotSchema::new(
            StandardSchema::Anything,
            StandardSchema::OfKind(ValueKind::Int32),
        )))),
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::Field(SlotSchema::new(StandardSchema::text("a"), i32::schema())),
                    true,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(StandardSchema::text("b"), i32::schema())),
                    true,
                ),
            ],
            exhaustive: false,
        },
    ]);

    let valid = S { a: 1, b: 2 }.as_value();
    let invalid_value = Value::Record(
        vec![Attr::of("S")],
        vec![
            Item::Slot(Value::text("a"), Value::Int32Value(1)),
            Item::Slot(Value::text("b"), Value::text("invalid")),
        ],
    );

    assert_eq!(valid, valid);
    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&valid));
    assert!(!S::schema().matches(&invalid_value));
}

#[test]
fn container_anything() {
    #[derive(Form, ValidatedForm)]
    #[form(schema(anything))]
    struct S {
        a: i32,
        b: i32,
    }

    let expected_schema = StandardSchema::Anything;

    let value = S { a: 1, b: 2 }.as_value();

    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&value));
    assert!(S::schema().matches(&Value::Int32Value(1)));
}

#[test]
fn text() {
    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(schema(text = "swim"))]
        a: String,
    }

    let expected_schema = StandardSchema::And(vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "S",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Anything),
        },
        StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("a"),
                    StandardSchema::text("swim"),
                )),
                true,
            )],
            exhaustive: false,
        },
    ]);

    let value = S {
        a: String::from("swim"),
    }
    .as_value();

    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&value));
    assert!(!S::schema().matches(&Value::Int32Value(1)));
}

#[test]
fn container_nothing() {
    #[derive(Form, ValidatedForm)]
    #[form(schema(nothing))]
    struct S {
        a: i32,
        b: i32,
    }

    let expected_schema = StandardSchema::Nothing;

    let value = S { a: 1, b: 2 }.as_value();

    assert_eq!(S::schema(), expected_schema);
    assert!(!S::schema().matches(&value));
    assert!(!S::schema().matches(&Value::Int32Value(1)));
}

#[test]
fn num_items_attrs() {
    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(schema(and(num_items = 2, num_attrs = 3)))]
        a: Value,
    }

    let expected_schema = StandardSchema::And(vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "S",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Anything),
        },
        StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("a"),
                    StandardSchema::And(vec![
                        StandardSchema::NumItems(2),
                        StandardSchema::NumAttrs(3),
                    ]),
                )),
                true,
            )],
            exhaustive: false,
        },
    ]);

    let value = S {
        a: Value::Record(
            vec![Attr::of("a"), Attr::of("b"), Attr::of("c")],
            vec![
                Item::ValueItem(Value::Int32Value(1)),
                Item::ValueItem(Value::Int32Value(2)),
            ],
        ),
    }
    .as_value();

    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&value));
    assert!(!S::schema().matches(&Value::Int32Value(1)));
}

#[test]
fn num_items() {
    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(schema(num_items = 2))]
        a: Value,
    }

    let expected_schema = StandardSchema::And(vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "S",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Anything),
        },
        StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("a"),
                    StandardSchema::NumItems(2),
                )),
                true,
            )],
            exhaustive: false,
        },
    ]);

    let value = S {
        a: Value::Record(
            vec![],
            vec![
                Item::ValueItem(Value::Int32Value(1)),
                Item::ValueItem(Value::Int32Value(2)),
            ],
        ),
    }
    .as_value();

    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&value));
    assert!(!S::schema().matches(&Value::Int32Value(1)));
}

#[test]
fn num_attrs() {
    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(schema(num_attrs = 3))]
        a: Value,
    }

    let expected_schema = StandardSchema::And(vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "S",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Anything),
        },
        StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("a"),
                    StandardSchema::NumAttrs(3),
                )),
                true,
            )],
            exhaustive: false,
        },
    ]);

    let value = S {
        a: Value::Record(vec![Attr::of("a"), Attr::of("b"), Attr::of("c")], vec![]),
    }
    .as_value();

    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&value));
    assert!(!S::schema().matches(&Value::Int32Value(1)));
}

#[test]
fn and() {
    fn s_value() -> Value {
        Value::text("text")
    }

    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(schema(and(of_kind(ValueKind::Text), equal = "s_value")))]
        a: String,
    }

    let expected_schema = StandardSchema::And(vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "S",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Anything),
        },
        StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("a"),
                    StandardSchema::And(vec![
                        StandardSchema::OfKind(ValueKind::Text),
                        StandardSchema::Equal(s_value()),
                    ]),
                )),
                true,
            )],
            exhaustive: false,
        },
    ]);
    let value = S {
        a: String::from("text"),
    }
    .as_value();

    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&value));
    assert!(!S::schema().matches(&Value::Int32Value(1)));
}

#[test]
fn container_equal() {
    fn container_expected() -> Value {
        Value::Record(
            vec![Attr::of("S")],
            vec![
                Item::Slot(Value::text("a"), Value::Int32Value(1)),
                Item::Slot(Value::text("b"), Value::Int32Value(2)),
            ],
        )
    }

    #[derive(Form, ValidatedForm)]
    #[form(schema(equal = "container_expected"))]
    struct S {
        a: i32,
        b: i32,
    }

    let expected_schema = StandardSchema::And(vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "S",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Anything),
        },
        StandardSchema::Equal(container_expected()),
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::Field(SlotSchema::new(StandardSchema::text("a"), i32::schema())),
                    true,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(StandardSchema::text("b"), i32::schema())),
                    true,
                ),
            ],
            exhaustive: false,
        },
    ]);

    let valid_value = container_expected();
    let invalid_value = Value::Int32Value(1);

    assert_eq!(S { a: 1, b: 2 }.as_value(), valid_value);
    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&valid_value));
    assert!(!S::schema().matches(&invalid_value));
}

#[test]
fn field_equal() {
    fn field_expected() -> Value {
        Value::Int32Value(1)
    }

    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(schema(equal = "field_expected"))]
        a: Value,
        b: i32,
    }

    let expected_schema = StandardSchema::And(vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "S",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Anything),
        },
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::text("a"),
                        StandardSchema::Equal(field_expected()),
                    )),
                    true,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(StandardSchema::text("b"), i32::schema())),
                    true,
                ),
            ],
            exhaustive: false,
        },
    ]);

    let valid_value = S {
        a: Value::Int32Value(1),
        b: 2,
    }
    .as_value();
    let invalid_value = Value::Int32Value(1);

    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&valid_value));
    assert!(!S::schema().matches(&invalid_value));
}

#[test]
fn not() {
    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(schema(not(of_kind(ValueKind::Text))))]
        a: Value,
    }

    let expected_schema = StandardSchema::And(vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "S",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Anything),
        },
        StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("a"),
                    StandardSchema::Not(Box::new(StandardSchema::OfKind(ValueKind::Text))),
                )),
                true,
            )],
            exhaustive: false,
        },
    ]);

    let valid_value = S {
        a: Value::Int32Value(1),
    }
    .as_value();
    let invalid_value = Value::Record(
        vec![Attr::of("S")],
        vec![Item::Slot(Value::text("a"), Value::text("invalid"))],
    );

    let schema = S::schema();

    assert_eq!(schema, expected_schema);
    assert!(schema.matches(&valid_value));
    assert!(!schema.matches(&invalid_value));
}

#[test]
fn or() {
    fn s_value() -> Value {
        Value::text("text")
    }

    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(schema(or(of_kind(ValueKind::Int32), equal = "s_value")))]
        a: String,
    }

    let expected_schema = StandardSchema::And(vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "S",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Anything),
        },
        StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("a"),
                    StandardSchema::Or(vec![
                        StandardSchema::OfKind(ValueKind::Int32),
                        StandardSchema::Equal(s_value()),
                    ]),
                )),
                true,
            )],
            exhaustive: false,
        },
    ]);

    let value = S {
        a: String::from("text"),
    }
    .as_value();

    let partial_value = Value::Record(
        vec![Attr::of("S")],
        vec![Item::Slot(Value::text("a"), Value::Int32Value(1))],
    );

    let schema = S::schema();

    assert_eq!(schema, expected_schema);
    assert!(schema.matches(&value));
    assert!(schema.matches(&partial_value));
    assert!(!schema.matches(&Value::Int32Value(1)));
}

#[test]
fn generic_value() {
    fn expected() -> Value {
        Value::Int32Value(i32::max_value())
    }

    #[derive(Form, ValidatedForm)]
    struct S<F>
    where
        F: ValidatedForm,
    {
        #[form(schema(equal = "expected"))]
        f: F,
    }

    let s = S {
        f: Value::Int32Value(i32::max_value()),
    };

    let expected_schema = StandardSchema::And(vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "S",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Anything),
        },
        StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("f"),
                    StandardSchema::Equal(expected()),
                )),
                true,
            )],
            exhaustive: false,
        },
    ]);

    let valid_value = S {
        f: Value::Int32Value(i32::max_value()),
    }
    .as_value();

    let schema = S::<Value>::schema();

    assert_eq!(s.as_value(), valid_value);
    assert_eq!(schema, expected_schema);
    assert!(schema.matches(&valid_value));
    assert!(!schema.matches(&Value::Int32Value(1)));
}

#[test]
fn tuple_struct() {
    #[derive(Form, ValidatedForm)]
    struct S(i32, i64, String);

    let expected_schema = StandardSchema::And(vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "S",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Anything),
        },
        StandardSchema::Layout {
            items: vec![
                (ItemSchema::ValueItem(i32::schema()), true),
                (ItemSchema::ValueItem(i64::schema()), true),
                (ItemSchema::ValueItem(String::schema()), true),
            ],
            exhaustive: false,
        },
    ]);

    assert_eq!(S::schema(), expected_schema);
}

#[test]
fn tuple_struct_attrs() {
    fn int_eq() -> Value {
        Value::Int64Value(i64::max_value())
    }

    #[derive(Form, ValidatedForm)]
    struct S(
        #[form(schema(of_kind(ValueKind::Int32)))] i32,
        #[form(schema(equal = "int_eq"))] i64,
        #[form(schema(text = "swim"))] String,
    );

    let expected_schema = StandardSchema::And(vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "S",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Anything),
        },
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::ValueItem(StandardSchema::OfKind(ValueKind::Int32)),
                    true,
                ),
                (ItemSchema::ValueItem(StandardSchema::Equal(int_eq())), true),
                (ItemSchema::ValueItem(StandardSchema::text("swim")), true),
            ],
            exhaustive: false,
        },
    ]);

    assert_eq!(S::schema(), expected_schema);
}

#[test]
fn unit_struct() {
    #[derive(Form, ValidatedForm)]
    struct S;

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Anything),
    };

    assert_eq!(S::schema(), expected_schema);
}

#[test]
fn field_anything() {
    #[derive(Form, ValidatedForm)]
    struct S {
        a: i32,
        #[form(schema(anything))]
        b: Value,
    }

    let expected_schema = StandardSchema::And(vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "S",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Anything),
        },
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::Field(SlotSchema::new(StandardSchema::text("a"), i32::schema())),
                    true,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::text("b"),
                        StandardSchema::Anything,
                    )),
                    true,
                ),
            ],
            exhaustive: false,
        },
    ]);

    let schema = S::schema();
    let value = S {
        a: 1,
        b: Value::text("hello"),
    }
    .as_value();

    assert_eq!(schema, expected_schema);
    assert!(schema.matches(&value));
    assert!(!schema.matches(&Value::Int32Value(1)));
}

#[test]
fn field_non_nan() {
    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(schema(non_nan))]
        f: f64,
    }

    let expected_schema = StandardSchema::And(vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "S",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Anything),
        },
        StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("f"),
                    StandardSchema::NonNan,
                )),
                true,
            )],
            exhaustive: false,
        },
    ]);

    let schema = S::schema();

    assert_eq!(schema, expected_schema);

    assert!(schema.matches(&S { f: 0.0 }.as_value()));
    assert!(schema.matches(&S { f: 1.0 }.as_value()));
    assert!(schema.matches(&S { f: -1.0 }.as_value()));
    assert!(schema.matches(&S { f: f64::INFINITY }.as_value()));
    assert!(schema.matches(
        &S {
            f: f64::NEG_INFINITY
        }
        .as_value()
    ));
    assert!(!schema.matches(&S { f: f64::NAN }.as_value()));

    assert!(!schema.matches(&Value::Int32Value(1)));
}

#[test]
fn field_finite() {
    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(schema(finite))]
        f: f64,
    }

    let expected_schema = StandardSchema::And(vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "S",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Anything),
        },
        StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("f"),
                    StandardSchema::Finite,
                )),
                true,
            )],
            exhaustive: false,
        },
    ]);

    let schema = S::schema();

    assert_eq!(schema, expected_schema);

    assert!(schema.matches(&S { f: 0.0 }.as_value()));
    assert!(schema.matches(&S { f: 1.0 }.as_value()));
    assert!(schema.matches(&S { f: -1.0 }.as_value()));
    assert!(!schema.matches(&S { f: f64::INFINITY }.as_value()));
    assert!(!schema.matches(
        &S {
            f: f64::NEG_INFINITY
        }
        .as_value()
    ));
    assert!(!schema.matches(&S { f: f64::NAN }.as_value()));

    assert!(!schema.matches(&Value::Int32Value(1)));
}

#[test]
fn complex() {
    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(schema(or(
            and(of_kind(ValueKind::Record), num_attrs = 1, num_items = 2),
            and(of_kind(ValueKind::Record), num_attrs = 2, num_items = 1),
            of_kind(ValueKind::Int32)
        )))]
        value: Value,
    }

    let expected_schema = StandardSchema::And(vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "S",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Anything),
        },
        StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("value"),
                    StandardSchema::Or(vec![
                        StandardSchema::And(vec![
                            StandardSchema::OfKind(ValueKind::Record),
                            StandardSchema::NumAttrs(1),
                            StandardSchema::NumItems(2),
                        ]),
                        StandardSchema::And(vec![
                            StandardSchema::OfKind(ValueKind::Record),
                            StandardSchema::NumAttrs(2),
                            StandardSchema::NumItems(1),
                        ]),
                        StandardSchema::OfKind(ValueKind::Int32),
                    ]),
                )),
                true,
            )],
            exhaustive: false,
        },
    ]);

    let schema = S::schema();

    assert_eq!(schema, expected_schema);

    let first = Value::Record(
        vec![Attr::of("a")],
        vec![
            Item::ValueItem(Value::text("1")),
            Item::ValueItem(Value::text("2")),
        ],
    );
    let second = Value::Record(
        vec![Attr::of("a"), Attr::of("1")],
        vec![Item::ValueItem(Value::text("1"))],
    );
    let third = Value::Int32Value(i32::max_value());

    assert!(schema.matches(&S { value: first }.as_value()));
    assert!(schema.matches(&S { value: second }.as_value()));
    assert!(schema.matches(&S { value: third }.as_value()));
    assert!(!schema.matches(&Value::Int64Value(i64::max_value())));
}
