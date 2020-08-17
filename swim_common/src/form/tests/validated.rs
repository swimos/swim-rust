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

use num_bigint::BigInt;
use num_traits::Float;

use crate::form::Form;
use crate::form::ValidatedForm;
use crate::model::schema::attr::AttrSchema;
use crate::model::schema::slot::SlotSchema;
use crate::model::schema::Schema;
use crate::model::schema::StandardSchema;
use crate::model::schema::{FieldSpec, ItemSchema};
use crate::model::Item;
use crate::model::ValueKind;
use crate::model::{Attr, Value};
use std::collections::BTreeMap;

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
        remainder: Box::new(StandardSchema::Layout {
            items: vec![],
            exhaustive: true,
        }),
    };

    let value = S {}.as_value();

    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&value));
}

#[test]
fn all_items_named() {
    #[derive(Form, ValidatedForm)]
    #[form(schema(all_items(of_kind(ValueKind::Int32))))]
    struct S {
        a: i32,
        b: i32,
    }

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::And(vec![
            StandardSchema::AllItems(Box::new(ItemSchema::Field(SlotSchema::new(
                StandardSchema::Anything,
                StandardSchema::OfKind(ValueKind::Int32),
            )))),
            StandardSchema::Layout {
                items: vec![
                    (
                        ItemSchema::Field(SlotSchema::new(
                            StandardSchema::text("a"),
                            i32::schema(),
                        )),
                        true,
                    ),
                    (
                        ItemSchema::Field(SlotSchema::new(
                            StandardSchema::text("b"),
                            i32::schema(),
                        )),
                        true,
                    ),
                ],
                exhaustive: true,
            },
        ])),
    };

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
fn all_items_tuple() {
    #[derive(Form, ValidatedForm)]
    #[form(schema(all_items(of_kind(ValueKind::Int32))))]
    struct S(i32, i64);

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::And(vec![
            StandardSchema::AllItems(Box::new(ItemSchema::ValueItem(StandardSchema::OfKind(
                ValueKind::Int32,
            )))),
            StandardSchema::Layout {
                items: vec![
                    (ItemSchema::ValueItem(i32::schema()), true),
                    (ItemSchema::ValueItem(i64::schema()), true),
                ],
                exhaustive: true,
            },
        ])),
    };

    let valid = S(1, 2).as_value();
    let invalid_value = Value::Record(
        vec![Attr::of("S")],
        vec![
            Item::ValueItem(Value::Int32Value(1)),
            Item::ValueItem(Value::text("invalid")),
        ],
    );

    assert_eq!(valid, valid);
    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&valid));
    assert!(!S::schema().matches(&invalid_value));
}

#[test]
fn all_items_new_type() {
    #[derive(Form, ValidatedForm)]
    #[form(schema(all_items(of_kind(ValueKind::Int32))))]
    struct S(i32);

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::And(vec![
            StandardSchema::AllItems(Box::new(ItemSchema::ValueItem(StandardSchema::OfKind(
                ValueKind::Int32,
            )))),
            StandardSchema::Layout {
                items: vec![(ItemSchema::ValueItem(i32::schema()), true)],
                exhaustive: true,
            },
        ])),
    };

    let valid = S(1).as_value();
    let invalid_value = Value::Record(
        vec![Attr::of("S")],
        vec![Item::ValueItem(Value::text("invalid"))],
    );

    assert_eq!(valid, valid);
    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&valid));
    assert!(!S::schema().matches(&invalid_value));
}

#[test]
fn text() {
    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(schema(text = "swim"))]
        a: String,
    }

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("a"),
                    StandardSchema::text("swim"),
                )),
                true,
            )],
            exhaustive: true,
        }),
    };

    let value = S {
        a: String::from("swim"),
    }
    .as_value();

    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&value));
    assert!(!S::schema().matches(&Value::Int32Value(1)));
}

#[test]
fn num_items_attrs() {
    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(schema(and(num_items = 2, num_attrs = 3)))]
        a: Value,
    }

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
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
            exhaustive: true,
        }),
    };

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

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("a"),
                    StandardSchema::NumItems(2),
                )),
                true,
            )],
            exhaustive: true,
        }),
    };

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

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("a"),
                    StandardSchema::NumAttrs(3),
                )),
                true,
            )],
            exhaustive: true,
        }),
    };

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

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
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
            exhaustive: true,
        }),
    };

    let value = S {
        a: String::from("text"),
    }
    .as_value();

    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&value));
    assert!(!S::schema().matches(&Value::Int32Value(1)));
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

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
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
            exhaustive: true,
        }),
    };

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

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("a"),
                    StandardSchema::Not(Box::new(StandardSchema::OfKind(ValueKind::Text))),
                )),
                true,
            )],
            exhaustive: true,
        }),
    };

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

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
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
            exhaustive: true,
        }),
    };

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

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("f"),
                    StandardSchema::Equal(expected()),
                )),
                true,
            )],
            exhaustive: true,
        }),
    };

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

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![
                (ItemSchema::ValueItem(i32::schema()), true),
                (ItemSchema::ValueItem(i64::schema()), true),
                (ItemSchema::ValueItem(String::schema()), true),
            ],
            exhaustive: true,
        }),
    };

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

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::ValueItem(StandardSchema::OfKind(ValueKind::Int32)),
                    true,
                ),
                (ItemSchema::ValueItem(StandardSchema::Equal(int_eq())), true),
                (ItemSchema::ValueItem(StandardSchema::text("swim")), true),
            ],
            exhaustive: true,
        }),
    };

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
        remainder: Box::new(StandardSchema::Layout {
            items: vec![],
            exhaustive: true,
        }),
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

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
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
            exhaustive: true,
        }),
    };

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

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("f"),
                    StandardSchema::NonNan,
                )),
                true,
            )],
            exhaustive: true,
        }),
    };

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

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("f"),
                    StandardSchema::Finite,
                )),
                true,
            )],
            exhaustive: true,
        }),
    };

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

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
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
            exhaustive: true,
        }),
    };

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

#[test]
fn int_range_inclusive() {
    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(schema(int_range = "0..=10"))]
        f: i64,
    }

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("f"),
                    StandardSchema::inclusive_int_range(0, 10),
                )),
                true,
            )],
            exhaustive: true,
        }),
    };

    let schema = S::schema();

    assert_eq!(schema, expected_schema);
    assert!(schema.matches(&S { f: 0 }.as_value()));
    assert!(schema.matches(&S { f: 10 }.as_value()));
    assert!(!schema.matches(&S { f: 11 }.as_value()));
    assert!(!schema.matches(&S { f: -1 }.as_value()));
}

#[test]
fn uint_range_inclusive() {
    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(schema(uint_range = "0..=10"))]
        f: u64,
    }

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("f"),
                    StandardSchema::inclusive_uint_range(0, 10),
                )),
                true,
            )],
            exhaustive: true,
        }),
    };

    let schema = S::schema();

    assert_eq!(schema, expected_schema);
    assert!(schema.matches(&S { f: 0 }.as_value()));
    assert!(schema.matches(&S { f: 10 }.as_value()));
    assert!(!schema.matches(&S { f: 11 }.as_value()));
}

#[test]
fn int_range() {
    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(schema(int_range = "0..10"))]
        f: i64,
    }

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("f"),
                    StandardSchema::int_range(0, 10),
                )),
                true,
            )],
            exhaustive: true,
        }),
    };

    let schema = S::schema();

    assert_eq!(schema, expected_schema);
    assert!(schema.matches(&S { f: 0 }.as_value()));
    assert!(schema.matches(&S { f: 9 }.as_value()));
    assert!(!schema.matches(&S { f: -1 }.as_value()));
    assert!(!schema.matches(&S { f: 10 }.as_value()));
    assert!(!schema.matches(&S { f: 11 }.as_value()));
}

#[test]
fn float_range_inclusive() {
    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(schema(float_range = "0.0..=10.1"))]
        f: f64,
    }

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("f"),
                    StandardSchema::inclusive_float_range(0.0, 10.1),
                )),
                true,
            )],
            exhaustive: true,
        }),
    };

    let schema = S::schema();

    assert_eq!(schema, expected_schema);
    assert!(schema.matches(&S { f: 0.0 }.as_value()));
    assert!(schema.matches(&S { f: 10.1 }.as_value()));
    assert!(!schema.matches(&S { f: 11.0 }.as_value()));
}

#[test]
fn float_range() {
    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(schema(float_range = "0..10"))]
        f: f64,
    }

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("f"),
                    StandardSchema::float_range(0.0, 10.0),
                )),
                true,
            )],
            exhaustive: true,
        }),
    };

    let schema = S::schema();

    assert_eq!(schema, expected_schema);
    assert!(schema.matches(&S { f: 0.0 }.as_value()));
    assert!(schema.matches(&S { f: 9.9 }.as_value()));
    assert!(!schema.matches(&S { f: -1.0 }.as_value()));
    assert!(!schema.matches(&S { f: 10.1 }.as_value()));
    assert!(!schema.matches(&S { f: f64::infinity() }.as_value()));
}

#[test]
fn uint_range() {
    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(schema(uint_range = "0..10"))]
        f: u64,
    }

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("f"),
                    StandardSchema::uint_range(0, 10),
                )),
                true,
            )],
            exhaustive: true,
        }),
    };

    let schema = S::schema();

    assert_eq!(schema, expected_schema);
    assert!(schema.matches(&S { f: 0 }.as_value()));
    assert!(schema.matches(&S { f: 9 }.as_value()));
    assert!(!schema.matches(&S { f: 10 }.as_value()));
    assert!(!schema.matches(&S { f: 11 }.as_value()));
}

#[test]
fn big_int_range_inclusive() {
    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(schema(big_int_range = "123456..=789101112"))]
        f: BigInt,
    }

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("f"),
                    StandardSchema::inclusive_big_int_range(
                        BigInt::from(123456),
                        BigInt::from(789101112),
                    ),
                )),
                true,
            )],
            exhaustive: true,
        }),
    };

    let schema = S::schema();

    assert_eq!(schema, expected_schema);
    assert!(schema.matches(
        &S {
            f: BigInt::from(123456)
        }
        .as_value()
    ));
    assert!(schema.matches(
        &S {
            f: BigInt::from(789101112)
        }
        .as_value()
    ));
    assert!(!schema.matches(
        &S {
            f: BigInt::from(i32::max_value())
        }
        .as_value()
    ));
    assert!(!schema.matches(
        &S {
            f: BigInt::from(-1)
        }
        .as_value()
    ));
}

#[test]
fn big_int_range() {
    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(schema(big_int_range = "100..300"))]
        f: BigInt,
    }

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("f"),
                    StandardSchema::big_int_range(BigInt::from(100), BigInt::from(300)),
                )),
                true,
            )],
            exhaustive: true,
        }),
    };

    let schema = S::schema();

    assert_eq!(schema, expected_schema);
    assert!(schema.matches(
        &S {
            f: BigInt::from(100)
        }
        .as_value()
    ));
    assert!(schema.matches(
        &S {
            f: BigInt::from(299)
        }
        .as_value()
    ));
    assert!(!schema.matches(
        &S {
            f: BigInt::from(300)
        }
        .as_value()
    ));
    assert!(!schema.matches(
        &S {
            f: BigInt::from(i32::max_value())
        }
        .as_value()
    ));
    assert!(!schema.matches(
        &S {
            f: BigInt::from(-1)
        }
        .as_value()
    ));
}

#[test]
fn container_anything() {
    #[derive(Form, ValidatedForm)]
    #[form(schema(anything))]
    struct S {
        a: i32,
        b: i32,
    }

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::And(vec![
            StandardSchema::Anything,
            StandardSchema::Layout {
                items: vec![
                    (
                        ItemSchema::Field(SlotSchema::new(
                            StandardSchema::text("a"),
                            i32::schema(),
                        )),
                        true,
                    ),
                    (
                        ItemSchema::Field(SlotSchema::new(
                            StandardSchema::text("b"),
                            i32::schema(),
                        )),
                        true,
                    ),
                ],
                exhaustive: true,
            },
        ])),
    };

    let value = S { a: 1, b: 2 }.as_value();

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

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::And(vec![
            StandardSchema::Nothing,
            StandardSchema::Layout {
                items: vec![
                    (
                        ItemSchema::Field(SlotSchema::new(
                            StandardSchema::text("a"),
                            i32::schema(),
                        )),
                        true,
                    ),
                    (
                        ItemSchema::Field(SlotSchema::new(
                            StandardSchema::text("b"),
                            i32::schema(),
                        )),
                        true,
                    ),
                ],
                exhaustive: true,
            },
        ])),
    };

    let value = S { a: 1, b: 2 }.as_value();

    assert_eq!(S::schema(), expected_schema);
    assert!(!S::schema().matches(&value));
    assert!(!S::schema().matches(&Value::Int32Value(1)));
}

#[test]
fn form_header_body() {
    fn eq() -> Value {
        Value::Int32Value(i32::max_value())
    }

    #[derive(Form, ValidatedForm)]
    struct S {
        a: i32,
        #[form(header_body, schema(equal = "eq"))]
        b: i32,
    }

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named("S", StandardSchema::Equal(eq()))),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(StandardSchema::text("a"), i32::schema())),
                true,
            )],
            exhaustive: true,
        }),
    };

    let value = S {
        a: 1,
        b: i32::max_value(),
    }
    .as_value();
    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&value));
    assert!(!S::schema().matches(&Value::Int32Value(1)));
}

#[test]
fn form_attr() {
    #[derive(Form, ValidatedForm)]
    struct S {
        a: i32,
        #[form(attr, schema(int_range = "0..=11"))]
        b: i32,
    }

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::And(vec![
            StandardSchema::HasAttributes {
                attributes: vec![FieldSpec::new(
                    AttrSchema::named("b", StandardSchema::inclusive_int_range(0, 11)),
                    true,
                    true,
                )],
                exhaustive: true,
            },
            StandardSchema::Layout {
                items: vec![(
                    ItemSchema::Field(SlotSchema::new(StandardSchema::text("a"), i32::schema())),
                    true,
                )],
                exhaustive: true,
            },
        ])),
    };

    let value = S { a: 1, b: 2 }.as_value();
    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&value));
    assert!(!S::schema().matches(&Value::Int32Value(1)));
}

#[test]
fn form_header() {
    fn eq() -> Value {
        Value::Int32Value(i32::max_value())
    }

    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(header_body)]
        a: i32,
        #[form(header)]
        b: i32,
    }

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::Layout {
                items: vec![
                    (ItemSchema::ValueItem(i32::schema()), true),
                    (
                        ItemSchema::Field(SlotSchema::new(
                            StandardSchema::text("b"),
                            i32::schema(),
                        )),
                        true,
                    ),
                ],
                exhaustive: true,
            },
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![],
            exhaustive: true,
        }),
    };

    let value = S { a: 1, b: 2 }.as_value();
    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&value));
    assert!(!S::schema().matches(&Value::Int32Value(1)));
}

#[test]
fn form_body() {
    #[derive(Form, ValidatedForm)]
    struct S {
        a: i32,
        #[form(body)]
        b: i32,
    }

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::Layout {
                items: vec![(
                    ItemSchema::Field(SlotSchema::new(StandardSchema::text("a"), i32::schema())),
                    true,
                )],
                exhaustive: true,
            },
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![(ItemSchema::ValueItem(i32::schema()), true)],
            exhaustive: true,
        }),
    };

    let value = S { a: 1, b: 2 }.as_value();

    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&value));
    assert!(!S::schema().matches(&Value::Int32Value(1)));
}

#[test]
fn form_single_enum() {
    #[derive(Form, ValidatedForm)]
    enum E {
        A,
    }

    let expected_schema = StandardSchema::Or(vec![StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "A",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![],
            exhaustive: true,
        }),
    }]);

    let value = E::A.as_value();

    assert_eq!(E::schema(), expected_schema);
    assert!(E::schema().matches(&value));
    assert!(!E::schema().matches(&Value::Int32Value(1)));
}

#[test]
fn form_enum_tag() {
    #[derive(Form, ValidatedForm)]
    enum E {
        #[form(tag = "Enumeration")]
        A,
    }

    let expected_schema = StandardSchema::Or(vec![StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "Enumeration",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![],
            exhaustive: true,
        }),
    }]);

    let value = E::A.as_value();

    assert_eq!(E::schema(), expected_schema);
    assert!(E::schema().matches(&value));
    assert!(!E::schema().matches(&Value::Int32Value(1)));
}

#[test]
fn form_enum_variants() {
    #[derive(Form, ValidatedForm)]
    enum E {
        UnitEnum,
        NewTypeEnum(i32),
        TupleEnum(i32, i64, String),
        StructEnum { a: i32, b: i64, c: String },
    }

    let expected_schema = StandardSchema::Or(vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "UnitEnum",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Layout {
                items: vec![],
                exhaustive: true,
            }),
        },
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "NewTypeEnum",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Layout {
                items: vec![(ItemSchema::ValueItem(i32::schema()), true)],
                exhaustive: true,
            }),
        },
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "TupleEnum",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Layout {
                items: vec![
                    (ItemSchema::ValueItem(i32::schema()), true),
                    (ItemSchema::ValueItem(i64::schema()), true),
                    (ItemSchema::ValueItem(String::schema()), true),
                ],
                exhaustive: true,
            }),
        },
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "StructEnum",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Layout {
                items: vec![
                    (
                        ItemSchema::Field(SlotSchema::new(
                            StandardSchema::text("a"),
                            i32::schema(),
                        )),
                        true,
                    ),
                    (
                        ItemSchema::Field(SlotSchema::new(
                            StandardSchema::text("b"),
                            i64::schema(),
                        )),
                        true,
                    ),
                    (
                        ItemSchema::Field(SlotSchema::new(
                            StandardSchema::text("c"),
                            String::schema(),
                        )),
                        true,
                    ),
                ],
                exhaustive: true,
            }),
        },
    ]);

    let enums = vec![
        E::UnitEnum,
        E::NewTypeEnum(i32::max_value()),
        E::TupleEnum(i32::max_value(), i64::max_value(), String::from("swim")),
        E::StructEnum {
            a: i32::max_value(),
            b: i64::max_value(),
            c: String::from("swim"),
        },
    ];

    let schema = E::schema();
    assert_eq!(schema, expected_schema);

    enums.iter().for_each(|e| {
        let value = e.as_value();

        assert!(schema.matches(&value));
        assert!(!schema.matches(&Value::Int32Value(1)));
    });
}

#[test]
fn form_enum_attrs() {
    fn i64_eq() -> Value {
        Value::Int64Value(100)
    }

    #[derive(Form, ValidatedForm)]
    enum E {
        #[form(tag = "Enumeration")]
        A {
            #[form(rename = "integer")]
            a: i32,
            #[form(body, schema(equal = "i64_eq"))]
            b: i64,
            #[form(skip)]
            c: i32,
            #[form(schema(int_range = "0..=11"), rename = "ranged_integer")]
            d: i32,
        },
    }

    let expected_schema = StandardSchema::Or(vec![StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "Enumeration",
            StandardSchema::Layout {
                items: vec![
                    (
                        ItemSchema::Field(SlotSchema::new(
                            StandardSchema::text("integer"),
                            i32::schema(),
                        )),
                        true,
                    ),
                    (
                        ItemSchema::Field(SlotSchema::new(
                            StandardSchema::text("ranged_integer"),
                            StandardSchema::inclusive_int_range(0, 11),
                        )),
                        true,
                    ),
                ],
                exhaustive: true,
            },
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![(ItemSchema::ValueItem(StandardSchema::Equal(i64_eq())), true)],
            exhaustive: true,
        }),
    }]);

    let value = E::A {
        a: 1,
        b: 100,
        c: i32::max_value(),
        d: 9,
    }
    .as_value();

    assert_eq!(E::schema(), expected_schema);
    assert!(E::schema().matches(&value));
    assert!(!E::schema().matches(&Value::Int32Value(1)));
}

#[test]
fn test_vector() {
    #[derive(Form, ValidatedForm)]
    struct S {
        v: Vec<i32>,
    }

    let value = S {
        v: vec![1, 2, 3, 4, 5],
    }
    .as_value();

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("v"),
                    <Vec<i32> as ValidatedForm>::schema(),
                )),
                true,
            )],
            exhaustive: true,
        }),
    };

    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&value));
    assert!(!S::schema().matches(&Value::Int32Value(1)));
}

#[test]
fn test_hash_map() {
    #[derive(Form, ValidatedForm)]
    struct S {
        v: BTreeMap<String, i32>,
    }

    let value = S {
        v: {
            let mut map = BTreeMap::new();
            map.insert("a".into(), 1);
            map.insert("b".into(), 2);
            map.insert("c".into(), 3);
            map
        },
    }
    .as_value();

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::text("v"),
                    <BTreeMap<String, i32> as ValidatedForm>::schema(),
                )),
                true,
            )],
            exhaustive: true,
        }),
    };

    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&value));
    assert!(!S::schema().matches(&Value::Int32Value(1)));
}

#[test]
fn test_nested() {
    #[derive(Form, ValidatedForm)]
    struct Parent {
        a: i32,
        b: Child,
        #[form(skip)]
        e: f64,
    }

    #[derive(Form, ValidatedForm)]
    enum Child {
        ChildA,
        ChildB { c: i32, d: f64 },
    }

    let child_schema = StandardSchema::Or(vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "ChildA",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Layout {
                items: vec![],
                exhaustive: true,
            }),
        },
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "ChildB",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Layout {
                items: vec![
                    (
                        ItemSchema::Field(SlotSchema::new(
                            StandardSchema::text("c"),
                            i32::schema(),
                        )),
                        true,
                    ),
                    (
                        ItemSchema::Field(SlotSchema::new(
                            StandardSchema::text("d"),
                            f64::schema(),
                        )),
                        true,
                    ),
                ],
                exhaustive: true,
            }),
        },
    ]);

    assert_eq!(Child::schema(), child_schema);

    let parent_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "Parent",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::Field(SlotSchema::new(StandardSchema::text("a"), i32::schema())),
                    true,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(StandardSchema::text("b"), child_schema)),
                    true,
                ),
            ],
            exhaustive: true,
        }),
    };

    assert_eq!(Parent::schema(), parent_schema);

    let value = Parent {
        a: 1,
        b: Child::ChildB { c: 2, d: 3.0 },
        e: 4.0,
    }
    .as_value();

    assert!(Parent::schema().matches(&value));
    assert!(!Parent::schema().matches(&Value::Int32Value(1)));
}

#[test]
fn t() {
    #[derive(Form, ValidatedForm)]
    #[form(tag = "Structure")]
    struct S {
        a: i32,
        b: i64,
    }

    let expected_schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "Structure",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::Field(SlotSchema::new(StandardSchema::text("a"), i32::schema())),
                    true,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(StandardSchema::text("b"), i64::schema())),
                    true,
                ),
            ],
            exhaustive: true,
        }),
    };

    assert_eq!(S::schema(), expected_schema);
}
