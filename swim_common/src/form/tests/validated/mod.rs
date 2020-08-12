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
use crate::model::schema::Schema;
use crate::model::schema::StandardSchema;
use crate::model::ValueKind;
use crate::model::{Attr, Value};

mod derive;

mod swim_common {
    pub use crate::*;
}

use crate::model::schema::slot::SlotSchema;
use crate::model::schema::ItemSchema;
use crate::model::Item;

#[test]
fn test_tag() {
    #[derive(Form, ValidatedForm)]
    struct S;

    let expected_schema = StandardSchema::And(vec![StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::named(
            "S",
            StandardSchema::OfKind(ValueKind::Extant),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Anything),
    }]);

    let value = S {}.as_value();
    let expected_value = Value::Record(vec![Attr::of("S")], Vec::new());

    assert_eq!(value, expected_value);
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
    let valid_value = Value::Record(
        vec![Attr::of("S")],
        vec![
            Item::Slot(Value::text("a"), Value::Int32Value(1)),
            Item::Slot(Value::text("b"), Value::Int32Value(2)),
        ],
    );

    let invalid_value = Value::Record(
        vec![Attr::of("S")],
        vec![
            Item::Slot(Value::text("a"), Value::Int32Value(1)),
            Item::Slot(Value::text("b"), Value::text("invalid")),
        ],
    );

    assert_eq!(valid, valid_value);
    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&valid));
    assert!(!S::schema().matches(&invalid_value));
}

#[test]
fn anything() {
    #[derive(Form, ValidatedForm)]
    #[form(schema(anything))]
    struct S {
        a: i32,
        b: i32,
    }

    let expected_schema = StandardSchema::Anything;

    let value = S { a: 1, b: 2 }.as_value();
    let expected_value = Value::Record(
        vec![Attr::of("S")],
        vec![
            Item::Slot(Value::text("a"), Value::Int32Value(1)),
            Item::Slot(Value::text("b"), Value::Int32Value(2)),
        ],
    );

    assert_eq!(value, expected_value);
    assert_eq!(S::schema(), expected_schema);
    assert!(S::schema().matches(&value));
    assert!(S::schema().matches(&Value::Int32Value(1)));
}

#[test]
fn nothing() {
    #[derive(Form, ValidatedForm)]
    #[form(schema(nothing))]
    struct S {
        a: i32,
        b: i32,
    }

    let expected_schema = StandardSchema::Nothing;

    let value = S { a: 1, b: 2 }.as_value();
    let expected_value = Value::Record(
        vec![Attr::of("S")],
        vec![
            Item::Slot(Value::text("a"), Value::Int32Value(1)),
            Item::Slot(Value::text("b"), Value::Int32Value(2)),
        ],
    );

    assert_eq!(value, expected_value);
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

    let inner_value = Value::Record(
        vec![Attr::of("a"), Attr::of("b"), Attr::of("c")],
        vec![
            Item::ValueItem(Value::Int32Value(1)),
            Item::ValueItem(Value::Int32Value(2)),
        ],
    );

    let value = S {
        a: inner_value.clone(),
    }
    .as_value();
    let expected_value = Value::Record(
        vec![Attr::of("S")],
        vec![Item::Slot(Value::text("a"), inner_value)],
    );

    assert_eq!(value, expected_value);
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

    let inner_value = Value::Record(
        vec![],
        vec![
            Item::ValueItem(Value::Int32Value(1)),
            Item::ValueItem(Value::Int32Value(2)),
        ],
    );

    let value = S {
        a: inner_value.clone(),
    }
    .as_value();
    let expected_value = Value::Record(
        vec![Attr::of("S")],
        vec![Item::Slot(Value::text("a"), inner_value)],
    );

    assert_eq!(value, expected_value);
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

    let inner_value = Value::Record(vec![Attr::of("a"), Attr::of("b"), Attr::of("c")], vec![]);

    let value = S {
        a: inner_value.clone(),
    }
    .as_value();
    let expected_value = Value::Record(
        vec![Attr::of("S")],
        vec![Item::Slot(Value::text("a"), inner_value)],
    );

    assert_eq!(value, expected_value);
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

    let expected_value = Value::Record(
        vec![Attr::of("S")],
        vec![Item::Slot(Value::text("a"), Value::text("text"))],
    );

    assert_eq!(value, expected_value);
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

    let valid_value = Value::Record(
        vec![Attr::of("S")],
        vec![
            Item::Slot(Value::text("a"), Value::Int32Value(1)),
            Item::Slot(Value::text("b"), Value::Int32Value(2)),
        ],
    );
    let invalid_value = Value::Int32Value(1);

    assert_eq!(
        S {
            a: Value::Int32Value(1),
            b: 2
        }
        .as_value(),
        valid_value
    );
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

    let valid_value = Value::Record(
        vec![Attr::of("S")],
        vec![Item::Slot(Value::text("a"), Value::Int32Value(1))],
    );
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

    let expected_value = Value::Record(
        vec![Attr::of("S")],
        vec![Item::Slot(Value::text("a"), Value::text("text"))],
    );

    let partial_value = Value::Record(
        vec![Attr::of("S")],
        vec![Item::Slot(Value::text("a"), Value::Int32Value(1))],
    );

    let schema = S::schema();

    assert_eq!(value, expected_value);
    assert_eq!(schema, expected_schema);
    assert!(schema.matches(&expected_value));
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

    let expected_value = Value::Record(
        vec![Attr::of("S")],
        vec![Item::Slot(
            Value::text("f"),
            Value::Int32Value(i32::max_value()),
        )],
    );

    let schema = S::<Value>::schema();

    assert_eq!(s.as_value(), expected_value);
    assert_eq!(schema, expected_schema);
    assert!(schema.matches(&expected_value));
    assert!(!schema.matches(&Value::Int32Value(1)));
}

#[test]
fn tuple_struct() {
    #[derive(Form, ValidatedForm)]
    struct S(i32, i64, String);
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
