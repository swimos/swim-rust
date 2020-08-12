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
//
// #[test]
// fn test_schema_derive() {
//     #[derive(Form, ValidatedForm)]
//     struct S {
//         field_a: i32,
//         field_b: i64,
//     }
//
//     let schema = S::schema();
//     println!("{:?}", schema);
// }
//
// #[test]
// #[allow(warnings)]
// fn t() {
//     #[derive(Form, ValidatedForm)]
//     struct S {
//         #[form(schema(int_range(0, 20)))]
//         field_a: i32,
//         #[form(schema(int_range(0, 20)))]
//         field_b: i64,
//         field_c: i32,
//     }
//
//     let schema = StandardSchema::And(vec![
//         StandardSchema::HeadAttribute {
//             schema: Box::new(AttrSchema::named(
//                 "S",
//                 StandardSchema::OfKind(ValueKind::Extant),
//             )),
//             required: true,
//             remainder: Box::from(StandardSchema::OfKind(ValueKind::Extant)),
//         },
//         StandardSchema::Layout {
//             items: vec![
//                 (
//                     ItemSchema::Field(SlotSchema::new(
//                         StandardSchema::text("field_a"),
//                         StandardSchema::inclusive_int_range(0, 20),
//                     )),
//                     true,
//                 ),
//                 (
//                     ItemSchema::Field(SlotSchema::new(
//                         StandardSchema::text("field_b"),
//                         StandardSchema::inclusive_int_range(0, 20),
//                     )),
//                     true,
//                 ),
//             ],
//             exhaustive: false,
//         },
//     ]);
//
//     let rec = Value::Record(
//         vec![Attr::of("S")],
//         vec![
//             Item::Slot(Value::text("field_a"), Value::Int32Value(1)),
//             Item::Slot(Value::text("field_b"), Value::Int64Value(1)),
//         ],
//     );
//
//     let r = schema.matches(&rec);
//     println!("{}", r);
// }

#[cfg(test)]
mod container_attrs {
    use super::*;
    use crate::model::schema::slot::SlotSchema;
    use crate::model::schema::ItemSchema;
    use crate::model::Item;

    // #[test]
    // fn test_tag() {
    //     #[derive(Form, ValidatedForm)]
    //     struct S;
    //
    //     let expected_schema = StandardSchema::And(vec![StandardSchema::HeadAttribute {
    //         schema: Box::new(AttrSchema::named(
    //             "S",
    //             StandardSchema::OfKind(ValueKind::Extant),
    //         )),
    //         required: true,
    //         remainder: Box::new(StandardSchema::Anything),
    //     }]);
    //
    //     let value = S {}.as_value();
    //     let expected_value = Value::Record(vec![Attr::of("S")], Vec::new());
    //
    //     assert_eq!(value, expected_value);
    //     assert_eq!(S::schema(), expected_schema);
    //     assert!(S::schema().matches(&value));
    // }

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

    // #[test]
    // fn anything() {
    //     #[derive(Form, ValidatedForm)]
    //     // #[form(schema(all_items(of_kind = "ValueKind::Int32")))]
    //     struct S {
    //         a: i32,
    //         b: i32,
    //     }
    //
    //     let expected_schema = StandardSchema::HeadAttribute {
    //         schema: Box::new(AttrSchema::named(
    //             "S",
    //             StandardSchema::OfKind(ValueKind::Extant),
    //         )),
    //         required: true,
    //         remainder: Box::new(StandardSchema::Anything),
    //     };
    //
    //     let value = S { a: 1, b: 2 }.as_value();
    //     let expected_value = Value::Record(
    //         vec![Attr::of("S")],
    //         vec![
    //             Item::Slot(Value::text("a"), Value::Int32Value(1)),
    //             Item::Slot(Value::text("b"), Value::Int32Value(2)),
    //         ],
    //     );
    //
    //     assert_eq!(value, expected_value);
    //     assert_eq!(S::schema(), expected_schema);
    //     assert!(S::schema().matches(&value));
    // }
    //
    // #[test]
    // fn num_items() {
    //     #[derive(Form, ValidatedForm)]
    //     #[form(tag = "taggy", schema(num_items = 5, num_attrs = 50))]
    //     struct S {
    //         #[form(body)]
    //         a: i32,
    //     }
    // }
    //
    // #[test]
    // fn and() {
    //     #[derive(Form, ValidatedForm)]
    //     struct S {
    //         #[form(schema(and(num_items = 5, num_attrs = 50)))]
    //         a: i32,
    //     }
    // }
    //
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
    //
    // #[test]
    // fn or() {
    //     #[derive(Form, ValidatedForm)]
    //     struct S {
    //         #[form(schema(or(num_items = 5, num_attrs = 5)))]
    //         a: i32,
    //     }
    // }
}
