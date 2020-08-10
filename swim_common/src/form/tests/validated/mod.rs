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

use crate::form::{Form, ValidatedForm};
use crate::model::schema::attr::AttrSchema;
use crate::model::schema::slot::SlotSchema;
use crate::model::schema::{ItemSchema, Schema, StandardSchema};
use crate::model::{Attr, Item, Value, ValueKind};

mod swim_common {
    pub use crate::*;
}

#[test]
fn test_schema_derive() {
    #[derive(Form, ValidatedForm)]
    struct S {
        field_a: i32,
        field_b: i64,
    }

    let schema = S::schema();
    println!("{:?}", schema);
}

#[test]
#[allow(warnings)]
fn t() {
    #[derive(Form, ValidatedForm)]
    struct S {
        #[form(valid(int_range(0, 20)))]
        field_a: i32,
        #[form(valid(int_range(0, 20)))]
        field_b: i64,
        field_c: i32,
    }

    let schema = StandardSchema::And(vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::named(
                "S",
                StandardSchema::OfKind(ValueKind::Extant),
            )),
            required: true,
            remainder: Box::from(StandardSchema::OfKind(ValueKind::Extant)),
        },
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::text("field_a"),
                        StandardSchema::inclusive_int_range(0, 20),
                    )),
                    true,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::text("field_b"),
                        StandardSchema::inclusive_int_range(0, 20),
                    )),
                    true,
                ),
            ],
            exhaustive: false,
        },
    ]);

    let rec = Value::Record(
        vec![Attr::of("S")],
        vec![
            Item::Slot(Value::text("field_a"), Value::Int32Value(1)),
            Item::Slot(Value::text("field_b"), Value::Int64Value(1)),
        ],
    );

    let r = schema.matches(&rec);
    println!("{}", r);
}
