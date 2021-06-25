// Copyright 2015-2021 SWIM.AI inc.
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

use num_bigint::{BigInt, BigUint};
use std::borrow::Cow;
use swim_common::form::structural::write::{
    BodyWriter, HeaderWriter, Label, PrimitiveWriter, RecordBodyKind, StructuralWritable,
    StructuralWriter,
};
use swim_common::model::{Attr, Item, Value};

#[derive(Default, Debug, PartialEq, Eq)]
struct Validator {
    depth: usize,
    num_attrs: usize,
    kind: Option<RecordBodyKind>,
    num_items: usize,
}

#[derive(Debug)]
struct EarlyTerm(Validator);

impl PrimitiveWriter for Validator {
    type Repr = Self;
    type Error = EarlyTerm;

    fn write_extant(self) -> Result<Self::Repr, Self::Error> {
        Err(EarlyTerm(self))
    }

    fn write_i32(self, _value: i32) -> Result<Self::Repr, Self::Error> {
        Err(EarlyTerm(self))
    }

    fn write_i64(self, _value: i64) -> Result<Self::Repr, Self::Error> {
        Err(EarlyTerm(self))
    }

    fn write_u32(self, _value: u32) -> Result<Self::Repr, Self::Error> {
        Err(EarlyTerm(self))
    }

    fn write_u64(self, _value: u64) -> Result<Self::Repr, Self::Error> {
        Err(EarlyTerm(self))
    }

    fn write_f64(self, _value: f64) -> Result<Self::Repr, Self::Error> {
        Err(EarlyTerm(self))
    }

    fn write_bool(self, _value: bool) -> Result<Self::Repr, Self::Error> {
        Err(EarlyTerm(self))
    }

    fn write_big_int(self, _value: BigInt) -> Result<Self::Repr, Self::Error> {
        Err(EarlyTerm(self))
    }

    fn write_big_uint(self, _value: BigUint) -> Result<Self::Repr, Self::Error> {
        Err(EarlyTerm(self))
    }

    fn write_text<T: Label>(self, _value: T) -> Result<Self::Repr, Self::Error> {
        Err(EarlyTerm(self))
    }

    fn write_blob_vec(self, _blob: Vec<u8>) -> Result<Self::Repr, Self::Error> {
        Err(EarlyTerm(self))
    }

    fn write_blob(self, _value: &[u8]) -> Result<Self::Repr, Self::Error> {
        Err(EarlyTerm(self))
    }
}

impl StructuralWriter for Validator {
    type Header = Self;
    type Body = Self;

    fn record(mut self, num_attrs: usize) -> Result<Self::Header, Self::Error> {
        if self.depth == 0 {
            self.num_attrs = num_attrs;
        }
        self.depth += 1;
        Ok(self)
    }
}

impl HeaderWriter for Validator {
    type Repr = Self;
    type Error = EarlyTerm;
    type Body = Self;

    fn write_attr<V: StructuralWritable>(
        self,
        _name: Cow<'_, str>,
        _value: &V,
    ) -> Result<Self, Self::Error> {
        Ok(self)
    }

    fn delegate<V: StructuralWritable>(self, value: &V) -> Result<Self::Repr, Self::Error> {
        value.write_with(self)
    }

    fn write_attr_into<L: Label, V: StructuralWritable>(
        self,
        _name: L,
        _value: V,
    ) -> Result<Self, Self::Error> {
        Ok(self)
    }

    fn delegate_into<V: StructuralWritable>(self, value: V) -> Result<Self::Repr, Self::Error> {
        value.write_into(self)
    }

    fn complete_header(
        mut self,
        kind: RecordBodyKind,
        num_items: usize,
    ) -> Result<Self::Body, Self::Error> {
        self.kind = Some(kind);
        self.num_items = num_items;
        Ok(self)
    }
}

impl BodyWriter for Validator {
    type Repr = Self;
    type Error = EarlyTerm;

    fn write_value<V: StructuralWritable>(self, _value: &V) -> Result<Self, Self::Error> {
        Ok(self)
    }

    fn write_slot<K: StructuralWritable, V: StructuralWritable>(
        self,
        _key: &K,
        _value: &V,
    ) -> Result<Self, Self::Error> {
        Ok(self)
    }

    fn write_value_into<V: StructuralWritable>(self, _value: V) -> Result<Self, Self::Error> {
        Ok(self)
    }

    fn write_slot_into<K: StructuralWritable, V: StructuralWritable>(
        self,
        _key: K,
        _value: V,
    ) -> Result<Self, Self::Error> {
        Ok(self)
    }

    fn done(self) -> Result<Self::Repr, Self::Error> {
        Ok(self)
    }
}

fn validate<T: StructuralWritable>(
    instance: T,
    num_attrs: usize,
    kind: RecordBodyKind,
    num_items: usize,
    depth: usize,
) {
    let val = Validator::default();
    let result = instance.write_with(val).unwrap();
    assert_eq!(
        result,
        Validator {
            depth,
            num_attrs,
            kind: Some(kind),
            num_items
        }
    );

    let val = Validator::default();
    let result = instance.write_into(val).unwrap();
    assert_eq!(
        result,
        Validator {
            depth,
            num_attrs,
            kind: Some(kind),
            num_items
        }
    );
}

#[test]
fn derive_unit_struct() {
    #[derive(StructuralWritable)]
    struct Unit;

    let unit = Unit;

    let value: Value = unit.structure();

    assert_eq!(value, Value::of_attr("Unit"));

    validate(unit, 1, RecordBodyKind::ArrayLike, 0, 1);
}

#[test]
fn derive_simple_struct() {
    #[derive(StructuralWritable)]
    struct Simple {
        first: i32,
    }

    let simple = Simple { first: 1 };

    let value: Value = simple.structure();

    assert_eq!(
        value,
        Value::Record(vec![Attr::of("Simple")], vec![Item::slot("first", 1)])
    );

    validate(simple, 1, RecordBodyKind::MapLike, 1, 1);
}

#[test]
fn derive_two_field_struct() {
    #[derive(StructuralWritable)]
    struct TwoFields {
        first: i32,
        second: String,
    }

    let two_fields = TwoFields {
        first: 2,
        second: "hello".to_string(),
    };

    let value: Value = two_fields.structure();

    assert_eq!(
        value,
        Value::Record(
            vec![Attr::of("TwoFields")],
            vec![Item::slot("first", 2), Item::slot("second", "hello"),]
        )
    );

    validate(two_fields, 1, RecordBodyKind::MapLike, 2, 1);
}

#[test]
fn derive_two_field_tuple_struct() {
    #[derive(StructuralWritable)]
    struct TwoFields(i32, String);

    let two_fields = TwoFields(2, "hello".to_string());

    let value: Value = two_fields.structure();

    assert_eq!(
        value,
        Value::Record(
            vec![Attr::of("TwoFields")],
            vec![Item::of(2), Item::of("hello"),]
        )
    );

    validate(two_fields, 1, RecordBodyKind::ArrayLike, 2, 1);
}

#[test]
fn derive_struct_lift_attr() {
    #[derive(StructuralWritable)]
    struct MyStruct {
        #[form(attr)]
        in_attr: bool,
        first: i32,
        second: String,
    }

    let instance = MyStruct {
        in_attr: true,
        first: 2,
        second: "hello".to_string(),
    };

    let value: Value = instance.structure();

    assert_eq!(
        value,
        Value::Record(
            vec![Attr::of("MyStruct"), Attr::of(("in_attr", true))],
            vec![Item::slot("first", 2), Item::slot("second", "hello"),]
        )
    );

    validate(instance, 2, RecordBodyKind::MapLike, 2, 1);
}

#[test]
fn derive_struct_lift_header_body() {
    #[derive(StructuralWritable)]
    struct MyStruct {
        #[form(header_body)]
        in_header: bool,
        first: i32,
        second: String,
    }

    let instance = MyStruct {
        in_header: true,
        first: 2,
        second: "hello".to_string(),
    };

    let value: Value = instance.structure();

    assert_eq!(
        value,
        Value::Record(
            vec![Attr::of(("MyStruct", true))],
            vec![Item::slot("first", 2), Item::slot("second", "hello"),]
        )
    );

    validate(instance, 1, RecordBodyKind::MapLike, 2, 1);
}

#[test]
fn derive_struct_lift_header_slot() {
    #[derive(StructuralWritable)]
    struct MyStruct {
        #[form(header)]
        in_header_slot: bool,
        first: i32,
        second: String,
    }

    let instance = MyStruct {
        in_header_slot: true,
        first: 2,
        second: "hello".to_string(),
    };

    let value: Value = instance.structure();

    assert_eq!(
        value,
        Value::Record(
            vec![Attr::of((
                "MyStruct",
                Value::record(vec![Item::slot("in_header_slot", true)])
            ))],
            vec![Item::slot("first", 2), Item::slot("second", "hello"),]
        )
    );

    validate(instance, 1, RecordBodyKind::MapLike, 2, 1);
}

#[test]
fn derive_struct_lift_header_slots() {
    #[derive(StructuralWritable)]
    struct MyStruct {
        #[form(header)]
        node: String,
        #[form(header)]
        lane: String,
        first: i32,
        second: String,
    }

    let instance = MyStruct {
        node: "node_uri".to_string(),
        lane: "lane_uri".to_string(),
        first: 2,
        second: "hello".to_string(),
    };

    let value: Value = instance.structure();

    assert_eq!(
        value,
        Value::Record(
            vec![Attr::of((
                "MyStruct",
                Value::record(vec![
                    Item::slot("node", "node_uri"),
                    Item::slot("lane", "lane_uri")
                ])
            ))],
            vec![Item::slot("first", 2), Item::slot("second", "hello"),]
        )
    );

    validate(instance, 1, RecordBodyKind::MapLike, 2, 1);
}

#[test]
fn derive_struct_lift_complex_header() {
    #[derive(StructuralWritable)]
    struct MyStruct {
        #[form(header_body)]
        count: i32,
        #[form(header)]
        node: String,
        #[form(header)]
        lane: String,
        first: i32,
        second: String,
    }

    let instance = MyStruct {
        count: 4,
        node: "node_uri".to_string(),
        lane: "lane_uri".to_string(),
        first: 2,
        second: "hello".to_string(),
    };

    let value: Value = instance.structure();

    assert_eq!(
        value,
        Value::Record(
            vec![Attr::of((
                "MyStruct",
                Value::record(vec![
                    Item::of(4),
                    Item::slot("node", "node_uri"),
                    Item::slot("lane", "lane_uri")
                ])
            ))],
            vec![Item::slot("first", 2), Item::slot("second", "hello"),]
        )
    );

    validate(instance, 1, RecordBodyKind::MapLike, 2, 1);
}

#[test]
fn derive_struct_rename_slot() {
    #[derive(StructuralWritable)]
    struct MyStruct {
        #[form(name = "renamed")]
        first: i32,
        second: String,
    }

    let instance = MyStruct {
        first: 2,
        second: "hello".to_string(),
    };

    let value: Value = instance.structure();

    assert_eq!(
        value,
        Value::Record(
            vec![Attr::of("MyStruct")],
            vec![Item::slot("renamed", 2), Item::slot("second", "hello"),]
        )
    );

    validate(instance, 1, RecordBodyKind::MapLike, 2, 1);
}

#[test]
fn derive_struct_rename_tuple_values() {
    #[derive(StructuralWritable)]
    struct MyStruct(#[form(name = "first")] i32, #[form(name = "second")] String);

    let instance = MyStruct(2, "hello".to_string());

    let value: Value = instance.structure();

    assert_eq!(
        value,
        Value::Record(
            vec![Attr::of("MyStruct")],
            vec![Item::slot("first", 2), Item::slot("second", "hello"),]
        )
    );

    validate(instance, 1, RecordBodyKind::MapLike, 2, 1);
}

#[test]
fn derive_struct_rename_tag() {
    #[derive(StructuralWritable)]
    #[form(tag = "Renamed")]
    struct MyStruct {
        first: i32,
        second: String,
    }

    let instance = MyStruct {
        first: 2,
        second: "hello".to_string(),
    };

    let value: Value = instance.structure();

    assert_eq!(
        value,
        Value::Record(
            vec![Attr::of("Renamed")],
            vec![Item::slot("first", 2), Item::slot("second", "hello"),]
        )
    );

    validate(instance, 1, RecordBodyKind::MapLike, 2, 1);
}

#[test]
fn derive_struct_tag_from_field() {
    #[derive(StructuralWritable)]
    struct MyStruct {
        first: i32,
        #[form(tag)]
        second: String,
    }

    let instance = MyStruct {
        first: 2,
        second: "Hello".to_string(),
    };

    let value: Value = instance.structure();

    assert_eq!(
        value,
        Value::Record(vec![Attr::of("Hello")], vec![Item::slot("first", 2),])
    );

    validate(instance, 1, RecordBodyKind::MapLike, 1, 1);
}

#[test]
fn derive_nested_structs() {
    #[derive(StructuralWritable)]
    struct Inner {
        first: i32,
        second: String,
    }

    #[derive(StructuralWritable)]
    struct Outer {
        inner: Inner,
    }

    let instance = Outer {
        inner: Inner {
            first: 13,
            second: "stuff".to_string(),
        },
    };

    let value: Value = instance.structure();

    let expected_inner = Value::Record(
        vec![Attr::of("Inner")],
        vec![Item::slot("first", 13), Item::slot("second", "stuff")],
    );

    assert_eq!(
        value,
        Value::Record(
            vec![Attr::of("Outer")],
            vec![Item::slot("inner", expected_inner),]
        )
    );

    validate(instance, 1, RecordBodyKind::MapLike, 1, 1);
}

#[test]
fn derive_struct_simple_body_replacement() {
    #[derive(StructuralWritable)]
    struct MyStruct {
        first: i32,
        #[form(body)]
        second: String,
    }

    let instance = MyStruct {
        first: 2,
        second: "Hello".to_string(),
    };

    let value: Value = instance.structure();

    assert_eq!(
        value,
        Value::Record(
            vec![Attr::of((
                "MyStruct",
                Value::record(vec![Item::slot("first", 2)])
            ))],
            vec![Item::of("Hello")]
        )
    );

    let validator = Validator::default();
    let EarlyTerm(val) = instance.write_with(validator).err().unwrap();
    assert_eq!(val.depth, 1);
    assert_eq!(val.num_attrs, 1);

    let validator = Validator::default();
    let EarlyTerm(val) = instance.write_into(validator).err().unwrap();
    assert_eq!(val.depth, 1);
    assert_eq!(val.num_attrs, 1);
}

#[test]
fn derive_struct_complex_body_replacement() {
    #[derive(StructuralWritable)]
    struct Inner {
        first: i32,
        second: String,
    }

    #[derive(StructuralWritable)]
    struct Outer {
        node: String,
        #[form(body)]
        inner: Inner,
    }

    let instance = Outer {
        node: "node_uri".to_string(),
        inner: Inner {
            first: 34,
            second: "stuff".to_string(),
        },
    };

    let value: Value = instance.structure();

    assert_eq!(
        value,
        Value::Record(
            vec![
                Attr::of(("Outer", Value::record(vec![Item::slot("node", "node_uri")]))),
                Attr::of("Inner"),
            ],
            vec![Item::slot("first", 34), Item::slot("second", "stuff"),]
        )
    );

    validate(instance, 2, RecordBodyKind::MapLike, 2, 2);
}

#[test]
fn derive_unit_enum_variant() {
    #[derive(StructuralWritable)]
    enum UnitEnum {
        Variant0,
    }

    let unit = UnitEnum::Variant0;

    let value: Value = unit.structure();

    assert_eq!(value, Value::of_attr("Variant0"));

    validate(unit, 1, RecordBodyKind::ArrayLike, 0, 1);
}

#[test]
fn derive_labelled_enum_variant() {
    #[derive(StructuralWritable)]
    enum LabelledEnum {
        Variant1 { first: String, second: i64 },
    }

    let instance = LabelledEnum::Variant1 {
        first: "hello".to_string(),
        second: 5,
    };

    let value: Value = instance.structure();

    assert_eq!(
        value,
        Value::Record(
            vec![Attr::of("Variant1")],
            vec![Item::slot("first", "hello"), Item::slot("second", 5i64),]
        )
    );

    validate(instance, 1, RecordBodyKind::MapLike, 2, 1);
}

#[test]
fn derive_tuple_enum_variant() {
    #[derive(StructuralWritable)]
    enum TupleEnum {
        Variant2(String, i64),
    }

    let instance = TupleEnum::Variant2("hello".to_string(), 5);

    let value: Value = instance.structure();

    assert_eq!(
        value,
        Value::Record(
            vec![Attr::of("Variant2")],
            vec![Item::of("hello"), Item::of(5i64),]
        )
    );

    validate(instance, 1, RecordBodyKind::ArrayLike, 2, 1);
}

#[test]
fn derive_mixed_enum_type() {
    #[derive(StructuralWritable)]
    enum MixedEnum {
        Variant0,
        Variant1 { first: String, second: i64 },
        Variant2(String, i64),
    }

    let unit = MixedEnum::Variant0;

    let value: Value = unit.structure();

    assert_eq!(value, Value::of_attr("Variant0"));

    validate(unit, 1, RecordBodyKind::ArrayLike, 0, 1);

    let instance = MixedEnum::Variant1 {
        first: "hello".to_string(),
        second: 5,
    };

    let value: Value = instance.structure();

    assert_eq!(
        value,
        Value::Record(
            vec![Attr::of("Variant1")],
            vec![Item::slot("first", "hello"), Item::slot("second", 5i64),]
        )
    );

    validate(instance, 1, RecordBodyKind::MapLike, 2, 1);

    let instance = MixedEnum::Variant2("hello".to_string(), 5);

    let value: Value = instance.structure();

    assert_eq!(
        value,
        Value::Record(
            vec![Attr::of("Variant2")],
            vec![Item::of("hello"), Item::of(5i64),]
        )
    );

    validate(instance, 1, RecordBodyKind::ArrayLike, 2, 1);
}

#[test]
fn derive_delegated_enum_type() {
    #[derive(StructuralWritable)]
    struct Inner {
        value: i32,
    }

    #[derive(StructuralWritable)]
    enum MixedEnum {
        Variant1 {
            first: String,
            #[form(body)]
            second: Inner,
        },
        Variant2(#[form(name = "first")] String, #[form(body)] Inner),
    }

    let instance = MixedEnum::Variant1 {
        first: "hello".to_string(),
        second: Inner { value: 5 },
    };

    let value: Value = instance.structure();

    assert_eq!(
        value,
        Value::Record(
            vec![
                Attr::of((
                    "Variant1",
                    Value::record(vec![Item::slot("first", "hello")])
                )),
                Attr::of("Inner"),
            ],
            vec![Item::slot("value", 5i32),]
        )
    );

    validate(instance, 2, RecordBodyKind::MapLike, 1, 2);

    let instance = MixedEnum::Variant2("hello".to_string(), Inner { value: 6 });

    let value: Value = instance.structure();

    assert_eq!(
        value,
        Value::Record(
            vec![
                Attr::of((
                    "Variant2",
                    Value::record(vec![Item::slot("first", "hello")])
                )),
                Attr::of("Inner"),
            ],
            vec![Item::slot("value", 6i32),]
        )
    );

    validate(instance, 2, RecordBodyKind::MapLike, 1, 2);
}

#[test]
fn derive_skipped_field_struct() {
    #[derive(StructuralWritable)]
    struct Skippy {
        present: i32,
        #[form(skip)]
        skipped: String,
    }

    let skippy = Skippy {
        present: 2,
        skipped: "hello".to_string(),
    };

    let value: Value = skippy.structure();

    assert_eq!(
        value,
        Value::Record(vec![Attr::of("Skippy")], vec![Item::slot("present", 2)])
    );

    validate(skippy, 1, RecordBodyKind::MapLike, 1, 1);
}

#[test]
fn derive_skipped_field_tuple_struct() {
    #[derive(StructuralWritable)]
    struct Skippy(i32, #[form(skip)] String);

    let skippy = Skippy(2, "hello".to_string());

    let value: Value = skippy.structure();

    assert_eq!(
        value,
        Value::Record(vec![Attr::of("Skippy")], vec![Item::of(2)])
    );

    validate(skippy, 1, RecordBodyKind::ArrayLike, 1, 1);
}

#[test]
fn derive_two_field_generic_struct() {
    #[derive(StructuralWritable)]
    struct TwoFields<S, T> {
        first: S,
        second: T,
    }

    let two_fields = TwoFields {
        first: 2,
        second: "hello".to_string(),
    };

    let value: Value = two_fields.structure();

    assert_eq!(
        value,
        Value::Record(
            vec![Attr::of("TwoFields")],
            vec![Item::slot("first", 2), Item::slot("second", "hello"),]
        )
    );

    validate(two_fields, 1, RecordBodyKind::MapLike, 2, 1);
}

#[test]
fn derive_generic_enum() {
    #[derive(StructuralWritable)]
    enum Mixed<S, T> {
        Variant0,
        Variant1 { value: S },
        Variant2(T),
    }

    let unit: Mixed<i32, String> = Mixed::Variant0;

    let value: Value = unit.structure();

    assert_eq!(value, Value::of_attr("Variant0"));

    validate(unit, 1, RecordBodyKind::ArrayLike, 0, 1);

    let instance: Mixed<i32, String> = Mixed::Variant1 { value: 5 };

    let value: Value = instance.structure();

    assert_eq!(
        value,
        Value::Record(vec![Attr::of("Variant1")], vec![Item::slot("value", 5i32),])
    );

    validate(instance, 1, RecordBodyKind::MapLike, 1, 1);

    let instance: Mixed<i32, String> = Mixed::Variant2("hello".to_string());

    let value: Value = instance.structure();

    assert_eq!(
        value,
        Value::Record(vec![Attr::of("Variant2")], vec![Item::of("hello")])
    );

    validate(instance, 1, RecordBodyKind::ArrayLike, 1, 1);
}
