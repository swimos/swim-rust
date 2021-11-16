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

use std::sync::Arc;
use swim_form::structural::read::StructuralReadable;
use swim_form::structural::Tag;
use swim_model::Value;
use swim_recon::parser::{parse_recognize, Span};

fn run_recognizer<T: StructuralReadable>(rep: &str) -> T {
    let span = Span::new(rep);
    parse_recognize(span, false).unwrap()
}

#[test]
fn derive_unit_struct() {
    #[derive(StructuralReadable)]
    struct Unit;

    run_recognizer::<Unit>("@Unit");
}

#[test]
fn derive_simple_struct() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    struct Simple {
        first: i32,
    }

    let instance = run_recognizer::<Simple>("@Simple { first: 12 }");
    assert_eq!(instance, Simple { first: 12 });
}

#[test]
fn derive_two_field_struct() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    struct TwoFields {
        first: i32,
        second: String,
    }

    let instance = run_recognizer::<TwoFields>("@TwoFields { first: 12, second: hello }");
    assert_eq!(
        instance,
        TwoFields {
            first: 12,
            second: "hello".to_string()
        }
    );

    let instance = run_recognizer::<TwoFields>("@TwoFields { second: hello, first: 12 }");
    assert_eq!(
        instance,
        TwoFields {
            first: 12,
            second: "hello".to_string()
        }
    );
}

#[test]
fn derive_two_field_tuple_struct() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    struct TwoFields(i32, String);

    let instance = run_recognizer::<TwoFields>("@TwoFields { 12, hello }");
    assert_eq!(instance, TwoFields(12, "hello".to_string()));
}

#[test]
fn derive_struct_lift_attr() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    struct MyStruct {
        #[form(attr)]
        in_attr: bool,
        first: i32,
        second: String,
    }

    let instance =
        run_recognizer::<MyStruct>("@MyStruct @in_attr(true) { first: -34, second: \"name\" }");
    assert_eq!(
        instance,
        MyStruct {
            in_attr: true,
            first: -34,
            second: "name".to_string(),
        }
    );
}

#[test]
fn derive_struct_lift_header_body() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    struct MyStruct {
        #[form(header_body)]
        in_header: bool,
        first: i32,
        second: String,
    }

    let instance = run_recognizer::<MyStruct>("@MyStruct(false) { first: -34, second: \"name\" }");
    assert_eq!(
        instance,
        MyStruct {
            in_header: false,
            first: -34,
            second: "name".to_string(),
        }
    );
}

#[test]
fn derive_struct_lift_header_body_complex() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    struct MyStruct {
        #[form(header_body)]
        in_header: Vec<bool>,
        first: i32,
        second: String,
    }

    let instance =
        run_recognizer::<MyStruct>("@MyStruct({false, true}) { first: -34, second: \"name\" }");
    assert_eq!(
        instance,
        MyStruct {
            in_header: vec![false, true],
            first: -34,
            second: "name".to_string(),
        }
    );

    let instance =
        run_recognizer::<MyStruct>("@MyStruct(false, true) { first: -34, second: \"name\" }");
    assert_eq!(
        instance,
        MyStruct {
            in_header: vec![false, true],
            first: -34,
            second: "name".to_string(),
        }
    );
}

#[test]
fn derive_struct_lift_header_slot() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    struct MyStruct {
        #[form(header)]
        in_header_slot: bool,
        first: i32,
        second: String,
    }

    let instance = run_recognizer::<MyStruct>(
        "@MyStruct(in_header_slot: true) { first: -34, second: \"name\" }",
    );
    assert_eq!(
        instance,
        MyStruct {
            in_header_slot: true,
            first: -34,
            second: "name".to_string(),
        }
    );
}

#[test]
fn derive_struct_lift_header_slots() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    struct MyStruct {
        #[form(header)]
        node: String,
        #[form(header)]
        lane: String,
        first: i32,
        second: String,
    }

    let instance = run_recognizer::<MyStruct>(
        "@MyStruct(node: node_uri, lane: lane_uri) { first: -34, second: \"name\" }",
    );
    assert_eq!(
        instance,
        MyStruct {
            node: "node_uri".to_string(),
            lane: "lane_uri".to_string(),
            first: -34,
            second: "name".to_string(),
        }
    );

    let instance = run_recognizer::<MyStruct>(
        "@MyStruct(lane: lane_uri, node: node_uri) { first: -34, second: \"name\" }",
    );
    assert_eq!(
        instance,
        MyStruct {
            node: "node_uri".to_string(),
            lane: "lane_uri".to_string(),
            first: -34,
            second: "name".to_string(),
        }
    );

    let instance = run_recognizer::<MyStruct>(
        "@MyStruct({lane: lane_uri, node: node_uri}) { first: -34, second: \"name\" }",
    );
    assert_eq!(
        instance,
        MyStruct {
            node: "node_uri".to_string(),
            lane: "lane_uri".to_string(),
            first: -34,
            second: "name".to_string(),
        }
    );
}

#[test]
fn derive_struct_lift_complex_header() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
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

    let instance = run_recognizer::<MyStruct>(
        "@MyStruct(6, node: node_uri, lane: lane_uri) { first: -34, second: \"name\" }",
    );
    assert_eq!(
        instance,
        MyStruct {
            count: 6,
            node: "node_uri".to_string(),
            lane: "lane_uri".to_string(),
            first: -34,
            second: "name".to_string(),
        }
    );

    let instance = run_recognizer::<MyStruct>(
        "@MyStruct(6, lane: lane_uri, node: node_uri) { first: -34, second: \"name\" }",
    );
    assert_eq!(
        instance,
        MyStruct {
            count: 6,
            node: "node_uri".to_string(),
            lane: "lane_uri".to_string(),
            first: -34,
            second: "name".to_string(),
        }
    );

    let instance = run_recognizer::<MyStruct>(
        "@MyStruct({6, lane: lane_uri, node: node_uri}) { first: -34, second: \"name\" }",
    );
    assert_eq!(
        instance,
        MyStruct {
            count: 6,
            node: "node_uri".to_string(),
            lane: "lane_uri".to_string(),
            first: -34,
            second: "name".to_string(),
        }
    );
}

#[test]
fn derive_struct_rename_slot() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    struct MyStruct {
        #[form(name = "renamed")]
        first: i32,
        second: String,
    }

    let instance = run_recognizer::<MyStruct>("@MyStruct { renamed: -34, second: \"name\" }");
    assert_eq!(
        instance,
        MyStruct {
            first: -34,
            second: "name".to_string(),
        }
    );
}

#[test]
fn derive_struct_rename_tuple_values() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    struct MyStruct(#[form(name = "first")] i32, #[form(name = "second")] String);

    let instance = run_recognizer::<MyStruct>("@MyStruct { first: -34, second: \"name\" }");
    assert_eq!(instance, MyStruct(-34, "name".to_string()));
}

#[test]
fn derive_struct_rename_tag() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    #[form(tag = "Renamed")]
    struct MyStruct {
        first: i32,
        second: String,
    }

    let instance = run_recognizer::<MyStruct>("@Renamed { first: -34, second: \"name\" }");
    assert_eq!(
        instance,
        MyStruct {
            first: -34,
            second: "name".to_string(),
        }
    );
}

#[derive(Tag, PartialEq, Eq, Debug)]
enum TagType {
    Tag,
    Other,
}

#[test]
fn derive_struct_tag_from_field() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    struct MyStruct {
        first: i32,
        #[form(tag)]
        second: TagType,
    }

    let instance = run_recognizer::<MyStruct>("@Tag { first: -34 }");
    assert_eq!(
        instance,
        MyStruct {
            first: -34,
            second: TagType::Tag,
        }
    );
}

#[test]
fn derive_nested_structs() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    struct Inner {
        first: i32,
        second: String,
    }

    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    struct Outer {
        inner: Inner,
    }

    let instance =
        run_recognizer::<Outer>("@Outer { inner: @Inner { first: 42, second: \"I'm inside!\"} }");
    assert_eq!(
        instance,
        Outer {
            inner: Inner {
                first: 42,
                second: "I'm inside!".to_string()
            }
        }
    );
}

#[test]
fn derive_struct_simple_body_replacement() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    struct MyStruct {
        first: i32,
        #[form(body)]
        second: String,
    }

    let instance = run_recognizer::<MyStruct>("@MyStruct(first: 10) \"content\"");
    assert_eq!(
        instance,
        MyStruct {
            first: 10,
            second: "content".to_string(),
        }
    );

    let instance = run_recognizer::<MyStruct>("@MyStruct(first: 10) { \"content\" }");
    assert_eq!(
        instance,
        MyStruct {
            first: 10,
            second: "content".to_string(),
        }
    );
}

#[test]
fn derive_struct_complex_body_replacement() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    struct Inner {
        first: i32,
        second: String,
    }

    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    struct Outer {
        node: String,
        #[form(body)]
        inner: Inner,
    }

    let instance =
        run_recognizer::<Outer>("@Outer(node: node_uri) @Inner { first: 1034, second: inside }");
    assert_eq!(
        instance,
        Outer {
            node: "node_uri".to_string(),
            inner: Inner {
                first: 1034,
                second: "inside".to_string(),
            }
        }
    );
}

#[test]
fn derive_unit_enum_variant() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    enum UnitEnum {
        Variant0,
    }

    let instance = run_recognizer::<UnitEnum>("@Variant0");

    assert_eq!(instance, UnitEnum::Variant0);
}

#[test]
fn derive_labelled_enum_variant() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    enum LabelledEnum {
        Variant1 { first: String, second: i64 },
    }

    let instance = run_recognizer::<LabelledEnum>("@Variant1 { first: hello, second: 67 }");

    assert_eq!(
        instance,
        LabelledEnum::Variant1 {
            first: "hello".to_string(),
            second: 67
        }
    );
}

#[test]
fn derive_tuple_enum_variant() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    enum TupleEnum {
        Variant2(String, i64),
    }

    let instance = run_recognizer::<TupleEnum>("@Variant2 { hello, 67 }");

    assert_eq!(instance, TupleEnum::Variant2("hello".to_string(), 67));
}

#[test]
fn derive_mixed_enum_type() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    enum MixedEnum {
        Variant0,
        Variant1 { first: String, second: i64 },
        Variant2(String, i64),
    }

    let instance = run_recognizer::<MixedEnum>("@Variant0");

    assert_eq!(instance, MixedEnum::Variant0);

    let instance = run_recognizer::<MixedEnum>("@Variant1 { first: hello, second: 67 }");

    assert_eq!(
        instance,
        MixedEnum::Variant1 {
            first: "hello".to_string(),
            second: 67
        }
    );

    let instance = run_recognizer::<MixedEnum>("@Variant2 { hello, 67 }");

    assert_eq!(instance, MixedEnum::Variant2("hello".to_string(), 67));
}

#[test]
fn derive_skipped_field_struct() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    struct Skippy {
        present: i32,
        #[form(skip)]
        skipped: String,
    }

    let instance = run_recognizer::<Skippy>("@Skippy { present: 12 }");
    assert_eq!(
        instance,
        Skippy {
            present: 12,
            skipped: "".to_string()
        }
    );
}

#[test]
fn derive_skipped_field_tuple_struct() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    struct Skippy(#[form(skip)] i32, String);

    let instance = run_recognizer::<Skippy>("@Skippy { hello }");
    assert_eq!(instance, Skippy(0, "hello".to_string()));
}

#[test]
fn derive_generic_two_field_struct() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    struct TwoFields<S, T> {
        first: S,
        second: T,
    }

    let instance =
        run_recognizer::<TwoFields<i32, String>>("@TwoFields { first: 12, second: hello }");
    assert_eq!(
        instance,
        TwoFields {
            first: 12,
            second: "hello".to_string()
        }
    );

    let instance =
        run_recognizer::<TwoFields<i32, String>>("@TwoFields { second: hello, first: 12 }");
    assert_eq!(
        instance,
        TwoFields {
            first: 12,
            second: "hello".to_string()
        }
    );
}

#[test]
fn derive_generic_enum() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    enum Mixed<S, T> {
        Variant0,
        Variant1 { value: S },
        Variant2(T),
    }

    let instance = run_recognizer::<Mixed<i32, String>>("@Variant0");
    assert_eq!(instance, Mixed::Variant0);

    let instance = run_recognizer::<Mixed<i32, String>>("@Variant1 { value: 45 }");
    assert_eq!(instance, Mixed::Variant1 { value: 45 });

    let instance = run_recognizer::<Mixed<i32, String>>("@Variant2 { content }");
    assert_eq!(instance, Mixed::Variant2("content".to_string()));
}

#[test]
fn derive_empty_enum() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    enum Empty {}

    let span = Span::new("");
    assert!(parse_recognize::<Empty>(span, false).is_err());
}

#[test]
fn derive_generic_header() {
    #[derive(StructuralReadable, PartialEq, Eq, Debug)]
    struct GenericHeader<S, T, U> {
        #[form(header)]
        first: S,
        #[form(header_body)]
        second: T,
        third: U,
    }

    let instance = run_recognizer::<GenericHeader<i32, String, i32>>(
        "@GenericHeader(hello, first: 12) { third: 6 }",
    );
    assert_eq!(
        instance,
        GenericHeader {
            first: 12,
            second: "hello".to_string(),
            third: 6,
        }
    );

    let instance = run_recognizer::<GenericHeader<i32, String, i32>>(
        "@GenericHeader({hello, first: 12}) { third: 6 }",
    );
    assert_eq!(
        instance,
        GenericHeader {
            first: 12,
            second: "hello".to_string(),
            third: 6,
        }
    );
}

#[test]
fn derive_map_update() {
    // The most complex derivation in the codebase, used here as a stress test.
    #[derive(StructuralReadable, Debug, PartialEq, Eq)]
    enum MapUpdate<K, V> {
        #[form(tag = "update")]
        Update(#[form(header, name = "key")] K, #[form(body)] Arc<V>),
        #[form(tag = "remove")]
        Remove(#[form(header, name = "key")] K),
        #[form(tag = "clear")]
        Clear,
    }

    type Upd = MapUpdate<String, Value>;

    let clear = run_recognizer::<Upd>("@clear");
    assert_eq!(clear, Upd::Clear);

    let remove = run_recognizer::<Upd>("@remove(key: thing)");
    assert_eq!(remove, Upd::Remove("thing".to_string()));
    let remove = run_recognizer::<Upd>("@remove({key: thing}) {}");
    assert_eq!(remove, Upd::Remove("thing".to_string()));

    let update = run_recognizer::<Upd>("@update(key: thing) 76");
    assert_eq!(
        update,
        Upd::Update("thing".to_string(), Arc::new(Value::Int32Value(76)))
    );
}

#[test]
fn optional_slot() {
    #[derive(StructuralReadable, Debug, PartialEq, Eq)]
    struct MyStruct {
        first: Option<i32>,
        second: String,
    }

    let instance = run_recognizer::<MyStruct>("@MyStruct { first:, second: Hello }");
    assert_eq!(
        instance,
        MyStruct {
            first: None,
            second: "Hello".to_string()
        }
    );

    let instance = run_recognizer::<MyStruct>("@MyStruct { second: Hello }");
    assert_eq!(
        instance,
        MyStruct {
            first: None,
            second: "Hello".to_string()
        }
    );

    let instance = run_recognizer::<MyStruct>("@MyStruct { first: 2, second: Hello }");
    assert_eq!(
        instance,
        MyStruct {
            first: Some(2),
            second: "Hello".to_string()
        }
    );
}

#[test]
fn optional_slot_in_header() {
    #[derive(StructuralReadable, Debug, PartialEq, Eq)]
    struct MyStruct {
        #[form(header)]
        first: Option<i32>,
        #[form(header)]
        second: String,
    }

    let instance = run_recognizer::<MyStruct>("@MyStruct(first:, second: Hello)");
    assert_eq!(
        instance,
        MyStruct {
            first: None,
            second: "Hello".to_string()
        }
    );

    let instance = run_recognizer::<MyStruct>("@MyStruct(second: Hello)");
    assert_eq!(
        instance,
        MyStruct {
            first: None,
            second: "Hello".to_string()
        }
    );

    let instance = run_recognizer::<MyStruct>("@MyStruct(first: 2, second: Hello)");
    assert_eq!(
        instance,
        MyStruct {
            first: Some(2),
            second: "Hello".to_string()
        }
    );
}
