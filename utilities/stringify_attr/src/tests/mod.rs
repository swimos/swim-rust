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

#![allow(non_snake_case, non_camel_case_types)]

use serde::Serialize;
use trybuild::TestCases;

#[test]
fn test_derive() {
    let t = TestCases::new();

    t.compile_fail("src/tests/derive/*.rs");
}

#[test]
fn struct_field_rename() {
    macro_rules! t {
        ($name :ident, $field_name:ident) => {
            #[stringify_attr]
            #[derive(Serialize)]
            struct $name {
                #[stringify(serde(rename($field_name)))]
                $field_name: i32,
            }
        };
    }

    t!(A, B);

    let result = serde_json::to_string(&A { B: 1 });
    assert_eq!(result.unwrap(), "{\"B\":1}".to_string());
}

#[test]
fn struct_field_multiple_alias() {
    macro_rules! t {
        ($name :ident, $field_name:ident, $($alias:ident),*) => {
            #[stringify_attr]
            #[derive(Serialize)]
            struct $name {
                #[stringify(serde(rename($field_name)))]
                $(#[stringify(serde(alias($alias)))])*
                $field_name: i32,
            }
        };
    }

    t!(A, B, C, D, E);

    let result = serde_json::to_string(&A { B: 1 });
    assert_eq!(result.unwrap(), "{\"B\":1}".to_string());
}

#[test]
fn struct_field_rename_raw() {
    macro_rules! t {
        ($name :ident, $field_name:ident, $alias:ident) => {
            #[stringify_attr]
            #[derive(Serialize)]
            struct $name {
                #[stringify_raw(
                    path = "serde",
                    in(rename($field_name), alias($alias)),
                    raw(alias = "C")
                )]
                $field_name: i32,
            }
        };
    }

    t!(A, B, C);

    let result = serde_json::to_string(&A { B: 1 });
    assert_eq!(result.unwrap(), "{\"B\":1}".to_string());
}

#[test]
fn struct_container_attr() {
    macro_rules! t {
        ($name :ident, $field_name:ident, $style:ident) => {
            #[stringify_attr(serde(rename_all($style)))]
            #[derive(Serialize)]
            struct $name {
                $field_name: i32,
            }
        };
    }

    t!(Structure, somefieldinlowercase, UPPERCASE);

    let result = serde_json::to_string(&Structure {
        somefieldinlowercase: 0,
    });
    assert_eq!(result.unwrap(), "{\"SOMEFIELDINLOWERCASE\":0}".to_string());
}

#[test]
fn enum_field_rename() {
    macro_rules! t {
        ($name :ident, $field_name:ident) => {
            #[stringify_attr]
            #[derive(Serialize)]
            enum $name {
                A {
                    #[stringify(serde(rename($field_name)))]
                    $field_name: i32,
                },
            }
        };
    }

    t!(A, B);

    let result = serde_json::to_string(&A::A { B: 0 });
    assert_eq!(result.unwrap(), "{\"A\":{\"B\":0}}".to_string());
}

#[test]
fn enum_field_rename_raw() {
    macro_rules! t {
        ($name :ident, $field_name:ident) => {
            #[stringify_attr]
            #[derive(Serialize)]
            enum $name {
                A {
                    #[stringify_raw(path = "serde", in(rename($field_name)), raw(alias = "C"))]
                    $field_name: i32,
                },
            }
        };
    }

    t!(A, B);

    let result = serde_json::to_string(&A::A { B: 0 });
    assert_eq!(result.unwrap(), "{\"A\":{\"B\":0}}".to_string());
}

#[test]
fn enum_variant_rename_raw() {
    macro_rules! t {
        ($name :ident, $variant_name:ident) => {
            #[stringify_attr]
            #[derive(Serialize)]
            enum $name {
                #[stringify_raw(path = "serde", in(rename($variant_name)), raw(alias = "C"))]
                A { a: i32 },
            }
        };
    }

    t!(A, A);

    let result = serde_json::to_string(&A::A { a: 0 });
    assert_eq!(result.unwrap(), "{\"A\":{\"a\":0}}".to_string());
}

#[test]
fn enum_variant_rename() {
    macro_rules! t {
        ($name :ident, $variant_name:ident) => {
            #[stringify_attr]
            #[derive(Serialize)]
            enum $name {
                #[stringify(serde(rename($variant_name)))]
                A { a: i32 },
            }
        };
    }

    t!(A, A);

    let result = serde_json::to_string(&A::A { a: 0 });
    assert_eq!(result.unwrap(), "{\"A\":{\"a\":0}}".to_string());
}

#[test]
fn full_struct() {
    macro_rules! t {
        ($name:ident, $field1:ident, $field2:ident) => {
            #[stringify_attr]
            #[derive(Serialize)]
            struct $name {
                #[stringify_raw(
                    path = "serde",
                    in(rename($field1), alias($field1)),
                    raw(alias = "field1")
                )]
                $field1: i32,
                #[stringify(serde(rename($field2), alias($field2)))]
                $field2: i32,
            }
        };
    }

    t!(StructA, FieldA, FieldB);

    let result = serde_json::to_string(&StructA {
        FieldA: 1,
        FieldB: 2,
    });
    assert_eq!(result.unwrap(), "{\"FieldA\":1,\"FieldB\":2}".to_string());
}

#[test]
fn full_enum() {
    macro_rules! t {
        ($enum_name:ident, $rename_a:ident, $rename_b:ident, $rename_c:ident, $rename_d_variant:ident, $rename_d_field:ident) => {
            #[stringify_attr(serde(rename(Vibe)))]
            #[derive(Serialize)]
            enum $enum_name {
                #[stringify(serde(rename($rename_a)))]
                A,
                #[stringify_raw(
                    path = "serde",
                    in(rename($rename_b), alias($rename_b)),
                    raw(alias = "B")
                )]
                B(i32),
                C(
                    #[stringify_raw(
                        path = "serde",
                        in(rename($rename_c), alias($rename_c)),
                        raw(alias = "B")
                    )]
                    i32,
                ),
                #[stringify_raw(
                    path = "serde",
                    in(rename($rename_d_variant), alias($rename_d_variant)),
                    raw(alias = "B")
                )]
                D {
                    #[stringify_raw(
                        path = "serde",
                        in(rename($rename_d_field), alias($rename_d_field)),
                        raw(alias = "B")
                    )]
                    a: i32,
                },
            }
        };
    }

    t!(EnumA, VariantA, VariantB, FieldC, VariantD, FieldD);

    let a = EnumA::A;
    assert_eq!(
        serde_json::to_string(&a).unwrap(),
        "\"VariantA\"".to_string()
    );

    let b = EnumA::B(1);
    assert_eq!(
        serde_json::to_string(&b).unwrap(),
        "{\"VariantB\":1}".to_string()
    );

    let c = EnumA::C(2);
    assert_eq!(serde_json::to_string(&c).unwrap(), "{\"C\":2}".to_string());

    let d = EnumA::D { a: 3 };
    assert_eq!(
        serde_json::to_string(&d).unwrap(),
        "{\"VariantD\":{\"FieldD\":3}}".to_string()
    );
}

#[test]
fn generic_impl() {
    macro_rules! t {
        ($name:ident, $field:ident) => {
            #[stringify_attr]
            #[derive(Serialize)]
            struct $name<$field> {
                #[stringify(serde(rename($field), alias($field)))]
                key: $field,
            }
        };
    }

    t!(Structure, A);

    let s = Structure { key: 1 };
    let result = serde_json::to_string(&s);
    assert_eq!(result.unwrap(), "{\"A\":1}".to_string());
}
