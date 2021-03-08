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

use crate::validated_form::range::{parse_range_str, Range};
use crate::validated_form::vf_parser::{
    StandardSchema, ALL_ITEMS_PATH, AND_PATH, ANYTHING_PATH, BIG_INT_RANGE_PATH, EQUAL_PATH,
    FINITE_PATH, FLOAT_RANGE_PATH, INT_RANGE_PATH, NON_NAN_PATH, NOTHING_PATH, NOT_PATH,
    NUM_ATTRS_PATH, NUM_ITEMS_PATH, OF_KIND_PATH, OR_PATH, TEXT_PATH, UINT_RANGE_PATH,
};
use macro_helpers::{lit_str_to_expr_path, Context};
use quote::ToTokens;
use std::fmt::Display;
use std::str::FromStr;
use syn::punctuated::Punctuated;
#[allow(unused_imports)]
use syn::token::Token;
use syn::{Lit, Meta, NestedMeta};

/// Parses a literal into a `usize`. If the literal is not an integer or the integer is invalid,
/// then an error is added to the `Context` and `Option::None` is returned.
fn parse_lit_to_int(lit: &Lit, context: &mut Context) -> Option<usize> {
    match lit {
        Lit::Int(int) => match int.base10_parse::<usize>() {
            Ok(int) => Some(int),
            Err(e) => {
                context.error_spanned_by(int, e.to_string());
                None
            }
        },
        _ => {
            context.error_spanned_by(lit, "Expected an int literal");
            None
        }
    }
}

/// Builds a range from a string literal. If the literal is not a string or the string is not in the
/// valid range syntax then an error is added to the `Context` and `Option::None` is returned.
fn build_range<T, E>(lit: &Lit, context: &mut Context) -> Option<Range<T>>
where
    T: FromStr<Err = E> + Default,
    E: Display,
{
    match lit {
        Lit::Str(str) => match parse_range_str::<T, _>(&str.value()) {
            Ok(range) => Some(range),
            Err(e) => {
                context.error_spanned_by(
                    str,
                    &format!("Failed to parse range. {} at index {}", e.0, e.1),
                );
                None
            }
        },
        _ => {
            context.error_spanned_by(lit, "Expected a String literal");
            None
        }
    }
}

/// Parses the punctuated sequence of attributes and derives a `StandardSchema`. For nested
/// attributes this function recursively called.
///
/// * `default_schema` - is the *starting* schema. This should be `StandardSchema::None`.
/// * `context` - the current `Context`. Any errors encountered will be added to this.
/// * `nested` - a punctuated sequence of attributes.
pub fn parse_schema_meta(
    default_schema: StandardSchema,
    context: &mut Context,
    nested: &Punctuated<NestedMeta, Token![,]>,
) -> StandardSchema {
    nested
        .iter()
        .fold(
            (default_schema, false),
            |(mut schema, mut schema_applied), meta| {
                // If the current schema is an operator (and, or, all_items) then the schema is
                // pushed into the vector of schemas. The first time this function is called, the
                // current schema is changed to the derived schema.
                let mut push_element =
                    |elem, borrowed_schema: &mut StandardSchema, context: &mut Context| {
                        match borrowed_schema {
                            StandardSchema::And(vec) => vec.push(elem),
                            StandardSchema::Or(vec) => vec.push(elem),
                            StandardSchema::AllItems(boxed) => *boxed = Box::new(elem),
                            StandardSchema::Not(boxed) => *boxed = Box::new(elem),
                            _ => {
                                // If a schema has already been derived and we're not reading
                                // a schema that is an operator, then the format of the attributes
                                // is incorrect.
                                if schema_applied {
                                    context.error_spanned_by(
                                        meta,
                                        "Use schema operators for chained schemas",
                                    )
                                } else {
                                    schema_applied = true;
                                    *borrowed_schema = elem;
                                }
                            }
                        }
                    };

                match meta {
                    // [`#[form(schema(int_range = "0..=10"))]`
                    NestedMeta::Meta(Meta::NameValue(name)) if name.path == INT_RANGE_PATH => {
                        if let Some(range) = build_range(&name.lit, context) {
                            push_element(StandardSchema::IntRange(range), &mut schema, context);
                        }
                    }
                    // [`#[form(schema(uint_range = "0..=10"))]`
                    NestedMeta::Meta(Meta::NameValue(name)) if name.path == UINT_RANGE_PATH => {
                        if let Some(range) = build_range(&name.lit, context) {
                            push_element(StandardSchema::UintRange(range), &mut schema, context);
                        }
                    }
                    // [`#[form(schema(float_range = "0.1..=10.1"))]`
                    NestedMeta::Meta(Meta::NameValue(name)) if name.path == FLOAT_RANGE_PATH => {
                        if let Some(range) = build_range(&name.lit, context) {
                            push_element(StandardSchema::FloatRange(range), &mut schema, context);
                        }
                    }
                    // [`#[form(schema(big_int_range = "10000..=200000000"))]`
                    NestedMeta::Meta(Meta::NameValue(name)) if name.path == BIG_INT_RANGE_PATH => {
                        if let Some(range) = build_range(&name.lit, context) {
                            push_element(StandardSchema::BigIntRange(range), &mut schema, context);
                        }
                    }
                    // [`#[form(schema(anything))]`
                    NestedMeta::Meta(Meta::Path(path)) if path == ANYTHING_PATH => {
                        push_element(StandardSchema::Anything, &mut schema, context);
                    }
                    // [`#[form(schema(nothing))]`
                    NestedMeta::Meta(Meta::Path(path)) if path == NOTHING_PATH => {
                        context.error_spanned_by(
                            path,
                            "StandardSchema::Nothing is not permitted on fields",
                        );
                    }
                    // [`#[form(schema(non_nan))]`
                    NestedMeta::Meta(Meta::Path(path)) if path == NON_NAN_PATH => {
                        push_element(StandardSchema::NonNan, &mut schema, context);
                    }
                    // [`#[form(schema(finite))]`
                    NestedMeta::Meta(Meta::Path(path)) if path == FINITE_PATH => {
                        push_element(StandardSchema::Finite, &mut schema, context);
                    }
                    // [`#[form(schema(text = "value"))]`
                    NestedMeta::Meta(Meta::NameValue(name)) if name.path == TEXT_PATH => {
                        match &name.lit {
                            Lit::Str(str) => {
                                push_element(
                                    StandardSchema::Text(str.value()),
                                    &mut schema,
                                    context,
                                );
                            }
                            lit => {
                                context.error_spanned_by(lit, "Expected a string literal");
                            }
                        }
                    }
                    // [`#[form(schema(num_attrs = no))]`
                    NestedMeta::Meta(Meta::NameValue(name)) if name.path == NUM_ATTRS_PATH => {
                        if let Some(int) = parse_lit_to_int(&name.lit, context) {
                            push_element(StandardSchema::NumAttrs(int), &mut schema, context);
                        }
                    }
                    // [`#[form(schema(num_items = no))]`
                    NestedMeta::Meta(Meta::NameValue(name)) if name.path == NUM_ITEMS_PATH => {
                        if let Some(int) = parse_lit_to_int(&name.lit, context) {
                            push_element(StandardSchema::NumItems(int), &mut schema, context);
                        }
                    }
                    // [`#[form(schema(of_kind(ValueKind::Int32))]`
                    //
                    // To use the attribute in this way (`Ident::Variant`) requires the metadata to
                    // be in a list format.
                    NestedMeta::Meta(Meta::List(list)) if list.path == OF_KIND_PATH => {
                        if list.nested.len() != 1 {
                            context.error_spanned_by(list, "Only one argument may be provided");
                        } else {
                            push_element(
                                StandardSchema::OfKind(list.nested.to_token_stream()),
                                &mut schema,
                                context,
                            );
                        }
                    }
                    // [`#[form(schema(equal = "fn")]`
                    NestedMeta::Meta(Meta::NameValue(name)) if name.path == EQUAL_PATH => {
                        if let Ok(path) = lit_str_to_expr_path(context, &name.lit) {
                            push_element(StandardSchema::Equal(path), &mut schema, context);
                        }
                    }
                    // [`#[form(schema(and(schema1, schema2))]`
                    NestedMeta::Meta(Meta::List(list)) if list.path == AND_PATH => {
                        if list.nested.len() < 2 {
                            context.error_spanned_by(
                                list,
                                "At least two arguments must be provided to an AND operator",
                            );
                        } else {
                            let s = parse_schema_meta(
                                StandardSchema::And(Vec::new()),
                                context,
                                &list.nested,
                            );
                            push_element(s, &mut schema, context);
                        }
                    }
                    // [`#[form(schema(or(schema1, schema2))]`
                    NestedMeta::Meta(Meta::List(list)) if list.path == OR_PATH => {
                        if list.nested.len() < 2 {
                            context.error_spanned_by(
                                list,
                                "At least two arguments must be provided to an OR operator",
                            );
                        } else {
                            push_element(
                                parse_schema_meta(
                                    StandardSchema::Or(Vec::new()),
                                    context,
                                    &list.nested,
                                ),
                                &mut schema,
                                context,
                            );
                        }
                    }
                    // [`#[form(schema(not(schema))]`
                    NestedMeta::Meta(Meta::List(list)) if list.path == NOT_PATH => {
                        if list.nested.len() > 1 {
                            context.error_spanned_by(
                                list,
                                "Only one argument is permitted in a NOT operator",
                            )
                        } else {
                            push_element(
                                parse_schema_meta(
                                    StandardSchema::Not(Box::new(StandardSchema::None)),
                                    context,
                                    &list.nested,
                                ),
                                &mut schema,
                                context,
                            );
                        }
                    }
                    // [`#[form(schema(all_items(schema))]`
                    NestedMeta::Meta(Meta::List(list)) if list.path == ALL_ITEMS_PATH => {
                        //
                        if list.nested.len() > 1 {
                            context.error_spanned_by(
                                list,
                                "Only one argument is permitted as the schema for all items",
                            )
                        } else {
                            push_element(
                                parse_schema_meta(
                                    StandardSchema::AllItems(Box::new(StandardSchema::None)),
                                    context,
                                    &list.nested,
                                ),
                                &mut schema,
                                context,
                            );
                        }
                    }
                    meta => context.error_spanned_by(meta, "Unknown schema"),
                }

                (schema, schema_applied)
            },
        )
        .0
}
