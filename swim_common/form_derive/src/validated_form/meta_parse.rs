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

use crate::validated_form::vf_parser::{
    StandardSchema, ALL_ITEMS_PATH, AND_PATH, ANYTHING_PATH, BIG_INT_RANGE_PATH, EQUAL_PATH,
    FINITE_PATH, FLOAT_RANGE_PATH, INT_RANGE_PATH, NON_NAN_PATH, NOTHING_PATH, NOT_PATH,
    NUM_ATTRS_PATH, NUM_ITEMS_PATH, OF_KIND_PATH, OR_PATH, TEXT_PATH, UINT_RANGE_PATH,
};
use macro_helpers::{lit_str_to_expr_path, Context};
use quote::ToTokens;
use std::ops::Range;
use syn::punctuated::Punctuated;
#[allow(unused_imports)]
use syn::token::Token;
use syn::{Lit, Meta, NestedMeta};

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

macro_rules! build_std_range {
    ($nested:expr, $context:expr, $variant:ident, $target:ty, $type:expr) => {{
        let mut lower_opt = None;
        let mut upper_opt = None;
        let mut inclusive = false;

        $nested.iter().for_each(|n| match n {
            NestedMeta::Lit(Lit::$variant(int)) => match int.base10_parse::<$target>() {
                Ok(int) => {
                    if lower_opt.is_none() {
                        lower_opt = Some(int);
                    } else if upper_opt.is_none() {
                        upper_opt = Some(int);
                    } else {
                        $context
                            .error_spanned_by(int, "Expected only an upper and lower range bound");
                    }
                }
                Err(e) => $context.error_spanned_by(int, e),
            },
            NestedMeta::Lit(Lit::Bool(bool)) => inclusive = bool.value,
            _ => $context.error_spanned_by(n, format!("Expected a {} literal", $type)),
        });

        if lower_opt.is_none() || upper_opt.is_none() {
            $context.error_spanned_by(
                $nested,
                "Expected a range in the format of (lower, upper, inclusive (optional))",
            );
            None
        } else {
            Some((lower_opt.unwrap(), upper_opt.unwrap(), inclusive))
        }
    }};
}

fn build_big_int_range(
    context: &mut Context,
    nested: &Punctuated<NestedMeta, Token![,]>,
) -> Option<(Range<String>, bool)> {
    let mut lower_opt = None;
    let mut upper_opt = None;
    let mut inclusive = false;

    nested.iter().for_each(|n| match n {
        NestedMeta::Lit(Lit::Str(str)) => {
            if lower_opt.is_none() {
                lower_opt = Some(str.value());
            } else if upper_opt.is_none() {
                upper_opt = Some(str.value());
            } else {
                context.error_spanned_by(str, "Expected only an upper and lower range bound");
            }
        }
        NestedMeta::Lit(Lit::Bool(bool)) => inclusive = bool.value,
        _ => context.error_spanned_by(n, format!("Expected a String literal")),
    });

    Some((
        Range {
            start: lower_opt?,
            end: upper_opt?,
        },
        inclusive,
    ))
}

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
                let mut push_element =
                    |elem, borrowed_schema: &mut StandardSchema, context: &mut Context| {
                        match borrowed_schema {
                            StandardSchema::And(vec) => vec.push(elem),
                            StandardSchema::Or(vec) => vec.push(elem),
                            StandardSchema::AllItems(boxed) => *boxed = Box::new(elem),
                            StandardSchema::Not(boxed) => *boxed = Box::new(elem),
                            _ => {
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
                    NestedMeta::Meta(Meta::List(list)) if list.path == INT_RANGE_PATH => {
                        if let Some((start, end, inclusive)) =
                            build_std_range!(&list.nested, context, Int, i64, "integer")
                        {
                            push_element(
                                StandardSchema::IntRange((Range { start, end }, inclusive)),
                                &mut schema,
                                context,
                            );
                        }
                    }
                    NestedMeta::Meta(Meta::List(list)) if list.path == UINT_RANGE_PATH => {
                        if let Some((start, end, inclusive)) =
                            build_std_range!(&list.nested, context, Int, u64, "unsigned integer")
                        {
                            push_element(
                                StandardSchema::UintRange((Range { start, end }, inclusive)),
                                &mut schema,
                                context,
                            );
                        }
                    }
                    NestedMeta::Meta(Meta::List(list)) if list.path == FLOAT_RANGE_PATH => {
                        if let Some((start, end, inclusive)) =
                            build_std_range!(&list.nested, context, Float, f64, "float")
                        {
                            push_element(
                                StandardSchema::FloatRange((Range { start, end }, inclusive)),
                                &mut schema,
                                context,
                            );
                        }
                    }
                    NestedMeta::Meta(Meta::List(list)) if list.path == BIG_INT_RANGE_PATH => {
                        match build_big_int_range(context, &list.nested) {
                            Some((range, inclusive)) => push_element(
                                StandardSchema::BigIntRange((range, inclusive)),
                                &mut schema,
                                context,
                            ),
                            None => {
                                context.error_spanned_by(list, "Malformatted Big Integer range");
                            }
                        }
                    }
                    NestedMeta::Meta(Meta::Path(path)) if path == ANYTHING_PATH => {
                        push_element(StandardSchema::Anything, &mut schema, context);
                    }
                    NestedMeta::Meta(Meta::Path(path)) if path == NOTHING_PATH => {
                        context.error_spanned_by(
                            path,
                            "StandardSchema::Nothing is not permitted on fields",
                        );
                    }
                    NestedMeta::Meta(Meta::Path(path)) if path == NON_NAN_PATH => {
                        push_element(StandardSchema::NonNan, &mut schema, context);
                    }
                    NestedMeta::Meta(Meta::Path(path)) if path == FINITE_PATH => {
                        push_element(StandardSchema::Finite, &mut schema, context);
                    }
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
                    NestedMeta::Meta(Meta::NameValue(name)) if name.path == NUM_ATTRS_PATH => {
                        if let Some(int) = parse_lit_to_int(&name.lit, context) {
                            push_element(StandardSchema::NumAttrs(int), &mut schema, context);
                        }
                    }
                    NestedMeta::Meta(Meta::NameValue(name)) if name.path == NUM_ITEMS_PATH => {
                        if let Some(int) = parse_lit_to_int(&name.lit, context) {
                            push_element(StandardSchema::NumItems(int), &mut schema, context);
                        }
                    }
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
                    NestedMeta::Meta(Meta::NameValue(name)) if name.path == EQUAL_PATH => {
                        if let Ok(path) = lit_str_to_expr_path(context, &name.lit) {
                            push_element(StandardSchema::Equal(path), &mut schema, context);
                        }
                    }
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
                    NestedMeta::Meta(Meta::List(list)) if list.path == ALL_ITEMS_PATH => {
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
