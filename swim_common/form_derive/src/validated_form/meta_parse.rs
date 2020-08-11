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
    StandardSchema, ALL_ITEMS_PATH, AND_PATH, NOT_PATH, NUM_ATTRS_PATH, NUM_ITEMS_PATH, OR_PATH,
};
use macro_helpers::Context;
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

pub fn parse_schema_meta(
    schema: StandardSchema,
    context: &mut Context,
    nested: &Punctuated<NestedMeta, Token![,]>,
) -> StandardSchema {
    nested.iter().fold(schema, |mut schema, meta| {
        let push_element = |elem, borrowed_schema: &mut StandardSchema| match borrowed_schema {
            StandardSchema::And(vec) => vec.push(elem),
            StandardSchema::Or(vec) => vec.push(elem),
            StandardSchema::AllItems(boxed) => *boxed = Box::new(elem),
            StandardSchema::Not(boxed) => *boxed = Box::new(elem),
            _ => *borrowed_schema = elem,
        };

        match meta {
            NestedMeta::Meta(Meta::NameValue(name)) if name.path == NUM_ATTRS_PATH => {
                if let Some(int) = parse_lit_to_int(&name.lit, context) {
                    push_element(StandardSchema::NumAttrs(int), &mut schema);
                }
            }
            NestedMeta::Meta(Meta::NameValue(name)) if name.path == NUM_ITEMS_PATH => {
                if let Some(int) = parse_lit_to_int(&name.lit, context) {
                    push_element(StandardSchema::NumItems(int), &mut schema);
                }
            }
            NestedMeta::Meta(Meta::List(list)) if list.path == AND_PATH => {
                if list.nested.len() < 2 {
                    context.error_spanned_by(
                        list,
                        "At least two arguments must be provided to an AND operator",
                    );
                } else {
                    push_element(
                        parse_schema_meta(StandardSchema::And(Vec::new()), context, &list.nested),
                        &mut schema,
                    );
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
                        parse_schema_meta(StandardSchema::Or(Vec::new()), context, &list.nested),
                        &mut schema,
                    );
                }
            }
            NestedMeta::Meta(Meta::List(list)) if list.path == NOT_PATH => {
                if list.nested.len() > 1 {
                    context
                        .error_spanned_by(list, "Only one argument is permitted in a NOT operator")
                } else {
                    push_element(
                        parse_schema_meta(
                            StandardSchema::Not(Box::new(StandardSchema::None)),
                            context,
                            &list.nested,
                        ),
                        &mut schema,
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
                    );
                }
            }
            _ => context.error_spanned_by(meta, "Unknown field attribute"),
        }

        schema
    })
}
