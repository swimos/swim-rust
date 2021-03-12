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

use crate::models::Preprocessed;
use crate::util::{check_opt, list_to_str, pop_list, try_with_context};
use macro_helpers::{get_lit_str, Context, Symbol};
use quote::quote;
use quote::ToTokens;
use syn::{parse_quote, Attribute, Meta, MetaList, NestedMeta, Path};

const STRINGIFY_PATH: Symbol = Symbol("stringify");
const STRINGIFY_RAW_PATH: Symbol = Symbol("stringify_raw");
const PATH: Symbol = Symbol("path");
const IN_PATH: Symbol = Symbol("in");
const RAW_PATH: Symbol = Symbol("raw");

const INVALID_OUTER_ATTRIBUTE: &str = "Expected an outer attribute in a list format";
pub const UNEXPECTED_ATTR: &str = "Unexpected attribute";
pub const UNEXPECTED_LIT: &str = "Unexpected literal";
pub const EXPECTED_LIST: &str = "Expected a list";

const FIELD_PATH: &str = "path";
const FIELD_INPUT: &str = "input";
const FIELD_RAW: &str = "raw";

pub fn parse_attr_input(ctx: &mut Context, meta: NestedMeta) -> Result<Vec<Meta>, ()> {
    match meta.clone() {
        NestedMeta::Meta(Meta::List(list)) if !list.nested.is_empty() => {
            let MetaList { path, nested, .. } = list;
            Ok(list_to_str(ctx, path, nested))
        }
        n => {
            ctx.error_spanned_by(
                n,
                format!(
                    "{}. Found `{}`",
                    EXPECTED_LIST,
                    meta.to_token_stream().to_string()
                ),
            );
            Err(())
        }
    }
}

pub fn stringify_attributes(ctx: &mut Context, attributes: &mut Vec<Attribute>) {
    let mut new_attributes = Vec::new();

    attributes.retain(|attr| {
        parse_field_meta(ctx, attr)
            .and_then(|meta| match meta.parse(ctx) {
                Ok(processed) => {
                    new_attributes.push(processed);
                    Some(())
                }
                Err(()) => None,
            })
            .is_none()
    });

    new_attributes.into_iter().for_each(|attr| {
        let parsed: Attribute = parse_quote!(#attr);
        attributes.push(parsed);
    })
}

fn meta_stringify<F>(ctx: &mut Context, meta: Meta, then: F) -> Option<Preprocessed>
where
    F: Fn(&mut Context, MetaList) -> Option<Preprocessed>,
{
    match meta {
        Meta::List(input) => then(ctx, input),
        meta => {
            ctx.error_spanned_by(meta, INVALID_OUTER_ATTRIBUTE);
            None
        }
    }
}

fn parse_stringify_raw(ctx: &mut Context, meta: MetaList) -> Option<Preprocessed> {
    let location = meta.path;

    let mut path_opt = None;
    let mut in_opt = None;
    let mut raw_opt = None;

    for meta in meta.nested {
        match meta {
            NestedMeta::Meta(meta) => match meta {
                Meta::List(list) if list.path == IN_PATH => {
                    in_opt = Some(list.nested.into_iter().collect());
                }
                Meta::List(list) if list.path == RAW_PATH => {
                    raw_opt = Some(list.nested);
                }
                Meta::NameValue(pair) if pair.path == PATH => {
                    let str = get_lit_str(ctx, &pair.lit).ok()?;
                    let value = try_with_context(&pair, ctx, || str.parse::<Path>())?;

                    path_opt = Some(value);
                }
                m => ctx.error_spanned_by(m, UNEXPECTED_ATTR),
            },
            m => ctx.error_spanned_by(m, UNEXPECTED_ATTR),
        }
    }

    let stringify = Preprocessed::StringifyPartial {
        path: check_opt(path_opt, ctx, &location, FIELD_PATH)?,
        stringify: check_opt(in_opt, ctx, &location, FIELD_INPUT)?,
        raw: check_opt(raw_opt, ctx, &location, FIELD_RAW).map(|r| quote!(#r))?,
    };

    Some(stringify)
}

fn parse_field_meta(ctx: &mut Context, attr: &Attribute) -> Option<Preprocessed> {
    match attr.parse_meta() {
        Ok(meta) => match &attr.path {
            p if p == STRINGIFY_PATH => meta_stringify(ctx, meta, |ctx, list| {
                let MetaList { nested, path, .. } = pop_list(ctx, list)?;
                Some(Preprocessed::StringifyAll {
                    path,
                    stringify: nested.into_iter().collect(),
                })
            }),
            p if p == STRINGIFY_RAW_PATH => meta_stringify(ctx, meta, parse_stringify_raw),
            _ => None,
        },
        Err(e) => {
            ctx.error_spanned_by(attr, e);
            None
        }
    }
}

pub fn stringify_container_attrs(
    ctx: &mut Context,
    meta: Vec<NestedMeta>,
) -> Option<Vec<Attribute>> {
    let mut new_attrs = Vec::new();

    for nested in meta {
        match nested {
            NestedMeta::Meta(Meta::List(list)) => {
                let MetaList { nested, path, .. } = list;

                let pre_processed = Preprocessed::StringifyAll {
                    path,
                    stringify: nested.into_iter().collect(),
                };

                new_attrs.push(pre_processed);
            }
            m => {
                ctx.error_spanned_by(m, EXPECTED_LIST);
            }
        }
    }

    let result = new_attrs
        .into_iter()
        .filter_map(|attr| {
            attr.parse(ctx)
                .map(|processed| {
                    let parsed: Attribute = parse_quote!(#processed);
                    parsed
                })
                .ok()
        })
        .collect::<Vec<Attribute>>();

    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

pub fn stringify_container_attrs_raw(
    ctx: &mut Context,
    meta: Vec<NestedMeta>,
) -> Option<Attribute> {
    let list: MetaList = parse_quote!(stringify_raw(#(#meta),*));
    let pre_processed = parse_stringify_raw(ctx, list)?;

    let parsed = pre_processed
        .parse(ctx)
        .map(|processed| {
            let parsed: Attribute = parse_quote!(#processed);
            parsed
        })
        .ok()?;

    Some(parsed)
}
