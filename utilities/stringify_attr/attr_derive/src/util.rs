use crate::parse::{EXPECTED_LIST, UNEXPECTED_ATTR, UNEXPECTED_LIT};
use macro_helpers::Context;
use quote::ToTokens;
use std::fmt::Display;
use syn::punctuated::Punctuated;
use syn::Token;
use syn::{parse_quote, Meta, MetaList, MetaNameValue, NestedMeta, Path};

const EXPECTED_SINGLE_LIST: &str = "Expected a single item list";

/// Executes `func` and if it returns an `Err` then a new error spanned by `location` is writen to
/// `context` with the error's message. Returns `Some` if `func` returns an `Ok` variant or `None`
/// otherwise.
pub fn try_with_context<A, F, O, E>(location: &A, context: &mut Context, func: F) -> Option<O>
where
    A: ToTokens,
    F: Fn() -> Result<O, E>,
    E: Display,
{
    match func() {
        Ok(o) => Some(o),
        Err(e) => {
            context.error_spanned_by(location, e);
            None
        }
    }
}

/// Folds the nested meta in `meta`, parsing each nested `Meta::Path` into a name-value pair keyed
/// by `path`. This function expects every item in `meta` to be `NestedMeta::Meta(Meta::Path)`.
///
/// Any errors encountered will be written to `ctx` and spanned by their source tree.
pub fn list_to_str(
    ctx: &mut Context,
    path: Path,
    meta: Punctuated<NestedMeta, Token![,]>,
) -> Vec<Meta> {
    let seed = Vec::with_capacity(meta.len());

    meta.into_iter().fold(seed, |mut vec: Vec<Meta>, meta| {
        match meta {
            NestedMeta::Meta(meta) => match meta {
                Meta::Path(inner) => {
                    let seg = inner.segments;
                    let segment_string = seg.to_token_stream().to_string();
                    let lit: MetaNameValue = parse_quote!(#path = #segment_string);

                    vec.push(Meta::NameValue(lit));
                }
                m => {
                    ctx.error_spanned_by(m, UNEXPECTED_ATTR);
                }
            },
            NestedMeta::Lit(lit) => ctx.error_spanned_by(lit, UNEXPECTED_LIT),
        }

        vec
    })
}

/// Writes an error spanned by `location` if `opt` is not `Some`. Reporting that the `field_name`
/// was missing.
pub fn check_opt<O, A, N>(
    opt: Option<O>,
    ctx: &mut Context,
    location: A,
    field_name: N,
) -> Option<O>
where
    A: ToTokens,
    N: Display,
{
    opt.or_else(|| {
        ctx.error_spanned_by(location, format!("Missing attribute: {}", field_name));
        None
    })
}

/// Asserts that `list` is a single item list and pops the item.
pub fn pop_list(ctx: &mut Context, mut list: MetaList) -> Option<MetaList> {
    match list.nested.len() {
        1 => match list.nested.pop().unwrap().into_value() {
            NestedMeta::Meta(Meta::List(list)) => Some(list),
            n => {
                ctx.error_spanned_by(n, EXPECTED_LIST);
                None
            }
        },
        _ => {
            ctx.error_spanned_by(list, EXPECTED_SINGLE_LIST);
            None
        }
    }
}
