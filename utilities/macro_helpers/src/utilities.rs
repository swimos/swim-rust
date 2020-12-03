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

use crate::label::Label;
use crate::{CompoundTypeKind, Context, Symbol};
use proc_macro2::TokenStream;
use syn::Lit;
use syn::{ExprPath, Meta};

/// Consumes a vector of errors and produces a compiler error.
pub fn to_compile_errors(errors: Vec<syn::Error>) -> proc_macro2::TokenStream {
    let compile_errors = errors.iter().map(syn::Error::to_compile_error);
    quote!(#(#compile_errors)*)
}

/// Deconstructs a structure or enumeration into its fields. For example:
/// ```
/// struct S {
///     a: i32,
///     b: i32
/// }
/// ```
///
/// Will produce the following:
/// ```compile_fail
/// { a, b }
/// ```
pub fn deconstruct_type(
    compound_type: &CompoundTypeKind,
    fields: &[&Label],
    as_ref: bool,
) -> TokenStream {
    let fields: Vec<_> = fields
        .iter()
        .map(|name| match &name {
            Label::Unmodified(ident) => {
                quote! { #ident }
            }
            Label::Renamed { old_label, .. } => {
                quote! { #old_label }
            }
            Label::Foreign(ident, ..) => {
                quote! { #ident }
            }
            un @ Label::Anonymous(_) => {
                let binding = &un.as_ident();
                quote! { #binding }
            }
        })
        .collect();

    if as_ref {
        match compound_type {
            CompoundTypeKind::Struct => quote! { { #(ref #fields,)* } },
            CompoundTypeKind::Tuple => quote! { ( #(ref #fields,)* ) },
            CompoundTypeKind::NewType => quote! { ( #(ref #fields,)* ) },
            CompoundTypeKind::Unit => quote!(),
        }
    } else {
        match compound_type {
            CompoundTypeKind::Struct => quote! { { #(#fields,)* } },
            CompoundTypeKind::Tuple => quote! { ( #(#fields,)* ) },
            CompoundTypeKind::NewType => quote! { ( #(#fields,)* ) },
            CompoundTypeKind::Unit => quote!(),
        }
    }
}

/// Returns a vector of metadata for the provided [`Attribute`] that matches the provided
/// [`Symbol`]. An error that is encountered is added to the [`Context`] and a [`Result::Err`] is
/// returned.
pub fn get_attribute_meta(
    ctx: &mut Context,
    attr: &syn::Attribute,
    path: Symbol,
) -> Result<Vec<syn::NestedMeta>, ()> {
    if attr.path != path {
        Ok(Vec::new())
    } else {
        match attr.parse_meta() {
            Ok(Meta::List(meta)) => Ok(meta.nested.into_iter().collect()),
            Ok(other) => {
                ctx.error_spanned_by(
                    other,
                    &format!("Invalid attribute. Expected #[{}(...)]", path),
                );
                Err(())
            }
            Err(e) => {
                ctx.error_spanned_by(attr, e.to_compile_error());
                Err(())
            }
        }
    }
}

pub fn lit_str_to_expr_path(ctx: &mut Context, lit: &Lit) -> Result<ExprPath, ()> {
    match lit {
        Lit::Str(lit_str) => {
            let token_stream = syn::parse_str(&lit_str.value()).map_err(|e| {
                ctx.error_spanned_by(lit_str, e.to_string());
            })?;
            match syn::parse2::<ExprPath>(token_stream) {
                Ok(path) => Ok(path),
                Err(e) => {
                    ctx.error_spanned_by(lit, e.to_string());
                    Err(())
                }
            }
        }
        _ => {
            ctx.error_spanned_by(lit, "Expected a String literal");
            Err(())
        }
    }
}
