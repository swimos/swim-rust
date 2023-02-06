// Copyright 2015-2021 Swim Inc.
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

extern crate proc_macro;
extern crate proc_macro2;
#[macro_use]
extern crate quote;
#[macro_use]
extern crate syn;

use proc_macro::TokenStream;

use syn::punctuated::Pair;
use syn::{DeriveInput, Meta, NestedMeta};

use crate::structural::{
    build_derive_structural_form, build_derive_structural_readable,
    build_derive_structural_writable,
};
use crate::tag::build_derive_tag;
use swim_utilities::errors::validation::Validation;
use swim_utilities::errors::Errors;

mod modifiers;
mod structural;
mod tag;

fn default_root() -> syn::Path {
    parse_quote!(::swim::form)
}

#[proc_macro_derive(Form, attributes(form, form_root))]
pub fn derive_form(input: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    let root = extract_replace_root(&mut input.attrs).unwrap_or_else(default_root);
    build_derive_structural_form(root, input)
        .unwrap_or_else(errs_to_compile_errors)
        .into()
}

#[proc_macro_derive(Tag, attributes(form, form_root))]
pub fn derive_tag(input: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    let root = extract_replace_root(&mut input.attrs).unwrap_or_else(default_root);
    build_derive_tag(root, input)
        .unwrap_or_else(errs_to_compile_errors)
        .into()
}

#[proc_macro_derive(StructuralWritable, attributes(form, form_root))]
pub fn derive_structural_writable(input: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    let root = extract_replace_root(&mut input.attrs).unwrap_or_else(default_root);
    build_derive_structural_writable(root, input)
        .unwrap_or_else(errs_to_compile_errors)
        .into()
}

#[proc_macro_derive(StructuralReadable, attributes(form, form_root))]
pub fn derive_structural_readable(input: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    let root = extract_replace_root(&mut input.attrs).unwrap_or_else(default_root);
    build_derive_structural_readable(root, input)
        .unwrap_or_else(errs_to_compile_errors)
        .into()
}

type SynValidation<T> = Validation<T, Errors<syn::Error>>;

fn errs_to_compile_errors(errors: Errors<syn::Error>) -> proc_macro2::TokenStream {
    let compile_errors = errors
        .into_vec()
        .into_iter()
        .map(|e| syn::Error::to_compile_error(&e));
    quote!(#(#compile_errors)*)
}

fn extract_replace_root(attrs: &mut Vec<syn::Attribute>) -> Option<syn::Path> {
    if let Some((i, p)) = attrs.iter_mut().enumerate().find_map(|(i, attr)| {
        if attr.path.is_ident("form_root") {
            match attr.parse_meta() {
                Ok(Meta::List(lst)) => {
                    let mut nested = lst.nested;
                    match nested.pop().map(Pair::into_value) {
                        Some(NestedMeta::Meta(Meta::Path(p))) if nested.is_empty() => Some((i, p)),
                        _ => None,
                    }
                }
                _ => None,
            }
        } else {
            None
        }
    }) {
        attrs.remove(i);
        Some(p)
    } else {
        None
    }
}
