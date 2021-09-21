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

extern crate proc_macro;
extern crate proc_macro2;
#[macro_use]
extern crate quote;
#[macro_use]
extern crate syn;

use proc_macro::TokenStream;

use syn::DeriveInput;

use macro_utilities::to_compile_errors;

use crate::structural::{
    build_derive_structural_form, build_derive_structural_readable,
    build_derive_structural_writable,
};
use crate::tag::build_derive_tag;
use crate::validated_form::build_validated_form;
use utilities::algebra::Errors;
use utilities::validation::Validation;

mod form;
mod modifiers;
mod parser;
mod structural;
mod tag;
mod validated_form;

#[proc_macro_derive(Form, attributes(form))]
pub fn derive_form(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    build_derive_structural_form(input)
        .unwrap_or_else(errs_to_compile_errors)
        .into()
}

#[proc_macro_derive(Tag, attributes(form))]
pub fn derive_tag(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    build_derive_tag(input)
        .unwrap_or_else(errs_to_compile_errors)
        .into()
}

#[proc_macro_derive(ValidatedForm, attributes(form))]
pub fn derive_validated_form(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    build_validated_form(input)
        .unwrap_or_else(to_compile_errors)
        .into()
}

#[proc_macro_derive(StructuralWritable, attributes(form))]
pub fn derive_structural_writable(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    build_derive_structural_writable(input)
        .unwrap_or_else(errs_to_compile_errors)
        .into()
}

#[proc_macro_derive(StructuralReadable, attributes(form))]
pub fn derive_structural_readable(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    build_derive_structural_readable(input)
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
