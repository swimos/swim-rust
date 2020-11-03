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

extern crate proc_macro;
extern crate proc_macro2;
#[macro_use]
extern crate quote;
#[macro_use]
extern crate syn;

use proc_macro::TokenStream;

use syn::DeriveInput;

use macro_helpers::to_compile_errors;

use crate::form::build_derive_form;
use crate::tag::build_tag;
use crate::validated_form::build_validated_form;

mod form;
mod parser;
mod tag;
mod validated_form;

#[proc_macro_derive(Form, attributes(form))]
pub fn derive_form(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    build_derive_form(input)
        .unwrap_or_else(to_compile_errors)
        .into()
}

#[proc_macro_derive(ValidatedForm, attributes(form))]
pub fn derive_validated_form(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    build_validated_form(input)
        .unwrap_or_else(to_compile_errors)
        .into()
}

#[proc_macro_derive(Tag)]
pub fn derive_tag(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    build_tag(input).unwrap_or_else(to_compile_errors).into()
}
