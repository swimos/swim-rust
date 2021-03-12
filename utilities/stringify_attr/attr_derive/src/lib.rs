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

///! A macro to parse macro attributes in a path format into a string literal.
///!
///! See the `stringify_attr` crate root for more details.
mod models;
mod parse;
mod util;

use crate::models::Visitor;
use crate::parse::{stringify_container_attrs, stringify_container_attrs_raw};
use macro_helpers::Context;
use proc_macro::TokenStream;
use quote::quote;
use syn::__private::TokenStream2;
use syn::visit_mut::VisitMut;
use syn::{parse_macro_input, AttributeArgs, DeriveInput};

#[proc_macro_attribute]
pub fn stringify_attr(args: TokenStream, input: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    let args = parse_macro_input!(args as AttributeArgs);

    let mut context = Context::default();
    let container_attrs = stringify_container_attrs(&mut context, args).unwrap_or_default();
    let container_ts = quote!(#(#container_attrs)*);

    let mut visitor = Visitor::new(context);
    visitor.visit_data_mut(&mut input.data);

    if let Err(errors) = visitor.context.check() {
        let compile_errors = errors.iter().map(syn::Error::to_compile_error);
        return quote!(#(#compile_errors)*).into();
    }

    let output = quote! {
        #container_ts
        #input
    };

    output.into()
}

#[proc_macro_attribute]
pub fn stringify_attr_raw(args: TokenStream, input: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    let args = parse_macro_input!(args as AttributeArgs);

    let mut context = Context::default();
    let container_opt = stringify_container_attrs_raw(&mut context, args);
    let container_ts = match container_opt {
        Some(attr) => {
            quote!(#attr)
        }
        None => TokenStream2::new(),
    };

    let mut visitor = Visitor::new(context);
    visitor.visit_data_mut(&mut input.data);

    if let Err(errors) = visitor.context.check() {
        let compile_errors = errors.iter().map(syn::Error::to_compile_error);
        return quote!(#(#compile_errors)*).into();
    }

    let output = quote! {
        #container_ts
        #input
    };

    output.into()
}
