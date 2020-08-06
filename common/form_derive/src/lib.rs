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

use macro_helpers::{to_compile_errors, Context};

use crate::parser::{FormDescriptor, TypeContents};

mod parser;
mod to_value;
use to_value::to_value;

#[proc_macro_derive(Form, attributes(form))]
pub fn derive_form(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    build_derive_form(input)
        .unwrap_or_else(to_compile_errors)
        .into()
}

fn build_derive_form(input: DeriveInput) -> Result<proc_macro2::TokenStream, Vec<syn::Error>> {
    let mut context = Context::default();
    let descriptor = FormDescriptor::from_ast(&mut context, &input);
    let structure_name = descriptor.name.original_ident.clone();
    let type_contents = match TypeContents::from(&mut context, &input) {
        Some(cont) => cont,
        None => return Err(context.check().unwrap_err()),
    };

    let as_value_body = to_value(type_contents, &structure_name, descriptor);

    context.check()?;

    let (impl_generics, ty_generics, where_clause) = &input.generics.split_for_impl();

    let ts = quote! {
        impl #impl_generics swim_common::form::Form for #structure_name #ty_generics #where_clause
        {
            #[inline]
            #[allow(non_snake_case)]
            fn as_value(&self) -> swim_common::model::Value {
                #as_value_body
            }

            #[inline]
            #[allow(non_snake_case)]
            fn try_from_value(value: &swim_common::model::Value) -> Result<Self, swim_common::form::FormErr> {
                unimplemented!()
            }
        }
    };

    Ok(ts)
}
