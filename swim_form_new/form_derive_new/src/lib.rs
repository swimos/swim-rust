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

#![allow(clippy::match_wild_err_arm)]

extern crate proc_macro;
extern crate proc_macro2;
#[macro_use]
extern crate quote;
#[macro_use]
extern crate syn;

use proc_macro::TokenStream;

use proc_macro2::{Ident, Span};
use syn::{AttributeArgs, DeriveInput, NestedMeta};

use crate::parser::{Context, Parser};

#[allow(dead_code, unused_variables)]
mod parser;

#[proc_macro_attribute]
pub fn form(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as AttributeArgs);

    if args.len() != 1 {
        return syn::Error::new(
            Span::call_site(),
            "Exactly one Value name binding must be provided",
        )
        .to_compile_error()
        .into();
    }

    let value_name_binding = args.get(0).unwrap();
    let input = parse_macro_input!(input as DeriveInput);
    let derived: proc_macro2::TokenStream =
        expand_derive_form(&input, value_name_binding).unwrap_or_else(to_compile_errors);

    let q = quote! {
        #input

        #derived
    };

    q.into()
}

fn expand_derive_form(
    input: &syn::DeriveInput,
    value_name_binding: &NestedMeta,
) -> Result<proc_macro2::TokenStream, Vec<syn::Error>> {
    let context = Context::new();
    let parser = match Parser::from_ast(&context, input) {
        Some(cont) => cont,
        None => return Err(context.check().unwrap_err()),
    };

    context.check()?;

    let ident = parser.ident.clone();
    let name = parser.ident.to_string().trim_start_matches("r#").to_owned();
    let const_name = Ident::new(&format!("_IMPL_FORM_FOR_{}", name), Span::call_site());
    let serialized_fields = parser.serialize_fields();

    let (impl_generics, type_generics, where_clause) = parser.generics.split_for_impl();

    let quote = quote! {
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl #impl_generics swim_form::Form for #ident #type_generics #where_clause {
            #[inline]
            fn as_value(&self) -> #value_name_binding {
                 crate::serialize::as_value(self)
            }

            #[inline]
            fn try_from_value(value: &#value_name_binding) -> Result<Self, swim_form::FormDeserializeErr> {
                unimplemented!()
            }
        }

        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl #impl_generics swim_form::SerializeToValue for #ident #type_generics #where_clause {
            #[inline]
            fn serialize(&self, serializer: &mut swim_form::ValueSerializer, _properties: Option<swim_form::SerializerProps>) {
                #serialized_fields
            }
        }
    };

    let res = quote! {
        const #const_name: () = {
            #quote
        };
    };

    Ok(res)
}

fn to_compile_errors(errors: Vec<syn::Error>) -> proc_macro2::TokenStream {
    let compile_errors = errors.iter().map(syn::Error::to_compile_error);
    quote!(#(#compile_errors)*)
}
