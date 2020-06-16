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

use proc_macro2::{Ident, Span};
use syn::DeriveInput;

use crate::parser::{Context, Parser};

#[allow(dead_code, unused_variables)]
mod parser;

#[proc_macro_derive(Form)]
pub fn derive_form(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    expand_derive_form(&input)
        .unwrap_or_else(to_compile_errors)
        .into()
}

#[proc_macro_attribute]
pub fn form(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ident = input.ident.clone();

    let ser = Ident::new(
        &format!("{}Serialize", ident.to_string()),
        Span::call_site(),
    );
    let de = Ident::new(
        &format!("{}Deserialize", ident.to_string()),
        Span::call_site(),
    );

    let q = quote! {
        use serde::Serialize as #ser;
        use serde::Deserialize as #de;

        #[derive(Form, #ser, #de)]
        #input
    };

    q.into()
}

fn expand_derive_form(
    input: &syn::DeriveInput,
) -> Result<proc_macro2::TokenStream, Vec<syn::Error>> {
    let context = Context::new();
    let parser = match Parser::from_ast(&context, input) {
        Some(cont) => cont,
        None => return Err(context.check().unwrap_err()),
    };

    context.check()?;

    let ident = parser.ident.clone();
    let assertions = parser.receiver_assert_quote();
    let name = parser.ident.to_string().trim_start_matches("r#").to_owned();
    let const_name = Ident::new(&format!("_IMPL_FORM_FOR_{}", name), Span::call_site());

    let quote = quote! {
        struct AssertReceiver;

        #[automatically_derived]
        impl AssertReceiver {
            fn __assert() {
                 #(#assertions)*
            }
        }

        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl Form for #ident {
            #[inline]
            fn as_value(&self) -> common::model::Value {
                let mut serializer = _serialize::ValueSerializer::default();
                match self.serialize(&mut serializer) {
                    Ok(_) => serializer.output(),
                    Err(e) => unreachable!(e),
                }
            }

            #[inline]
            fn try_from_value(value: &common::model::Value) -> Result<Self, _deserialize::FormDeserializeErr> {
                let mut deserializer = match value {
                    common::model::Value::Record(_, _) => _deserialize::ValueDeserializer::for_values(value),
                    _ => _deserialize::ValueDeserializer::for_single_value(value),
                };

                let result = Self::deserialize(&mut deserializer)?;
                Ok(result)
            }
        }
    };

    let res = quote! {
        const #const_name: () = {
            use swim_form::common;
            use swim_form::_serialize;
            use swim_form::_deserialize;
            use swim_form::Form;

            #quote
        };
    };

    Ok(res)
}

fn to_compile_errors(errors: Vec<syn::Error>) -> proc_macro2::TokenStream {
    let compile_errors = errors.iter().map(syn::Error::to_compile_error);
    quote!(#(#compile_errors)*)
}
