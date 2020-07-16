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
use syn::visit_mut::VisitMut;
use syn::{AttributeArgs, DeriveInput, Fields, NestedMeta};

use crate::parser::{Context, Parser};

#[allow(dead_code, unused_variables)]
mod parser;

const ATTRIBUTE_PATH: &str = "form";

#[proc_macro_attribute]
pub fn form(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as AttributeArgs);
    let value_name_binding = args.get(0).unwrap();
    let mut input = parse_macro_input!(input as DeriveInput);
    let derived: proc_macro2::TokenStream =
        expand_derive_form(&input, value_name_binding, &args).unwrap_or_else(to_compile_errors);

    remove_form_attributes(&mut input);

    let q = quote! {
        #input

        #derived
    };

    q.into()
}

fn remove_form_attributes(input: &mut syn::DeriveInput) {
    struct FieldVisitor;
    impl VisitMut for FieldVisitor {
        fn visit_fields_mut(&mut self, fields: &mut Fields) {
            fields.iter_mut().for_each(|f| {
                let to_remove = f
                    .attrs
                    .iter_mut()
                    .enumerate()
                    .filter(|(_idx, attr)| attr.path.is_ident(ATTRIBUTE_PATH))
                    .fold(Vec::new(), |mut v, (idx, _attr)| {
                        v.push(idx);
                        v
                    });
                to_remove.iter().for_each(|i| {
                    f.attrs.remove(*i);
                })
            });
        }
    }

    match &mut input.data {
        syn::Data::Enum(ref mut data) => {
            data.variants.iter_mut().for_each(|v| {
                FieldVisitor.visit_fields_mut(&mut v.fields);
            });
        }
        syn::Data::Struct(ref mut data) => {
            FieldVisitor.visit_fields_mut(&mut data.fields);
        }
        syn::Data::Union(_) => panic!("Unions are not supported"),
    }
}

fn expand_derive_form(
    input: &syn::DeriveInput,
    value_name_binding: &NestedMeta,
    args: &AttributeArgs,
) -> Result<proc_macro2::TokenStream, Vec<syn::Error>> {
    let context = Context::new();
    let parser = match Parser::from_ast(&context, input, args) {
        Some(cont) if cont.value_path.is_some() => cont,
        Some(cont) if cont.value_path.is_none() => {
            return Err(vec![syn::Error::new(
                Span::call_site(),
                "Exactly one Value name binding must be provided",
            )]);
        }
        _ => return Err(context.check().unwrap_err()),
    };

    context.check()?;

    let ident = parser.name.ident.clone();
    let name = parser
        .name
        .ident
        .to_string()
        .trim_start_matches("r#")
        .to_owned();
    let const_name = Ident::new(&format!("_IMPL_FORM_FOR_{}", name), Span::call_site());
    let transmute_to_fields = parser.transmute_fields();

    let (impl_generics, type_generics, where_clause) = parser.generics.split_for_impl();

    let quote = quote! {
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl #impl_generics swim_form::Form for #ident #type_generics #where_clause {
            #[inline]
            fn as_value(&self) -> #value_name_binding {
                 crate::writer::as_value(self)
            }

            #[inline]
            fn try_from_value(value: &#value_name_binding) -> Result<Self, swim_form::ValueReadError> {
                unimplemented!()
            }
        }

        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl #impl_generics swim_form::TransmuteValue for #ident #type_generics #where_clause {
            #[inline]
            fn transmute_to_value(&self, writer: &mut swim_form::ValueWriter) {
                #transmute_to_fields
            }

            #[inline]
            fn transmute_from_value(&self, reader: &mut swim_form::ValueReader) -> Result<Self, swim_form::ValueReadError> {
                unimplemented!()
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
