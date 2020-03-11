// #![feature(trace_macros)]

// trace_macros!(true);
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

#[allow(dead_code, unused_imports, unused_variables)]
mod parser;

#[proc_macro_derive(Form)]
pub fn derive_form(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    expand_derive_serialize(&input)
        .unwrap_or_else(to_compile_errors)
        .into()
}

fn expand_derive_serialize(
    input: &syn::DeriveInput,
) -> Result<proc_macro2::TokenStream, Vec<syn::Error>> {
    let context = Context::new();
    let parser = match Parser::from_ast(&context, input) {
        Some(cont) => cont,
        None => return Err(context.check().unwrap_err()),
    };

    context.check()?;

    let ident = parser.ident.clone();
    let field_assertions = parser.receiver_match_arm();
    let fields = parser.match_funcs();
    let name = parser.ident.to_string().trim_start_matches("r#").to_owned();
    let dummy_const = Ident::new(&format!("_IMPL_FORM_FOR_{}", name), Span::call_site());

    let quote = quote! {
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl form_model::Form for #ident {
            fn __assert_receiver_is_total_form(&self) {
                match self {
                    #ident { #(#field_assertions),*  } => {
                        #(#fields)*
                    }
                    _=> panic!("quote catch all")
                }
            }

            #[inline]
            fn try_into_value(&self) -> Result<_common::model::Value, _common::structure::form::FormParseErr> {
                unimplemented!()
            }

            #[inline]
            fn try_from_value(value: &_common::model::Value) -> Result<Self, _common::structure::form::FormParseErr> {
                unimplemented!()
            }
        }
    };

    let res = quote! {
        const #dummy_const: () = {
            use common as _common;

            #quote
        };
    };

    Ok(res)
}

fn to_compile_errors(errors: Vec<syn::Error>) -> proc_macro2::TokenStream {
    let compile_errors = errors.iter().map(syn::Error::to_compile_error);
    quote!(#(#compile_errors)*)
}
