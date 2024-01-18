use proc_macro::TokenStream;

use quote::quote;
use syn::{parse_macro_input, Error, Item};

use macro_utilities::to_compile_errors;
use swim_utilities::errors::validation::Validation;
use swim_utilities::errors::Errors;

use crate::agent::parse_agent;
use crate::connector::parse_connector;

mod agent;
mod connector;

const NO_ARGS: &str = "Attribute does not accept any arguments";

#[proc_macro_attribute]
pub fn wasm_agent(attr: TokenStream, item: TokenStream) -> TokenStream {
    let item: Item = parse_macro_input!(item as Item);
    validate_attr(attr)
        .and_then(|_| parse_agent(&item))
        .into_result()
        .map(|proj| {
            quote! {
                #item
                #proj
            }
        })
        .unwrap_or_else(|errs| to_compile_errors(errs.into_vec()))
        .into()
}

#[proc_macro_attribute]
pub fn connector(attr: TokenStream, item: TokenStream) -> TokenStream {
    let item: Item = parse_macro_input!(item as Item);
    validate_attr(attr)
        .and_then(|_| parse_connector(&item))
        .into_result()
        .map(|connector| {
            quote! {
                #connector
                #item
            }
        })
        .unwrap_or_else(|errs| to_compile_errors(errs.into_vec()))
        .into()
}

fn validate_attr(attr: TokenStream) -> Validation<(), Errors<Error>> {
    let tokens: proc_macro2::TokenStream = attr.into();

    if tokens.is_empty() {
        Validation::valid(())
    } else {
        Validation::fail(Errors::of(Error::new_spanned(tokens, NO_ARGS)))
    }
}
