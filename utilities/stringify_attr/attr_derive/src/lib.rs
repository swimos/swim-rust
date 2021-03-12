mod models;
mod parse;
mod util;

use crate::models::Visitor;
use crate::parse::stringify_container_attrs;
use macro_helpers::Context;
use proc_macro::TokenStream;
use quote::quote;
use syn::visit_mut::VisitMut;
use syn::{parse_macro_input, AttributeArgs, DeriveInput};

/// A macro to parse macro attributes in a path format into a string literal.
///
/// See the `stringify_attr` crate root for more details.
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
