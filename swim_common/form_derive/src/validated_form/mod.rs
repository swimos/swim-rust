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

use quote::ToTokens;
use syn::DeriveInput;

use macro_helpers::Context;

use crate::form::form_parser::build_type_contents;
use crate::validated_form::vf_parser::type_contents_to_validated;

mod meta_parse;
mod vf_parser;

mod range;

pub fn build_validated_form(
    input: DeriveInput,
) -> Result<proc_macro2::TokenStream, Vec<syn::Error>> {
    let mut context = Context::default();
    let type_contents = match build_type_contents(&mut context, &input) {
        Some(cont) => type_contents_to_validated(&mut context, &input.ident, cont),
        None => return Err(context.check().unwrap_err()),
    };

    context.check()?;

    let structure_name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = &input.generics.split_for_impl();
    let schema = type_contents.into_token_stream();

    let ts = quote! {
        impl #impl_generics swim_common::form::ValidatedForm for #structure_name #ty_generics #where_clause
        {
            fn schema() -> swim_common::model::schema::StandardSchema {
                #schema
            }
        }
    };

    // println!("{}", ts.to_string());

    Ok(ts)
}
