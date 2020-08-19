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

use crate::form::form_parser::build_type_contents;
use macro_helpers::Context;
use syn::DeriveInput;

pub mod form_parser;
mod from_value;
use from_value::from_value;
mod to_value;
use to_value::to_value;

pub fn build_derive_form(input: DeriveInput) -> Result<proc_macro2::TokenStream, Vec<syn::Error>> {
    let mut context = Context::default();
    let type_contents = match build_type_contents(&mut context, &input) {
        Some(cont) => cont,
        None => return Err(context.check().unwrap_err()),
    };

    let structure_name = &input.ident;
    let from_value_body = from_value(&type_contents, &structure_name);
    let as_value_body = to_value(type_contents, structure_name);

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
                #from_value_body
            }
        }
    };

    Ok(ts)
}
