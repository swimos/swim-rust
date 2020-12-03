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

use syn::DeriveInput;

use from_value::from_value;
use macro_helpers::Context;
use to_value::to_value;

use crate::form::form_parser::build_type_contents;

pub mod form_parser;
mod from_value;
mod to_value;

pub fn build_derive_form(input: DeriveInput) -> Result<proc_macro2::TokenStream, Vec<syn::Error>> {
    let mut context = Context::default();
    let structure_name = &input.ident;
    let type_contents = match build_type_contents(&mut context, &input) {
        Some(cont) => cont,
        None => return Err(context.check().unwrap_err()),
    };

    let from_value_body = from_value(
        &type_contents,
        &structure_name,
        |value| parse_quote!(swim_common::form::Form::try_from_value(#value)),
        false,
    );
    let try_convert_body = from_value(
        &type_contents,
        &structure_name,
        |value| parse_quote!(swim_common::form::Form::try_convert(#value)),
        true,
    );
    let as_value_body = to_value(
        type_contents.clone(),
        &structure_name,
        |ident| parse_quote!(#ident.as_value()),
        true,
    );
    let into_value_body = to_value(
        type_contents,
        &structure_name,
        |ident| parse_quote!(#ident.into_value()),
        false,
    );

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
            #[allow(non_snake_case, unused_variables)]
            fn into_value(self) -> swim_common::model::Value {
                #into_value_body
            }

            #[inline]
            #[allow(non_snake_case)]
            fn try_from_value(value: &swim_common::model::Value) -> Result<Self, swim_common::form::FormErr> {
                #from_value_body
            }

            #[inline]
            #[allow(non_snake_case)]
            fn try_convert(value: swim_common::model::Value) -> Result<Self, swim_common::form::FormErr> {
                #try_convert_body
            }
        }
    };

    Ok(ts)
}

//
// fn build_generics(type_contents: &TypeContents<FormDescriptor, FormField>) -> Generics {
//     match type_contents {
//         TypeContents::Enum(repr) => {}
//         TypeContents::Struct(repr) => repr.fields.get(0).unwrap().original.ty,
//     }
//
//     let generics = match *cont.attrs.default() {
//         attr::Default::Default => {
//             bound::with_self_bound(cont, &generics, &parse_quote!(_serde::export::Default))
//         }
//         attr::Default::None | attr::Default::Path(_) => generics,
//     };
//
//     let generics = bound::with_bound(
//         cont,
//         &generics,
//         needs_deserialize_bound,
//         &parse_quote!(_serde::Deserialize<#delife>),
//     );
//
//     bound::with_bound(
//         cont,
//         &generics,
//         requires_default,
//         &parse_quote!(_serde::export::Default),
//     );
//
//     unimplemented!()
// }
