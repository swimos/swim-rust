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

use syn::export::TokenStream2;

use macro_helpers::Identity;

use crate::validated_form::vf_parser::{StandardSchema, ValidatedField};

pub fn build_head_attribute(
    compound_identity: &Identity,
    remainder: TokenStream2,
    fields: &[ValidatedField],
) -> TokenStream2 {
    let field_opt = fields
        .iter()
        .filter(|f| f.form_field.is_header_body())
        .collect::<Vec<_>>();
    let schema = match field_opt.first() {
        Some(field) => field.field_schema.clone(),
        None => StandardSchema::OfKind(quote!(swim_common::model::ValueKind::Extant)),
    };

    let compound_identity = compound_identity.to_string();
    let attr_schema = quote! {
        swim_common::model::schema::attr::AttrSchema::named(
            #compound_identity,
            #schema,
        )
    };

    quote! {
        swim_common::model::schema::StandardSchema::HeadAttribute {
            schema: std::boxed::Box::new(#attr_schema),
            required: true,
            remainder: std::boxed::Box::new(#remainder),
        }
    }
}

pub fn build_attrs(fields: &[ValidatedField]) -> TokenStream2 {
    let header_schemas = fields.iter().filter(|f| f.form_field.is_header()).fold(
        TokenStream2::new(),
        |ts, field| {
            let item = field.as_attr();

            quote! {
                #ts
                #item,
            }
        },
    );

    let mut attrs =
        fields
            .iter()
            .filter(|f| f.form_field.is_attr())
            .fold(TokenStream2::new(), |ts, f| {
                let attr = f.as_attr();
                quote! {
                    #ts
                    #attr,
                }
            });

    if !attrs.is_empty() || !header_schemas.is_empty() {
        attrs = quote! {
            swim_common::model::schema::StandardSchema::HasAttributes {
                attributes: vec![
                    #header_schemas
                    #attrs
                ],
                exhaustive: true,
            }
        };
    }

    attrs
}
