// Copyright 2015-2021 SWIM.AI inc.
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

use proc_macro2::TokenStream as TokenStream2;

use crate::parser::FieldManifest;
use crate::validated_form::vf_parser::{StandardSchema, ValidatedField};
use macro_helpers::Label;

/// Builds a `StandardSchema::HeadAttribute` schema for the compound type.
///
/// * `compound_label` - the identifier of the structure or the variant (not the identifier of the
/// enumeration).
/// * `remainder` - the remaining schema for the compound type. This could be a
/// `StandardSchema::Layout` or similar.
/// * `fields` - the fields in the structure or variant.
/// * `manifest` - the `FieldManifest` for the structure or variant.
pub fn build_head_attribute(
    compound_label: &Label,
    remainder: TokenStream2,
    fields: &[ValidatedField],
    manifest: &FieldManifest,
) -> TokenStream2 {
    let header_body_opt = fields
        .iter()
        .filter(|f| f.form_field.is_header_body())
        .collect::<Vec<_>>();

    let header_schemas = fields
        .iter()
        .filter(|f| f.form_field.is_header() || (f.form_field.is_slot() && manifest.replaces_body))
        .fold(TokenStream2::new(), |ts, field| {
            let item = field.as_item();

            quote! {
                #ts
                (#item, true),
            }
        });

    let tag_value_schema = match header_body_opt.first() {
        Some(field) => {
            let schema = &field.field_schema;
            if !header_schemas.is_empty() {
                quote! {
                    swim_common::model::schema::StandardSchema::Layout {
                        items: vec![
                            (swim_common::model::schema::ItemSchema::ValueItem(#schema), true),
                            #header_schemas
                        ],
                        exhaustive: true
                    }
                }
            } else {
                quote!(#schema)
            }
        }
        None => {
            if !header_schemas.is_empty() {
                quote! {
                    swim_common::model::schema::StandardSchema::Layout {
                        items: vec![
                            #header_schemas
                        ],
                        exhaustive: true
                    }
                }
            } else {
                let schema = StandardSchema::OfKind(quote!(swim_common::model::ValueKind::Extant));
                quote!(#schema)
            }
        }
    };

    let attr_schema = match compound_label {
        Label::Foreign(_new_ident, ty, _old_ident) => {
            quote! {
                {
                    let enumerated = <#ty as swim_common::form::structural::StringRepresentable>::universe();
                    let len = enumerated.len();

                    let attr_schemas = enumerated.into_iter().fold(Vec::with_capacity(len), |mut vec, variant| {
                        let schema = swim_common::model::schema::text::TextSchema::exact(variant);
                        vec.push(schema);
                        vec
                    });

                    swim_common::model::schema::attr::AttrSchema::new(swim_common::model::schema::text::TextSchema::Or(attr_schemas), #tag_value_schema)
                }
            }
        }
        l => {
            let name = l.to_name(false);

            quote! {
                swim_common::model::schema::attr::AttrSchema::named(
                    #name,
                    #tag_value_schema,
                )
            }
        }
    };

    quote! {
        swim_common::model::schema::StandardSchema::HeadAttribute {
            schema: std::boxed::Box::new(#attr_schema),
            required: true,
            remainder: std::boxed::Box::new(#remainder),
        }
    }
}

/// Builds a `StandardSchema::HasAttributes` schema for the fields.
pub fn build_attrs(fields: &[ValidatedField]) -> TokenStream2 {
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

    if !attrs.is_empty() {
        attrs = quote! {
            swim_common::model::schema::StandardSchema::HasAttributes {
                attributes: vec![
                    #attrs
                ],
                exhaustive: true,
            }
        };
    }

    attrs
}
