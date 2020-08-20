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

use macro_helpers::{CompoundTypeKind, Context, Label};

use crate::form::form_parser::build_type_contents;
use crate::parser::{FieldManifest, TypeContents};
use crate::validated_form::attrs::{build_attrs, build_head_attribute};
use crate::validated_form::vf_parser::{
    type_contents_to_validated, StandardSchema, ValidatedField, ValidatedFormDescriptor,
};
use proc_macro2::TokenStream;
use quote::ToTokens;
use syn::export::TokenStream2;

mod attrs;
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

    let ts = quote! {
        impl #impl_generics swim_common::form::ValidatedForm for #structure_name #ty_generics #where_clause
        {
            fn schema() -> swim_common::model::schema::StandardSchema {
                #type_contents
            }
        }
    };

    Ok(ts)
}

fn derive_container_schema(
    compound_type: &CompoundTypeKind,
    container_schema: &StandardSchema,
) -> TokenStream2 {
    match &container_schema {
        StandardSchema::AllItems(items) => match compound_type {
            CompoundTypeKind::Struct => {
                quote! {
                    swim_common::model::schema::StandardSchema::AllItems(
                        std::boxed::Box::new(
                            swim_common::model::schema::ItemSchema::Field(
                                swim_common::model::schema::slot::SlotSchema::new(
                                    swim_common::model::schema::StandardSchema::Anything,
                                    #items,
                                )
                            )
                        )
                    ),
                }
            }
            CompoundTypeKind::Tuple | CompoundTypeKind::NewType => {
                quote! {
                    swim_common::model::schema::StandardSchema::AllItems(
                        std::boxed::Box::new(
                            swim_common::model::schema::ItemSchema::ValueItem(#items)
                        )
                    ),
                }
            }
            CompoundTypeKind::Unit => {
                // Caught during the attribute parsing
                unreachable!()
            }
        },
        schema => quote!(#schema,),
    }
}

fn derive_items(fields: &[ValidatedField], descriptor: &FieldManifest) -> TokenStream2 {
    let fields: Vec<&ValidatedField> = if descriptor.replaces_body {
        fields.iter().filter(|f| f.form_field.is_body()).collect()
    } else {
        fields
            .iter()
            .filter(|f| f.form_field.is_slot() || f.form_field.is_body())
            .collect()
    };

    let mut schemas = fields.iter().fold(TokenStream2::new(), |ts, field| {
        if field.form_field.is_body() {
            let schema = &field.field_schema;
            quote!((swim_common::model::schema::ItemSchema::ValueItem(#schema), true))
        } else {
            let item = field.as_item();

            quote! {
                #ts
                (#item, true),
            }
        }
    });

    schemas = quote! {
        swim_common::model::schema::StandardSchema::Layout {
            items: vec![
                #schemas
            ],
            exhaustive: true
        }
    };

    schemas
}

fn derive_compound_schema(
    fields: &[ValidatedField],
    compound_type: &CompoundTypeKind,
    descriptor: &ValidatedFormDescriptor,
    ident: &Label,
) -> TokenStream2 {
    let manifest = &descriptor.form_descriptor.manifest;
    let attr_schemas = build_attrs(fields);

    let item_schemas = {
        match &descriptor.schema {
            StandardSchema::None => derive_items(fields, manifest),
            _ => {
                let container_schema = derive_container_schema(compound_type, &descriptor.schema);
                let item_schema = derive_items(fields, manifest);

                quote! {
                    swim_common::model::schema::StandardSchema::And(vec![
                        #container_schema
                        #item_schema
                    ])
                }
            }
        }
    };

    let remainder = if attr_schemas.is_empty() {
        item_schemas
    } else {
        quote! {
            swim_common::model::schema::StandardSchema::And(vec![
                #attr_schemas,
                #item_schemas
            ])
        }
    };

    build_head_attribute(ident, remainder, fields, manifest)
}

impl<'t> ToTokens for TypeContents<'t, ValidatedFormDescriptor, ValidatedField<'t>> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        match self {
            TypeContents::Struct(repr) => {
                let schema = derive_compound_schema(
                    &repr.fields,
                    &repr.compound_type,
                    &repr.descriptor,
                    &repr.descriptor.identity,
                );

                schema.to_tokens(tokens);
            }
            TypeContents::Enum(variants) => {
                let schemas = variants.iter().fold(TokenStream2::new(), |ts, variant| {
                    let schema = derive_compound_schema(
                        &variant.fields,
                        &variant.compound_type,
                        &variant.descriptor,
                        &variant.name,
                    );

                    quote! {
                        #ts
                        #schema,
                    }
                });

                let schemas = quote! {
                    swim_common::model::schema::StandardSchema::Or(vec![
                        #schemas
                    ])
                };

                schemas.to_tokens(tokens);
            }
        }
    }
}
