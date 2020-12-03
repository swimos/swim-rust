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

use syn::{DeriveInput, Generics};

use macro_helpers::{CompoundTypeKind, Context};

use crate::form::form_parser::build_type_contents;
use crate::parser::FieldManifest;
use crate::validated_form::attrs::{build_attrs, build_head_attribute};
use crate::validated_form::vf_parser::{
    type_contents_to_validated, StandardSchema, ValidatedField, ValidatedFormDescriptor,
};
use macro_helpers::add_bound;
use macro_helpers::Label;
use macro_helpers::{EnumRepr, TypeContents};
use proc_macro2::TokenStream;
use syn::export::TokenStream2;

mod attrs;
mod meta_parse;
mod vf_parser;

mod range;

// Builds the ValidatedForm implementation from the DeriveInput.
pub fn build_validated_form(
    input: DeriveInput,
) -> Result<proc_macro2::TokenStream, Vec<syn::Error>> {
    let mut context = Context::default();
    let type_contents = match build_type_contents(&mut context, &input) {
        Some(cont) => type_contents_to_validated(&mut context, cont),
        None => return Err(context.check().unwrap_err()),
    };

    context.check()?;

    let generics = build_generics(&type_contents, &input.generics);

    let structure_name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let type_contents = type_contents_to_tokens(&type_contents);

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

fn type_contents_to_tokens(
    type_contents: &TypeContents<'_, ValidatedFormDescriptor, ValidatedField>,
) -> TokenStream {
    match type_contents {
        TypeContents::Struct(repr) => {
            let schema = derive_compound_schema(
                &repr.fields,
                &repr.compound_type,
                &repr.descriptor,
                &repr.descriptor.label,
            );

            quote!(#schema)
        }
        TypeContents::Enum(EnumRepr { variants, .. }) => {
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

            quote! {
                swim_common::model::schema::StandardSchema::Or(vec![
                    #schemas
                ])
            }
        }
    }
}

/// Derives a container schema for the provided `StandardSchema`
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

/// Derives a `StandardSchema::Layout` for the provided fields.
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

/// Derives the final `StandardSchema` for the structure. The final schema asserts the head
/// attribute, the attributes and items for the provided structure or variant.
///
/// * `fields` - the fields in the structure or variant.
/// * `compound_type` - the compound type kind.
/// * `descriptor` - a derived descriptor of the structure.
/// * `ident` - the identifier of the structure or the variant (not the identifier of the
/// enumeration).
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

fn build_generics(
    type_contents: &TypeContents<ValidatedFormDescriptor, ValidatedField>,
    generics: &Generics,
) -> Generics {
    let generics = add_bound(
        type_contents,
        generics,
        |f| !f.form_field.is_skipped(),
        &parse_quote!(swim_common::form::ValidatedForm),
    );

    add_bound(
        type_contents,
        &generics,
        |f| f.form_field.is_skipped(),
        &parse_quote!(std::default::Default),
    )
}
