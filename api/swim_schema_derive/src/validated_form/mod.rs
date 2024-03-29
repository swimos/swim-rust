// Copyright 2015-2023 Swim Inc.
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

use macro_utilities::{CompoundTypeKind, Context};

use crate::form::form_parser::build_type_contents;
use crate::parser::FieldManifest;
use crate::validated_form::attrs::{build_attrs, build_head_attribute};
use crate::validated_form::vf_parser::{
    type_contents_to_validated, StandardSchema, ValidatedField, ValueSchemaDescriptor,
};
use macro_utilities::add_bound;
use macro_utilities::Label;
use macro_utilities::{EnumRepr, TypeContents};
use proc_macro2::TokenStream;
use proc_macro2::TokenStream as TokenStream2;

mod attrs;
mod meta_parse;
mod vf_parser;

mod range;

// Builds the ValueSchema implementation from the DeriveInput.
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
        impl #impl_generics swim_schema::ValueSchema for #structure_name #ty_generics #where_clause
        {
            fn schema() -> swim_schema::schema::StandardSchema {
                #type_contents
            }
        }
    };

    Ok(ts)
}

fn type_contents_to_tokens(
    type_contents: &TypeContents<'_, ValueSchemaDescriptor, ValidatedField>,
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
                swim_schema::schema::StandardSchema::Or(vec![
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
            CompoundTypeKind::Labelled => {
                quote! {
                    swim_schema::schema::StandardSchema::AllItems(
                        std::boxed::Box::new(
                            swim_schema::schema::ItemSchema::Field(
                                swim_schema::schema::slot::SlotSchema::new(
                                    swim_schema::schema::StandardSchema::Anything,
                                    #items,
                                )
                            )
                        )
                    ),
                }
            }
            CompoundTypeKind::Tuple | CompoundTypeKind::NewType => {
                quote! {
                    swim_schema::schema::StandardSchema::AllItems(
                        std::boxed::Box::new(
                            swim_schema::schema::ItemSchema::ValueItem(#items)
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

    if descriptor.replaces_body {
        // The descriptor is built from the fields, so this cannot fail.
        let field = fields.iter().find(|f| f.form_field.is_body()).unwrap();
        let ty = &field.form_field.original.ty;

        return quote!(<#ty as swim_schema::ValueSchema>::schema());
    }

    let mut schemas = fields.iter().fold(TokenStream2::new(), |ts, field| {
        let item = field.as_item();

        quote! {
            #ts
            (#item, true),
        }
    });

    schemas = quote! {
        swim_schema::schema::StandardSchema::Layout {
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
    descriptor: &ValueSchemaDescriptor,
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
                    swim_schema::schema::StandardSchema::And(vec![
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
            swim_schema::schema::StandardSchema::And(vec![
                #attr_schemas,
                #item_schemas
            ])
        }
    };

    build_head_attribute(ident, remainder, fields, manifest)
}

fn build_generics(
    type_contents: &TypeContents<ValueSchemaDescriptor, ValidatedField>,
    generics: &Generics,
) -> Generics {
    let generics = add_bound(
        type_contents,
        generics,
        |f| !f.form_field.is_skipped(),
        &parse_quote!(swim_schema::ValueSchema),
    );

    add_bound(
        type_contents,
        &generics,
        |f| f.form_field.is_skipped(),
        &parse_quote!(std::default::Default),
    )
}
