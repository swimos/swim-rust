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

use crate::parser::{
    Attributes, EnumVariant, FormDescriptor, FormField, StructRepr, TypeContents, FORM_PATH,
    VALID_PATH,
};
use macro_helpers::{Context, FieldIdentity};
use proc_macro2::Ident;
use quote::ToTokens;
use syn::export::TokenStream2;
use syn::punctuated::Punctuated;
use syn::{Data, DeriveInput, Fields, Meta, NestedMeta};

pub fn build_validated_form(
    input: DeriveInput,
) -> Result<proc_macro2::TokenStream, Vec<syn::Error>> {
    let mut context = Context::default();
    let descriptor = FormDescriptor::from_ast(&mut context, &input);
    let structure_name = descriptor.name.original_ident.clone();
    let type_contents = match TypeContents::from(&mut context, &input) {
        Some(cont) => type_contents_to_validated(&mut context, cont),
        None => return Err(context.check().unwrap_err()),
    };

    let (impl_generics, ty_generics, where_clause) = &input.generics.split_for_impl();

    let ts = quote! {
        impl #impl_generics swim_common::form::ValidatedForm for #structure_name #ty_generics #where_clause
        {
            #[inline]
            fn schema() -> swim_common::model::schema::StandardSchema {
                swim_common::model::schema::StandardSchema::Anything
            }
        }
    };

    Ok(ts)
}

struct ValidatedField<'f> {
    form_field: FormField<'f>,
    schema: Option<TokenStream2>,
}

fn type_contents_to_validated<'f>(
    ctx: &mut Context,
    type_contents: TypeContents<FormField<'f>>,
) -> TypeContents<ValidatedField<'f>> {
    match type_contents {
        TypeContents::Struct(mut repr) => TypeContents::Struct(StructRepr {
            compound_type: repr.compound_type,
            fields: map_fields_to_validated(ctx, repr.fields),
            manifest: repr.manifest,
        }),
        TypeContents::Enum(variants) => {
            // for each variant, parse the fields
            let variants = variants
                .into_iter()
                .map(|variant| EnumVariant {
                    name: variant.name,
                    compound_type: variant.compound_type,
                    fields: map_fields_to_validated(ctx, variant.fields),
                    manifest: variant.manifest,
                })
                .collect();

            TypeContents::Enum(variants)
        }
    }
}

fn map_fields_to_validated<'f>(
    context: &mut Context,
    fields: Vec<FormField<'f>>,
) -> Vec<ValidatedField<'f>> {
    fields
        .into_iter()
        .map(|form_field| {
            let attrs = form_field.original.attrs.get_attributes(context, FORM_PATH);
            match attrs.first() {
                Some(attr) => match attr {
                    NestedMeta::Meta(Meta::List(list)) if list.path == VALID_PATH => {
                        ValidatedField {
                            schema: Some(list.to_token_stream()),
                            form_field,
                        }
                    }
                    _ => {
                        // Already checked by TypeContents parser
                        unreachable!()
                    }
                },
                None => ValidatedField {
                    form_field,
                    schema: None,
                },
            }
        })
        .collect()
}
