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

use crate::form::form_parser::FormDescriptor;
use crate::parser::{
    Attributes, EnumVariant, FormField, StructRepr, TypeContents, FORM_PATH, SCHEMA_PATH, TAG_PATH,
};
use macro_helpers::{Context, Identity, Symbol};
use proc_macro2::Ident;
use quote::ToTokens;
use syn::export::TokenStream2;
use syn::{Lit, Meta, MetaNameValue, NestedMeta};

pub const ANYTHING_PATH: Symbol = Symbol("anything");
pub const NOTHING_PATH: Symbol = Symbol("nothing");
pub const NUM_ATTRS_PATH: Symbol = Symbol("num_attrs");
pub const NUM_ITEMS_PATH: Symbol = Symbol("num_items");
pub const ALL_ITEMS_PATH: Symbol = Symbol("all_items");

pub struct ValidatedFormDescriptor {
    pub body_replaced: bool,
    pub name: Identity,
    pub container_schema: Option<ContainerSchema>,
    pub num_items: Option<usize>,
    pub num_attrs: Option<usize>,
}

impl ValidatedFormDescriptor {
    /// Builds a [`ValidatedFormDescriptor`] for the provided [`DeriveInput`]. An errors encountered
    /// while parsing the [`DeriveInput`] will be added to the [`Context`].
    pub fn from_attributes(
        context: &mut Context,
        ident: &Ident,
        attributes: Vec<NestedMeta>,
    ) -> ValidatedFormDescriptor {
        let mut desc = ValidatedFormDescriptor {
            body_replaced: false,
            name: Identity::Named(ident.clone()),
            container_schema: None,
            num_items: None,
            num_attrs: None,
        };

        attributes.iter().for_each(|meta: &NestedMeta| match meta {
            NestedMeta::Meta(Meta::NameValue(name)) if name.path == TAG_PATH => match &name.lit {
                Lit::Str(s) => {
                    let tag = s.value();
                    if tag.is_empty() {
                        context.error_spanned_by(meta, "New name cannot be empty")
                    } else {
                        desc.name = Identity::Renamed {
                            new_identity: tag,
                            old_identity: ident.clone(),
                        }
                    }
                }
                _ => context.error_spanned_by(meta, "Expected string argument"),
            },
            NestedMeta::Meta(Meta::List(list)) if list.path == SCHEMA_PATH => {
                list.nested.iter().for_each(|nested| {
                    let set_int_opt =
                        |name: &MetaNameValue, target: &mut Option<usize>, ctx: &mut Context| {
                            match &name.lit {
                                Lit::Int(int) => match int.base10_parse::<usize>() {
                                    Ok(i) => match target {
                                        Some(_) => {
                                            ctx.error_spanned_by(int, "Duplicate schema definition")
                                        }
                                        None => *target = Some(i),
                                    },
                                    Err(e) => ctx.error_spanned_by(name, e.to_string()),
                                },
                                _ => ctx.error_spanned_by(name, "Expected an integer"),
                            }
                        };

                    let set_container_schema =
                        |path,
                         target: &mut Option<ContainerSchema>,
                         ctx: &mut Context,
                         schema: ContainerSchema| {
                            match target {
                                Some(_) => ctx.error_spanned_by(
                                    path,
                                    "Container schema has already been applied",
                                ),
                                None => {
                                    *target = Some(schema);
                                }
                            }
                        };

                    match nested {
                        NestedMeta::Meta(Meta::NameValue(name)) if name.path == NUM_ATTRS_PATH => {
                            set_int_opt(name, &mut desc.num_attrs, context);
                        }
                        NestedMeta::Meta(Meta::NameValue(name)) if name.path == NUM_ITEMS_PATH => {
                            set_int_opt(name, &mut desc.num_items, context);
                        }
                        NestedMeta::Meta(Meta::NameValue(name)) if name.path == ALL_ITEMS_PATH => {
                            set_container_schema(
                                name.to_token_stream(),
                                &mut desc.container_schema,
                                context,
                                ContainerSchema::AllItems,
                            )
                        }
                        NestedMeta::Meta(Meta::Path(path)) if path == ANYTHING_PATH => {
                            set_container_schema(
                                path.to_token_stream(),
                                &mut desc.container_schema,
                                context,
                                ContainerSchema::Anything,
                            )
                        }
                        NestedMeta::Meta(Meta::Path(path)) if path == NOTHING_PATH => {
                            set_container_schema(
                                path.to_token_stream(),
                                &mut desc.container_schema,
                                context,
                                ContainerSchema::Nothing,
                            )
                        }
                        _ => context.error_spanned_by(meta, "Unknown schema container attribute"),
                    }
                })
            }
            _ => context.error_spanned_by(meta, "Unknown schema container attribute"),
        });

        desc
    }
}

pub enum ContainerSchema {
    AllItems,
    Anything,
    Nothing,
}

pub fn derive_head_attribute(
    type_contents: &TypeContents<ValidatedFormDescriptor, ValidatedField>,
) -> TokenStream2 {
    match type_contents {
        TypeContents::Struct(repr) => {
            let ident = repr.descriptor.name.to_string();
            quote! {
                StandardSchema::HeadAttribute {
                    schema: Box::new(AttrSchema::named(
                        #ident,
                        // todo: header body check here
                        StandardSchema::OfKind(ValueKind::Extant),
                    )),
                    required: true,
                    remainder: Box::new(StandardSchema::Anything),
                }
            }
        }
        TypeContents::Enum(_variants) => unimplemented!(),
    }
}

pub struct ValidatedField<'f> {
    form_field: FormField<'f>,
    schema: Option<TokenStream2>,
}

pub fn type_contents_to_validated<'f>(
    ctx: &mut Context,
    ident: &Ident,
    type_contents: TypeContents<'f, FormDescriptor, FormField<'f>>,
) -> TypeContents<'f, ValidatedFormDescriptor, ValidatedField<'f>> {
    match type_contents {
        TypeContents::Struct(repr) => TypeContents::Struct({
            let attrs = repr.input.attrs.get_attributes(ctx, FORM_PATH);

            StructRepr {
                input: repr.input,
                compound_type: repr.compound_type,
                fields: map_fields_to_validated(ctx, repr.fields),
                manifest: repr.manifest,
                descriptor: ValidatedFormDescriptor::from_attributes(ctx, ident, attrs),
            }
        }),
        TypeContents::Enum(variants) => {
            // for each variant, parse the fields
            let variants = variants
                .into_iter()
                .map(|variant| {
                    let attrs = variant.syn_variant.attrs.get_attributes(ctx, FORM_PATH);

                    EnumVariant {
                        syn_variant: variant.syn_variant,
                        name: variant.name,
                        compound_type: variant.compound_type,
                        fields: map_fields_to_validated(ctx, variant.fields),
                        manifest: variant.manifest,
                        descriptor: ValidatedFormDescriptor::from_attributes(ctx, ident, attrs),
                    }
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
                    NestedMeta::Meta(Meta::List(list)) if list.path == SCHEMA_PATH => {
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
