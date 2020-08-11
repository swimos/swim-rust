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

use std::fmt::Debug;

use proc_macro2::{Ident, TokenStream};
use quote::ToTokens;
use syn::export::TokenStream2;
use syn::{Lit, Meta, MetaNameValue, NestedMeta, Type};

use macro_helpers::{Context, Identity, Symbol};

use crate::form::form_parser::FormDescriptor;
use crate::parser::{
    Attributes, EnumVariant, FormField, StructRepr, TypeContents, FORM_PATH, SCHEMA_PATH, TAG_PATH,
};
use crate::validated_form::meta_parse::parse_schema_meta;

pub const ANYTHING_PATH: Symbol = Symbol("anything");
pub const NOTHING_PATH: Symbol = Symbol("nothing");
pub const NUM_ATTRS_PATH: Symbol = Symbol("num_attrs");
pub const NUM_ITEMS_PATH: Symbol = Symbol("num_items");
pub const ALL_ITEMS_PATH: Symbol = Symbol("all_items");
pub const AND_PATH: Symbol = Symbol("and");
pub const OR_PATH: Symbol = Symbol("or");
pub const NOT_PATH: Symbol = Symbol("not");

#[derive(Debug)]
pub struct ValidatedFormDescriptor {
    pub body_replaced: bool,
    pub name: Identity,
    pub schema: Option<StandardSchema>,
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
            schema: None,
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
                         target: &mut Option<StandardSchema>,
                         ctx: &mut Context,
                         schema: StandardSchema| {
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
                        NestedMeta::Meta(Meta::List(list)) if list.path == ALL_ITEMS_PATH => {
                            let schema =
                                parse_schema_meta(StandardSchema::None, context, &list.nested);

                            set_container_schema(
                                list.to_token_stream(),
                                &mut desc.schema,
                                context,
                                StandardSchema::AllItems(Box::new(schema)),
                            )
                        }
                        NestedMeta::Meta(Meta::Path(path)) if path == ANYTHING_PATH => {
                            set_container_schema(
                                path.to_token_stream(),
                                &mut desc.schema,
                                context,
                                StandardSchema::Anything,
                            )
                        }
                        NestedMeta::Meta(Meta::Path(path)) if path == NOTHING_PATH => {
                            set_container_schema(
                                path.to_token_stream(),
                                &mut desc.schema,
                                context,
                                StandardSchema::Nothing,
                            )
                        }
                        _ => context.error_spanned_by(
                            meta,
                            "Unknown or malformatted schema container attribute",
                        ),
                    }
                })
            }
            _ => {
                context.error_spanned_by(meta, "Unknown or malformatted schema container attribute")
            }
        });

        desc
    }
}

fn derive_head_attribute(
    type_contents: &TypeContents<ValidatedFormDescriptor, ValidatedField>,
) -> TokenStream2 {
    match type_contents {
        TypeContents::Struct(repr) => {
            let ident = repr.descriptor.name.to_string();
            quote! {
                swim_common::model::schema::StandardSchema::HeadAttribute {
                    schema: std::boxed::Box::new(swim_common::model::schema::attr::AttrSchema::named(
                        #ident,
                        // todo: header body check here
                        swim_common::model::schema::StandardSchema::OfKind(swim_common::model::ValueKind::Extant),
                    )),
                    required: true,
                    remainder: std::boxed::Box::new(swim_common::model::schema::StandardSchema::Anything),
                }
            }
        }
        TypeContents::Enum(_variants) => unimplemented!(),
    }
}

pub struct ValidatedField<'f> {
    pub form_field: FormField<'f>,
    pub field_schema: StandardSchema,
}

impl<'f> ToTokens for ValidatedField<'f> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        // todo: Create fieldspecs from field identities
        let field_schema = self.field_schema.to_token_stream();
        match &self.form_field.identity {
            Identity::Named(_ident) => {}
            Identity::Renamed { .. } => {}
            _anon @ Identity::Anonymous(_) => {}
        }
        field_schema.to_tokens(tokens);
    }
}

#[derive(Debug)]
#[allow(warnings)]
pub enum StandardSchema {
    None,
    /// Uses the implementation on the Type
    Type(TokenStream2),
    /// This field/container should be equal to the provided [`TokenStream`]
    Equal(TokenStream2),
    /// This field/container should be of the kind provided by the [`TokenStream`]
    OfKind(TokenStream2),
    IntRange,
    UintRange,
    FloatRange,
    BigIntRange,
    NonNan,
    Finite,
    Text(String),
    Anything,
    Nothing,
    DataLength(usize),
    NumAttrs(usize),
    NumItems(usize),
    And(Vec<StandardSchema>),
    Or(Vec<StandardSchema>),
    Not(Box<StandardSchema>),
    AllItems(Box<StandardSchema>),
}

impl ToTokens for StandardSchema {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let quote = match self {
            StandardSchema::Type(ty) => quote!(#ty::schema()),
            StandardSchema::Equal(_) => unimplemented!(),
            StandardSchema::OfKind(_) => unimplemented!(),
            StandardSchema::IntRange => unimplemented!(),
            StandardSchema::UintRange => unimplemented!(),
            StandardSchema::FloatRange => unimplemented!(),
            StandardSchema::BigIntRange => unimplemented!(),
            StandardSchema::NonNan => unimplemented!(),
            StandardSchema::Finite => unimplemented!(),
            StandardSchema::Text(_) => unimplemented!(),
            StandardSchema::Anything => {
                quote!(swim_common::model::schema::StandardSchema::Anything)
            }
            StandardSchema::Nothing => quote!(swim_common::model::schema::StandardSchema::Nothing),
            StandardSchema::DataLength(_) => unimplemented!(),
            StandardSchema::NumAttrs(num) => {
                quote!(swim_common::model::schema::StandardSchema::NumAttrs(#num))
            }
            StandardSchema::NumItems(num) => {
                quote!(swim_common::model::schema::StandardSchema::NumItems(#num))
            }
            StandardSchema::And(and_schema) => quote!(
                swim_common::model::schema::StandardSchema::And(vec![#(#and_schema,)*])
            ),
            StandardSchema::Or(or_schema) => quote!(
                swim_common::model::schema::StandardSchema::Or(vec![#(#or_schema,)*])
            ),
            StandardSchema::Not(not_schema) => quote!(
                swim_common::model::schema::StandardSchema::Not(std::boxed::Box::new(#not_schema))
            ),
            StandardSchema::AllItems(_) => unimplemented!(),
            StandardSchema::None => unimplemented!(),
        };

        quote.to_tokens(tokens);
    }
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
            let initial_schema = match &form_field.original.ty {
                Type::Path(path) => StandardSchema::Type(path.to_token_stream()),
                ty => {
                    context.error_spanned_by(
                        ty,
                        format!("Unsupported type: {}", ty.to_token_stream().to_string()),
                    );
                    StandardSchema::None
                }
            };

            form_field
                .original
                .attrs
                .get_attributes(context, FORM_PATH)
                .iter()
                .fold(
                    ValidatedField {
                        field_schema: initial_schema,
                        form_field,
                    },
                    |mut field, attr| match attr {
                        NestedMeta::Meta(Meta::List(list)) if list.path == SCHEMA_PATH => {
                            field.field_schema =
                                parse_schema_meta(StandardSchema::None, context, &list.nested);
                            field
                        }
                        _ => field,
                    },
                )
        })
        .collect()
}

impl<'t> ToTokens for TypeContents<'t, ValidatedFormDescriptor, ValidatedField<'t>> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        match self {
            TypeContents::Struct(repr) => {
                let head_attribute = derive_head_attribute(&self);
                let schemas = repr.fields.iter().fold(TokenStream2::new(), |ts, field| {
                    quote! {
                        #ts
                        #field,
                    }
                });

                let quote = quote! {
                    swim_common::model::schema::StandardSchema::And(vec![
                        #head_attribute,
                        #schemas
                    ])
                };

                quote.to_tokens(tokens);
            }
            TypeContents::Enum(_variants) => unimplemented!(),
        }
    }
}
