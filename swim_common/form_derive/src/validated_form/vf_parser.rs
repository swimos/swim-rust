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

use proc_macro2::{Ident, TokenStream};
use quote::ToTokens;
use syn::export::TokenStream2;
use syn::{ExprPath, Lit, Meta, NestedMeta, Type};

use macro_helpers::{CompoundTypeKind, Context, Identity, Symbol};

use crate::form::form_parser::FormDescriptor;
use crate::parser::{
    Attributes, EnumVariant, FieldManifest, FormField, StructRepr, TypeContents, FORM_PATH,
    SCHEMA_PATH, TAG_PATH,
};
use crate::validated_form::attrs::{build_attrs, build_head_attribute};
use crate::validated_form::meta_parse::parse_schema_meta;
use crate::validated_form::range::Range;
use num_bigint::BigInt;

pub const ANYTHING_PATH: Symbol = Symbol("anything");
pub const NOTHING_PATH: Symbol = Symbol("nothing");
pub const NUM_ATTRS_PATH: Symbol = Symbol("num_attrs");
pub const NUM_ITEMS_PATH: Symbol = Symbol("num_items");
pub const OF_KIND_PATH: Symbol = Symbol("of_kind");
pub const EQUAL_PATH: Symbol = Symbol("equal");
pub const TEXT_PATH: Symbol = Symbol("text");
pub const NON_NAN_PATH: Symbol = Symbol("non_nan");
pub const FINITE_PATH: Symbol = Symbol("finite");
pub const INT_RANGE_PATH: Symbol = Symbol("int_range");
pub const UINT_RANGE_PATH: Symbol = Symbol("uint_range");
pub const FLOAT_RANGE_PATH: Symbol = Symbol("float_range");
pub const BIG_INT_RANGE_PATH: Symbol = Symbol("big_int_range");
pub const ALL_ITEMS_PATH: Symbol = Symbol("all_items");
pub const AND_PATH: Symbol = Symbol("and");
pub const OR_PATH: Symbol = Symbol("or");
pub const NOT_PATH: Symbol = Symbol("not");

#[derive(Debug)]
pub struct ValidatedFormDescriptor {
    pub identity: Identity,
    pub schema: StandardSchema,
    pub all_items: bool,
}

impl ValidatedFormDescriptor {
    /// Builds a [`ValidatedFormDescriptor`] for the provided [`DeriveInput`]. An errors encountered
    /// while parsing the [`DeriveInput`] will be added to the [`Context`].
    pub fn from(
        context: &mut Context,
        ident: &Ident,
        attributes: Vec<NestedMeta>,
        kind: CompoundTypeKind,
    ) -> ValidatedFormDescriptor {
        let mut schema_opt = None;
        let mut all_items = false;
        let mut identity = Identity::Named(ident.clone());

        attributes.iter().for_each(|meta: &NestedMeta| match meta {
            NestedMeta::Meta(Meta::NameValue(name)) if name.path == TAG_PATH => match &name.lit {
                Lit::Str(s) => {
                    let tag = s.value();
                    if tag.is_empty() {
                        context.error_spanned_by(meta, "New name cannot be empty")
                    } else {
                        identity = Identity::Renamed {
                            new_identity: tag,
                            old_identity: ident.clone(),
                        }
                    }
                }
                _ => context.error_spanned_by(meta, "Expected string literal"),
            },
            NestedMeta::Meta(Meta::List(list)) if list.path == SCHEMA_PATH => {
                list.nested.iter().for_each(|nested| {
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
                        NestedMeta::Meta(Meta::List(list)) if list.path == ALL_ITEMS_PATH => {
                            if let CompoundTypeKind::Unit = kind {
                                context.error_spanned_by(
                                    list,
                                    "Attribute not supported by unit compound types",
                                )
                            } else {
                                let schema =
                                    parse_schema_meta(StandardSchema::None, context, &list.nested);
                                all_items = true;

                                set_container_schema(
                                    list.to_token_stream(),
                                    &mut schema_opt,
                                    context,
                                    StandardSchema::AllItems(Box::new(schema)),
                                )
                            }
                        }
                        NestedMeta::Meta(Meta::Path(path)) if path == ANYTHING_PATH => {
                            set_container_schema(
                                path.to_token_stream(),
                                &mut schema_opt,
                                context,
                                StandardSchema::Anything,
                            )
                        }
                        NestedMeta::Meta(Meta::Path(path)) if path == NOTHING_PATH => {
                            set_container_schema(
                                path.to_token_stream(),
                                &mut schema_opt,
                                context,
                                StandardSchema::Nothing,
                            )
                        }
                        _ => context.error_spanned_by(meta, "Unknown schema container attribute"),
                    }
                })
            }
            _ => context.error_spanned_by(meta, "Unknown schema container attribute"),
        });

        ValidatedFormDescriptor {
            identity,
            schema: schema_opt.unwrap_or(StandardSchema::None),
            all_items,
        }
    }
}

pub struct ValidatedField<'f> {
    pub form_field: FormField<'f>,
    pub field_schema: StandardSchema,
}

impl<'f> ValidatedField<'f> {
    pub fn as_attr(&self) -> TokenStream2 {
        let ValidatedField {
            form_field,
            field_schema,
        } = self;

        let field_schema = field_schema.to_token_stream();
        let ident = match &form_field.identity {
            Identity::Named(ident) => ident.to_string(),
            Identity::Renamed { new_identity, .. } => new_identity.clone(),
            Identity::Anonymous(_) => {
                // Caught by the form descriptor parser
                unreachable!()
            }
        };

        quote! {
            swim_common::model::schema::FieldSpec::default(
                swim_common::model::schema::attr::AttrSchema::named(
                    #ident,
                    #field_schema,
                )
            )
        }
    }

    pub fn as_item(&self) -> TokenStream2 {
        let ValidatedField {
            form_field,
            field_schema,
        } = self;

        let field_schema = field_schema.to_token_stream();
        let build_named = |name| {
            quote! {
                swim_common::model::schema::ItemSchema::Field(
                    swim_common::model::schema::slot::SlotSchema::new(
                        swim_common::model::schema::StandardSchema::text(#name),
                        #field_schema,
                    )
                )
            }
        };

        match &form_field.identity {
            Identity::Named(ident) => build_named(ident.to_string()),
            Identity::Renamed { new_identity, .. } => build_named(new_identity.to_string()),
            Identity::Anonymous(_) => quote!(
                swim_common::model::schema::ItemSchema::ValueItem(#field_schema)
            ),
        }
    }
}

#[allow(warnings)]
#[derive(Clone, Debug)]
pub enum StandardSchema {
    None,
    /// Uses the implementation on the Type
    Type(TokenStream2),
    Equal(ExprPath),
    /// This field/container should be of the kind provided by the [`TokenStream`]
    OfKind(TokenStream2),
    IntRange(Range<i64>),
    UintRange(Range<u64>),
    FloatRange(Range<f64>),
    BigIntRange(Range<BigInt>),
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
            StandardSchema::Equal(path) => quote! (
                    swim_common::model::schema::StandardSchema::Equal(#path())
            ),
            StandardSchema::OfKind(kind) => {
                quote!(swim_common::model::schema::StandardSchema::OfKind(#kind))
            }
            StandardSchema::IntRange(Range {
                lower,
                upper,
                inclusive,
            }) => {
                if *inclusive {
                    quote!(swim_common::model::schema::StandardSchema::inclusive_int_range(#lower, #upper))
                } else {
                    quote!(swim_common::model::schema::StandardSchema::int_range(#lower, #upper))
                }
            }
            StandardSchema::UintRange(Range {
                lower,
                upper,
                inclusive,
            }) => {
                if *inclusive {
                    quote!(swim_common::model::schema::StandardSchema::inclusive_uint_range(#lower, #upper))
                } else {
                    quote!(swim_common::model::schema::StandardSchema::uint_range(#lower, #upper))
                }
            }
            StandardSchema::FloatRange(Range {
                lower,
                upper,
                inclusive,
            }) => {
                if *inclusive {
                    quote!(swim_common::model::schema::StandardSchema::inclusive_float_range(#lower, #upper))
                } else {
                    quote!(swim_common::model::schema::StandardSchema::float_range(#lower, #upper))
                }
            }
            StandardSchema::BigIntRange(Range {
                lower,
                upper,
                inclusive,
            }) => {
                let lower = {
                    let lower_str = lower.to_string();
                    quote! {
                        std::str::FromStr::from_str(&#lower_str).unwrap()
                    }
                };
                let upper = {
                    let upper_str = upper.to_string();
                    quote! {
                        std::str::FromStr::from_str(&#upper_str).unwrap()
                    }
                };

                if *inclusive {
                    quote!(swim_common::model::schema::StandardSchema::inclusive_big_int_range(#lower, #upper))
                } else {
                    quote!(swim_common::model::schema::StandardSchema::big_int_range(#lower, #upper))
                }
            }
            StandardSchema::NonNan => quote!(swim_common::model::schema::StandardSchema::NonNan),
            StandardSchema::Finite => quote!(swim_common::model::schema::StandardSchema::Finite),
            StandardSchema::Text(text) => {
                quote!(swim_common::model::schema::StandardSchema::text(&#text))
            }
            StandardSchema::Anything => {
                quote!(swim_common::model::schema::StandardSchema::Anything)
            }
            StandardSchema::Nothing => quote!(swim_common::model::schema::StandardSchema::Nothing),
            StandardSchema::DataLength(len) => {
                quote!(swim_common::model::schema::StandardSchema::binary_length(#len))
            }
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
            StandardSchema::AllItems(ts) => {
                quote!(swim_common::model::schema::StandardSchema::AllItems(std::boxed::Box::new(#ts)))
            }
            StandardSchema::None => {
                // no-op as this will be a container schema
                quote!()
            }
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
            let descriptor = ValidatedFormDescriptor::from(ctx, ident, attrs, repr.compound_type);

            StructRepr {
                input: repr.input,
                compound_type: repr.compound_type,
                fields: map_fields_to_validated(&repr.input, ctx, repr.fields, &descriptor),
                manifest: repr.manifest,
                descriptor,
            }
        }),
        TypeContents::Enum(variants) => {
            let variants = variants
                .into_iter()
                .map(|variant| {
                    let attrs = variant.syn_variant.attrs.get_attributes(ctx, FORM_PATH);
                    let descriptor =
                        ValidatedFormDescriptor::from(ctx, ident, attrs, variant.compound_type);

                    EnumVariant {
                        syn_variant: variant.syn_variant,
                        name: variant.name,
                        compound_type: variant.compound_type,
                        fields: map_fields_to_validated(
                            &variant.syn_variant,
                            ctx,
                            variant.fields,
                            &descriptor,
                        ),
                        manifest: variant.manifest,
                        descriptor,
                    }
                })
                .collect();

            TypeContents::Enum(variants)
        }
    }
}

fn map_fields_to_validated<'f, T>(
    loc: &T,
    context: &mut Context,
    fields: Vec<FormField<'f>>,
    descriptor: &ValidatedFormDescriptor,
) -> Vec<ValidatedField<'f>>
where
    T: ToTokens,
{
    if fields.iter().all(FormField::is_skipped) && descriptor.all_items {
        context.error_spanned_by(
            loc,
            "All items schema not valid when all fields are skipped",
        )
    }

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
                            if field.form_field.is_skipped() {
                                context.error_spanned_by(
                                    list,
                                    "Cannot derive a schema for a field that is skipped",
                                )
                            } else {
                                field.field_schema =
                                    parse_schema_meta(StandardSchema::None, context, &list.nested);
                            }
                            field
                        }
                        _ => field,
                    },
                )
        })
        .collect()
}

fn derive_container_schema(
    type_contents: &TypeContents<ValidatedFormDescriptor, ValidatedField>,
) -> TokenStream2 {
    match type_contents {
        TypeContents::Struct(repr) => match &repr.descriptor.schema {
            StandardSchema::AllItems(items) => match &repr.compound_type {
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
        },
        TypeContents::Enum(_variants) => unimplemented!(),
    }
}

fn build_items(fields: &[ValidatedField], descriptor: &FieldManifest) -> TokenStream2 {
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

impl<'t> ToTokens for TypeContents<'t, ValidatedFormDescriptor, ValidatedField<'t>> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        match self {
            TypeContents::Struct(repr) => {
                let attr_schemas = build_attrs(&repr.fields);
                let item_schemas = {
                    match &repr.descriptor.schema {
                        StandardSchema::None => build_items(&repr.fields, &repr.manifest),
                        _ => {
                            let container_schema = derive_container_schema(self);
                            let item_schema = build_items(&repr.fields, &repr.manifest);
                            quote! {
                                swim_common::model::schema::StandardSchema::And(vec![
                                    #container_schema
                                    #item_schema
                                ])
                            }
                        }
                    }
                };

                let remainder = {
                    if attr_schemas.is_empty() {
                        item_schemas
                    } else {
                        quote! {
                            swim_common::model::schema::StandardSchema::And(vec![
                                #attr_schemas,
                                #item_schemas
                            ])
                        }
                    }
                };

                let head_attribute = build_head_attribute(
                    &repr.descriptor.identity,
                    remainder,
                    &repr.fields,
                    &repr.manifest,
                );

                head_attribute.to_tokens(tokens);
            }
            TypeContents::Enum(_variants) => unimplemented!(),
        }
    }
}
