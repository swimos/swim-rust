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

use num_bigint::BigInt;
use proc_macro2::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::ToTokens;
use syn::{ExprPath, Field, Meta, NestedMeta, Type};

use macro_helpers::{Attributes, CompoundTypeKind, Context, Symbol, SynOriginal};

use crate::form::form_parser::FormDescriptor;
use crate::parser::{FORM_PATH, SCHEMA_PATH, TAG_PATH};
use crate::validated_form::meta_parse::parse_schema_meta;
use crate::validated_form::range::Range;
use macro_helpers::Label;
use macro_helpers::{EnumRepr, EnumVariant, FormField, StructRepr, TypeContents};

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
    pub label: Label,
    pub schema: StandardSchema,
    pub all_items: bool,
    pub form_descriptor: FormDescriptor,
}

impl ValidatedFormDescriptor {
    /// Builds a [`ValidatedFormDescriptor`] for the provided [`DeriveInput`]. An errors encountered
    /// while parsing the [`DeriveInput`] will be added to the [`Context`].
    pub fn from(
        context: &mut Context,
        attributes: Vec<NestedMeta>,
        kind: CompoundTypeKind,
        form_descriptor: FormDescriptor,
    ) -> ValidatedFormDescriptor {
        let mut schema_opt = None;
        let mut all_items = false;

        attributes.iter().for_each(|meta: &NestedMeta| match meta {
            NestedMeta::Meta(Meta::NameValue(name)) if name.path == TAG_PATH => {
                // no-op
            }
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
            label: form_descriptor.label.clone(),
            schema: schema_opt.unwrap_or(StandardSchema::None),
            all_items,
            form_descriptor,
        }
    }
}

pub struct ValidatedField<'f> {
    pub form_field: FormField<'f>,
    pub field_schema: StandardSchema,
}

impl<'f> SynOriginal for ValidatedField<'f> {
    fn original(&self) -> &Field {
        &self.form_field.original
    }
}

impl<'f> ValidatedField<'f> {
    /// Writes this field into a `TokenStream2` as an attribute.
    pub fn as_attr(&self) -> TokenStream2 {
        let ValidatedField {
            form_field,
            field_schema,
        } = self;

        let field_schema = field_schema.to_token_stream();

        let ident = match &form_field.label {
            Label::Unmodified(ident) => ident.to_string(),
            Label::Renamed { new_label, .. } => new_label.clone(),
            _ => {
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

    /// Writes this field into a `TokenStream2` as an item. If this field is named then a
    /// `Slot` is derived, otherwise, a `ValueItem` is derived.
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

        match &form_field.label {
            Label::Unmodified(ident) => build_named(ident.to_string()),
            Label::Renamed { new_label, .. } => build_named(new_label.to_string()),
            Label::Anonymous(_) => quote!(
                swim_common::model::schema::ItemSchema::ValueItem(#field_schema)
            ),
            Label::Foreign(..) => unreachable!("Attempted to derive a tag as an item"),
        }
    }
}

/// A representation used derive to `swim_common::model::schema::StandardSchema`. Where fields are
/// their AST representation.
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

impl StandardSchema {
    pub fn is_assigned(&self) -> bool {
        !matches!(self, StandardSchema::None | StandardSchema::Type(_))
    }
}

impl ToTokens for StandardSchema {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let quote = match self {
            StandardSchema::Type(ty) => quote!(#ty::schema()),
            StandardSchema::Equal(path) => quote!(
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

/// Converts between `TypeContents<'f, FormDescriptor, FormField<'f>>` and
/// `TypeContents<'f, ValidatedFormDescriptor, ValidatedField<'f>>`. Parsing any attributes of the
/// path `#[form(schema(..))]`.
pub fn type_contents_to_validated<'f>(
    ctx: &mut Context,
    type_contents: TypeContents<'f, FormDescriptor, FormField<'f>>,
) -> TypeContents<'f, ValidatedFormDescriptor, ValidatedField<'f>> {
    match type_contents {
        TypeContents::Struct(repr) => TypeContents::Struct({
            let attrs = repr.input.attrs.get_attributes(ctx, FORM_PATH);
            let descriptor =
                ValidatedFormDescriptor::from(ctx, attrs, repr.compound_type, repr.descriptor);

            StructRepr {
                input: repr.input,
                compound_type: repr.compound_type,
                fields: map_fields_to_validated(&repr.input, ctx, repr.fields, &descriptor),
                descriptor,
            }
        }),
        TypeContents::Enum(EnumRepr { input, variants }) => {
            let variants = variants
                .into_iter()
                .map(|variant| {
                    let attrs = variant.syn_variant.attrs.get_attributes(ctx, FORM_PATH);
                    let descriptor = ValidatedFormDescriptor::from(
                        ctx,
                        attrs,
                        variant.compound_type,
                        variant.descriptor,
                    );

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
                        descriptor,
                    }
                })
                .collect();

            TypeContents::Enum(EnumRepr { input, variants })
        }
    }
}

/// Maps `FormField`s to `ValidatedField` and parses any attributes of the path
/// `#[form(schema(..))]`.
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
                Type::Path(path) => {
                    let path = quote! {
                        <#path as swim_common::form::ValidatedForm>
                    };

                    StandardSchema::Type(path)
                }
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
                    |mut field, attr| {
                        let validated_field = match attr {
                            NestedMeta::Meta(Meta::List(list)) if list.path == SCHEMA_PATH => {
                                if field.form_field.is_skipped() {
                                    context.error_spanned_by(
                                        list,
                                        "Cannot derive a schema for a field that is skipped",
                                    )
                                } else {
                                    field.field_schema = parse_schema_meta(
                                        StandardSchema::None,
                                        context,
                                        &list.nested,
                                    );
                                }
                                field
                            }
                            _ => field,
                        };

                        if validated_field.form_field.is_body()
                            && validated_field.field_schema.is_assigned()
                        {
                            context.error_spanned_by(
                                validated_field.form_field.original,
                                "Fields marked as #[form(body)] cannot contain a schema",
                            );
                        }

                        validated_field
                    },
                )
        })
        .collect()
}
