// Copyright 2015-2021 Swim Inc.
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

use crate::parser::{parse_struct, FieldManifest, FORM_PATH, SCHEMA_PATH, TAG_PATH};
use macro_utilities::Label;
use macro_utilities::{Attributes, Context, StructureKind};
use macro_utilities::{EnumRepr, EnumVariant, FormField, StructRepr, TypeContents};
use proc_macro2::Ident;
use syn::{Data, Lit, Meta, NestedMeta};

/// Build a [`TypeContents`] input from an abstract syntax tree. Returns [`Option::None`] if
/// there was an error that was encountered while parsing the tree. The underlying error is
/// added to the [`Context]`. If [`derive_valid`] is `true`, then [`[form(valid(..))]`]
/// attributes are parsed into the [`TypeContents`] representation.
pub fn build_type_contents<'t>(
    context: &mut Context,
    input: &'t syn::DeriveInput,
) -> Option<TypeContents<'t, FormDescriptor, FormField<'t>>> {
    let type_contents = match &input.data {
        Data::Enum(data) => {
            if !input.attrs.get_attributes(context, FORM_PATH).is_empty() {
                context.error_spanned_by(input, "Tags are only supported on enumeration variants.");
                return None;
            }

            let variants = data
                .variants
                .iter()
                .map(|variant| {
                    let attributes = variant.attrs.get_attributes(context, FORM_PATH);
                    let mut container_label =
                        parse_container_tag(context, &variant.ident, attributes);
                    let (compound_type, fields, manifest) = parse_struct(
                        context,
                        &variant.fields,
                        &mut container_label,
                        StructureKind::Enum,
                    );
                    let descriptor = FormDescriptor::from(container_label, manifest);

                    EnumVariant {
                        syn_variant: variant,
                        name: descriptor.label.clone(),
                        compound_type,
                        fields,
                        descriptor,
                    }
                })
                .collect();

            TypeContents::Enum(EnumRepr { input, variants })
        }
        Data::Struct(data) => {
            let attributes = input.attrs.get_attributes(context, FORM_PATH);
            let mut container_label = parse_container_tag(context, &input.ident, attributes);
            let (compound_type, fields, manifest) = parse_struct(
                context,
                &data.fields,
                &mut container_label,
                StructureKind::Struct,
            );
            let descriptor = FormDescriptor::from(container_label, manifest);

            TypeContents::Struct(StructRepr {
                input,
                compound_type,
                fields,
                descriptor,
            })
        }
        Data::Union(_) => {
            context.error_spanned_by(input, "Unions are not supported");
            return None;
        }
    };

    Some(type_contents)
}

/// A [`FormDescriptor`] is a representation of a [`Form`] that is built from a [`DeriveInput`],
/// containing the name of the compound type that it represents and whether or not the body of the
/// produced [`Value`] is replaced. A field annotated with [`[form(body)]` will cause this to
/// happen. A compound type annotated with [`[form(tag = "name")]` will set the structure's output
/// value to be replaced with the provided literal.
#[derive(Clone, Debug)]
pub struct FormDescriptor {
    /// Denotes whether or not the body of the produced record is replaced by a field in the
    /// compound type.
    pub body_replaced: bool,
    /// The name that the compound type will be transmuted with.
    pub label: Label,
    /// A derived [`FieldManifest`] from the attributes on the members.
    pub manifest: FieldManifest,
}

pub fn parse_container_tag(
    context: &mut Context,
    ident: &Ident,
    attributes: Vec<NestedMeta>,
) -> Label {
    let mut name_opt = None;

    attributes.iter().for_each(|meta: &NestedMeta| match meta {
        NestedMeta::Meta(Meta::NameValue(name)) if name.path == TAG_PATH => match &name.lit {
            Lit::Str(s) => {
                let tag = s.value();
                if tag.is_empty() {
                    context.error_spanned_by(meta, "New tag cannot be empty")
                } else {
                    match name_opt {
                        Some(_) => context.error_spanned_by(s, "Duplicate tag"),
                        None => {
                            name_opt = Some(Label::Renamed {
                                new_label: tag,
                                old_label: ident.clone(),
                            });
                        }
                    }
                }
            }
            _ => context.error_spanned_by(meta, "Expected string argument"),
        },
        NestedMeta::Meta(Meta::List(list)) if list.path == SCHEMA_PATH => {
            // handled by the validated form derive
        }
        _ => context.error_spanned_by(meta, "Unknown container attribute"),
    });

    name_opt.unwrap_or_else(|| Label::Unmodified(ident.clone()))
}

impl FormDescriptor {
    pub fn from(label: Label, manifest: FieldManifest) -> FormDescriptor {
        FormDescriptor {
            body_replaced: false,
            label,
            manifest,
        }
    }
}
