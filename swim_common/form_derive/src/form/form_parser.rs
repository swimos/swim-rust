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
    parse_struct, Attributes, EnumVariant, FormField, StructRepr, TypeContents, FORM_PATH,
    SCHEMA_PATH, TAG_PATH,
};
use macro_helpers::{Context, Identity};
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
                context.error_spanned_by(input, "Form attributes are not valid on enumerations");
                return None;
            }

            let variants = data
                .variants
                .iter()
                .map(|variant| {
                    let (compound_type, fields, manifest) = parse_struct(context, &variant.fields);
                    let attributes = variant.attrs.get_attributes(context, FORM_PATH);
                    let descriptor =
                        FormDescriptor::from_attributes(context, &variant.ident, attributes);

                    EnumVariant {
                        syn_variant: variant,
                        name: descriptor.name.clone(),
                        compound_type,
                        fields,
                        manifest,
                        descriptor,
                    }
                })
                .collect();

            TypeContents::Enum(variants)
        }
        Data::Struct(data) => {
            let (compound_type, fields, manifest) = parse_struct(context, &data.fields);
            let attributes = input.attrs.get_attributes(context, FORM_PATH);
            let descriptor = FormDescriptor::from_attributes(context, &input.ident, attributes);

            TypeContents::Struct(StructRepr {
                input,
                compound_type,
                fields,
                manifest,
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
#[derive(Clone)]
pub struct FormDescriptor {
    /// Denotes whether or not the body of the produced record is replaced by a field in the
    /// compound type.
    pub body_replaced: bool,
    /// The name that the compound type will be transmuted with.
    pub name: Identity,
}

impl FormDescriptor {
    /// Builds a [`FormDescriptor`] from the provided attributes. An errors encountered while
    /// parsing the attributes are added to the [`Context`].
    pub fn from_attributes(
        context: &mut Context,
        ident: &Ident,
        attributes: Vec<NestedMeta>,
    ) -> FormDescriptor {
        let mut name_opt = None;

        attributes.iter().for_each(|meta: &NestedMeta| match meta {
            NestedMeta::Meta(Meta::NameValue(name)) if name.path == TAG_PATH => match &name.lit {
                Lit::Str(s) => {
                    let tag = s.value();
                    if tag.is_empty() {
                        context.error_spanned_by(meta, "New name cannot be empty")
                    } else {
                        match name_opt {
                            Some(_) => context.error_spanned_by(s, "Duplicate tag"),
                            None => {
                                name_opt = Some(Identity::Renamed {
                                    new_identity: tag,
                                    old_identity: ident.clone(),
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

        FormDescriptor {
            body_replaced: false,
            name: name_opt.unwrap_or(Identity::Named(ident.clone())),
        }
    }

    pub fn has_body_replaced(&self) -> bool {
        self.body_replaced
    }
}
