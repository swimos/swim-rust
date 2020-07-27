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
use syn::Attribute;
use syn::{Lit, Meta, NestedMeta};

use macro_helpers::{get_attribute_meta, Context, Field, FieldName};

const FORM_PATH: &str = "form";
const HEADER_PATH: &str = "header";
const ATTR_PATH: &str = "attr";
const SLOT_PATH: &str = "slot";
const BODY_PATH: &str = "body";
const HEADER_BODY_PATH: &str = "header_body";
const RENAME_PATH: &str = "rename";
const TAG_PATH: &str = "tag";

#[derive(Debug)]
pub struct FormDescriptor {
    pub body_replaced: bool,
    pub header_body_field: Option<Ident>,
    pub name: Name,
}

#[derive(Debug)]
pub struct Name {
    /// The original name of the structure.
    pub original_ident: Ident,
    /// Tag for the structure.
    pub tag_ident: Ident,
}

pub trait Attributes {
    fn get_attributes(&self, ctx: &Context, path: &'static str) -> Vec<NestedMeta>;
}

impl Attributes for Vec<Attribute> {
    fn get_attributes(&self, ctx: &Context, path: &'static str) -> Vec<NestedMeta> {
        self.iter()
            .flat_map(|a| get_attribute_meta(ctx, a, path))
            .flatten()
            .collect()
    }
}

impl FormDescriptor {
    pub fn from_ast(ctx: &Context, input: &syn::DeriveInput) -> FormDescriptor {
        let mut desc = FormDescriptor {
            body_replaced: false,
            header_body_field: None,
            name: Name {
                original_ident: input.ident.clone(),
                tag_ident: input.ident.clone(),
            },
        };

        input.attrs.get_attributes(ctx, FORM_PATH).iter().for_each(
            |meta: &NestedMeta| match meta {
                NestedMeta::Meta(Meta::NameValue(name)) if name.path.is_ident(TAG_PATH) => {
                    match &name.lit {
                        Lit::Str(s) => {
                            let tag = s.value();
                            if tag.len() == 0 {
                                ctx.error_spanned_by(
                                    meta.to_token_stream(),
                                    "New name cannot be empty",
                                )
                            } else {
                                desc.name.tag_ident = Ident::new(&*tag, s.span());
                            }
                        }
                        _ => {
                            ctx.error_spanned_by(meta.to_token_stream(), "Expected string argument")
                        }
                    }
                }
                _ => ctx.error_spanned_by(meta.to_token_stream(), "Unknown attribute"),
            },
        );

        desc
    }
    pub fn repr_to_record() {}

    pub fn repr_from_record() {}

    pub fn has_body_replaced(&self) -> bool {
        self.body_replaced
    }

    pub fn has_header_body_field(&self) -> &Option<Ident> {
        &self.header_body_field
    }
}

/// Enumeration of ways in which fields can be serialized in Recon documents. Unannotated fields
/// are assumed to be annotated as [`Item::Slot`].
#[derive(PartialEq, Debug, Eq, Hash, Copy, Clone)]
pub enum FieldKind {
    /// The field should be written as a slot in the tag attribute.
    Header,
    /// The field should be written as an attribute.
    Attr,
    /// The field should be written as a slot in the main body (or the header if another field is
    /// marked as [`FieldKind::Body`]
    Slot,
    /// The field should be used to form the entire body of the record, all other fields that are
    /// marked as slots will be promoted to headers. At most one field may be marked with this.
    Body,
    /// The field should be moved into the body of the tag attribute (unlabelled). If there are no
    /// header fields it will form the entire body of the tag, otherwise it will be the first item
    /// of the tag body. At most one field may be marked with this.
    HeaderBody,
}

impl Default for FieldKind {
    fn default() -> Self {
        FieldKind::Slot
    }
}

pub struct FieldManifest {
    pub renamed_to: Option<FieldName>,
    pub field_kind: FieldKind,
    pub replaces_body: bool,
    pub header_body: bool,
    pub has_attr_fields: bool,
    pub has_slot_fields: bool,
    pub has_header_fields: bool,
}

impl Default for FieldManifest {
    fn default() -> FieldManifest {
        FieldManifest {
            renamed_to: None,
            field_kind: FieldKind::Slot,
            replaces_body: false,
            header_body: false,
            has_attr_fields: false,
            has_slot_fields: false,
            has_header_fields: false,
        }
    }
}

pub struct FieldWrapper<'t> {
    pub field: Field<'t>,
    pub kind: FieldKind,
    pub name: FieldName,
}

/// Computes a [`FieldManifest`] from the given fields as well as wrapping the provided fields and
/// attaching a [`FieldKind`] in a [`FieldWrapper`].
pub fn compute_field_manifest<'t>(
    ctx: &Context,
    fields: Vec<Field<'t>>,
) -> (Vec<FieldWrapper<'t>>, FieldManifest) {
    fields.into_iter().fold(
        (Vec::new(), FieldManifest::default()),
        |(mut vec, mut field_manifest), field| {
            let mut kind_opt = None;
            let mut set_kind = |kind| match kind_opt {
                Some(_) => ctx
                    .error_spanned_by(field.original, "A field can be marked by at most one kind."),
                None => kind_opt = Some(kind),
            };

            let mut renamed = None;

            field
                .original
                .attrs
                .get_attributes(ctx, FORM_PATH)
                .iter()
                .fold(&mut field_manifest, |mut manifest, meta| {
                    match meta {
                        NestedMeta::Meta(Meta::Path(path)) if path.is_ident(HEADER_PATH) => {
                            manifest.has_header_fields = true;
                            set_kind(FieldKind::Header);
                        }
                        NestedMeta::Meta(Meta::Path(path)) if path.is_ident(ATTR_PATH) => {
                            set_kind(FieldKind::Attr);
                            manifest.has_attr_fields = true;
                        }
                        NestedMeta::Meta(Meta::Path(path)) if path.is_ident(SLOT_PATH) => {
                            set_kind(FieldKind::Slot);

                            if manifest.replaces_body {
                                manifest.has_header_fields = true;
                            } else {
                                manifest.has_slot_fields = true;
                            }
                        }
                        NestedMeta::Meta(Meta::Path(path)) if path.is_ident(BODY_PATH) => {
                            set_kind(FieldKind::Body);

                            if manifest.replaces_body {
                                ctx.error_spanned_by(
                                    path.to_token_stream(),
                                    "At most one field can replace the body.",
                                )
                            } else if manifest.has_slot_fields {
                                manifest.has_slot_fields = false;
                                manifest.has_header_fields = true;
                                manifest.replaces_body = true;
                            } else {
                                manifest.replaces_body = true;
                            }
                        }
                        NestedMeta::Meta(Meta::Path(path)) if path.is_ident(HEADER_BODY_PATH) => {
                            set_kind(FieldKind::HeaderBody);

                            if manifest.header_body {
                                ctx.error_spanned_by(
                                    path.to_token_stream(),
                                    "At most one field can replace the tag attribute body.",
                                )
                            } else {
                                manifest.header_body = true;
                            }
                        }
                        NestedMeta::Meta(Meta::NameValue(name))
                            if name.path.is_ident(RENAME_PATH) =>
                        {
                            match &name.lit {
                                Lit::Str(s) => {
                                    let old_ident = field
                                        .original
                                        .ident
                                        .clone()
                                        .unwrap_or(field.name.as_ident());

                                    renamed = Some(FieldName::Renamed(
                                        Ident::new(&*s.value(), s.span()),
                                        old_ident,
                                    ));
                                }
                                _ => ctx.error_spanned_by(
                                    meta.to_token_stream(),
                                    "Expected string argument",
                                ),
                            }
                        }
                        _ => ctx.error_spanned_by(meta.to_token_stream(), "Unknown attribute"),
                    }
                    manifest
                });

            let name = renamed.unwrap_or(field.name.clone());

            let wrap = FieldWrapper {
                field,
                kind: kind_opt.unwrap_or(FieldKind::Slot),
                name,
            };

            vec.push(wrap);

            (vec, field_manifest)
        },
    )
}
