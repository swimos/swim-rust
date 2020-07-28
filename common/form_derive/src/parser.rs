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

use proc_macro2::Ident;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::Attribute;
use syn::{Lit, Meta, NestedMeta};

use macro_helpers::{get_attribute_meta, StructureKind, Symbol};
use macro_helpers::{CompoundType, Context, FieldName};

pub const FORM_PATH: Symbol = Symbol("form");
pub const HEADER_PATH: Symbol = Symbol("header");
pub const ATTR_PATH: Symbol = Symbol("attr");
pub const SLOT_PATH: Symbol = Symbol("slot");
pub const BODY_PATH: Symbol = Symbol("body");
pub const HEADER_BODY_PATH: Symbol = Symbol("header_body");
pub const RENAME_PATH: Symbol = Symbol("rename");
pub const TAG_PATH: Symbol = Symbol("tag");

pub enum TypeContents<'t> {
    Enum(Vec<EnumVariant<'t>>),
    Struct(StructRepr<'t>),
}

pub struct StructRepr<'t> {
    pub compound_type: CompoundType,
    pub fields: Vec<Field<'t>>,
    pub manifest: FieldManifest,
}

pub struct EnumVariant<'a> {
    pub name: FieldName,
    pub compound_type: CompoundType,
    pub fields: Vec<Field<'a>>,
    pub manifest: FieldManifest,
}

pub struct Field<'a> {
    pub original: &'a syn::Field,
    pub name: FieldName,
    pub kind: FieldKind,
}

pub fn parse_struct<'a>(
    context: &Context,
    fields: &'a syn::Fields,
) -> (CompoundType, Vec<Field<'a>>, FieldManifest) {
    match fields {
        syn::Fields::Named(fields) => {
            let (fields, manifest) = fields_from_ast(context, &fields.named);
            (CompoundType::Struct, fields, manifest)
        }
        syn::Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
            let (fields, manifest) = fields_from_ast(context, &fields.unnamed);
            (CompoundType::NewType, fields, manifest)
        }
        syn::Fields::Unnamed(fields) => {
            let (fields, manifest) = fields_from_ast(context, &fields.unnamed);
            (CompoundType::Tuple, fields, manifest)
        }
        syn::Fields::Unit => (CompoundType::Unit, Vec::new(), FieldManifest::default()),
    }
}

pub fn fields_from_ast<'t>(
    ctx: &Context,
    fields: &'t Punctuated<syn::Field, syn::Token![,]>,
) -> (Vec<Field<'t>>, FieldManifest) {
    let mut manifest = FieldManifest::default();
    let fields = fields
        .iter()
        .enumerate()
        .map(|(index, original)| {
            let mut kind_opt = None;
            let mut set_kind = |kind| match kind_opt {
                Some(_) => {
                    ctx.error_spanned_by(original, "A field can be marked by at most one kind.")
                }
                None => kind_opt = Some(kind),
            };

            let mut renamed = None;

            original.attrs.get_attributes(ctx, FORM_PATH).iter().fold(
                &mut manifest,
                |mut manifest, meta| {
                    match meta {
                        NestedMeta::Meta(Meta::Path(path)) if path == HEADER_PATH => {
                            manifest.has_header_fields = true;
                            set_kind(FieldKind::Header);
                        }
                        NestedMeta::Meta(Meta::Path(path)) if path == ATTR_PATH => {
                            set_kind(FieldKind::Attr);
                            manifest.has_attr_fields = true;
                        }
                        NestedMeta::Meta(Meta::Path(path)) if path == SLOT_PATH => {
                            set_kind(FieldKind::Slot);

                            if manifest.replaces_body {
                                manifest.has_header_fields = true;
                            } else {
                                manifest.has_slot_fields = true;
                            }
                        }
                        NestedMeta::Meta(Meta::Path(path)) if path == BODY_PATH => {
                            set_kind(FieldKind::Body);

                            if manifest.replaces_body {
                                ctx.error_spanned_by(
                                    path,
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
                        NestedMeta::Meta(Meta::Path(path)) if path == HEADER_BODY_PATH => {
                            set_kind(FieldKind::HeaderBody);

                            if manifest.header_body {
                                ctx.error_spanned_by(
                                    path,
                                    "At most one field can replace the tag attribute body.",
                                )
                            } else {
                                manifest.header_body = true;
                            }
                        }
                        NestedMeta::Meta(Meta::NameValue(name)) if name.path == RENAME_PATH => {
                            match &name.lit {
                                Lit::Str(s) => {
                                    let old_ident = original.ident.clone().unwrap_or_else(|| {
                                        Ident::new(&format!("__self_{}", index), original.span())
                                    });

                                    renamed = Some(FieldName::Renamed(s.value(), old_ident));
                                }
                                _ => ctx.error_spanned_by(meta, "Expected string argument"),
                            }
                        }
                        _ => ctx.error_spanned_by(meta, "Unknown attribute"),
                    }
                    manifest
                },
            );

            let name = renamed.unwrap_or_else(|| match &original.ident {
                Some(ident) => FieldName::Named(ident.clone()),
                None => FieldName::Unnamed(index.into()),
            });
            let kind = kind_opt.unwrap_or(FieldKind::Slot);

            if let (FieldName::Unnamed(_), FieldKind::Attr) = (&name, &kind) {
                ctx.error_spanned_by(
                    original,
                    "An unnamed field cannot be promoted to an attribute.",
                )
            }

            Field {
                original,
                name,
                kind: kind_opt.unwrap_or(FieldKind::Slot),
            }
        })
        .collect();

    (fields, manifest)
}

#[derive(Debug, Clone)]
pub struct FormDescriptor {
    pub body_replaced: bool,
    pub header_body_field: Option<Ident>,
    pub name: Name,
}

#[derive(Debug, Clone)]
pub struct Name {
    /// The original name of the structure.
    pub original_ident: Ident,
    /// Tag for the structure.
    pub tag_ident: Ident,
}

pub trait Attributes {
    fn get_attributes(&self, ctx: &Context, symbol: Symbol) -> Vec<NestedMeta>;
}

impl Attributes for Vec<Attribute> {
    fn get_attributes(&self, ctx: &Context, symbol: Symbol) -> Vec<NestedMeta> {
        self.iter()
            .flat_map(|a| get_attribute_meta(ctx, a, symbol))
            .flatten()
            .collect()
    }
}

impl FormDescriptor {
    pub fn from_ast(ctx: &Context, input: &syn::DeriveInput) -> FormDescriptor {
        let kind = StructureKind::from(&input.data);

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
                NestedMeta::Meta(Meta::NameValue(name)) if name.path == TAG_PATH => {
                    if let StructureKind::Enum = kind {
                        ctx.error_spanned_by(
                            meta,
                            "Tags are only supported on enumeration variants.",
                        )
                    } else {
                        match &name.lit {
                            Lit::Str(s) => {
                                let tag = s.value();
                                if tag.is_empty() {
                                    ctx.error_spanned_by(meta, "New name cannot be empty")
                                } else {
                                    desc.name.tag_ident = Ident::new(&*tag, s.span());
                                }
                            }
                            _ => ctx.error_spanned_by(meta, "Expected string argument"),
                        }
                    }
                }
                _ => ctx.error_spanned_by(meta, "Unknown attribute"),
            },
        );

        desc
    }

    pub fn has_body_replaced(&self) -> bool {
        self.body_replaced
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
            field_kind: FieldKind::Slot,
            replaces_body: false,
            header_body: false,
            has_attr_fields: false,
            has_slot_fields: false,
            has_header_fields: false,
        }
    }
}
