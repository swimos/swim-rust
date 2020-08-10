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

use macro_helpers::{
    get_attribute_meta, CompoundTypeKind, Context, FieldIdentity, StructureKind, Symbol,
};
use proc_macro2::Ident;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{Attribute, Data, Lit, Meta, NestedMeta};

pub const FORM_PATH: Symbol = Symbol("form");
pub const HEADER_PATH: Symbol = Symbol("header");
pub const ATTR_PATH: Symbol = Symbol("attr");
pub const SLOT_PATH: Symbol = Symbol("slot");
pub const BODY_PATH: Symbol = Symbol("body");
pub const HEADER_BODY_PATH: Symbol = Symbol("header_body");
pub const RENAME_PATH: Symbol = Symbol("rename");
pub const TAG_PATH: Symbol = Symbol("tag");
pub const SKIP_PATH: Symbol = Symbol("skip");
pub const VALID_PATH: Symbol = Symbol("valid");

/// An enumeration representing the contents of an input.
pub enum TypeContents<F> {
    /// An enumeration input. Containing a vector of enumeration variants.
    Enum(Vec<EnumVariant<F>>),
    /// A struct input containing its representation.
    Struct(StructRepr<F>),
}

impl<'t> TypeContents<FormField<'t>> {
    /// Build a [`TypeContents`] input from an abstract syntax tree. Returns [`Option::None`] if
    /// there was an error that was encountered while parsing the tree. The underlying error is
    /// added to the [`Context]`. If [`derive_valid`] is `true`, then [`[form(valid(..))]`]
    /// attributes are parsed into the [`TypeContents`] representation.
    pub fn from(context: &mut Context, input: &'t syn::DeriveInput) -> Option<Self> {
        let type_contents = match &input.data {
            Data::Enum(data) => {
                let variants =
                    data.variants
                        .iter()
                        .map(|variant| {
                            let mut name_opt = None;

                            variant
                                .attrs
                                .get_attributes(context, FORM_PATH)
                                .iter()
                                .for_each(|meta| match meta {
                                    NestedMeta::Meta(Meta::NameValue(name))
                                        if name.path == TAG_PATH =>
                                    {
                                        match &name.lit {
                                            Lit::Str(s) => {
                                                name_opt = Some(FieldIdentity::Renamed {
                                                    new_identity: s.value(),
                                                    old_identity: variant.ident.clone(),
                                                });
                                            }
                                            _ => context
                                                .error_spanned_by(meta, "Expected string argument"),
                                        }
                                    }
                                    _ => context
                                        .error_spanned_by(meta, "Unknown enumeration attribute"),
                                });

                            let (compound_type, fields, manifest) =
                                parse_struct(context, &variant.fields);

                            EnumVariant {
                                name: name_opt
                                    .unwrap_or_else(|| FieldIdentity::Named(variant.ident.clone())),
                                compound_type,
                                fields,
                                manifest,
                            }
                        })
                        .collect();

                TypeContents::Enum(variants)
            }
            Data::Struct(data) => {
                let (compound_type, fields, manifest) = parse_struct(context, &data.fields);

                TypeContents::Struct(StructRepr {
                    compound_type,
                    fields,
                    manifest,
                })
            }
            Data::Union(_) => {
                context.error_spanned_by(input, "Unions are not supported");
                return None;
            }
        };

        Some(type_contents)
    }
}

/// A representation of a parsed struct from the AST.
pub struct StructRepr<F> {
    /// The struct's type: tuple, named, unit.
    pub compound_type: CompoundTypeKind,
    /// The field members of the struct.
    pub fields: Vec<F>,
    /// A derived [`FieldManifest`] from the attributes on the members.
    pub manifest: FieldManifest,
}

/// A representation of a parsed enumeration from the AST.
pub struct EnumVariant<F> {
    /// The name of the variant.
    pub name: FieldIdentity,
    /// The variant's type: tuple, named, unit.
    pub compound_type: CompoundTypeKind,
    /// The field members of the variant.
    pub fields: Vec<F>,
    /// A derived [`FieldManifest`] from the attributes on the members.
    pub manifest: FieldManifest,
}

/// A representation of a parsed field for a form from the AST.
pub struct FormField<'a> {
    /// The original field from the [`DeriveInput`].
    pub original: &'a syn::Field,
    /// The name of the field.
    pub name: FieldIdentity,
    /// The kind of the field from its attribute.
    pub kind: FieldKind,
}

#[derive(Debug, Clone)]
pub struct Name {
    /// The original name of the structure.
    pub original_ident: Ident,
    /// Tag for the structure.
    pub tag_ident: Ident,
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
    /// The field will be ignored during transformations. The decorated field must implement
    /// [`Default`].
    Skip,
}

impl Default for FieldKind {
    fn default() -> Self {
        FieldKind::Slot
    }
}

/// A structure representing what fields in the compound type are annotated with.
#[derive(Default)]
pub struct FieldManifest {
    /// Whether or not there is a field in the compound type that replaces the body of the output
    /// record.
    pub replaces_body: bool,
    /// Whether or not there is a field in the compound type that is promoted to the header's body.
    pub header_body: bool,
    /// Whether or not there are fields that are written to the attributes vector in the record.
    pub has_attr_fields: bool,
    /// Whether or not there are fields that are written to the slot vector in the record.
    pub has_slot_fields: bool,
    /// Whether or not there are fields tha are written as headers in the record.
    pub has_header_fields: bool,
}

/// A [`FormDescriptor`] is a representation of a [`Form`] that is built from a [`DeriveInput`],
/// containing the name of the compound type that it represents and whether or not the body of the
/// produced [`Value`] is replaced. A field annotated with [`[form(body)]` will cause this to
/// happen. A compound type annotated with [`[form(tag = "name")]` will set the structure's output
/// value to be replaced with the provided literal.
#[derive(Debug, Clone)]
pub struct FormDescriptor {
    /// Denotes whether or not the body of the produced record is replaced by a field in the
    /// compound type.
    pub body_replaced: bool,
    /// The name that the compound type will be transmuted with.
    pub name: Name,
}

impl FormDescriptor {
    /// Builds a [`FormDescriptor`] for the provided [`DeriveInput`]. An errors encountered while
    /// parsing the [`DeriveInput`] will be added to the [`Context`].
    pub fn from_ast(ctx: &mut Context, input: &syn::DeriveInput) -> FormDescriptor {
        let kind = StructureKind::from(&input.data);

        let mut desc = FormDescriptor {
            body_replaced: false,
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
                _ => ctx.error_spanned_by(meta, "Unknown container attribute"),
            },
        );

        desc
    }

    pub fn has_body_replaced(&self) -> bool {
        self.body_replaced
    }
}

/// Parse a structure's fields from the [`DeriveInput`]'s fields. Returns the type of the fields,
/// parsed fields that contain a name and kind, and a derived [`FieldManifest]`. Any errors
/// encountered are added to the [`Context]`.
pub fn parse_struct<'a>(
    context: &mut Context,
    fields: &'a syn::Fields,
) -> (CompoundTypeKind, Vec<FormField<'a>>, FieldManifest) {
    match fields {
        syn::Fields::Named(fields) => {
            let (fields, manifest) = fields_from_ast(context, &fields.named);
            (CompoundTypeKind::Struct, fields, manifest)
        }
        syn::Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
            let (fields, manifest) = fields_from_ast(context, &fields.unnamed);
            (CompoundTypeKind::NewType, fields, manifest)
        }
        syn::Fields::Unnamed(fields) => {
            let (fields, manifest) = fields_from_ast(context, &fields.unnamed);
            (CompoundTypeKind::Tuple, fields, manifest)
        }
        syn::Fields::Unit => (CompoundTypeKind::Unit, Vec::new(), FieldManifest::default()),
    }
}

/// Parses an AST of fields and produces a vector of fields that contain their final identities
/// and their kind as well as producing a [`FieldManifest`].
pub fn fields_from_ast<'t>(
    ctx: &mut Context,
    fields: &'t Punctuated<syn::Field, syn::Token![,]>,
) -> (Vec<FormField<'t>>, FieldManifest) {
    let mut manifest = FieldManifest::default();
    let fields = fields
        .iter()
        .enumerate()
        .map(|(index, original)| {
            let mut kind_opt = None;
            let mut set_kind = |kind, ctx: &mut Context| match kind_opt {
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
                            set_kind(FieldKind::Header, ctx);
                        }
                        NestedMeta::Meta(Meta::Path(path)) if path == ATTR_PATH => {
                            set_kind(FieldKind::Attr, ctx);
                            manifest.has_attr_fields = true;
                        }
                        NestedMeta::Meta(Meta::Path(path)) if path == SLOT_PATH => {
                            set_kind(FieldKind::Slot, ctx);

                            if manifest.replaces_body {
                                manifest.has_header_fields = true;
                            } else {
                                manifest.has_slot_fields = true;
                            }
                        }
                        NestedMeta::Meta(Meta::Path(path)) if path == BODY_PATH => {
                            set_kind(FieldKind::Body, ctx);

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
                            set_kind(FieldKind::HeaderBody, ctx);

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

                                    renamed = Some(FieldIdentity::Renamed {
                                        new_identity: s.value(),
                                        old_identity: old_ident,
                                    });
                                }
                                _ => ctx.error_spanned_by(meta, "Expected string argument"),
                            }
                        }
                        NestedMeta::Meta(Meta::Path(path)) if path == SKIP_PATH => {
                            set_kind(FieldKind::Skip, ctx);
                        }
                        NestedMeta::Meta(Meta::List(list)) if list.path == VALID_PATH => {
                            // no-op as this is parsed by the validated form derive macro
                        }
                        _ => ctx.error_spanned_by(meta, "Unknown attribute"),
                    }

                    manifest
                },
            );

            let name = renamed.unwrap_or_else(|| match &original.ident {
                Some(ident) => FieldIdentity::Named(ident.clone()),
                None => FieldIdentity::Anonymous(index.into()),
            });

            let kind = kind_opt.unwrap_or(FieldKind::Slot);

            if let (FieldIdentity::Anonymous(_), FieldKind::Attr) = (&name, &kind) {
                ctx.error_spanned_by(
                    original,
                    "An unnamed field cannot be promoted to an attribute.",
                )
            }

            FormField {
                original,
                name,
                kind: kind_opt.unwrap_or(FieldKind::Slot),
            }
        })
        .collect();

    (fields, manifest)
}

/// A trait for retrieving attributes on a field or compound type that are prefixed by the provided
/// [`symbol`]. For example calling this on a [`DeriveInput`] that represents the following:
///```compile_fail
///struct Person {
///    #[form(skip)]
///    name: String,
///    age: i32,
/// }
///```
/// will return a [`Vector`] that contains the [`NestedMeta`] for the field [`name`].
pub trait Attributes {
    /// Returns a vector of [`NestedMeta`] for all attributes that contain a path that matches the
    /// provided symbol or an empty vector if there are no matches.
    fn get_attributes(&self, ctx: &mut Context, symbol: Symbol) -> Vec<NestedMeta>;
}

impl Attributes for Vec<Attribute> {
    fn get_attributes(&self, ctx: &mut Context, symbol: Symbol) -> Vec<NestedMeta> {
        self.iter()
            .flat_map(|a| get_attribute_meta(ctx, a, symbol))
            .flatten()
            .collect()
    }
}
