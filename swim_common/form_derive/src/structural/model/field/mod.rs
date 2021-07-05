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

use super::ValidateFrom;
use crate::parser::{
    ATTR_PATH, BODY_PATH, HEADER_BODY_PATH, HEADER_PATH, NAME_PATH, SKIP_PATH, SLOT_PATH, TAG_PATH,
};
use crate::structural::model::{NameTransform, SynValidation};
use macro_helpers::{FieldKind, Symbol};
use proc_macro2::TokenStream;
use quote::ToTokens;
use std::convert::TryFrom;
use std::ops::Add;
use syn::{Field, Ident, Lit, Meta, NestedMeta, Type};
use utilities::validation::Validation;

/// Describes how to extract a field from a struct.
pub enum FieldIndex<'a> {
    ///Field in a lablled struct (identified by name).
    Named(&'a Ident),
    ///Field in a tuple struct (identified by its index).
    Ordinal(usize),
}

// Consistently gives the same name to a given field wherever is is referred to.
impl<'a> ToTokens for FieldIndex<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        match self {
            FieldIndex::Named(id) => id.to_tokens(tokens),
            FieldIndex::Ordinal(i) => format_ident!("value_{}", *i).to_tokens(tokens),
        }
    }
}

/// Description of a field within a struct.
pub struct FieldModel<'a> {
    /// Means to index the field from an instanced of the struct.
    pub name: FieldIndex<'a>,
    /// Definition ordinal of the field within the struct.
    pub ordinal: usize,
    /// Optional transformation for the name of the type for the tag attribute.
    pub transform: Option<NameTransform>,
    /// The type of the field.
    pub field_ty: &'a Type,
}

impl<'a> FieldModel<'a> {
    /// Get the (potentially renamed) name of the field as a string literal.
    pub fn resolve_name(&self) -> ResolvedName {
        ResolvedName(self)
    }
}

pub struct ResolvedName<'a, 'b>(&'b FieldModel<'a>);

impl<'a, 'b> ToTokens for ResolvedName<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ResolvedName(field) = self;
        if let Some(trans) = field.transform.as_ref() {
            match trans {
                NameTransform::Rename(name) => proc_macro2::Literal::string(&name),
            }
        } else {
            match field.name {
                FieldIndex::Ordinal(i) => {
                    let string = format!("value_{}", i);
                    proc_macro2::Literal::string(&string)
                }
                FieldIndex::Named(id) => proc_macro2::Literal::string(&id.to_string()),
            }
        }
        .to_tokens(tokens);
    }
}

/// A field model with a modifier describing how it should be serialized.
pub struct TaggedFieldModel<'a> {
    pub model: FieldModel<'a>,
    pub directive: FieldKind,
}

impl<'a> TaggedFieldModel<'a> {
    /// Determine whether the serialized from of the field should have a label.
    pub fn is_labelled(&self) -> bool {
        !matches!(
            &self.model,
            FieldModel {
                name: FieldIndex::Ordinal(_),
                transform: None,
                ..
            }
        )
    }

    /// Determine if the serialization directive applied to the field is valid (header and attribute
    /// fields must be labelled).
    pub fn is_valid(&self) -> bool {
        match self.directive {
            FieldKind::Header | FieldKind::Attr => self.is_labelled(),
            _ => true,
        }
    }
}

/// Description of modifications that can be applied to a field by attributes placed upon it.
enum FieldAttr {
    /// Rename the field in the serialized form.
    Transform(NameTransform),
    /// Specify where the field should occur in the serialized record.
    Kind(FieldKind),
}

/// Validated attributes for a field.
#[derive(Default)]
struct FieldAttributes {
    transform: Option<NameTransform>,
    directive: Option<FieldKind>,
}

impl FieldAttributes {
    /// Attempt to apply another attribyte, failing of the combined effect is invalid.
    fn add(mut self, field: &syn::Field, attr: FieldAttr) -> SynValidation<FieldAttributes> {
        let FieldAttributes {
            transform,
            directive,
        } = &mut self;
        match attr {
            FieldAttr::Transform(t) => {
                if transform.is_some() {
                    let err = syn::Error::new_spanned(field, "Field renamed multiple times");
                    Validation::Validated(self, err.into())
                } else {
                    *transform = Some(t);
                    Validation::valid(self)
                }
            }
            FieldAttr::Kind(k) => {
                if directive.is_some() {
                    let err = syn::Error::new_spanned(field, "Field has multiple kind tags");
                    Validation::Validated(self, err.into())
                } else {
                    *directive = Some(k);
                    Validation::valid(self)
                }
            }
        }
    }
}

pub struct FieldWithIndex<'a>(pub &'a Field, pub usize);

impl<'a> ValidateFrom<FieldWithIndex<'a>> for TaggedFieldModel<'a> {
    fn validate(input: FieldWithIndex<'a>) -> SynValidation<Self> {
        let FieldWithIndex(field, i) = input;
        let Field {
            attrs, ident, ty, ..
        } = field;
        let field_attrs =
            super::fold_attr_meta(attrs.iter(), FieldAttributes::default(), |attrs, nested| {
                match FieldAttr::try_from(nested) {
                    Ok(field_attr) => attrs.add(field, field_attr),
                    Err(e) => Validation::Validated(attrs, e.into()),
                }
            });

        field_attrs.and_then(
            |FieldAttributes {
                 transform,
                 directive,
             }| {
                let model = TaggedFieldModel {
                    model: FieldModel {
                        name: ident
                            .as_ref()
                            .map(FieldIndex::Named)
                            .unwrap_or_else(|| FieldIndex::Ordinal(i)),
                        ordinal: i,
                        transform,
                        field_ty: ty,
                    },
                    directive: directive.unwrap_or(FieldKind::Item),
                };
                if model.is_valid() {
                    Validation::valid(model)
                } else {
                    let err = syn::Error::new_spanned(
                        field,
                        "Header and attribute fields must be labelled",
                    );
                    Validation::Validated(model, err.into())
                }
            },
        )
    }
}

/// Mapping from attribute values to field kind tags.
const KIND_MAPPING: [(&Symbol, FieldKind); 7] = [
    (&HEADER_PATH, FieldKind::Header),
    (&ATTR_PATH, FieldKind::Attr),
    (&SLOT_PATH, FieldKind::Item),
    (&BODY_PATH, FieldKind::Body),
    (&HEADER_BODY_PATH, FieldKind::HeaderBody),
    (&SKIP_PATH, FieldKind::Skip),
    (&TAG_PATH, FieldKind::Tagged),
];

impl TryFrom<NestedMeta> for FieldAttr {
    type Error = syn::Error;

    fn try_from(input: NestedMeta) -> Result<Self, Self::Error> {
        match &input {
            NestedMeta::Meta(Meta::Path(path)) => {
                for (path_name, kind) in &KIND_MAPPING {
                    if path == *path_name {
                        return Ok(FieldAttr::Kind(*kind));
                    }
                }
                Err(syn::Error::new_spanned(path, "Unknown attribute"))
            }
            NestedMeta::Meta(Meta::NameValue(named)) if named.path == NAME_PATH => {
                if let Lit::Str(new_name) = &named.lit {
                    Ok(FieldAttr::Transform(NameTransform::Rename(
                        new_name.value(),
                    )))
                } else {
                    Err(syn::Error::new_spanned(named, "Expected string argument"))
                }
            }
            _ => Err(syn::Error::new_spanned(input, "Unknown attribute")),
        }
    }
}

impl ValidateFrom<NestedMeta> for FieldAttr {
    fn validate(input: NestedMeta) -> SynValidation<Self> {
        let result = match &input {
            NestedMeta::Meta(Meta::Path(path)) => loop {
                let mut it = (&KIND_MAPPING).iter();
                if let Some((path_name, kind)) = it.next() {
                    if path == *path_name {
                        break Ok(FieldAttr::Kind(*kind));
                    }
                } else {
                    break Err(syn::Error::new_spanned(input, "Unrecognized field kind"));
                }
            },
            NestedMeta::Meta(Meta::NameValue(named)) if named.path == NAME_PATH => {
                if let Lit::Str(new_name) = &named.lit {
                    Ok(FieldAttr::Transform(NameTransform::Rename(
                        new_name.value(),
                    )))
                } else {
                    Err(syn::Error::new_spanned(input, "Expected string argument"))
                }
            }
            _ => Err(syn::Error::new_spanned(input, "Unknown attribute")),
        };
        Validation::from(result)
    }
}

/// Description of how fields should be written into the attributes of the record.
#[derive(Default)]
pub struct HeaderFields<'a, 'b> {
    /// A field that should be used to replaced the name of the tag attribute.
    pub tag_name: Option<&'b FieldModel<'a>>,
    /// A field that should be promoted to the body of the tag.
    pub tag_body: Option<&'b FieldModel<'a>>,
    /// Fields that should be promoted to the body of the tag (after the `tag_body` field, if it
    /// exists. These must be labelled.
    pub header_fields: Vec<&'b FieldModel<'a>>,
    /// Fields that should be promoted to an attribute.
    pub attributes: Vec<&'b FieldModel<'a>>,
}

/// The fields that should be written into the body of the record.
pub enum BodyFields<'a, 'b> {
    /// Simple items in the record body.
    ReplacedBody(&'b FieldModel<'a>),
    /// A single field is used to replace the entire body (potentially adding more attributes). All
    /// other fields must be lifted into the header. If this cannot be done (for example, some of
    /// those fields are not labelled) it is an error.
    StdBody(Vec<&'b FieldModel<'a>>),
}

impl<'a, 'b> Default for BodyFields<'a, 'b> {
    fn default() -> Self {
        BodyFields::StdBody(vec![])
    }
}

/// Description of how the fields of a type are written into a record.
#[derive(Default)]
pub struct SegregatedFields<'a, 'b> {
    pub header: HeaderFields<'a, 'b>,
    pub body: BodyFields<'a, 'b>,
}

impl<'a, 'b> SegregatedFields<'a, 'b> {
    pub fn not_skipped(&self) -> usize {
        let SegregatedFields {
            header:
                HeaderFields {
                    tag_name,
                    tag_body,
                    header_fields,
                    attributes,
                },
            body,
        } = self;
        let mut n = 0;
        if tag_name.is_some() {
            n += 1;
        }
        if tag_body.is_some() {
            n += 1;
        }
        n += header_fields.len();
        n += attributes.len();
        n += if let BodyFields::StdBody(v) = body {
            v.len()
        } else {
            1
        };
        n
    }
}

impl<'a, 'b> Add<&'b TaggedFieldModel<'a>> for SegregatedFields<'a, 'b> {
    type Output = Self;

    fn add(self, rhs: &'b TaggedFieldModel<'a>) -> Self::Output {
        let SegregatedFields {
            mut header,
            mut body,
        } = self;
        let TaggedFieldModel { model, directive } = rhs;
        match directive {
            FieldKind::HeaderBody => {
                if header.tag_body.is_none() {
                    header.tag_body = Some(model);
                }
            }
            FieldKind::Header => {
                header.header_fields.push(model);
            }
            FieldKind::Attr => {
                header.attributes.push(model);
            }
            FieldKind::Item => {
                if let BodyFields::StdBody(slots) = &mut body {
                    slots.push(model);
                } else {
                    header.header_fields.push(model);
                }
            }
            FieldKind::Body => {
                if let BodyFields::StdBody(slots) = body {
                    header.header_fields.extend(slots.into_iter());
                    body = BodyFields::ReplacedBody(model);
                }
            }
            FieldKind::Tagged => {
                if header.tag_name.is_none() {
                    header.tag_name = Some(model);
                }
            }
            _ => {}
        }
        SegregatedFields { header, body }
    }
}
