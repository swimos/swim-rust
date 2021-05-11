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

use super::TryValidate;
use crate::parser::{
    ATTR_PATH, BODY_PATH, HEADER_BODY_PATH, HEADER_PATH, NAME_PATH, SKIP_PATH, SLOT_PATH, TAG_PATH,
};
use crate::structural::model::{NameTransform, SynValidation};
use macro_helpers::{FieldKind, Symbol};
use std::convert::TryFrom;
use std::ops::Add;
use syn::{Field, Ident, Lit, Meta, NestedMeta, Type};
use utilities::validation::Validation;

pub struct FieldModel<'a> {
    pub name: Option<&'a Ident>,
    pub transform: Option<NameTransform>,
    pub field_ty: &'a Type,
}

pub struct TaggedFieldModel<'a> {
    pub model: FieldModel<'a>,
    pub directive: FieldKind,
}

enum FieldAttr {
    Transform(NameTransform),
    Kind(FieldKind),
}

#[derive(Default)]
struct FieldAttributes {
    transform: Option<NameTransform>,
    directive: Option<FieldKind>,
}

impl FieldAttributes {
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

impl<'a> TryValidate<&'a Field> for TaggedFieldModel<'a> {
    fn try_validate(input: &'a Field) -> SynValidation<Self> {
        let Field {
            attrs, ident, ty, ..
        } = input;

        let field_attrs =
            super::fold_attr_meta(attrs.iter(), FieldAttributes::default(), |attrs, nested| {
                match FieldAttr::try_from(nested) {
                    Ok(field_attr) => attrs.add(input, field_attr),
                    Err(e) => Validation::Validated(attrs, e.into()),
                }
            });

        field_attrs.map(
            |FieldAttributes {
                 transform,
                 directive,
             }| {
                TaggedFieldModel {
                    model: FieldModel {
                        name: ident.as_ref(),
                        transform,
                        field_ty: ty,
                    },
                    directive: directive.unwrap_or(FieldKind::Slot),
                }
            },
        )
    }
}

const KIND_MAPPING: [(&'static Symbol, FieldKind); 7] = [
    (&HEADER_PATH, FieldKind::Header),
    (&ATTR_PATH, FieldKind::Attr),
    (&SLOT_PATH, FieldKind::Slot),
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
                Err(syn::Error::new_spanned(input, "Unknown attribute"))
            }
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
        }
    }
}

impl TryValidate<NestedMeta> for FieldAttr {
    fn try_validate(input: NestedMeta) -> SynValidation<Self> {
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

pub struct HeaderFields<'a> {
    pub tag_body: Option<FieldModel<'a>>,
    pub header_fields: Vec<FieldModel<'a>>,
    pub attributes: Vec<FieldModel<'a>>,
}

pub enum BodyFields<'a> {
    ReplacedBody(FieldModel<'a>),
    SlotBody(Vec<FieldModel<'a>>),
}

impl<'a> Default for BodyFields<'a> {
    fn default() -> Self {
        BodyFields::SlotBody(vec![])
    }
}

pub struct SegregatedFields<'a> {
    pub header: HeaderFields<'a>,
    pub body: BodyFields<'a>,
}

impl<'a> Add<TaggedFieldModel<'a>> for SegregatedFields<'a> {
    type Output = Self;

    fn add(self, rhs: TaggedFieldModel<'a>) -> Self::Output {
        let SegregatedFields {
            mut header,
            mut body,
        } = self;
        let TaggedFieldModel { model, directive } = rhs;
        match directive {
            FieldKind::HeaderBody => {
                if !header.tag_body.is_some() {
                    header.tag_body = Some(model);
                }
            }
            FieldKind::Header => {
                header.header_fields.push(model);
            }
            FieldKind::Attr => {
                header.attributes.push(model);
            }
            FieldKind::Slot => {
                if let BodyFields::SlotBody(slots) = &mut body {
                    slots.push(model);
                } else {
                    header.header_fields.push(model);
                }
            }
            FieldKind::Body => {
                if let BodyFields::SlotBody(slots) = body {
                    header.header_fields.extend(slots.into_iter());
                    body = BodyFields::ReplacedBody(model);
                }
            }
            _ => {}
        }
        SegregatedFields { header, body }
    }
}
