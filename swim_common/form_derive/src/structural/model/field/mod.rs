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
use proc_macro2::TokenStream;
use quote::ToTokens;
use std::convert::TryFrom;
use std::ops::Add;
use syn::{Field, Ident, Lit, Meta, NestedMeta, Type};
use utilities::validation::Validation;

pub enum FieldIndex<'a> {
    Named(&'a Ident),
    Ordinal(usize),
}

impl<'a> ToTokens for FieldIndex<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        match self {
            FieldIndex::Named(id) => id.to_tokens(tokens),
            FieldIndex::Ordinal(i) => format_ident!("value_{}", *i).to_tokens(tokens),
        }
    }
}

pub struct FieldModel<'a> {
    pub name: FieldIndex<'a>,
    pub ordinal: usize,
    pub transform: Option<NameTransform>,
    pub field_ty: &'a Type,
}

impl<'a> FieldModel<'a> {
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

pub struct TaggedFieldModel<'a> {
    pub model: FieldModel<'a>,
    pub directive: FieldKind,
}

impl<'a> TaggedFieldModel<'a> {
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

    pub fn is_valid(&self) -> bool {
        !(matches!(self.directive, FieldKind::Header | FieldKind::Attr) && !self.is_labelled())
    }
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

pub struct FieldWithIndex<'a>(pub &'a Field, pub usize);

impl<'a> TryValidate<FieldWithIndex<'a>> for TaggedFieldModel<'a> {
    fn try_validate(input: FieldWithIndex<'a>) -> SynValidation<Self> {
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

const KIND_MAPPING: [(&'static Symbol, FieldKind); 7] = [
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

#[derive(Default)]
pub struct HeaderFields<'a, 'b> {
    pub tag_name: Option<&'b FieldModel<'a>>,
    pub tag_body: Option<&'b FieldModel<'a>>,
    pub header_fields: Vec<&'b FieldModel<'a>>,
    pub attributes: Vec<&'b FieldModel<'a>>,
}

pub enum BodyFields<'a, 'b> {
    ReplacedBody(&'b FieldModel<'a>),
    StdBody(Vec<&'b FieldModel<'a>>),
}

impl<'a, 'b> Default for BodyFields<'a, 'b> {
    fn default() -> Self {
        BodyFields::StdBody(vec![])
    }
}

#[derive(Default)]
pub struct SegregatedFields<'a, 'b> {
    pub header: HeaderFields<'a, 'b>,
    pub body: BodyFields<'a, 'b>,
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
                if !header.tag_name.is_some() {
                    header.tag_name = Some(model);
                }
            }
            _ => {}
        }
        SegregatedFields { header, body }
    }
}
