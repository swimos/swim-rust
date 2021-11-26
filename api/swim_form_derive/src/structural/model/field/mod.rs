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

use crate::modifiers::NameTransform;
use crate::SynValidation;
use macro_utilities::attr_names::{
    ATTR_PATH, BODY_PATH, FORM_PATH, HEADER_BODY_PATH, HEADER_PATH, NAME_PATH, SCHEMA_PATH,
    SKIP_PATH, SLOT_PATH, TAG_PATH,
};
use macro_utilities::{FieldKind, Symbol};
use proc_macro2::TokenStream;
use quote::{ToTokens, TokenStreamExt};
use std::convert::TryFrom;
use std::ops::Add;
use swim_utilities::errors::validation::Validation;
use syn::{Field, Ident, Lit, Meta, NestedMeta, Type};

/// Describes how to extract a field from a struct.
#[derive(Clone, Copy, Debug)]
pub enum FieldSelector<'a> {
    ///Field in a labelled struct (identified by name).
    Named(&'a Ident),
    ///Field in a tuple struct (identified by its index).
    Ordinal(usize),
}

impl<'a> FieldSelector<'a> {
    pub fn binder(self) -> Binder<'a> {
        Binder {
            field: self,
            is_default: false,
        }
    }

    pub fn default_binder(self) -> Binder<'a> {
        Binder {
            field: self,
            is_default: true,
        }
    }
}

pub struct Binder<'a> {
    field: FieldSelector<'a>,
    is_default: bool,
}

// Consistently gives the same name to a given field wherever it is referred to.
impl<'a> ToTokens for FieldSelector<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        match self {
            FieldSelector::Named(id) => format_ident!("_{}", *id).to_tokens(tokens),
            FieldSelector::Ordinal(i) => format_ident!("value_{}", *i).to_tokens(tokens),
        }
    }
}

impl<'a> ToTokens for Binder<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let Binder { field, is_default } = self;
        if *is_default {
            match field {
                FieldSelector::Named(id) => tokens.append_all(quote! {
                    #id: core::default::Default::default()
                }),
                FieldSelector::Ordinal(_) => tokens.append_all(quote! {
                    core::default::Default::default()
                }),
            }
        } else {
            match field {
                FieldSelector::Named(id) => {
                    let bind_name = format_ident!("_{}", *id);
                    tokens.append_all(quote! {
                        #id: #bind_name
                    })
                }
                FieldSelector::Ordinal(i) => format_ident!("value_{}", *i).to_tokens(tokens),
            }
        }
    }
}

/// Description of a field within a struct.
pub struct FieldModel<'a> {
    /// Means to select the field from an instance of the struct.
    pub selector: FieldSelector<'a>,
    /// Ordinal of the field within the struct, in order of definition.
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

pub struct ResolvedName<'a>(&'a FieldModel<'a>);

impl<'a> ToTokens for ResolvedName<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ResolvedName(field) = self;
        if let Some(trans) = field.transform.as_ref() {
            match trans {
                NameTransform::Rename(name) => proc_macro2::Literal::string(name),
            }
        } else {
            match field.selector {
                FieldSelector::Ordinal(i) => {
                    let string = format!("value_{}", i);
                    proc_macro2::Literal::string(&string)
                }
                FieldSelector::Named(id) => proc_macro2::Literal::string(&id.to_string()),
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
                selector: FieldSelector::Ordinal(_),
                transform: None,
                ..
            }
        )
    }

    /// Determine if the serialization directive applied to the field is valid (header, tag and
    /// attribute fields must be labelled).
    pub fn is_valid(&self) -> bool {
        match self.directive {
            FieldKind::Header | FieldKind::Attr | FieldKind::Tagged => self.is_labelled(),
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
    /// Some other form attribute.
    Other(Option<String>),
}

/// Validated attributes for a field.
#[derive(Default)]
struct FieldAttributes {
    transform: Option<NameTransform>,
    directive: Option<FieldKind>,
}

impl FieldAttributes {
    /// Attempt to apply another attribute, failing if the combined effect is invalid.
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
            FieldAttr::Other(name) => match name {
                Some(name) if name == SCHEMA_PATH => Validation::valid(self), //Overlap with schema attribute.
                _ => {
                    let err = syn::Error::new_spanned(field, "Unknown container attribute");
                    Validation::Validated(self, err.into())
                }
            },
        }
    }
}

#[derive(Clone, Copy)]
pub struct FieldWithIndex<'a>(pub &'a Field, pub usize);

/// Keeps track of unique kinds of fields in a type.
#[derive(Default)]
pub struct Manifest {
    has_header_body: bool,
    has_body: bool,
    has_tag: bool,
}

impl Manifest {
    pub fn validate_field<'a>(
        &mut self,
        input: FieldWithIndex<'a>,
    ) -> SynValidation<TaggedFieldModel<'a>> {
        let Manifest {
            has_header_body,
            has_body,
            has_tag,
        } = self;
        let FieldWithIndex(field, i) = input;
        let Field {
            attrs, ident, ty, ..
        } = field;

        let field_attrs = crate::modifiers::fold_attr_meta(
            FORM_PATH,
            attrs.iter(),
            FieldAttributes::default(),
            |attrs, nested| match FieldAttr::try_from(&nested) {
                Ok(field_attr) => {
                    let agg_err = match &field_attr {
                        FieldAttr::Kind(FieldKind::Body) => {
                            if *has_body {
                                let err = syn::Error::new_spanned(
                                    nested,
                                    "At most one field can replace the body.",
                                );
                                Some(err)
                            } else {
                                *has_body = true;
                                None
                            }
                        }
                        FieldAttr::Kind(FieldKind::Tagged) => {
                            if *has_tag {
                                let err = syn::Error::new_spanned(nested, "Duplicate tag.");
                                Some(err)
                            } else {
                                *has_tag = true;
                                None
                            }
                        }
                        FieldAttr::Kind(FieldKind::HeaderBody) => {
                            if *has_header_body {
                                let err = syn::Error::new_spanned(
                                    nested,
                                    "At most one field can replace the tag attribute body.",
                                );
                                Some(err)
                            } else {
                                *has_header_body = true;
                                None
                            }
                        }
                        _ => None,
                    };
                    let fld_result = attrs.add(field, field_attr);
                    if let Some(err) = agg_err {
                        fld_result.append_error(err)
                    } else {
                        fld_result
                    }
                }
                Err(e) => Validation::Validated(attrs, e.into()),
            },
        );
        field_attrs.and_then(
            |FieldAttributes {
                 transform,
                 directive,
             }| {
                let model = TaggedFieldModel {
                    model: FieldModel {
                        selector: ident
                            .as_ref()
                            .map(FieldSelector::Named)
                            .unwrap_or_else(|| FieldSelector::Ordinal(i)),
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
                        "Header, tag and attribute fields must be labelled",
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

impl<'a> TryFrom<&'a NestedMeta> for FieldAttr {
    type Error = syn::Error;

    fn try_from(input: &'a NestedMeta) -> Result<Self, Self::Error> {
        match input {
            NestedMeta::Meta(Meta::Path(path)) => {
                for (path_name, kind) in &KIND_MAPPING {
                    if path == *path_name {
                        return Ok(FieldAttr::Kind(*kind));
                    }
                }
                let name = path.get_ident().map(|id| id.to_string());
                Ok(FieldAttr::Other(name))
            }
            NestedMeta::Meta(Meta::NameValue(named)) => {
                if named.path == NAME_PATH {
                    if let Lit::Str(new_name) = &named.lit {
                        Ok(FieldAttr::Transform(NameTransform::Rename(
                            new_name.value(),
                        )))
                    } else {
                        Err(syn::Error::new_spanned(
                            &named.lit,
                            "Expected a string literal",
                        ))
                    }
                } else {
                    let name = named.path.get_ident().map(|id| id.to_string());
                    Ok(FieldAttr::Other(name))
                }
            }
            NestedMeta::Meta(Meta::List(lst)) => {
                let name = lst.path.get_ident().map(|id| id.to_string());
                Ok(FieldAttr::Other(name))
            }
            _ => Ok(FieldAttr::Other(None)),
        }
    }
}

/// Description of how fields should be written into the attributes of the record.
#[derive(Default, Clone)]
pub struct HeaderFields<'a> {
    /// A field that should be used to replaced the name of the tag attribute.
    pub tag_name: Option<&'a FieldModel<'a>>,
    /// A field that should be promoted to the body of the tag.
    pub tag_body: Option<&'a FieldModel<'a>>,
    /// Fields that should be promoted to the body of the tag (after the `tag_body` field, if it
    /// exists. These must be labelled.
    pub header_fields: Vec<&'a FieldModel<'a>>,
    /// Fields that should be promoted to an attribute.
    pub attributes: Vec<&'a FieldModel<'a>>,
}

/// The fields that should be written into the body of the record.
#[derive(Clone)]
pub enum BodyFields<'a> {
    /// Simple items in the record body.
    ReplacedBody(&'a FieldModel<'a>),
    /// A single field is used to replace the entire body (potentially adding more attributes). All
    /// other fields must be lifted into the header. If this cannot be done (for example, some of
    /// those fields are not labelled) it is an error.
    StdBody(Vec<&'a FieldModel<'a>>),
}

impl<'a> Default for BodyFields<'a> {
    fn default() -> Self {
        BodyFields::StdBody(vec![])
    }
}

/// Description of how the fields of a type are written into a record.
#[derive(Default, Clone)]
pub struct SegregatedFields<'a> {
    pub header: HeaderFields<'a>,
    pub body: BodyFields<'a>,
}

impl<'a> SegregatedFields<'a> {
    /// The number of field blocks in the type (most fields are a block in themself but the header,
    /// if it exists, is a single block).
    pub fn num_field_blocks(&self) -> usize {
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
        if tag_body.is_some() || !header_fields.is_empty() {
            n += 1;
        }
        n += attributes.len();
        n += if let BodyFields::StdBody(v) = body {
            v.len()
        } else {
            1
        };
        n
    }
}

impl<'a> Add<&'a TaggedFieldModel<'a>> for SegregatedFields<'a> {
    type Output = Self;

    fn add(self, rhs: &'a TaggedFieldModel<'a>) -> Self::Output {
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
