// Copyright 2015-2023 Swim Inc.
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

use super::field::FieldModel;
use super::ValidateFrom;
use crate::modifiers::StructTransform;
use crate::structural::model::field::{
    FieldSelector, FieldWithIndex, Manifest, SegregatedFields, TaggedFieldModel,
};
use crate::structural::model::StructLike;
use crate::SynValidation;
use macro_utilities::attr_names::FORM_PATH;
use macro_utilities::CompoundTypeKind;
use macro_utilities::FieldKind;
use proc_macro2::TokenStream;
use quote::ToTokens;
use std::ops::Add;
use swim_utilities::errors::validation::{validate2, Validation, ValidationItExt};
use syn::{Attribute, Fields, Ident};

const NEWTYPE_MULTI_FIELD_ERR: &str =
    "Cannot apply `newtype` attribute to a struct with multiple fields";
const NEWTYPE_EMPTY_ERR: &str = "Cannot apply `newtype` attribute to an empty struct";
const FIELD_TAG_ERR: &str =
    "Cannot apply a tag to a field when one has already been applied at the container level";

/// Description of the fields, taken from the derive input, preprocessed with any modifications
/// present in attributes on the fields.
pub struct FieldsModel<'a> {
    /// Kind of the underlying struct.
    pub type_kind: CompoundTypeKind,
    /// Kind of the body of generated record (after attributes and renaming have been applied).
    pub body_kind: CompoundTypeKind,
    /// Descriptors for each field, with attributes applied.
    pub fields: Vec<TaggedFieldModel<'a>>,
}

impl<'a> FieldsModel<'a> {
    pub fn has_tag_field(&self) -> bool {
        self.fields
            .iter()
            .any(|model| model.directive == FieldKind::Tagged)
    }

    pub fn newtype_field(&self) -> Result<FieldSelector<'a>, NewtypeFieldError> {
        let mut selector = None;
        for field in &self.fields {
            if field.directive != FieldKind::Skip {
                if selector.is_some() {
                    return Err(NewtypeFieldError::Multiple);
                } else {
                    selector = Some(field.model.selector);
                }
            };
        }

        selector.ok_or(NewtypeFieldError::Empty)
    }
}

pub enum NewtypeFieldError {
    Empty,
    Multiple,
}

/// Preprocessed description of a struct type.
#[non_exhaustive]
pub struct StructModel<'a> {
    pub root: &'a syn::Path,
    /// The original name of the type.
    pub name: &'a Ident,
    /// Description of the fields of the struct.
    pub fields_model: FieldsModel<'a>,
    /// Transformation to apply to the struct.
    pub transform: StructTransform<'a>,
}

impl<'a> StructModel<'a> {
    pub fn new(
        root: &'a syn::Path,
        name: &'a Ident,
        mut fields_model: FieldsModel<'a>,
        transform: StructTransform<'a>,
    ) -> Self {
        let FieldsModel { fields, .. } = &mut fields_model;
        if let StructTransform::Standard { field_rename, .. } = &transform {
            for TaggedFieldModel {
                model: FieldModel { transform, .. },
                ..
            } in fields
            {
                *transform = field_rename.resolve(std::mem::take(transform));
            }
        }

        StructModel {
            root,
            name,
            fields_model,
            transform,
        }
    }

    /// Get the (possible renamed) name of the type as a string literal.
    pub fn resolve_name(&self) -> ResolvedName<'_> {
        ResolvedName(self)
    }

    /// Returns the field selector if a newtype transform should be applied.
    pub fn newtype_selector(&self) -> Option<FieldSelector<'a>> {
        if let StructTransform::Newtype(Some(selector)) = self.transform {
            Some(selector)
        } else {
            None
        }
    }
}

pub struct ResolvedName<'a>(&'a StructModel<'a>);

impl<'a> ToTokens for ResolvedName<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ResolvedName(def) = self;
        if let StructTransform::Standard { rename, .. } = &def.transform {
            rename.transform(|| def.name.to_string())
        } else {
            proc_macro2::Literal::string(&def.name.to_string())
        }
        .to_tokens(tokens);
    }
}

/// Fully processed description of a struct type, used to generate the output of the derive macros.
#[derive(Clone)]
pub struct SegregatedStructModel<'a> {
    /// Preprocessed model with attribute information.
    pub inner: &'a StructModel<'a>,
    /// Description of where in the record each field should be written.
    pub fields: SegregatedFields<'a>,
}

impl<'a> From<&'a StructModel<'a>> for SegregatedStructModel<'a> {
    fn from(model: &'a StructModel<'a>) -> Self {
        let fields = &model.fields_model.fields;
        let mut segregated = SegregatedFields::default();
        for field in fields.iter() {
            segregated = segregated.add(field);
        }
        SegregatedStructModel {
            inner: model,
            fields: segregated,
        }
    }
}

pub(crate) struct StructDef<'a, Flds> {
    root: &'a syn::Path,
    name: &'a Ident,
    top: &'a dyn ToTokens,
    attributes: &'a [Attribute],
    definition: &'a Flds,
}

impl<'a, Flds> StructDef<'a, Flds> {
    pub(crate) fn new(
        root: &'a syn::Path,
        name: &'a Ident,
        top: &'a dyn ToTokens,
        attributes: &'a [Attribute],
        definition: &'a Flds,
    ) -> Self {
        StructDef {
            root,
            name,
            top,
            attributes,
            definition,
        }
    }
}

impl<'a, Flds> ValidateFrom<StructDef<'a, Flds>> for StructModel<'a>
where
    Flds: StructLike,
{
    fn validate(input: StructDef<'a, Flds>) -> SynValidation<Self> {
        let StructDef {
            root,
            name,
            top,
            attributes,
            definition,
        } = input;

        let fields_model = FieldsModel::validate(definition.fields());

        let transform = crate::modifiers::fold_attr_meta(
            FORM_PATH,
            attributes.iter(),
            StructTransform::default(),
            crate::modifiers::acc_struct_transform,
        );

        validate2(fields_model, transform).and_then(|(model, transform)| match transform {
            StructTransform::Newtype(_) => match model.newtype_field() {
                Ok(selector) => {
                    let struct_model = StructModel::new(
                        root,
                        name,
                        model,
                        StructTransform::Newtype(Some(selector)),
                    );
                    Validation::valid(struct_model)
                }
                Err(NewtypeFieldError::Multiple) => {
                    let struct_model =
                        StructModel::new(root, name, model, StructTransform::default());
                    let err = syn::Error::new_spanned(top, NEWTYPE_MULTI_FIELD_ERR);
                    Validation::Validated(struct_model, err.into())
                }
                Err(NewtypeFieldError::Empty) => {
                    let struct_model =
                        StructModel::new(root, name, model, StructTransform::default());
                    let err = syn::Error::new_spanned(top, NEWTYPE_EMPTY_ERR);
                    Validation::Validated(struct_model, err.into())
                }
            },
            StructTransform::Standard {
                rename,
                field_rename,
            } => {
                let is_id = rename.is_identity();
                let struct_model = StructModel::new(
                    root,
                    name,
                    model,
                    StructTransform::Standard {
                        rename,
                        field_rename,
                    },
                );
                if !is_id && struct_model.fields_model.has_tag_field() {
                    let err = syn::Error::new_spanned(top, FIELD_TAG_ERR);
                    Validation::Validated(struct_model, err.into())
                } else {
                    Validation::valid(struct_model)
                }
            }
        })
    }
}

impl<'a> ValidateFrom<&'a Fields> for FieldsModel<'a> {
    fn validate(definition: &'a Fields) -> SynValidation<Self> {
        let (type_kind, fields) = match definition {
            Fields::Named(fields) => (CompoundTypeKind::Labelled, Some(fields.named.iter())),
            Fields::Unnamed(fields) => {
                let kind = if fields.unnamed.len() == 1 {
                    CompoundTypeKind::NewType
                } else {
                    CompoundTypeKind::Tuple
                };
                (kind, Some(fields.unnamed.iter()))
            }
            _ => (CompoundTypeKind::Unit, None),
        };

        let mut manifest = Manifest::default();

        let field_models = if let Some(field_it) = fields {
            field_it
                .enumerate()
                .map(|(i, fld)| FieldWithIndex(fld, i))
                .validate_collect(true, |fld| manifest.validate_field(fld))
        } else {
            Validation::valid(vec![])
        };

        field_models.and_then(move |flds| {
            let kind = assess_kind(definition, flds.iter());
            kind.map(move |kind| FieldsModel {
                type_kind,
                body_kind: kind,
                fields: flds,
            })
        })
    }
}

const BAD_FIELDS: &str = "Body fields cannot be a mix of labelled and unlabelled";
const BAD_REPLACEMENT: &str =
    "Where a field replaces the body, all other body fields must be labelled";

fn assess_kind<'a, It>(definition: &'a Fields, fields: It) -> SynValidation<CompoundTypeKind>
where
    It: Iterator<Item = &'a TaggedFieldModel<'a>> + 'a,
{
    let mut kind = Some(CompoundTypeKind::Unit);
    for field in fields {
        let TaggedFieldModel { directive, .. } = field;
        match *directive {
            FieldKind::Item => match kind {
                Some(CompoundTypeKind::Labelled) => {
                    if !field.is_labelled() {
                        let err = syn::Error::new_spanned(definition, BAD_FIELDS);
                        return Validation::fail(err);
                    }
                }
                Some(CompoundTypeKind::Tuple) => {
                    if field.is_labelled() {
                        let err = syn::Error::new_spanned(definition, BAD_FIELDS);
                        return Validation::fail(err);
                    }
                }
                Some(CompoundTypeKind::NewType) => {
                    if field.is_labelled() {
                        let err = syn::Error::new_spanned(definition, BAD_FIELDS);
                        return Validation::fail(err);
                    }
                    kind = Some(CompoundTypeKind::Tuple);
                }
                Some(CompoundTypeKind::Unit) => {
                    kind = if field.is_labelled() {
                        Some(CompoundTypeKind::Labelled)
                    } else {
                        Some(CompoundTypeKind::NewType)
                    };
                }
                _ => {
                    if !field.is_labelled() {
                        let err = syn::Error::new_spanned(definition, BAD_REPLACEMENT);
                        return Validation::fail(err);
                    }
                }
            },
            FieldKind::Body => {
                if matches!(
                    kind,
                    Some(CompoundTypeKind::Tuple | CompoundTypeKind::NewType)
                ) {
                    let err = syn::Error::new_spanned(definition, BAD_REPLACEMENT);
                    return Validation::fail(err);
                }
                kind = None;
            }
            _ => {}
        }
    }
    Validation::valid(kind.unwrap_or(CompoundTypeKind::Unit))
}
