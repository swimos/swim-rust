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

use super::ValidateFrom;
use crate::modifiers::{NameTransform, StructTransform};
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

    pub fn newtype_field(&self) -> Option<FieldSelector<'a>> {
        let mut selector = None;
        for field in &self.fields {
            if field.directive != FieldKind::Skip {
                if selector.is_some() {
                    return None;
                } else {
                    selector = Some(field.model.selector);
                }
            };
        }

        selector
    }
}

/// Preprocessed description of a struct type.
pub struct StructModel<'a> {
    /// The original name of the type.
    pub name: &'a Ident,
    /// Description of the fields of the struct.
    pub fields_model: FieldsModel<'a>,
    /// Transformation to apply to the name for the tag attribute.
    pub transform: Option<NameTransform>,
    pub selector: Option<FieldSelector<'a>>,
}

impl<'a> StructModel<'a> {
    /// Get the (possible renamed) name of the type as a string literal.
    pub fn resolve_name(&self) -> ResolvedName<'_> {
        ResolvedName(self)
    }
}

pub struct ResolvedName<'a>(&'a StructModel<'a>);

impl<'a> ToTokens for ResolvedName<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ResolvedName(def) = self;
        if let Some(trans) = def.transform.as_ref() {
            match trans {
                NameTransform::Rename(name) => proc_macro2::Literal::string(name),
            }
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
    name: &'a Ident,
    top: &'a dyn ToTokens,
    attributes: &'a [Attribute],
    definition: &'a Flds,
}

impl<'a, Flds> StructDef<'a, Flds> {
    pub(crate) fn new(
        name: &'a Ident,
        top: &'a dyn ToTokens,
        attributes: &'a [Attribute],
        definition: &'a Flds,
    ) -> Self {
        StructDef {
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
            name,
            top,
            attributes,
            definition,
        } = input;

        let fields_model = FieldsModel::validate(definition.fields());

        let rename = crate::modifiers::fold_attr_meta(
            FORM_PATH,
            attributes.iter(),
            None,
            crate::modifiers::acc_struct_transform,
        );

        validate2(fields_model, rename).and_then(|(model, transform)| {
            if let Some(StructTransform::Newtype) = transform {
                if let Some(selector) = model.newtype_field() {
                    let struct_model = StructModel { name, fields_model: model, transform: None, selector: Some(selector) };
                    Validation::valid(struct_model)
                }
                else {
                    let struct_model = StructModel { name, fields_model: model, transform: None, selector: None };
                    let err = syn::Error::new_spanned(top, "Foo error");
                    Validation::Validated(struct_model, err.into())
                }
            } else {
                let transform = transform.map(|a| match a {
                    StructTransform::Rename(a) => { a }
                    StructTransform::Newtype => { unreachable!() }
                });

                let struct_model = StructModel { name, fields_model: model, transform, selector: None };
                if struct_model.fields_model.has_tag_field() && struct_model.transform.is_some() {
                    let err = syn::Error::new_spanned(top, "Cannot apply a tag to a field when one has already been applied at the container level");
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
