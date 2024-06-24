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

use crate::modifiers::{combine_enum_trans_parts, EnumPartConsumer, StructTransform};
use crate::structural::model::record::{SegregatedStructModel, StructDef, StructModel};
use crate::structural::model::ValidateFrom;
use crate::SynValidation;
use macro_utilities::attr_names::FORM_NAME;
use macro_utilities::attributes::consume_attributes;
use quote::ToTokens;
use std::collections::HashSet;
use swimos_utilities::errors::Errors;
use swimos_utilities::errors::{Validation, ValidationItExt};
use syn::{Attribute, DataEnum, Ident};

/// Preprocessed description of an enum type.
#[non_exhaustive]
pub struct EnumModel<'a> {
    pub root: &'a syn::Path,
    /// The original name of the enum.
    pub name: &'a Ident,
    /// Preprocessed descriptions of each variant.
    pub variants: Vec<StructModel<'a>>,
}

impl<'a> EnumModel<'a> {
    pub fn new(root: &'a syn::Path, name: &'a Ident, variants: Vec<StructModel<'a>>) -> Self {
        EnumModel {
            root,
            name,
            variants,
        }
    }
}

/// Fully processed description of an enum type, used to generate the output of the derive macros.
#[derive(Clone)]
pub struct SegregatedEnumModel<'a> {
    /// Preprocessed model with attribute information.
    pub inner: &'a EnumModel<'a>,
    /// Description of where the fields should be written, for each variant.
    pub variants: Vec<SegregatedStructModel<'a>>,
}

impl<'a> From<&'a EnumModel<'a>> for SegregatedEnumModel<'a> {
    fn from(model: &'a EnumModel<'a>) -> Self {
        let EnumModel { variants, .. } = model;
        let seg_variants = variants.iter().map(Into::into).collect();
        SegregatedEnumModel {
            inner: model,
            variants: seg_variants,
        }
    }
}

pub(crate) struct EnumDef<'a> {
    root: &'a syn::Path,
    name: &'a Ident,
    top: &'a dyn ToTokens,
    attributes: &'a [Attribute],
    definition: &'a DataEnum,
}

impl<'a> EnumDef<'a> {
    pub fn new(
        root: &'a syn::Path,
        name: &'a Ident,
        top: &'a dyn ToTokens,
        attributes: &'a [Attribute],
        definition: &'a DataEnum,
    ) -> Self {
        EnumDef {
            root,
            name,
            top,
            attributes,
            definition,
        }
    }
}

const VARIANT_WITH_TAG: &str = "Enum variants cannot specify a tag field";
const NEWTYPE_SPECIFIED_FOR_VARIANT: &str = "Cannot use `newtype` annotation with enum variants";

impl<'a> ValidateFrom<EnumDef<'a>> for EnumModel<'a> {
    fn validate(input: EnumDef<'a>) -> SynValidation<Self> {
        let EnumDef {
            name,
            top,
            attributes,
            definition,
            root,
        } = input;
        let num_var = definition.variants.len();

        let (parts, errs) = consume_attributes(FORM_NAME, attributes, EnumPartConsumer::default());
        let transform = Validation::Validated(parts, Errors::from(errs))
            .and_then(|parts| combine_enum_trans_parts(top, parts));

        let init = transform.map(|t| (t, Vec::with_capacity(num_var)));
        let variants = definition.variants.iter().validate_fold(
            init,
            false,
            |(transform, mut var_models), variant| {
                let struct_def =
                    StructDef::new(root, &variant.ident, variant, &variant.attrs, variant);
                let model = StructModel::validate(struct_def)
                    .map(|mut v| {
                        v.apply(&transform);
                        v
                    })
                    .and_then(|v| {
                        if let Err(err) = v.check_field_names(variant) {
                            Validation::Validated(v, Errors::of(err))
                        } else {
                            Validation::valid(v)
                        }
                    })
                    .and_then(|model| {
                        if model.fields_model.has_tag_field() {
                            let err = syn::Error::new_spanned(variant, VARIANT_WITH_TAG);
                            Validation::Validated(model, err.into())
                        } else {
                            Validation::valid(model)
                        }
                    });
                match model {
                    Validation::Validated(model, errs) => {
                        var_models.push(model);
                        Validation::Validated((transform, var_models), errs)
                    }
                    Validation::Failed(errs) => {
                        Validation::Validated((transform, var_models), errs)
                    }
                }
            },
        );

        variants.and_then(|(_, mut variants)| {
            let names = variants.iter_mut().validate_fold(
                Validation::valid(HashSet::new()),
                false,
                |mut names, v| {
                    let name = match &mut v.transform {
                        StructTransform::Standard { rename, .. } => {
                            rename.transform(|| v.name.to_string()).to_string()
                        }
                        StructTransform::Newtype(_) => {
                            let err = syn::Error::new_spanned(top, NEWTYPE_SPECIFIED_FOR_VARIANT);
                            return Validation::Failed(err.into());
                        }
                    };
                    if names.contains(&name) {
                        let err = syn::Error::new_spanned(
                            top,
                            format!("Duplicate enumeration tag: {}", name),
                        );
                        Validation::Validated(names, Errors::of(err))
                    } else {
                        names.insert(name);
                        Validation::valid(names)
                    }
                },
            );

            names.and_then(move |_| {
                let enum_model = EnumModel::new(root, name, variants);
                Validation::valid(enum_model)
            })
        })
    }
}
