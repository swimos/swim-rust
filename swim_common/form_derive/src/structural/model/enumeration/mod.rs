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

use crate::structural::model::record::{SegregatedStructModel, StructDef, StructModel};
use crate::structural::model::{SynValidation, TryValidate};
use quote::ToTokens;
use syn::{Attribute, DataEnum, Ident};
use utilities::validation::{validate2, Validation, ValidationItExt};

pub struct EnumModel<'a> {
    pub name: &'a Ident,
    pub variants: Vec<StructModel<'a>>,
}

pub struct SegregatedEnumModel<'a, 'b> {
    pub inner: &'b EnumModel<'b>,
    pub variants: Vec<SegregatedStructModel<'a, 'b>>,
}

impl<'a, 'b> From<&'b EnumModel<'a>> for SegregatedEnumModel<'a, 'b> {
    fn from(model: &'b EnumModel<'a>) -> Self {
        let EnumModel { variants, .. } = model;
        let seg_variants = variants.iter().map(Into::into).collect();
        SegregatedEnumModel {
            inner: model,
            variants: seg_variants,
        }
    }
}

struct EnumDef<'a> {
    name: &'a Ident,
    top: &'a dyn ToTokens,
    attributes: &'a Vec<Attribute>,
    definition: &'a DataEnum,
}

impl<'a> TryValidate<EnumDef<'a>> for EnumModel<'a> {
    fn try_validate(input: EnumDef<'a>) -> SynValidation<Self> {
        let EnumDef {
            name,
            top,
            attributes,
            definition,
        } = input;
        let num_var = definition.variants.len();
        let init = Validation::valid(Vec::with_capacity(num_var));
        let variants =
            definition
                .variants
                .iter()
                .validate_fold(init, false, |mut var_models, variant| {
                    let struct_def =
                        StructDef::new(&variant.ident, variant, &variant.attrs, variant);
                    let model = StructModel::try_validate(struct_def);
                    match model {
                        Validation::Validated(model, errs) => {
                            var_models.push(model);
                            Validation::Validated(var_models, errs)
                        }
                        Validation::Failed(errs) => Validation::Validated(var_models, errs),
                    }
                });

        let rename = super::fold_attr_meta(attributes.iter(), None, super::acc_rename);

        validate2(variants, rename).and_then(|(variants, transform)| {
            let enum_model = EnumModel { name, variants };
            if transform.is_some() {
                let err = syn::Error::new_spanned(
                    top,
                    "Tags are only supported on enumeration variants.",
                );
                Validation::Validated(enum_model, err.into())
            } else {
                Validation::valid(enum_model)
            }
        })
    }
}
