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

use syn::{Fields, Attribute};
use macro_helpers::CompoundTypeKind;
use super::TryValidate;
use utilities::validation::{Validation, ValidationItExt, validate2};
use crate::structural::model::field::FieldModel;
use crate::parser::FieldManifest;
use crate::structural::model::{NameTransform, SynValidation, StructLike};
use utilities::FieldKind;
use quote::ToTokens;

pub struct FieldsModel<'a> {
    kind: CompoundTypeKind,
    manifest: FieldManifest,
    fields: Vec<FieldModel<'a>>,
}

pub struct StructModel<'a> {
    fields_model: FieldsModel<'a>,
    transform: Option<NameTransform>,
}

pub(crate) struct StructDef<'a, Flds> {
    top: &'a dyn ToTokens,
    attributes: &'a Vec<Attribute>,
    definition: &'a Flds,
}

impl<'a, Flds> StructDef<'a, Flds> {

    pub(crate) fn new(top: &'a dyn ToTokens,
                      attributes: &'a Vec<Attribute>,
                      definition: &'a Flds) -> Self {
        StructDef {
            top, attributes, definition,
        }
    }

}

impl<'a, Flds> TryValidate<StructDef<'a, Flds>> for StructModel<'a>
where
    Flds: StructLike,
{
    fn try_validate(input: StructDef<'a, Flds>) -> SynValidation<Self> {
        let StructDef { top, attributes, definition } = input;

        let fields_model = FieldsModel::try_validate(definition.fields());

        let rename = super::fold_attr_meta(attributes.iter(), None, super::acc_rename);

        validate2(fields_model, rename).and_then(|(model, transform)| {
            let struct_model = StructModel { fields_model: model, transform };
            if struct_model.fields_model.manifest.has_tag_field && struct_model.transform.is_some() {
                let err = syn::Error::new_spanned(top, "Cannot apply a tag using a field when one has already been applied at the container level");
                Validation::Validated(struct_model, err.into())
            } else {
                Validation::valid(struct_model)
            }
        })
    }
}

impl<'a> TryValidate<&'a Fields> for FieldsModel<'a> {
    fn try_validate(definition: &'a Fields) -> SynValidation<Self> {

        let (kind, fields) = match definition {
            Fields::Named(fields) => {
                (CompoundTypeKind::Struct, Some(fields.named.iter()))
            }
            Fields::Unnamed(fields) => {
                let kind = if fields.unnamed.len() == 1 {
                    CompoundTypeKind::NewType
                } else {
                    CompoundTypeKind::Tuple
                };
                (kind, Some(fields.unnamed.iter()))
            }
            Fields::Unit => {
                (CompoundTypeKind::Unit, None)
            }
        };

        let field_models = if let Some(field_it) = fields {
            field_it
                .validate_collect(true, FieldModel::try_validate)
        } else {
            Validation::valid(vec![])
        };

        field_models.and_then(move |flds| {
            let manifest = derive_manifest(definition, flds.iter());
            manifest.map(move |man| FieldsModel {
                kind,
                manifest: man,
                fields: flds
            })
        })
    }
}

fn derive_manifest<'a, It>(definition: &'a Fields, fields: It) -> SynValidation<FieldManifest>
where
    It: Iterator<Item = &'a FieldModel<'a>> + 'a,
{

    fields.validate_fold(Validation::valid(FieldManifest::default()), false, |mut manifest, field| {
        let FieldManifest {
            replaces_body,
            header_body,
            has_attr_fields,
            has_slot_fields,
            has_header_fields,
            has_tag_field,
        } = &mut manifest;

        let err = match &field.directive {
            FieldKind::Slot => {
                *has_slot_fields = true;
                None
            }
            FieldKind::HeaderBody => {
                if *header_body {
                    Some(syn::Error::new_spanned(definition, "At most one field can replace the tag attribute body."))
                } else {
                    *header_body = true;
                    None
                }
            }
            FieldKind::Header => {
                *has_header_fields = true;
                None
            }
            FieldKind::Body => {
                if *replaces_body {
                    Some(syn::Error::new_spanned(definition, "At most one field can replace the body."))
                } else {
                    *replaces_body = true;
                    None
                }
            }
            FieldKind::Attr => {
                *has_attr_fields = true;
                None
            }
            FieldKind::Tagged => {
                if *has_tag_field {
                    Some(syn::Error::new_spanned(definition, "Duplicate tag"))
                } else {
                    *has_tag_field = true;
                    None
                }
            }
            _ => None,
        };
        Validation::Validated(manifest, err.into())
    })
}
