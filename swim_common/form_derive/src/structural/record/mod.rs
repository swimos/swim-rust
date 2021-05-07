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

use syn::{Fields, Attribute, Meta, NestedMeta, Lit, DataStruct};
use macro_helpers::CompoundTypeKind;
use super::TryValidate;
//use crate::parser::{FORM_PATH, HEADER_PATH, HEADER_BODY_PATH, SLOT_PATH, SKIP_PATH, ATTR_PATH, TAG_PATH, BODY_PATH, NAME_PATH, FieldManifest};
use crate::parser::TAG_PATH;
use utilities::validation::{Validation, ValidationItExt, validate2};
use crate::structural::field::FieldModel;
use crate::parser::FieldManifest;
use crate::structural::{NameTransform, SynValidation};
use std::convert::TryFrom;
use utilities::FieldKind;
use quote::ToTokens;

pub struct StructModel<'a> {
    kind: CompoundTypeKind,
    manifest: FieldManifest,
    transform: Option<NameTransform>,
    fields: Vec<FieldModel<'a>>,
}

struct StructDef<'a> {
    top: &'a dyn ToTokens,
    attributes: &'a Vec<Attribute>,
    definition: &'a DataStruct,
}

impl<'a> TryValidate<StructDef<'a>> for StructModel<'a> {
    fn try_validate(input: StructDef<'a>) -> SynValidation<Self> {

        let StructDef { top, attributes, definition } = input;

        let (kind, fields) = match &definition.fields {
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

        let rename = super::fold_attr_meta(attributes.iter(), None, |mut state, nested_meta| {
            let err = match NameTransform::try_from(&nested_meta) {
                Ok(rename) => {
                    if state.is_some() {
                        Some(syn::Error::new_spanned(nested_meta, "Duplicate tag"))
                    } else {
                        state = Some(rename);
                        None
                    }
                }
                Err(e) => {
                    Some(e)
                }
            };
            Validation::Validated(state, err.into())
        });

        let flds_with_manifest = field_models.and_then(|flds| {
            let manifest = derive_manifest(definition, flds.iter());
            manifest.map(|man| (flds, man))
        });

        validate2(flds_with_manifest, rename).and_then(move |((flds, manifest), rename)| {
            let model = StructModel {
                kind,
                manifest,
                transform: rename,
                fields: flds
            };
            if model.manifest.has_tag_field && model.transform.is_some() {
                let err = syn::Error::new_spanned(top, "Cannot apply a tag using a field when one has already been applied at the container level");
                Validation::Validated(model, err.into())
            } else {
                Validation::valid(model)
            }
        })
    }
}

impl TryFrom<&NestedMeta> for NameTransform {
    type Error = syn::Error;

    fn try_from(nested_meta: &NestedMeta) -> Result<Self, Self::Error> {
        match nested_meta {
            NestedMeta::Meta(Meta::NameValue(name)) if name.path == TAG_PATH => match &name.lit {
                Lit::Str(s) => {
                    let tag = s.value();
                    if tag.is_empty() {
                        Err(syn::Error::new_spanned(nested_meta, "New tag cannot be empty"))
                    } else {
                        Ok(NameTransform::Rename(tag))
                    }
                }
                _ => Err(syn::Error::new_spanned(nested_meta, "Expecting string argument"))
            },
            _ => Err(syn::Error::new_spanned(nested_meta, "Unknown container artribute"))
        }
    }
}

fn derive_manifest<'a, It>(data_struct: &'a DataStruct, fields: It) -> SynValidation<FieldManifest>
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
                    Some(syn::Error::new_spanned(&data_struct.fields, "At most one field can replace the tag attribute body."))
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
                    Some(syn::Error::new_spanned(&data_struct.fields, "At most one field can replace the body."))
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
                    Some(syn::Error::new_spanned(&data_struct.fields, "Duplicate tag"))
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
