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

use crate::structural::model::field::FieldSelector;
use crate::SynValidation;
use macro_utilities::attr_names::{
    CONV_PATH, FIELDS_CONV_PATH, NEWTYPE_PATH, SCHEMA_PATH, TAG_PATH,
};
use macro_utilities::{
    name_transform_from_meta, type_name_transform_from_meta, NameTransform, NameTransformError,
    Symbol, TypeLevelNameTransform,
};
use std::convert::TryFrom;
use swim_utilities::errors::validation::{Validation, ValidationItExt};
use syn::NestedMeta;

/// Fold the attributes present on some syntactic element, accumulating errors.
pub fn fold_attr_meta<'a, It, S, F>(path: Symbol, attrs: It, init: S, mut f: F) -> SynValidation<S>
where
    It: Iterator<Item = &'a syn::Attribute> + 'a,
    F: FnMut(S, syn::NestedMeta) -> SynValidation<S>,
{
    attrs.filter(|a| a.path == path).validate_fold(
        Validation::valid(init),
        false,
        move |state, attribute| match attribute.parse_meta() {
            Ok(syn::Meta::List(list)) => {
                list.nested
                    .into_iter()
                    .validate_fold(Validation::valid(state), false, &mut f)
            }
            Ok(_) => {
                let err = syn::Error::new_spanned(
                    attribute,
                    format!("Invalid attribute. Expected #[{}(...)]", path),
                );
                Validation::Validated(state, err.into())
            }
            Err(e) => {
                let err = syn::Error::new_spanned(attribute, e.to_compile_error());
                Validation::Validated(state, err.into())
            }
        },
    )
}

/// Fold operation to extract a name transform from the attributes on a type or field.
pub fn acc_rename(
    mut state: NameTransform,
    nested_meta: syn::NestedMeta,
) -> SynValidation<NameTransform> {
    let err = match name_transform_from_meta(TAG_PATH, CONV_PATH, &nested_meta) {
        Ok(rename) => {
            if !matches!(state, NameTransform::Identity) {
                Some(syn::Error::new_spanned(nested_meta, "Duplicate tag"))
            } else {
                state = rename;
                None
            }
        }
        Err(NameTransformError::UnknownAttributeName(name, _)) if name == SCHEMA_PATH => None, //Overlap with other macros which we can ignore.
        Err(e) => Some(e.into()),
    };
    Validation::Validated(state, err.into())
}

pub enum StructTransformPart<'a> {
    Rename(NameTransform),
    FieldRename(TypeLevelNameTransform),
    Newtype(Option<FieldSelector<'a>>),
}

#[derive(Debug)]
pub enum StructTransform<'a> {
    Standard {
        rename: NameTransform,
        field_rename: TypeLevelNameTransform,
    },
    Newtype(Option<FieldSelector<'a>>),
}

impl<'a> Default for StructTransform<'a> {
    fn default() -> Self {
        StructTransform::Standard {
            rename: Default::default(),
            field_rename: Default::default(),
        }
    }
}

impl<'a> StructTransform<'a> {
    fn try_add(
        &mut self,
        nested_meta: &NestedMeta,
        part: StructTransformPart<'a>,
    ) -> Result<(), syn::Error> {
        match (&mut *self, part) {
            (StructTransform::Standard { rename, .. }, StructTransformPart::Rename(r)) => {
                if rename.is_identity() {
                    *rename = r;
                    Ok(())
                } else {
                    Err(syn::Error::new_spanned(
                        nested_meta,
                        "Duplicate `rename` tag",
                    ))
                }
            }
            (
                StructTransform::Standard { field_rename, .. },
                StructTransformPart::FieldRename(fr),
            ) => {
                if field_rename.is_identity() {
                    *field_rename = fr;
                    Ok(())
                } else {
                    Err(syn::Error::new_spanned(
                        nested_meta,
                        "Duplicate `fields` tag",
                    ))
                }
            }
            (
                StructTransform::Standard {
                    rename: NameTransform::Identity,
                    field_rename: TypeLevelNameTransform::Identity,
                },
                StructTransformPart::Newtype(t),
            ) => {
                *self = StructTransform::Newtype(t);
                Ok(())
            }
            (StructTransform::Newtype(_), StructTransformPart::Newtype(_)) => Err(
                syn::Error::new_spanned(nested_meta, "Duplicate `newtype` tag"),
            ),
            _ => Err(syn::Error::new_spanned(
                nested_meta,
                "Cannot use both `rename` or `fields` and `newtype`",
            )),
        }
    }
}

/// Fold operation to extract a struct transform from the attributes on a type.
pub fn acc_struct_transform(
    mut state: StructTransform,
    nested_meta: syn::NestedMeta,
) -> SynValidation<StructTransform> {
    let err = match StructTransformPart::try_from(&nested_meta) {
        Ok(part) => state.try_add(&nested_meta, part).err(),
        Err(NameTransformError::UnknownAttributeName(name, _)) if name == SCHEMA_PATH => None, //Overlap with other macros which we can ignore.
        Err(e) => Some(e.into()),
    };
    Validation::Validated(state, err.into())
}

impl<'a> TryFrom<&'a syn::NestedMeta> for StructTransformPart<'static> {
    type Error = NameTransformError<'a>;

    fn try_from(nested_meta: &'a syn::NestedMeta) -> Result<Self, Self::Error> {
        match nested_meta {
            syn::NestedMeta::Meta(syn::Meta::Path(path)) if path == NEWTYPE_PATH => {
                if path == NEWTYPE_PATH {
                    Ok(StructTransformPart::Newtype(None))
                } else if let Some(name_str) = path.get_ident().map(|id| id.to_string()) {
                    Err(NameTransformError::UnknownAttributeName(name_str, path))
                } else {
                    Err(NameTransformError::UnknownAttribute(nested_meta))
                }
            }
            syn::NestedMeta::Meta(syn::Meta::NameValue(name)) if name.path == TAG_PATH => {
                Ok(StructTransformPart::Rename(name_transform_from_meta(
                    TAG_PATH,
                    CONV_PATH,
                    nested_meta,
                )?))
            }
            syn::NestedMeta::Meta(syn::Meta::NameValue(name)) if name.path == FIELDS_CONV_PATH => {
                Ok(StructTransformPart::FieldRename(
                    type_name_transform_from_meta(FIELDS_CONV_PATH, nested_meta)?,
                ))
            }
            syn::NestedMeta::Meta(syn::Meta::List(lst)) => {
                if let Some(name_str) = lst.path.get_ident().map(|id| id.to_string()) {
                    Err(NameTransformError::UnknownAttributeName(name_str, lst))
                } else {
                    Err(NameTransformError::UnknownAttribute(nested_meta))
                }
            }
            _ => Err(NameTransformError::UnknownAttribute(nested_meta)),
        }
    }
}
