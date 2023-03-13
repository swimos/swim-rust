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
use macro_utilities::attr_names::{NEWTYPE_PATH, SCHEMA_PATH, TAG_PATH};
use macro_utilities::Symbol;
use quote::ToTokens;
use std::convert::TryFrom;
use swim_utilities::errors::validation::{Validation, ValidationItExt};

/// Description of how a type or field should be renamed in its serialized form.
pub enum NameTransform {
    /// Rename to a specific string.
    Rename(String),
}

/// Errors that can occur attempting to extract a name transformation from a macro attribute.
pub enum NameTransformError<'a> {
    NonStringName(&'a syn::Lit),
    EmptyName(&'a syn::LitStr),
    UnknownAttributeName(String, &'a dyn ToTokens),
    UnknownAttribute(&'a syn::NestedMeta),
}

impl<'a> From<NameTransformError<'a>> for syn::Error {
    fn from(err: NameTransformError<'a>) -> Self {
        match err {
            NameTransformError::NonStringName(name) => {
                syn::Error::new_spanned(name, "Expected a string literal")
            }
            NameTransformError::EmptyName(name) => {
                syn::Error::new_spanned(name, "New tag cannot be empty")
            }
            NameTransformError::UnknownAttributeName(name, tok) => {
                syn::Error::new_spanned(tok, format!("Unknown container attribute: {}", name))
            }
            NameTransformError::UnknownAttribute(tok) => {
                syn::Error::new_spanned(tok, "Unknown container attribute")
            }
        }
    }
}

impl<'a> TryFrom<&'a syn::NestedMeta> for NameTransform {
    type Error = NameTransformError<'a>;

    fn try_from(nested_meta: &'a syn::NestedMeta) -> Result<Self, Self::Error> {
        match nested_meta {
            syn::NestedMeta::Meta(syn::Meta::NameValue(name)) if name.path == TAG_PATH => {
                if name.path == TAG_PATH {
                    match &name.lit {
                        syn::Lit::Str(s) => {
                            let tag = s.value();
                            if tag.is_empty() {
                                Err(NameTransformError::EmptyName(s))
                            } else {
                                Ok(NameTransform::Rename(tag))
                            }
                        }
                        ow => Err(NameTransformError::NonStringName(ow)),
                    }
                } else if let Some(name_str) = name.path.get_ident().map(|id| id.to_string()) {
                    Err(NameTransformError::UnknownAttributeName(name_str, name))
                } else {
                    Err(NameTransformError::UnknownAttribute(nested_meta))
                }
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
    mut state: Option<NameTransform>,
    nested_meta: syn::NestedMeta,
) -> SynValidation<Option<NameTransform>> {
    let err = match NameTransform::try_from(&nested_meta) {
        Ok(rename) => {
            if state.is_some() {
                Some(syn::Error::new_spanned(nested_meta, "Duplicate tag"))
            } else {
                state = Some(rename);
                None
            }
        }
        Err(NameTransformError::UnknownAttributeName(name, _)) if name == SCHEMA_PATH => None, //Overlap with other macros which we can ignore.
        Err(e) => Some(e.into()),
    };
    Validation::Validated(state, err.into())
}

pub enum StructTransform<'a> {
    Rename(NameTransform),
    Newtype(Option<FieldSelector<'a>>),
}

/// Fold operation to extract a struct transform from the attributes on a type.
pub fn acc_struct_transform(
    mut state: Option<StructTransform>,
    nested_meta: syn::NestedMeta,
) -> SynValidation<Option<StructTransform>> {
    let err = match StructTransform::try_from(&nested_meta) {
        Ok(transform) => match (&mut state, transform) {
            (Some(StructTransform::Rename(_)), StructTransform::Rename(_)) => Some(
                syn::Error::new_spanned(nested_meta, "Duplicate `rename` tag"),
            ),
            (Some(StructTransform::Newtype(_)), StructTransform::Newtype(_)) => Some(
                syn::Error::new_spanned(nested_meta, "Duplicate `newtype` tag"),
            ),
            (None, transform) => match transform {
                StructTransform::Rename(rename) => {
                    state = Some(StructTransform::Rename(rename));
                    None
                }
                StructTransform::Newtype(_) => {
                    state = Some(StructTransform::Newtype(None));
                    None
                }
            },
            _ => Some(syn::Error::new_spanned(
                nested_meta,
                "Cannot use both `rename` and `newtype`",
            )),
        },
        Err(NameTransformError::UnknownAttributeName(name, _)) if name == SCHEMA_PATH => None, //Overlap with other macros which we can ignore.
        Err(e) => Some(e.into()),
    };
    Validation::Validated(state, err.into())
}

impl<'a> TryFrom<&'a syn::NestedMeta> for StructTransform<'a> {
    type Error = NameTransformError<'a>;

    fn try_from(nested_meta: &'a syn::NestedMeta) -> Result<Self, Self::Error> {
        match nested_meta {
            syn::NestedMeta::Meta(syn::Meta::Path(path)) if path == NEWTYPE_PATH => {
                if path == NEWTYPE_PATH {
                    Ok(StructTransform::Newtype(None))
                } else if let Some(name_str) = path.get_ident().map(|id| id.to_string()) {
                    Err(NameTransformError::UnknownAttributeName(name_str, path))
                } else {
                    Err(NameTransformError::UnknownAttribute(nested_meta))
                }
            }
            syn::NestedMeta::Meta(syn::Meta::NameValue(name)) if name.path == TAG_PATH => Ok(
                StructTransform::Rename(NameTransform::try_from(nested_meta)?),
            ),
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
