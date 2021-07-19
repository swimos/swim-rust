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

use crate::parser::TAG_PATH;
use crate::SynValidation;
use std::convert::TryFrom;
use utilities::validation::{Validation, ValidationItExt};
use utilities::Symbol;

/// Description of how a type or field should be renamed in its serialized form.
pub enum NameTransform {
    /// Rename to a specific string.
    Rename(String),
}

impl TryFrom<&syn::NestedMeta> for NameTransform {
    type Error = syn::Error;

    fn try_from(nested_meta: &syn::NestedMeta) -> Result<Self, Self::Error> {
        match nested_meta {
            syn::NestedMeta::Meta(syn::Meta::NameValue(name)) if name.path == TAG_PATH => {
                match &name.lit {
                    syn::Lit::Str(s) => {
                        let tag = s.value();
                        if tag.is_empty() {
                            Err(syn::Error::new_spanned(s, "New tag cannot be empty"))
                        } else {
                            Ok(NameTransform::Rename(tag))
                        }
                    }
                    _ => Err(syn::Error::new_spanned(name, "Expecting string argument")),
                }
            }
            _ => Err(syn::Error::new_spanned(
                nested_meta,
                "Unknown container attribute",
            )),
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
                    &format!("Invalid attribute. Expected #[{}(...)]", path),
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
        Err(_) => None, //Overlap with other macros which we can ignore.
    };
    Validation::Validated(state, err.into())
}
