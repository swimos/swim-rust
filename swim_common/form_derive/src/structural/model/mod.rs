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

use crate::parser::FORM_PATH;
use syn::{Attribute, DataStruct, Fields, Meta, NestedMeta, Variant};
use utilities::algebra::Errors;
use utilities::validation::{Validation, ValidationItExt};

pub mod enumeration;
pub mod field;
pub mod record;

use crate::parser::TAG_PATH;
use std::convert::TryFrom;
use syn::Lit;

pub type SynValidation<T> = Validation<T, Errors<syn::Error>>;

pub enum NameTransform {
    Rename(String),
}

trait TryValidate<T>: Sized {
    fn try_validate(input: T) -> SynValidation<Self>;
}

fn fold_attr_meta<'a, It, S, F>(attrs: It, init: S, mut f: F) -> SynValidation<S>
where
    It: Iterator<Item = &'a Attribute> + 'a,
    F: FnMut(S, NestedMeta) -> SynValidation<S>,
{
    attrs.filter(|a| a.path == FORM_PATH).validate_fold(
        Validation::valid(init),
        false,
        move |state, attribute| match attribute.parse_meta() {
            Ok(Meta::List(list)) => {
                list.nested
                    .into_iter()
                    .validate_fold(Validation::valid(state), false, &mut f)
            }
            Ok(_) => {
                let err = syn::Error::new_spanned(
                    attribute,
                    &format!("Invalid attribute. Expected #[{}(...)]", FORM_PATH),
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

impl TryFrom<&NestedMeta> for NameTransform {
    type Error = syn::Error;

    fn try_from(nested_meta: &NestedMeta) -> Result<Self, Self::Error> {
        match nested_meta {
            NestedMeta::Meta(Meta::NameValue(name)) if name.path == TAG_PATH => match &name.lit {
                Lit::Str(s) => {
                    let tag = s.value();
                    if tag.is_empty() {
                        Err(syn::Error::new_spanned(
                            nested_meta,
                            "New tag cannot be empty",
                        ))
                    } else {
                        Ok(NameTransform::Rename(tag))
                    }
                }
                _ => Err(syn::Error::new_spanned(
                    nested_meta,
                    "Expecting string argument",
                )),
            },
            _ => Err(syn::Error::new_spanned(
                nested_meta,
                "Unknown container artribute",
            )),
        }
    }
}

fn acc_rename(
    mut state: Option<NameTransform>,
    nested_meta: NestedMeta,
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
        Err(e) => Some(e),
    };
    Validation::Validated(state, err.into())
}

trait StructLike {
    fn fields(&self) -> &Fields;
}

impl<T: StructLike> StructLike for &T {
    fn fields(&self) -> &Fields {
        (*self).fields()
    }
}

impl StructLike for DataStruct {
    fn fields(&self) -> &Fields {
        &self.fields
    }
}

impl StructLike for Variant {
    fn fields(&self) -> &Fields {
        &self.fields
    }
}
