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
use utilities::validation::{Validation, ValidationItExt};
use utilities::algebra::Errors;
use syn::{NestedMeta, Attribute, Meta};

pub mod field;
pub mod record;

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
    attrs.filter(|a| a.path == FORM_PATH)
        .validate_fold(Validation::valid(init), false, move |state, attribute| {
            match attribute.parse_meta() {
                Ok(Meta::List(list)) => {
                    list.nested.into_iter().validate_fold(Validation::valid(state), false, &mut f)
                },
                Ok(_) => {
                    let err = syn::Error::new_spanned(attribute, &format!("Invalid attribute. Expected #[{}(...)]", FORM_PATH));
                    Validation::Validated(state, err.into())
                },
                Err(e) => {
                    let err = syn::Error::new_spanned(attribute, e.to_compile_error());
                    Validation::Validated(state, err.into())
                }
            }
        })
}