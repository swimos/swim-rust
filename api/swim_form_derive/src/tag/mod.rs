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

use crate::quote::TokenStreamExt;
use macro_utilities::attr_names::{CONV_NAME, FORM_NAME, TAG_NAME};
use macro_utilities::attributes::consume_attributes;
use macro_utilities::{combine_name_transform, NameTransform, NameTransformConsumer};
use proc_macro2::TokenStream;
use quote::ToTokens;
use std::fmt::{Display, Formatter};
use swim_utilities::errors::validation::{Validation, ValidationItExt};
use swim_utilities::errors::Errors;

/// Model for an enumeration where all variants have no fields.
pub struct UnitEnum<'a> {
    root: &'a syn::Path,
    /// The name of the enumeration.
    name: &'a syn::Ident,
    /// The name of each variant, in order.
    variants: Vec<(&'a syn::Ident, NameTransform)>,
}

impl<'a> UnitEnum<'a> {
    pub fn new(
        root: &'a syn::Path,
        name: &'a syn::Ident,
        variants: Vec<(&'a syn::Ident, NameTransform)>,
    ) -> Self {
        UnitEnum {
            root,
            name,
            variants,
        }
    }
}

/// Derives the `Tag` trait or a type.
pub struct DeriveTag<T>(pub T);

impl<'a> ToTokens for DeriveTag<UnitEnum<'a>> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let DeriveTag(UnitEnum {
            root,
            name,
            variants,
            ..
        }) = self;

        let var_as_str = variants.iter().map(|(var_name, rename)| {
            let lit = rename.transform(|| var_name.to_string());
            quote!(#name::#var_name => #lit)
        });

        let str_as_var = variants.iter().map(|(var_name, rename)| {
            let lit = rename.transform(|| var_name.to_string());
            quote!(#lit => ::core::result::Result::Ok(#name::#var_name))
        });

        let literals = variants
            .iter()
            .map(|(var_name, rename)| rename.transform(|| var_name.to_string()));

        let err_lit = format!("Possible values are: {}.", Variants(variants.as_slice()));
        let num_vars = variants.len();

        let as_ref_body = if num_vars == 0 {
            quote! {
                ::core::panic!("No members.")
            }
        } else {
            quote! {
                match self {
                    #(#var_as_str,)*
                }
            }
        };

        tokens.append_all(quote! {
            impl ::core::convert::AsRef<str> for #name {
                fn as_ref(&self) -> &str {
                    #as_ref_body
                }
            }

            impl ::core::str::FromStr for #name {
                type Err = #root::model::Text;

                fn from_str(txt: &str) -> core::result::Result<Self, Self::Err> {
                    match txt {
                        #(#str_as_var,)*
                        _ => core::result::Result::Err(#root::model::Text::new(#err_lit)),
                    }
                }
            }

            const _: () = {

                const VARIANT_NAMES: [&str; #num_vars] = [#(#literals),*];

                #[automatically_derived]
                impl #root::structural::Tag for #name {

                    const VARIANTS: &'static [&'static str] = &VARIANT_NAMES;
                }

            };
        });
    }
}

/// Format the variants of an enumeration into a string for the failed parse error message.
struct Variants<'a>(&'a [(&'a syn::Ident, NameTransform)]);

impl<'a> Display for Variants<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Variants(names) = self;
        let mut it = names.iter();
        if let Some((first, _)) = it.next() {
            write!(f, "'{}'", first)?;
        }
        for (name, _) in it {
            write!(f, ", '{}'", name)?;
        }
        Ok(())
    }
}

const ENUM_WITH_FIELDS_ERR: &str = "Only enumerations with no fields can be tags.";
const NON_ENUM_TYPE_ERR: &str = "Only enumeration types can be tags.";

pub fn build_derive_tag(
    root: syn::Path,
    input: syn::DeriveInput,
) -> Result<TokenStream, Errors<syn::Error>> {
    match &input.data {
        syn::Data::Enum(enum_ty) => {
            if enum_ty.variants.iter().any(|var| !var.fields.is_empty()) {
                Err(Errors::of(syn::Error::new_spanned(
                    input,
                    ENUM_WITH_FIELDS_ERR,
                )))
            } else {
                let validated = enum_ty
                    .variants
                    .iter()
                    .validate_collect(true, |v| {
                        let (transforms, errors) = consume_attributes(
                            FORM_NAME,
                            &v.attrs,
                            NameTransformConsumer::new(TAG_NAME, CONV_NAME),
                        );
                        let rename = Validation::Validated(transforms, Errors::from(errors))
                            .and_then(|transforms| match combine_name_transform(v, transforms) {
                                Ok(t) => Validation::valid(t),
                                Err(e) => Validation::Failed(Errors::of(e)),
                            });
                        rename.map(move |rename| (&v.ident, rename))
                    })
                    .map(|var_names| {
                        DeriveTag(UnitEnum::new(&root, &input.ident, var_names)).into_token_stream()
                    });
                validated.into_result()
            }
        }
        _ => Err(Errors::of(syn::Error::new_spanned(
            input,
            NON_ENUM_TYPE_ERR,
        ))),
    }
}
