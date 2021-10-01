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

use crate::modifiers::NameTransform;
use crate::parser::FORM_PATH;
use crate::quote::TokenStreamExt;
use proc_macro2::{Span, TokenStream};
use quote::ToTokens;
use std::fmt::{Display, Formatter};
use swim_utilities::errors::validation::ValidationItExt;
use swim_utilities::errors::Errors;

/// Model for an enumeration where all variants have no fields.
pub struct UnitEnum<'a> {
    /// The name of the enumeration.
    name: &'a syn::Ident,
    /// The name of each variant, in order.
    variants: Vec<(&'a syn::Ident, Option<NameTransform>)>,
}

impl<'a> UnitEnum<'a> {
    pub fn new(
        name: &'a syn::Ident,
        variants: Vec<(&'a syn::Ident, Option<NameTransform>)>,
    ) -> Self {
        UnitEnum { name, variants }
    }
}

/// Derives the `Tag` trait or a type.
pub struct DeriveTag<T>(pub T);

fn lit_name(var_name: &syn::Ident, rename: &Option<NameTransform>) -> syn::LitStr {
    if let Some(NameTransform::Rename(renamed)) = rename {
        syn::LitStr::new(renamed.as_ref(), Span::call_site())
    } else {
        syn::LitStr::new(&var_name.to_string(), Span::call_site())
    }
}

impl<'a> ToTokens for DeriveTag<UnitEnum<'a>> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let DeriveTag(UnitEnum { name, variants }) = self;

        let var_as_str = variants.iter().map(|(var_name, rename)| {
            let lit = lit_name(*var_name, rename);
            quote!(#name::#var_name => #lit)
        });

        let str_as_var = variants.iter().map(|(var_name, rename)| {
            let lit = lit_name(*var_name, rename);
            quote!(#lit => core::result::Result::Ok(#name::#var_name))
        });

        let literals = variants
            .iter()
            .map(|(var_name, rename)| lit_name(*var_name, rename));

        let err_lit = format!("Possible values are: {}.", Variants(variants.as_slice()));
        let num_vars = variants.len();

        let as_ref_body = if num_vars == 0 {
            quote! {
                core::panic!("No members.")
            }
        } else {
            quote! {
                match self {
                    #(#var_as_str,)*
                }
            }
        };

        tokens.append_all(quote! {
            impl core::convert::AsRef<str> for #name {
                fn as_ref(&self) -> &str {
                    #as_ref_body
                }
            }

            impl core::str::FromStr for #name {
                type Err = swim_model::Text;

                fn from_str(txt: &str) -> Result<Self, Self::Err> {
                    match txt {
                        #(#str_as_var,)*
                        _ => core::result::Result::Err(swim_model::Text::new(#err_lit)),
                    }
                }
            }

            const _: () = {

                const VARIANT_NAMES: [&str; #num_vars] = [#(#literals),*];

                #[automatically_derived]
                impl swim_form::structural::Tag for #name {

                    const VARIANTS: &'static [&'static str] = &VARIANT_NAMES;
                }

            };
        });
    }
}

/// Format the variants of an enumeration into a string for the failed parse error message.
struct Variants<'a>(&'a [(&'a syn::Ident, Option<NameTransform>)]);

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

pub fn build_derive_tag(input: syn::DeriveInput) -> Result<TokenStream, Errors<syn::Error>> {
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
                        let rename = crate::modifiers::fold_attr_meta(
                            FORM_PATH,
                            v.attrs.iter(),
                            None,
                            crate::modifiers::acc_rename,
                        );
                        rename.map(move |rename| (&v.ident, rename))
                    })
                    .map(|var_names| {
                        DeriveTag(UnitEnum::new(&input.ident, var_names)).into_token_stream()
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
