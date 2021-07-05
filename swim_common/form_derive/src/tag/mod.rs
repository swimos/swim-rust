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

use crate::quote::TokenStreamExt;
use proc_macro2::{Span, TokenStream};
use quote::ToTokens;
use std::fmt::{Display, Formatter};

/// Model for an enumeration where all variants have no fields.
pub struct UnitEnum<'a> {
    /// The name of the enumeration.
    name: &'a syn::Ident,
    /// The name of each variant, in order.
    variants: Vec<&'a syn::Ident>,
}

impl<'a> UnitEnum<'a> {
    pub fn new(name: &'a syn::Ident, variants: Vec<&'a syn::Ident>) -> Self {
        UnitEnum { name, variants }
    }
}

/// Derives the `Tag` trait or a type.
pub struct DeriveTag<T>(pub T);

impl<'a> ToTokens for DeriveTag<UnitEnum<'a>> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let DeriveTag(UnitEnum { name, variants }) = self;

        let var_as_str = variants.iter().map(|var_name| {
            let lit = syn::LitStr::new(&var_name.to_string(), Span::call_site());
            quote!(#name::#var_name => #lit)
        });

        let str_as_var = variants.iter().map(|var_name| {
            let lit = syn::LitStr::new(&var_name.to_string(), Span::call_site());
            quote!(#lit => core::result::Result::Ok(#name::#var_name))
        });

        let literals = variants.iter().map(|var_name| var_name.to_string());

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

            const _: () = {

                const VARIANT_NAMES: [&str; #num_vars] = [#(#literals),*];

                impl swim_common::form::structural::Tag for #name {
                    fn try_from_str(txt: &str) -> core::result::Result<Self, swim_common::model::text::Text> {
                        match txt {
                            #(#str_as_var,)*
                            _ => core::result::Result::Err(swim_common::model::text::Text::new(#err_lit)),
                        }
                    }

                    fn universe() -> &'static [&'static str] {
                        &VARIANT_NAMES
                    }
                }

            };
        });
    }
}

/// Format the variants of an enumeration into a string for the failed parse error message.
struct Variants<'a>(&'a [&'a syn::Ident]);

impl<'a> Display for Variants<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Variants(names) = self;
        let mut it = names.iter();
        if let Some(first) = it.next() {
            write!(f, "'{}'", first)?;
        }
        for name in it {
            write!(f, ", '{}'", name)?;
        }
        Ok(())
    }
}
