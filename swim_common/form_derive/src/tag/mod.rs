// Copyright 2015-2020 SWIM.AI inc.
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

use proc_macro2::Ident;
use syn::export::TokenStream2;
use syn::spanned::Spanned;
use syn::{Data, DeriveInput};

pub fn build_tag(input: DeriveInput) -> Result<proc_macro2::TokenStream, Vec<syn::Error>> {
    let ident = input.ident.clone();
    let variants = match &input.data {
        Data::Enum(data) => {
            let variants_len = data.variants.len();
            let variants = data.variants.iter().try_fold(
                Vec::with_capacity(variants_len),
                |mut vec, variant| {
                    if variant.fields.is_empty() {
                        vec.push(variant.ident.clone());
                        Ok(vec)
                    } else {
                        Err(vec![syn::Error::new(
                            variant.span(),
                            "Tagged variants cannot contain fields",
                        )])
                    }
                },
            )?;
            variants
        }
        _ => {
            return Err(vec![syn::Error::new(
                input.span(),
                "Tags can only be derived for enumerations",
            )])
        }
    };

    let from_string = derive_from_string(&ident, &variants);
    let as_string = derive_as_string(&ident, &variants);

    let structure_name = &input.ident;

    let ts = quote! {
        impl swim_common::form::Tag for #structure_name {
            fn from_string(tag: String) -> Result<Self, swim_common::form::TagConversionError> {
                #from_string
            }

            fn as_string(&self) -> String {
                #as_string
            }

            fn enumerated() -> Vec<Self> {
                vec![#(#ident::#variants,)*]
            }
        }
    };

    Ok(ts)
}

fn derive_from_string(compound_name: &Ident, variants: &[Ident]) -> TokenStream2 {
    let ts = variants.iter().fold(TokenStream2::new(), |ts, variant| {
        let name_str = variant.to_string().to_lowercase();
        quote! {
            #ts
            #name_str => Ok(#compound_name::#variant),
        }
    });

    quote! {
        match tag.to_lowercase().as_str() {
            #ts
            s => Err(swim_common::form::TagConversionError(format!("Unknown variant or struct: {}", s)))
        }
    }
}

fn derive_as_string(compound_name: &Ident, variants: &[Ident]) -> TokenStream2 {
    let ts = variants.iter().fold(TokenStream2::new(), |ts, variant| {
        let name_str = variant.to_string().to_lowercase();
        quote! {
            #ts
            #compound_name::#variant => #name_str,
        }
    });

    quote! {
        let s =  match self {
            #ts
        };
        s.to_string()
    }
}
