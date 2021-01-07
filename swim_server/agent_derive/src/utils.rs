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

use core::fmt;
use proc_macro2::{Ident, Span};
use quote::quote;
use quote::quote_spanned;
use quote::ToTokens;
use std::fmt::{Display, Formatter};
use syn::parse::Parser;
use syn::{Data, DeriveInput};

const SWIM_AGENT: &str = "Swim agent";
const LIFECYCLE: &str = "Lifecycle";

pub fn get_task_struct_name(name: &str) -> Ident {
    Ident::new(&format!("{}Task", name), Span::call_site())
}

#[derive(Debug)]
pub enum InputAstType {
    Agent,
    Lifecycle,
}

impl Display for InputAstType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            InputAstType::Agent => write!(f, "{}", SWIM_AGENT),
            InputAstType::Lifecycle => write!(f, "{}", LIFECYCLE),
        }
    }
}

#[derive(Debug)]
pub enum InputAstError {
    UnionError(InputAstType, Span),
    EnumError(InputAstType, Span),
    GenericError(InputAstType, Span),
}

impl ToTokens for InputAstError {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        *tokens = match self {
            InputAstError::EnumError(ty, span) => {
                let message = format! {"{} cannot be created from Enum.", ty};
                quote_spanned! { *span => compile_error!(#message); }
            }
            InputAstError::UnionError(ty, span) => {
                let message = format! {"{} cannot be created from Union.", ty};
                quote_spanned! { *span => compile_error!(#message); }
            }
            InputAstError::GenericError(ty, span) => {
                let message = format! {"{} cannot have generic parameters.", ty};
                quote_spanned! { *span => compile_error!(#message); }
            }
        };
    }
}

pub fn validate_input_ast(input_ast: &DeriveInput, ty: InputAstType) -> Result<(), InputAstError> {
    match input_ast.data {
        Data::Enum(_) => Err(InputAstError::EnumError(ty, input_ast.ident.span())),
        Data::Union(_) => Err(InputAstError::UnionError(ty, input_ast.ident.span())),
        _ => {
            if !input_ast.generics.params.is_empty() {
                Err(InputAstError::GenericError(ty, input_ast.ident.span()))
            } else {
                Ok(())
            }
        }
    }
}

pub fn add_previous_value(input_ast: &mut DeriveInput, value_type: &Ident) {
    match &mut input_ast.data {
        syn::Data::Struct(ref mut struct_data) => match &mut struct_data.fields {
            syn::Fields::Named(fields) => {
                fields.named.push(
                    syn::Field::parse_named
                        .parse2(quote! { previous_value: std::sync::Arc<#value_type> })
                        .unwrap(),
                );
            }
            _ => (),
        },
        _ => panic!("`previous_value` has to be used with structs "),
    }
}
