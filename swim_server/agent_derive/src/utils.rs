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
use darling::FromMeta;
use proc_macro2::{Ident, Span, TokenStream};
use quote::ToTokens;
use quote::{quote, quote_spanned};
use std::fmt::{Display, Formatter};
use syn::{Data, DeriveInput};

const SWIM_AGENT: &str = "Swim agent";
const LIFECYCLE: &str = "Lifecycle";

pub fn get_task_struct_name(name: &str) -> Ident {
    Ident::new(&format!("{}Task", name), Span::call_site())
}

#[derive(Debug, FromMeta)]
pub struct WatchStrategy {
    pub ty: Ident,
    pub param: Option<usize>,
    pub lifecycle_name: Ident,
}

impl ToTokens for WatchStrategy {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let watch_strat_type = &self.ty;
        let watch_strat_param = &self.param;
        let lifecycle_name = &self.lifecycle_name;

        let create = if watch_strat_param.is_none() {
            quote! {swim_server::agent::lane::strategy::#watch_strat_type::default()}
        } else {
            quote! {swim_server::agent::lane::strategy::#watch_strat_type(std::num::NonZeroUsize::new(#watch_strat_param).unwrap())}
        };

        quote! (
            #[automatically_derived]
            impl swim_server::agent::lane::lifecycle::StatefulLaneLifecycleBase for #lifecycle_name {
                type WatchStrategy = swim_server::agent::lane::strategy::#watch_strat_type;

                fn create_strategy(&self) -> Self::WatchStrategy {
                    #create
                }
            }
        ).to_tokens(tokens);
    }
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
