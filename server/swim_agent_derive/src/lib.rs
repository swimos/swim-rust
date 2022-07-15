// Copyright 2015-2021 Swim Inc.
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

use lane_model_derive::DeriveAgentLaneModel;
use lane_projections::ProjectionsImpl;
use proc_macro::TokenStream;
use quote::{quote, ToTokens};

use swim_utilities::errors::Errors;
use syn::{parse_macro_input, DeriveInput, Item, Path};

mod agent_lifecycle;
mod lane_model_derive;
mod lane_projections;

#[proc_macro_derive(AgentLaneModel)]
pub fn derive_agent_lane_model(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    lane_model_derive::validate_input(&input)
        .map(DeriveAgentLaneModel::new)
        .map(ToTokens::into_token_stream)
        .into_result()
        .unwrap_or_else(errs_to_compile_errors)
        .into()
}

#[proc_macro_attribute]
pub fn projections(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr_params = if attr.is_empty() {
        None
    } else {
        Some(parse_macro_input!(attr as proc_macro2::TokenStream))
    };
    let item = parse_macro_input!(item as Item);
    lane_projections::validate_input(attr_params.as_ref(), &item)
        .map(ProjectionsImpl::new)
        .map(|proj| {
            quote! {
                #item
                #proj
            }
        })
        .into_result()
        .unwrap_or_else(errs_to_compile_errors)
        .into()
}

#[proc_macro_attribute]
pub fn lifecycle(attr: TokenStream, item: TokenStream) -> TokenStream {
    let path = parse_macro_input!(attr as Path);
    let item = parse_macro_input!(item as Item);
    let _ = agent_lifecycle::validate_input(&path, &item);
    todo!()
}

fn errs_to_compile_errors(errors: Errors<syn::Error>) -> proc_macro2::TokenStream {
    let compile_errors = errors
        .into_vec()
        .into_iter()
        .map(|e| syn::Error::to_compile_error(&e));
    quote!(#(#compile_errors)*)
}
