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

use agent_lifecycle::ImplAgentLifecycle;
use lane_model_derive::{combine_agent_attrs, make_agent_attr_consumer, DeriveAgentLaneModel};
use lane_projections::ProjectionsImpl;
use proc_macro::TokenStream;
use quote::{quote, ToTokens};

use macro_utilities::{attributes::consume_attributes, to_compile_errors};
use swim_utilities::errors::{validation::Validation, Errors};
use syn::{parse_macro_input, parse_quote, AttributeArgs, DeriveInput, Item};

mod agent_lifecycle;
mod lane_model_derive;
mod lane_projections;

fn default_root() -> syn::Path {
    parse_quote!(::swim::agent)
}

const AGENT_TAG: &str = "agent";

#[proc_macro_derive(AgentLaneModel, attributes(agent, lane))]
pub fn derive_agent_lane_model(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let (item_attrs, errors) =
        consume_attributes(AGENT_TAG, &input.attrs, make_agent_attr_consumer());
    let modifiers = Validation::Validated(item_attrs, Errors::from(errors))
        .and_then(|item_attrs| combine_agent_attrs(&input, item_attrs));

    lane_model_derive::validate_input(&input)
        .join(modifiers)
        .and_then(|(model, modifiers)| DeriveAgentLaneModel::validate(&input, modifiers, model))
        .map(ToTokens::into_token_stream)
        .into_result()
        .unwrap_or_else(|errs| to_compile_errors(errs.into_vec()))
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
        .unwrap_or_else(|errs| to_compile_errors(errs.into_vec()))
        .into()
}

#[proc_macro_attribute]
pub fn lifecycle(attr: TokenStream, item: TokenStream) -> TokenStream {
    let meta = parse_macro_input!(attr as AttributeArgs);
    let mut item = parse_macro_input!(item as Item);
    let path = agent_lifecycle::validate_attr_args(&item, meta);
    let stripped_attrs = agent_lifecycle::strip_handler_attrs(&mut item);
    Validation::join(path, stripped_attrs)
        .and_then(|(path, stripped_attrs)| {
            agent_lifecycle::validate_with_attrs(path, &item, stripped_attrs, default_root())
        })
        .map(ImplAgentLifecycle::new)
        .map(|agent_lc| {
            quote! {
                #item
                #agent_lc
            }
        })
        .into_result()
        .unwrap_or_else(|errs| to_compile_errors(errs.into_vec()))
        .into()
}
