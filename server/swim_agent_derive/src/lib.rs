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

use agent_lifecycle::ImplAgentLifecycle;
use lane_model_derive::DeriveAgentLaneModel;
use lane_projections::ProjectionsImpl;
use proc_macro::TokenStream;
use quote::{quote, ToTokens};

use swim_utilities::errors::{validation::Validation, Errors};
use syn::{
    parse_macro_input, parse_quote, punctuated::Pair, AttributeArgs, DeriveInput, Item, Meta,
    NestedMeta,
};

mod agent_lifecycle;
mod lane_model_derive;
mod lane_projections;

fn default_root() -> syn::Path {
    parse_quote!(::swim::agent)
}

#[proc_macro_derive(AgentLaneModel, attributes(agent_root))]
pub fn derive_agent_lane_model(input: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    let root = extract_replace_root(&mut input.attrs).unwrap_or_else(default_root);
    lane_model_derive::validate_input(&input)
        .map(|model| DeriveAgentLaneModel::new(&root, model))
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
    let meta = parse_macro_input!(attr as AttributeArgs);
    let mut item = parse_macro_input!(item as Item);
    let path = agent_lifecycle::validate_attr_args(&item, meta);
    let stripped_attrs = agent_lifecycle::strip_handler_attrs(&mut item);
    Validation::join(path, stripped_attrs)
        .and_then(|(path, stripped_attrs)| {
            agent_lifecycle::validate_with_attrs(path, &item, stripped_attrs, default_root())
        })
        .map(|model| ImplAgentLifecycle::new(model))
        .map(|agent_lc| {
            quote! {
                #item
                #agent_lc
            }
        })
        .into_result()
        .unwrap_or_else(errs_to_compile_errors)
        .into()
}

fn errs_to_compile_errors(errors: Errors<syn::Error>) -> proc_macro2::TokenStream {
    let compile_errors = errors
        .into_vec()
        .into_iter()
        .map(|e| syn::Error::to_compile_error(&e));
    quote!(#(#compile_errors)*)
}

fn extract_replace_root(attrs: &mut Vec<syn::Attribute>) -> Option<syn::Path> {
    if let Some((i, p)) = attrs.iter_mut().enumerate().find_map(|(i, attr)| {
        if attr.path.is_ident("agent_root") {
            match attr.parse_meta() {
                Ok(Meta::List(lst)) => {
                    let mut nested = lst.nested;
                    match nested.pop().map(Pair::into_value) {
                        Some(NestedMeta::Meta(Meta::Path(p))) if nested.is_empty() => Some((i, p)),
                        _ => None,
                    }
                }
                _ => None,
            }
        } else {
            None
        }
    }) {
        attrs.remove(i);
        Some(p)
    } else {
        None
    }
}
