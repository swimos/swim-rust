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

use crate::internals::{default_on_event, default_on_start};
use crate::lanes::derive_lane;
use crate::utils::{get_task_struct_name, validate_input_ast, InputAstType};
use darling::FromMeta;
use macro_helpers::string_to_ident;
use proc_macro::TokenStream;
use quote::quote;
use syn::{AttributeArgs, DeriveInput, Ident};

#[derive(Debug, FromMeta)]
pub struct ValueAttrs {
    #[darling(map = "string_to_ident")]
    pub agent: Ident,
    #[darling(map = "string_to_ident")]
    pub event_type: Ident,
    #[darling(default = "default_on_start", map = "string_to_ident")]
    pub on_start: Ident,
    #[darling(default = "default_on_event", map = "string_to_ident")]
    pub on_event: Ident,
}

pub fn derive_value_lifecycle(attr_args: AttributeArgs, input_ast: DeriveInput) -> TokenStream {
    if let Err(error) = validate_input_ast(&input_ast, InputAstType::Lifecycle) {
        return TokenStream::from(quote! {#error});
    }

    let args = match ValueAttrs::from_list(&attr_args) {
        Ok(args) => args,
        Err(e) => {
            return TokenStream::from(e.write_errors());
        }
    };

    let lifecycle_name = input_ast.ident.clone();
    let task_name = get_task_struct_name(&input_ast.ident.to_string());
    let agent_name = args.agent.clone();
    let event_type = &args.event_type;
    let on_start_func = &args.on_start;
    let on_event_func = &args.on_event;

    let on_start = quote! {
        let #task_name { lifecycle, projection, .. } = self;
        let model = projection(context.agent());
        lifecycle.#on_start_func(model, context).boxed()
    };

    derive_lane(
        "ValueLifecycle",
        lifecycle_name,
        task_name,
        agent_name,
        input_ast,
        quote!(swim_server::agent::lane::model::value::ValueLane<#event_type>),
        quote!(std::sync::Arc<#event_type>),
        Some(on_start),
        quote! {
            tracing_futures::Instrument::instrument(
                lifecycle.#on_event_func(&event, &model, &context),
                tracing::span!(tracing::Level::TRACE, swim_server::agent::ON_EVENT, ?event)
            ).await;
        },
        quote! {
            use swim_server::agent::lane::model::value::ValueLane;
        },
    )
}
