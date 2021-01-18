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

use crate::internals::{
    default_on_event, default_on_start, default_watch_strategy, parse_strategy,
};
use crate::lanes::derive_lane;
use crate::utils::{get_task_struct_name, validate_input_ast, InputAstType};
use darling::FromMeta;
use macro_helpers::string_to_ident;
use proc_macro::TokenStream;
use quote::quote;
use syn::{AttributeArgs, DeriveInput, Ident};

#[derive(Debug, FromMeta)]
struct ValueAttrs {
    #[darling(map = "string_to_ident")]
    agent: Ident,
    #[darling(map = "string_to_ident")]
    event_type: Ident,
    #[darling(default = "default_on_start", map = "string_to_ident")]
    on_start: Ident,
    #[darling(default = "default_on_event", map = "string_to_ident")]
    on_event: Ident,
    #[darling(default = "default_watch_strategy")]
    watch_strat: String,
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
    let watch_strat = parse_strategy(args.watch_strat.clone(), lifecycle_name.clone());

    let on_start = quote! {
        let #task_name { lifecycle, projection, .. } = self;
        let model = projection(context.agent());
        lifecycle.#on_start_func(model, context).boxed()
    };
    let on_event = quote! {
        let #task_name {
            lifecycle,
            event_stream,
            projection,
            ..
        } = *self;

        let model = projection(context.agent()).clone();
        let mut events = event_stream.take_until(context.agent_stop_event());
        let mut events = unsafe { Pin::new_unchecked(&mut events) };

        while let Some(event) = events.next().await {
              tracing_futures::Instrument::instrument(
                lifecycle.#on_event_func(&event, &model, &context),
                tracing::span!(tracing::Level::TRACE, swim_server::agent::ON_EVENT, ?event)
            ).await;
        }
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
        on_event,
        quote! {
            use swim_server::agent::lane::model::value::ValueLane;
            use swim_server::agent::lane::strategy::*;
            use std::num::NonZeroUsize;
            use swim_server::agent::lane::lifecycle::StatefulLaneLifecycleBase;
        },
        None,
        Some(watch_strat),
    )
}
