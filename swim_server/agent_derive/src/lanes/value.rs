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
use macro_helpers::{Context, Symbol};
use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens};
use syn::spanned::Spanned;
use syn::{AttributeArgs, DeriveInput, Ident, Meta, NestedMeta};
const AGENT_PATH: Symbol = Symbol("agent");
const EVENT_TYPE: Symbol = Symbol("event_type");
const ON_START: Symbol = Symbol("on_start");
const ON_EVENT: Symbol = Symbol("on_event");

pub fn derive_value_lifecycle(
    attr_args: AttributeArgs,
    input_ast: DeriveInput,
) -> Result<TokenStream2, Vec<syn::Error>> {
    let mut context = Context::default();

    if validate_input_ast(&input_ast, InputAstType::Lifecycle, &mut context).is_err() {
        return Err(context.check().unwrap_err());
    }

    let mut agent_opt = None;
    let mut event_type_opt = None;
    let mut on_start_func = default_on_start();
    let mut on_event_func = default_on_event();

    attr_args.iter().for_each(|meta| match meta {
        NestedMeta::Meta(Meta::List(list)) if list.path == AGENT_PATH => {
            agent_opt = Some(Ident::new(
                &list.nested.to_token_stream().to_string(),
                list.span(),
            ));
        }
        NestedMeta::Meta(Meta::List(list)) if list.path == EVENT_TYPE => {
            event_type_opt = Some(Ident::new(
                &list.nested.to_token_stream().to_string(),
                list.span(),
            ));
        }
        NestedMeta::Meta(Meta::List(list)) if list.path == ON_START => {
            on_start_func = Ident::new(&list.nested.to_token_stream().to_string(), list.span());
        }
        NestedMeta::Meta(Meta::List(list)) if list.path == ON_EVENT => {
            on_event_func = Ident::new(&list.nested.to_token_stream().to_string(), list.span());
        }
        nm => {
            context.error_spanned_by(nm, "Unknown parameter");
        }
    });

    if let Err(errors) = context.check() {
        return Err(errors);
    }

    let lifecycle_name = input_ast.ident.clone();
    let task_name = get_task_struct_name(&input_ast.ident.to_string());
    let agent_name = syn_ok!(agent_opt, &input_ast, "Agent identity must be provided");
    let event_type = syn_ok!(event_type_opt, &input_ast, "Event type must be provided");

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

    Ok(derive_lane(
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
        },
        None,
    )
    .into())
}
