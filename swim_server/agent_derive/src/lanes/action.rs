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

use crate::internals::default_on_command;
use crate::lanes::derive_lane;
use crate::utils::{get_task_struct_name, validate_input_ast, InputAstType};
use macro_helpers::{Context, Symbol};
use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens};
use syn::spanned::Spanned;
use syn::{AttributeArgs, DeriveInput, Ident, Meta, NestedMeta};

const AGENT_PATH: Symbol = Symbol("agent");
const COMMAND_TYPE: Symbol = Symbol("command_type");
const RESPONSE_TYPE: Symbol = Symbol("response_type");
const ON_COMMAND: Symbol = Symbol("on_command");

pub fn derive_action_lifecycle(
    attr_args: AttributeArgs,
    input_ast: DeriveInput,
) -> Result<TokenStream2, Vec<syn::Error>> {
    let mut context = Context::default();

    if validate_input_ast(&input_ast, InputAstType::Lifecycle, &mut context).is_err() {
        return Err(context.check().unwrap_err());
    }

    let mut agent_opt = None;
    let mut command_type_opt = None;
    let mut response_type_opt = None;
    let mut on_command_func = default_on_command();

    attr_args.iter().for_each(|meta| match meta {
        NestedMeta::Meta(Meta::List(list)) if list.path == AGENT_PATH => {
            agent_opt = Some(Ident::new(
                &list.nested.to_token_stream().to_string(),
                list.span(),
            ));
        }
        NestedMeta::Meta(Meta::List(list)) if list.path == COMMAND_TYPE => {
            command_type_opt = Some(Ident::new(
                &list.nested.to_token_stream().to_string(),
                list.span(),
            ));
        }
        NestedMeta::Meta(Meta::List(list)) if list.path == RESPONSE_TYPE => {
            response_type_opt = Some(Ident::new(
                &list.nested.to_token_stream().to_string(),
                list.span(),
            ));
        }
        NestedMeta::Meta(Meta::List(list)) if list.path == ON_COMMAND => {
            on_command_func = Ident::new(&list.nested.to_token_stream().to_string(), list.span());
        }
        nm => {
            context.error_spanned_by(nm, "Unknown parameter");
        }
    });

    let lifecycle_name = input_ast.ident.clone();
    let task_name = get_task_struct_name(&input_ast.ident.to_string());
    let agent_name = syn_ok!(agent_opt, &input_ast, "Agent identity must be provided");
    let command_type = syn_ok!(
        command_type_opt,
        &input_ast,
        "Command type must be provided"
    );
    let response_type = syn_ok!(
        response_type_opt,
        &input_ast,
        "Response type must be provided"
    );

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
            let (command, responder) = event.destruct();

            tracing::event!(tracing::Level::TRACE, commanded = swim_server::agent::COMMANDED, ?command);

            let response = tracing_futures::Instrument::instrument(
                lifecycle.#on_command_func(command, &model, &context),
                tracing::span!(tracing::Level::TRACE, swim_server::agent::ON_COMMAND)
            ).await;

            tracing::event!(tracing::Level::TRACE, action_result = swim_server::agent::ACTION_RESULT, ?response);

            if let Some(tx) = responder {
                if tx.send(response).is_err() {
                    tracing::event!(tracing::Level::WARN, response_ingored = swim_server::agent::RESPONSE_IGNORED);
                }
            }
        }
    };

    Ok(derive_lane(
        "ActionLifecycle",
        lifecycle_name,
        task_name,
        agent_name,
        input_ast,
        quote!(swim_server::agent::lane::model::action::ActionLane<#command_type, #response_type>),
        quote!(swim_server::agent::lane::model::action::Action<#command_type, #response_type>),
        None,
        on_event,
        quote! {
            use swim_server::agent::lane::model::action::ActionLane;
            use swim_server::agent::lane::model::action::Action;
        },
        None,
    )
    .into())
}
