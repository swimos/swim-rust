// Copyright 2015-2021 SWIM.AI inc.
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

use crate::lanes::derive_lane;
use crate::utils::{
    get_task_struct_name, validate_input_ast, CallbackKind, InputAstType, LaneTasksImpl,
};
use crate::utils::{parse_callback, Callback};
use darling::FromMeta;
use macro_helpers::{has_fields, string_to_ident};
use proc_macro::TokenStream;
use quote::quote;
use syn::{AttributeArgs, DeriveInput, Ident};

#[derive(Debug, FromMeta)]
struct ActionAttrs {
    #[darling(map = "string_to_ident")]
    agent: Ident,
    #[darling(map = "string_to_ident")]
    command_type: Ident,
    #[darling(map = "string_to_ident")]
    response_type: Ident,
    #[darling(default)]
    on_command: Option<darling::Result<String>>,
}

pub fn derive_action_lifecycle(attr_args: AttributeArgs, input_ast: DeriveInput) -> TokenStream {
    if let Err(error) = validate_input_ast(&input_ast, InputAstType::Lifecycle) {
        return TokenStream::from(quote! {#error});
    }

    let args = match ActionAttrs::from_list(&attr_args) {
        Ok(args) => args,
        Err(e) => {
            return TokenStream::from(e.write_errors());
        }
    };

    let lifecycle_name = input_ast.ident.clone();
    let has_fields = has_fields(&input_ast.data);
    let task_name = get_task_struct_name(&input_ast.ident.to_string());
    let agent_name = args.agent.clone();
    let command_type = &args.command_type;
    let response_type = &args.response_type;
    let on_command_callback =
        parse_callback(&args.on_command, task_name.clone(), CallbackKind::Command);
    let lane_tasks_impl = LaneTasksImpl::Action {
        on_command: on_command_callback,
    };

    derive_lane(
        "ActionLifecycle",
        lifecycle_name,
        has_fields,
        task_name,
        agent_name,
        input_ast,
        quote!(swim_server::agent::lane::model::action::ActionLane<#command_type, #response_type>),
        quote!(swim_server::agent::lane::model::action::Action<#command_type, #response_type>),
        lane_tasks_impl,
        quote! {
            use swim_server::agent::lane::model::action::ActionLane;
            use swim_server::agent::lane::model::action::Action;
            use swim_server::agent::lane::lifecycle::LaneLifecycle;
        },
        None,
    )
}

pub fn derive_events_body(on_command: &Callback) -> proc_macro2::TokenStream {
    let task_name = &on_command.task_name;
    let on_action_func = &on_command.func_name;

    quote!(
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
                lifecycle.#on_action_func(command, &model, &context),
                tracing::span!(tracing::Level::TRACE, swim_server::agent::ON_COMMAND)
            ).await;

            tracing::event!(tracing::Level::TRACE, action_result = swim_server::agent::ACTION_RESULT, ?response);

            if let Some(tx) = responder {
                if tx.send(response).is_err() {
                    tracing::event!(tracing::Level::WARN, response_ingored = swim_server::agent::RESPONSE_IGNORED);
                }
            }
        }
    )
}
