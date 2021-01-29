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

use crate::utils::{
    get_task_struct_name, parse_callback, validate_input_ast, Callback, CallbackKind, InputAstType,
    LaneTasksImpl,
};
use darling::FromMeta;
use macro_helpers::{has_fields, string_to_ident};
use proc_macro::TokenStream;
use quote::quote;
use syn::{AttributeArgs, DeriveInput};

use crate::lanes::derive_lane;
use proc_macro2::Ident;

#[derive(Debug, FromMeta)]
struct CommandAttrs {
    #[darling(map = "string_to_ident")]
    agent: Ident,
    #[darling(map = "string_to_ident")]
    command_type: Ident,
    #[darling(default)]
    on_command: Option<darling::Result<String>>,
}

pub fn derive_command_lifecycle(attr_args: AttributeArgs, input_ast: DeriveInput) -> TokenStream {
    if let Err(error) = validate_input_ast(&input_ast, InputAstType::Lifecycle) {
        return TokenStream::from(quote! {#error});
    }

    let args = match CommandAttrs::from_list(&attr_args) {
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
    let on_command_callback =
        parse_callback(&args.on_command, task_name.clone(), CallbackKind::Command);
    let lane_tasks_impl = LaneTasksImpl::Command {
        on_command: on_command_callback,
    };

    derive_lane(
        "CommandLifecycle",
        lifecycle_name,
        has_fields,
        task_name,
        agent_name,
        input_ast,
        quote!(swim_server::agent::lane::model::command::CommandLane<#command_type>),
        quote!(swim_server::agent::lane::model::command::Command<#command_type>),
        lane_tasks_impl,
        quote! {
            use swim_server::agent::lane::model::command::CommandLane;
            use swim_server::agent::lane::model::command::Command;
            use swim_server::agent::lane::lifecycle::LaneLifecycle;
        },
        None,
    )
}

pub fn derive_events_body(on_command: &Callback) -> proc_macro2::TokenStream {
    let task_name = &on_command.task_name;
    let on_command_func = &on_command.func_name;

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

            tracing_futures::Instrument::instrument(
                lifecycle.#on_command_func(command.clone(), &model, &context),
                tracing::span!(tracing::Level::TRACE, swim_server::agent::ON_COMMAND)
            ).await;

            if let Some(tx) = responder {
                if tx.send(command).is_err() {
                    tracing::event!(tracing::Level::WARN, response_ingored = swim_server::agent::RESPONSE_IGNORED);
                }
            }
        }
    )
}
