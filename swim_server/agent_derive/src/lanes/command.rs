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

use crate::utils::{
    get_task_struct_name, parse_callback, validate_input_ast, CallbackKind, InputAstType,
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
    #[darling(default)]
    gen_lifecycle: Option<bool>,
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
    let gen_lifecycle = args
        .gen_lifecycle
        .unwrap_or_else(|| !has_fields(&input_ast.data));
    let task_name = get_task_struct_name(&input_ast.ident.to_string());
    let agent_name = args.agent.clone();
    let command_type = &args.command_type;
    let on_command_callback =
        parse_callback(&args.on_command, task_name.clone(), CallbackKind::Command);
    let lane_tasks_impl = LaneTasksImpl::Command {
        on_command: on_command_callback,
    };

    let extra_field = quote! {
        commands_tx: Option<swim_server::sync::circular_buffer::Sender<#command_type>>
    };

    derive_lane(
        "CommandLifecycle",
        lifecycle_name,
        gen_lifecycle,
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
        Some(extra_field),
        quote!(Command),
    )
}

pub fn derive_events_body(task_name: &Ident, on_command_func: &Ident) -> proc_macro2::TokenStream {
    quote!(
        let #task_name {
            mut lifecycle,
            event_stream,
            projection,
            mut commands_tx,
            ..
        } = *self;

        let model = projection(context.agent()).clone();
        let mut events = event_stream.take_until(context.agent_stop_event());
        let mut events = unsafe { Pin::new_unchecked(&mut events) };

        while let Some(event) = events.next().await {
            let (command, responder) = event.destruct();

            tracing::event!(tracing::Level::TRACE, commanded = swim_server::agent::COMMANDED, ?command);

            tracing_futures::Instrument::instrument(
                lifecycle.#on_command_func(&command, &model, &context),
                tracing::span!(tracing::Level::TRACE, swim_server::agent::ON_COMMAND)
            ).await;

            if let core::option::Option::Some(tx) = responder {
                if !tx.trigger() {
                    tracing::event!(tracing::Level::WARN, response_ingored = swim_server::agent::RESPONSE_IGNORED);
                }
            }
            if let core::option::Option::Some(tx) = commands_tx.as_mut() {
                if let core::result::Result::Err(swim_server::sync::circular_buffer::SendError(command)) = tx.try_send(command) {
                    tracing::event!(tracing::Level::ERROR, message = swim_server::agent::COMMAND_IO_DROPPED);
                    commands_tx = None;
                }
            }
        }
    )
}

pub fn default_events_body(task_name: &Ident) -> proc_macro2::TokenStream {
    quote!(
        let #task_name {
            event_stream,
            mut commands_tx,
            ..
        } = *self;

        let mut events = event_stream.take_until(context.agent_stop_event());
        let mut events = unsafe { Pin::new_unchecked(&mut events) };

        while let Some(event) = events.next().await {
            let (command, responder) = event.destruct();

            tracing::event!(tracing::Level::TRACE, commanded = swim_server::agent::COMMANDED, ?command);

            if let core::option::Option::Some(tx) = responder {
                if !tx.trigger() {
                    tracing::event!(tracing::Level::WARN, response_ingored = swim_server::agent::RESPONSE_IGNORED);
                }
            }
            if let core::option::Option::Some(tx) = commands_tx.as_mut() {
                if let core::result::Result::Err(swim_server::sync::circular_buffer::SendError(command)) = tx.try_send(command) {
                    tracing::event!(tracing::Level::ERROR, message = swim_server::agent::COMMAND_IO_DROPPED);
                    commands_tx = None;
                }
            }
        }
    )
}
