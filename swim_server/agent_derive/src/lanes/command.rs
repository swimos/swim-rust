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

use crate::utils::{get_task_struct_name, validate_input_ast, InputAstType};
use darling::FromMeta;
use macro_helpers::{as_const, string_to_ident};
use proc_macro::TokenStream;
use quote::quote;
use syn::{AttributeArgs, DeriveInput};

use crate::internals::default_on_command;
use proc_macro2::Ident;

#[derive(Debug, FromMeta)]
pub struct CommandAttrs {
    #[darling(map = "string_to_ident")]
    pub agent: Ident,
    #[darling(map = "string_to_ident")]
    pub command_type: Ident,
    #[darling(default = "default_on_command", map = "string_to_ident")]
    pub on_command: Ident,
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
    let task_name = get_task_struct_name(&input_ast.ident.to_string());
    let agent_name = &args.agent;
    let command_type = &args.command_type;
    let on_command_func = &args.on_command;

    let public_derived = quote! {
        #input_ast

        struct #task_name<T, S>
        where
            T: core::ops::Fn(&#agent_name) -> &swim_server::agent::lane::model::action::CommandLane<#command_type> + Send + Sync + 'static,
            S: futures::Stream<Item = swim_server::agent::lane::model::action::Action<#command_type, ()>> + Send + Sync + 'static
        {
            lifecycle: #lifecycle_name,
            name: String,
            event_stream: S,
            projection: T,
        }
    };

    let private_derived = quote! {
        use futures::FutureExt as _;
        use futures::StreamExt as _;
        use futures::Stream;
        use futures::future::{ready, BoxFuture};

        use swim_server::agent::lane::model::action::CommandLane;
        use swim_server::agent::lane::model::action::Action;
        use swim_server::agent::{Lane, LaneTasks};
        use swim_server::agent::AgentContext;
        use swim_server::agent::context::AgentExecutionContext;

        use core::pin::Pin;

        #[automatically_derived]
        impl<T, S> Lane for #task_name<T, S>
        where
            T: Fn(&#agent_name) -> &CommandLane<#command_type> + Send + Sync + 'static,
            S: Stream<Item = Action<#command_type, ()>> + Send + Sync + 'static
        {
            fn name(&self) -> &str {
                &self.name
            }
        }

        #[automatically_derived]
        impl<Context, T, S> LaneTasks<#agent_name, Context> for #task_name<T, S>
        where
            Context: AgentContext<#agent_name> + AgentExecutionContext + Send + Sync + 'static,
            T: Fn(&#agent_name) -> &CommandLane<#command_type> + Send + Sync + 'static,
            S: Stream<Item = Action<#command_type, ()>> + Send + Sync + 'static
        {
            fn start<'a>(&'a self, _context: &'a Context) -> BoxFuture<'a, ()> {
                ready(()).boxed()
            }

            fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
                async move {
                    let #task_name {
                        lifecycle,
                        event_stream,
                        projection,
                        ..
                    } = *self;

                    let model = projection(context.agent()).clone();
                    let mut events = event_stream.take_until(context.agent_stop_event());
                    let mut events = unsafe { Pin::new_unchecked(&mut events) };

                    while let Some(action) = events.next().await {
                        let (command, responder) = action.destruct();

                        tracing::event!(tracing::Level::TRACE, commanded = swim_server::agent::COMMANDED, ?command);

                        tracing_futures::Instrument::instrument(
                            lifecycle.#on_command_func(command, &model, &context),
                            tracing::span!(tracing::Level::TRACE, swim_server::agent::ON_COMMAND)
                        ).await;

                        if let Some(tx) = responder {
                            if tx.send(()).is_err() {
                                tracing::event!(tracing::Level::WARN, response_ingored = swim_server::agent::RESPONSE_IGNORED);
                            }
                        }
                    }
                }
                .boxed()
            }
        }
    };

    let wrapped = as_const("CommandLifecycle", lifecycle_name, private_derived);

    let derived = quote! {
        #public_derived

        #wrapped
    };

    derived.into()
}
