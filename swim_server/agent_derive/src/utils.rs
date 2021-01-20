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

use core::fmt;
use proc_macro2::{Ident, Span, TokenStream};
use quote::ToTokens;
use quote::{quote, quote_spanned};
use std::fmt::{Display, Formatter};
use syn::{Data, DeriveInput};

const SWIM_AGENT: &str = "Swim agent";
const LIFECYCLE: &str = "Lifecycle";

pub fn get_task_struct_name(name: &str) -> Ident {
    Ident::new(&format!("{}Task", name), Span::call_site())
}

#[derive(Debug)]
pub struct Callback {
    pub task_name: Ident,
    pub func_name: Ident,
    pub kind: CallbackKind,
}

#[derive(Debug)]
pub enum CallbackKind {
    Start,
    Command,
    Event,
    Cue,
    Sync,
}

#[derive(Debug)]
pub enum LaneTasksImpl {
    Action {
        on_command: Option<Callback>,
    },
    Command {
        on_command: Option<Callback>,
    },
    Value {
        on_start: Option<Callback>,
        on_event: Option<Callback>,
    },
    Map {
        on_start: Option<Callback>,
        on_event: Option<Callback>,
    },
    Demand {
        on_cue: Option<Callback>,
    },
    DemandMap {
        on_sync: Option<Callback>,
        on_cue: Option<Callback>,
    },
}

pub fn create_start_callback(body: Option<TokenStream>) -> TokenStream {
    let body = body.unwrap_or(quote! {
        ready(()).boxed()
    });

    quote!(
        fn start<'a>(&'a self, context: &'a Context) -> BoxFuture<'a, ()> {
            #body
        }
    )
}

pub fn create_events_callback(body: Option<TokenStream>) -> TokenStream {
    quote!(
         fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
            async move {
                #body
            }.boxed()
         }
    )
}

impl ToTokens for LaneTasksImpl {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let (start_callback, events_callback) = match self {
            LaneTasksImpl::Action {
                on_command: maybe_on_command,
            } => {
                let start_callback = create_start_callback(None);

                let events_callback = match maybe_on_command {
                    Some(on_command) => {
                        let task_name = &on_command.task_name;
                        let on_action_func = &on_command.func_name;

                        let events_body = quote!(
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
                        );

                        create_events_callback(Some(events_body))
                    }
                    None => create_events_callback(None),
                };

                (start_callback, events_callback)
            }
            LaneTasksImpl::Command {
                on_command: maybe_on_command,
            } => {
                let start_callback = create_start_callback(None);

                let events_callback = match maybe_on_command {
                    Some(on_command) => {
                        let task_name = &on_command.task_name;
                        let on_command_func = &on_command.func_name;

                        let events_body = quote!(
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
                                    lifecycle.#on_command_func(command, &model, &context),
                                    tracing::span!(tracing::Level::TRACE, swim_server::agent::ON_COMMAND)
                                ).await;

                                if let Some(tx) = responder {
                                    if tx.send(()).is_err() {
                                        tracing::event!(tracing::Level::WARN, response_ingored = swim_server::agent::RESPONSE_IGNORED);
                                    }
                                }
                            }
                        );

                        create_events_callback(Some(events_body))
                    }
                    None => create_events_callback(None),
                };

                (start_callback, events_callback)
            }
            LaneTasksImpl::Value {
                on_start: maybe_on_start,
                on_event: maybe_on_event,
            } => {
                let start_callback = match maybe_on_start {
                    Some(on_start) => {
                        let task_name = &on_start.task_name;
                        let on_start_func = &on_start.func_name;

                        let start_body = quote!(
                            let #task_name { lifecycle, projection, .. } = self;
                            let model = projection(context.agent());
                            lifecycle.#on_start_func(model, context).boxed()
                        );

                        create_start_callback(Some(start_body))
                    }
                    None => create_start_callback(None),
                };

                let events_callback = match maybe_on_event {
                    Some(on_event) => {
                        let task_name = &on_event.task_name;
                        let on_event_func_name = &on_event.func_name;

                        let events_body = quote!(
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
                                    lifecycle.#on_event_func_name(&event, &model, &context),
                                    tracing::span!(tracing::Level::TRACE, swim_server::agent::ON_EVENT, ?event)
                                ).await;
                            }
                        );

                        create_events_callback(Some(events_body))
                    }
                    None => create_events_callback(None),
                };

                (start_callback, events_callback)
            }
            LaneTasksImpl::Map {
                on_start: maybe_on_start,
                on_event: maybe_on_event,
            } => {
                let start_callback = match maybe_on_start {
                    Some(on_start) => {
                        let task_name = &on_start.task_name;
                        let on_start_func = &on_start.func_name;

                        let start_body = quote!(
                            let #task_name { lifecycle, projection, .. } = self;
                            let model = projection(context.agent());
                            lifecycle.#on_start_func(model, context).boxed()
                        );

                        create_start_callback(Some(start_body))
                    }
                    None => create_start_callback(None),
                };

                let events_callback = match maybe_on_event {
                    Some(on_event) => {
                        let task_name = &on_event.task_name;
                        let on_event_func_name = &on_event.func_name;

                        let events_body = quote!(
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
                                    lifecycle.#on_event_func_name(&event, &model, &context),
                                    tracing::span!(tracing::Level::TRACE, swim_server::agent::ON_EVENT, ?event)
                                ).await;
                            }
                        );

                        create_events_callback(Some(events_body))
                    }
                    None => create_events_callback(None),
                };

                (start_callback, events_callback)
            }
            LaneTasksImpl::Demand {
                on_cue: maybe_on_cue,
            } => {
                let start_callback = create_start_callback(None);

                let events_callback = match maybe_on_cue {
                    Some(on_cue) => {
                        let task_name = &on_cue.task_name;
                        let on_cue_func_name = &on_cue.func_name;

                        let events_body = quote!(
                            let #task_name {
                                lifecycle,
                                event_stream,
                                projection,
                                response_tx,
                                ..
                            } = *self;

                            let model = projection(context.agent()).clone();
                            let mut events = event_stream.take_until(context.agent_stop_event());
                            let mut events = unsafe { Pin::new_unchecked(&mut events) };

                            while let Some(event) = events.next().await {
                                if let Some(value) = lifecycle.#on_cue_func_name(&model, &context).await {
                                    let _ = response_tx.send(value).await;
                                }
                            }
                        );
                        create_events_callback(Some(events_body))
                    }
                    None => create_events_callback(None),
                };

                (start_callback, events_callback)
            }
            LaneTasksImpl::DemandMap {
                on_sync: maybe_on_sync,
                on_cue: maybe_on_cue,
            } => {
                let start_callback = create_start_callback(None);

                let events_callback = match (maybe_on_sync, maybe_on_cue) {
                    (Some(on_sync), Some(on_cue)) => {
                        let task_name = &on_sync.task_name;
                        let on_sync_func_name = &on_sync.func_name;
                        let on_cue_func_name = &on_cue.func_name;

                        let events_body = quote!(
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
                                match event {
                                    DemandMapLaneEvent::Sync(sender) => {
                                        let keys: Vec<_> = lifecycle.#on_sync_func_name(&model, &context).await;
                                        let keys_len = keys.len();

                                        let mut values = iter(keys)
                                            .fold(Vec::with_capacity(keys_len), |mut results, key| async {
                                                if let Some(value) =
                                                    lifecycle.#on_cue_func_name(&model, &context, key.clone()).await
                                                {
                                                    results.push(DemandMapLaneUpdate::make(key, value));
                                                }

                                                results
                                            })
                                            .await;

                                        values.shrink_to_fit();

                                        let _ = sender.send(values);
                                    }
                                    DemandMapLaneEvent::Cue(sender, key) => {
                                        let value = lifecycle.#on_cue_func_name(&model, &context, key).await;
                                        let _ = sender.send(value);
                                    }
                                }
                            }
                        );
                        create_events_callback(Some(events_body))
                    }
                    (Some(on_sync), None) => {
                        let task_name = &on_sync.task_name;
                        let on_sync_func_name = &on_sync.func_name;

                        let events_body = quote!(
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
                                match event {
                                    DemandMapLaneEvent::Sync(sender) => {
                                        let keys: Vec<_> = lifecycle.#on_sync_func_name(&model, &context).await;
                                        let keys_len = keys.len();

                                        let mut values = iter(keys)
                                            .fold(Vec::with_capacity(keys_len), |mut results, key| async {
                                                results
                                            })
                                            .await;

                                        values.shrink_to_fit();

                                        let _ = sender.send(values);
                                    }
                                    _ => ()
                                }
                            }
                        );
                        create_events_callback(Some(events_body))
                    }
                    (None, Some(on_cue)) => {
                        let task_name = &on_cue.task_name;
                        let on_cue_func_name = &on_cue.func_name;

                        let events_body = quote!(
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
                                match event {
                                    DemandMapLaneEvent::Cue(sender, key) => {
                                        let value = lifecycle.#on_cue_func_name(&model, &context, key).await;
                                        let _ = sender.send(value);
                                    }
                                    _ => ()
                                }
                            }
                        );
                        create_events_callback(Some(events_body))
                    }
                    (None, None) => create_events_callback(None),
                };

                (start_callback, events_callback)
            }
        };
        quote!(
            #start_callback
            #events_callback
        )
        .to_tokens(tokens)
    }
}

#[derive(Debug)]
pub enum InputAstType {
    Agent,
    Lifecycle,
}

impl Display for InputAstType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            InputAstType::Agent => write!(f, "{}", SWIM_AGENT),
            InputAstType::Lifecycle => write!(f, "{}", LIFECYCLE),
        }
    }
}

#[derive(Debug)]
pub enum InputAstError {
    UnionError(InputAstType, Span),
    EnumError(InputAstType, Span),
    GenericError(InputAstType, Span),
}

impl ToTokens for InputAstError {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        *tokens = match self {
            InputAstError::EnumError(ty, span) => {
                let message = format! {"{} cannot be created from Enum.", ty};
                quote_spanned! { *span => compile_error!(#message); }
            }
            InputAstError::UnionError(ty, span) => {
                let message = format! {"{} cannot be created from Union.", ty};
                quote_spanned! { *span => compile_error!(#message); }
            }
            InputAstError::GenericError(ty, span) => {
                let message = format! {"{} cannot have generic parameters.", ty};
                quote_spanned! { *span => compile_error!(#message); }
            }
        };
    }
}

pub fn validate_input_ast(input_ast: &DeriveInput, ty: InputAstType) -> Result<(), InputAstError> {
    match input_ast.data {
        Data::Enum(_) => Err(InputAstError::EnumError(ty, input_ast.ident.span())),
        Data::Union(_) => Err(InputAstError::UnionError(ty, input_ast.ident.span())),
        _ => {
            if !input_ast.generics.params.is_empty() {
                Err(InputAstError::GenericError(ty, input_ast.ident.span()))
            } else {
                Ok(())
            }
        }
    }
}
