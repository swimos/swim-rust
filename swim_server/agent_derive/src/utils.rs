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

use crate::lanes::{action, command, demand, demand_map, map, value};
use core::fmt;
use macro_utilities::str_to_ident;
use proc_macro2::{Ident, Span, TokenStream};
use quote::ToTokens;
use quote::{quote, quote_spanned};
use std::fmt::{Display, Formatter};
use syn::{parse_macro_input, AttributeArgs, Data, DeriveInput};

const SWIM_AGENT: &str = "Swim agent";
const LIFECYCLE: &str = "Lifecycle";
const DEFAULT_ON_START: &str = "on_start";
const DEFAULT_ON_COMMAND: &str = "on_command";
const DEFAULT_ON_EVENT: &str = "on_event";
const DEFAULT_ON_CUE: &str = "on_cue";
const DEFAULT_ON_SYNC: &str = "on_sync";
const DEFAULT_ON_REMOVE: &str = "on_remove";

pub fn default_on_remove() -> Ident {
    str_to_ident(DEFAULT_ON_REMOVE)
}

pub fn default_on_command() -> Ident {
    str_to_ident(DEFAULT_ON_COMMAND)
}

pub fn default_on_cue() -> Ident {
    str_to_ident(DEFAULT_ON_CUE)
}

pub fn default_on_sync() -> Ident {
    str_to_ident(DEFAULT_ON_SYNC)
}

pub fn default_on_start() -> Ident {
    str_to_ident(DEFAULT_ON_START)
}

pub fn default_on_event() -> Ident {
    str_to_ident(DEFAULT_ON_EVENT)
}

pub fn get_task_struct_name(name: &str) -> Ident {
    Ident::new(&format!("{}Task", name), Span::call_site())
}

pub fn parse_callback(
    callback: &Option<darling::Result<String>>,
    task_name: Ident,
    kind: CallbackKind,
) -> Callback {
    if let Some(name) = callback {
        if let Ok(name) = name {
            Callback::Custom {
                task_name,
                func_name: str_to_ident(name),
            }
        } else {
            match kind {
                CallbackKind::Start => Callback::Custom {
                    task_name,
                    func_name: default_on_start(),
                },
                CallbackKind::Command => Callback::Custom {
                    task_name,
                    func_name: default_on_command(),
                },
                CallbackKind::Event => Callback::Custom {
                    task_name,
                    func_name: default_on_event(),
                },
                CallbackKind::Cue => Callback::Custom {
                    task_name,
                    func_name: default_on_cue(),
                },
                CallbackKind::Sync => Callback::Custom {
                    task_name,
                    func_name: default_on_sync(),
                },
                CallbackKind::Remove => Callback::Custom {
                    task_name,
                    func_name: default_on_remove(),
                },
            }
        }
    } else {
        Callback::Default { task_name }
    }
}

#[derive(Debug)]
pub enum Callback {
    Default { task_name: Ident },
    Custom { task_name: Ident, func_name: Ident },
}

#[derive(Debug)]
pub enum CallbackKind {
    Start,
    Command,
    Event,
    Cue,
    Sync,
    Remove,
}

#[derive(Debug)]
pub enum LaneTasksImpl {
    Action {
        on_command: Callback,
    },
    Command {
        on_command: Callback,
    },
    Value {
        on_start: Callback,
        on_event: Callback,
    },
    Map {
        on_start: Callback,
        on_event: Callback,
    },
    Demand {
        on_cue: Callback,
    },
    DemandMap {
        on_sync: Callback,
        on_cue: Callback,
        on_remove: Callback,
    },
}

pub fn derive_start_callback(body: Option<TokenStream>) -> TokenStream {
    let body = body.unwrap_or(quote! {
        ready(()).boxed()
    });

    quote!(
        fn start<'a>(&'a self, context: &'a Context) -> BoxFuture<'a, ()> {
            #body
        }
    )
}

pub fn derive_events_callback(body: Option<TokenStream>) -> TokenStream {
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
            LaneTasksImpl::Action { on_command } => {
                let start_callback = derive_start_callback(None);

                let events_body = match on_command {
                    Callback::Default { .. } => None,
                    Callback::Custom {
                        task_name,
                        func_name,
                    } => Some(action::derive_events_body(task_name, func_name)),
                };
                let events_callback = derive_events_callback(events_body);

                (start_callback, events_callback)
            }
            LaneTasksImpl::Command { on_command } => {
                let start_callback = derive_start_callback(None);

                let events_body = match on_command {
                    Callback::Default { task_name } => {
                        Some(command::default_events_body(task_name))
                    }
                    Callback::Custom {
                        task_name,
                        func_name,
                    } => Some(command::derive_events_body(task_name, func_name)),
                };
                let events_callback = derive_events_callback(events_body);

                (start_callback, events_callback)
            }
            LaneTasksImpl::Value { on_start, on_event } => {
                let start_body = match on_start {
                    Callback::Default { .. } => None,
                    Callback::Custom {
                        task_name,
                        func_name,
                    } => Some(value::derive_start_body(task_name, func_name)),
                };
                let start_callback = derive_start_callback(start_body);

                let events_body = match on_event {
                    Callback::Default { .. } => None,
                    Callback::Custom {
                        task_name,
                        func_name,
                    } => Some(value::derive_events_body(task_name, func_name)),
                };
                let events_callback = derive_events_callback(events_body);

                (start_callback, events_callback)
            }
            LaneTasksImpl::Map { on_start, on_event } => {
                let start_body = match on_start {
                    Callback::Default { .. } => None,
                    Callback::Custom {
                        task_name,
                        func_name,
                    } => Some(map::derive_start_body(task_name, func_name)),
                };
                let start_callback = derive_start_callback(start_body);

                let events_body = match on_event {
                    Callback::Default { .. } => None,
                    Callback::Custom {
                        task_name,
                        func_name,
                    } => Some(map::derive_events_body(task_name, func_name)),
                };
                let events_callback = derive_events_callback(events_body);

                (start_callback, events_callback)
            }
            LaneTasksImpl::Demand { on_cue } => {
                let start_callback = derive_start_callback(None);

                let events_body = match on_cue {
                    Callback::Default { .. } => None,
                    Callback::Custom {
                        task_name,
                        func_name,
                    } => Some(demand::derive_events_body(task_name, func_name)),
                };
                let events_callback = derive_events_callback(events_body);

                (start_callback, events_callback)
            }
            LaneTasksImpl::DemandMap {
                on_sync,
                on_cue,
                on_remove,
            } => {
                let start_callback = derive_start_callback(None);

                let events_body = demand_map::derive_events_body(on_sync, on_cue, on_remove);
                let events_callback = derive_events_callback(events_body);

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
    Union(InputAstType, Span),
    Enum(InputAstType, Span),
    Generic(InputAstType, Span),
}

impl ToTokens for InputAstError {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        *tokens = match self {
            InputAstError::Enum(ty, span) => {
                let message = format! {"{} cannot be created from Enum.", ty};
                quote_spanned! { *span => compile_error!(#message); }
            }
            InputAstError::Union(ty, span) => {
                let message = format! {"{} cannot be created from Union.", ty};
                quote_spanned! { *span => compile_error!(#message); }
            }
            InputAstError::Generic(ty, span) => {
                let message = format! {"{} cannot have generic parameters.", ty};
                quote_spanned! { *span => compile_error!(#message); }
            }
        };
    }
}

pub fn validate_input_ast(input_ast: &DeriveInput, ty: InputAstType) -> Result<(), InputAstError> {
    match input_ast.data {
        Data::Enum(_) => Err(InputAstError::Enum(ty, input_ast.ident.span())),
        Data::Union(_) => Err(InputAstError::Union(ty, input_ast.ident.span())),
        _ => {
            if !input_ast.generics.params.is_empty() {
                Err(InputAstError::Generic(ty, input_ast.ident.span()))
            } else {
                Ok(())
            }
        }
    }
}

pub fn derive<F>(
    args: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
    f: F,
) -> proc_macro::TokenStream
where
    F: Fn(AttributeArgs, DeriveInput) -> proc_macro::TokenStream,
{
    let input = parse_macro_input!(input as DeriveInput);
    let args = parse_macro_input!(args as AttributeArgs);

    f(args, input)
}
