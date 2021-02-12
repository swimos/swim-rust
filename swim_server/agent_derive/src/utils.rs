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

use crate::lanes::{action, command, demand, demand_map, map, value};
use core::fmt;
use macro_helpers::str_to_ident;
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
) -> Option<Callback> {
    if let Some(name) = callback {
        if let Ok(name) = name {
            Some(Callback {
                task_name,
                func_name: str_to_ident(&name),
                kind,
            })
        } else {
            match kind {
                CallbackKind::Start => Some(Callback {
                    task_name,
                    func_name: default_on_start(),
                    kind,
                }),
                CallbackKind::Command => Some(Callback {
                    task_name,
                    func_name: default_on_command(),
                    kind,
                }),
                CallbackKind::Event => Some(Callback {
                    task_name,
                    func_name: default_on_event(),
                    kind,
                }),
                CallbackKind::Cue => Some(Callback {
                    task_name,
                    func_name: default_on_cue(),
                    kind,
                }),
                CallbackKind::Sync => Some(Callback {
                    task_name,
                    func_name: default_on_sync(),
                    kind,
                }),
                CallbackKind::Remove => Some(Callback {
                    task_name,
                    func_name: default_on_remove(),
                    kind,
                }),
            }
        }
    } else {
        None
    }
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
    Remove,
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
        on_remove: Option<Callback>,
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

                let events_body = on_command
                    .as_ref()
                    .map(|callback| action::derive_events_body(callback));
                let events_callback = derive_events_callback(events_body);

                (start_callback, events_callback)
            }
            LaneTasksImpl::Command { on_command } => {
                let start_callback = derive_start_callback(None);

                let events_body = on_command
                    .as_ref()
                    .map(|callback| command::derive_events_body(callback));
                let events_callback = derive_events_callback(events_body);

                (start_callback, events_callback)
            }
            LaneTasksImpl::Value { on_start, on_event } => {
                let start_body = on_start
                    .as_ref()
                    .map(|callback| value::derive_start_body(callback));
                let start_callback = derive_start_callback(start_body);

                let events_body = on_event
                    .as_ref()
                    .map(|callback| value::derive_events_body(callback));
                let events_callback = derive_events_callback(events_body);

                (start_callback, events_callback)
            }
            LaneTasksImpl::Map { on_start, on_event } => {
                let start_body = on_start
                    .as_ref()
                    .map(|callback| map::derive_start_body(callback));
                let start_callback = derive_start_callback(start_body);

                let events_body = on_event
                    .as_ref()
                    .map(|callback| map::derive_events_body(callback));
                let events_callback = derive_events_callback(events_body);

                (start_callback, events_callback)
            }
            LaneTasksImpl::Demand { on_cue } => {
                let start_callback = derive_start_callback(None);

                let events_body = on_cue
                    .as_ref()
                    .map(|callback| demand::derive_events_body(callback));
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
