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

use crate::args::{ConfigType, LaneType, SwimAgentAttrs};
use core::fmt;
use proc_macro2::{Ident, Literal, Span};
use quote::quote_spanned;
use quote::{quote, ToTokens};
use std::fmt::{Display, Formatter};
use syn::{Data, DeriveInput};

const SWIM_AGENT: &str = "Swim agent";
const LIFECYCLE: &str = "Lifecycle";

#[derive(Debug)]
pub struct AgentField {
    pub lane_name: Ident,
    pub task_name: Ident,
    pub lifecycle_ast: proc_macro2::TokenStream,
}

type AgentName = Ident;

pub fn get_agent_data(args: SwimAgentAttrs) -> (AgentName, ConfigType, Vec<AgentField>) {
    let SwimAgentAttrs {
        ident: agent_name,
        data: fields,
        config: config_type,
        ..
    } = args;

    let mut agent_fields = Vec::new();

    fields.map_struct_fields(|field| {
        if let (Some(lane_type), Some(lane_name), Some(lifecycle_name)) =
            (field.get_lane_type(), field.ident, field.name)
        {
            let lifecycle_name = Ident::new(&lifecycle_name, Span::call_site());

            let (lifecycle_ast, task_name) = create_lane(
                &lane_type,
                field.public,
                &agent_name,
                &lifecycle_name,
                &lane_name,
            );

            agent_fields.push(AgentField {
                lane_name,
                task_name,
                lifecycle_ast,
            });
        }
    });

    (agent_name, config_type, agent_fields)
}

pub fn get_task_struct_name(name: &str) -> Ident {
    Ident::new(&format!("{}Task", name), Span::call_site())
}

fn get_task_var_name(name: &str) -> Ident {
    Ident::new(&format!("{}_task", name), Span::call_site())
}

struct LaneData<'a> {
    agent_name: &'a Ident,
    is_public: bool,
    lifecycle: &'a Ident,
    lane_name: &'a Ident,
    task_variable: &'a Ident,
    task_structure: &'a Ident,
    lane_name_lit: &'a Literal,
}

fn create_lane(
    lane_type: &LaneType,
    is_public: bool,
    agent_name: &Ident,
    lifecycle: &Ident,
    lane_name: &Ident,
) -> (proc_macro2::TokenStream, Ident) {
    let lane_name_str = lane_name.to_string();
    let task_variable = get_task_var_name(&lane_name_str);
    let task_structure = get_task_struct_name(&lifecycle.to_string());
    let lane_name_lit = Literal::string(&lane_name_str);

    let lane_data = LaneData {
        agent_name,
        is_public,
        lifecycle,
        lane_name,
        task_variable: &task_variable,
        task_structure: &task_structure,
        lane_name_lit: &lane_name_lit,
    };

    match lane_type {
        LaneType::Command => (create_command_lane(lane_data), task_variable),
        LaneType::Action => (create_action_lane(lane_data), task_variable),
        LaneType::Value => (create_value_lane(lane_data), task_variable),
        LaneType::Map => (create_map_lane(lane_data), task_variable),
    }
}

fn create_command_lane(lane_data: LaneData) -> proc_macro2::TokenStream {
    let LaneData {
        agent_name,
        is_public,
        lifecycle,
        lane_name,
        task_variable,
        task_structure,
        lane_name_lit,
    } = lane_data;

    let io = if is_public {
        Some(quote! {
            io_map.insert (
                #lane_name_lit.to_string(),
                std::boxed::Box::new(swim_server::agent::ActionLaneIo::new_command(#lane_name.clone()))
            );
        })
    } else {
        None
    };

    quote! {
        let lifecycle = #lifecycle::create(configuration);
        let (#lane_name, event_stream) = swim_server::agent::lane::model::action::make_lane_model(exec_conf.action_buffer.clone());
        let #task_variable = #task_structure {
            lifecycle,
            name: #lane_name_lit.into(),
            event_stream,
            projection: |agent: &#agent_name| &agent.#lane_name,
        };

        #io
    }
}

fn create_action_lane(lane_data: LaneData) -> proc_macro2::TokenStream {
    let LaneData {
        agent_name,
        is_public,
        lifecycle,
        lane_name,
        task_variable,
        task_structure,
        lane_name_lit,
    } = lane_data;

    let io = if is_public {
        Some(quote! {
            io_map.insert (
                #lane_name_lit.to_string(),
                std::boxed::Box::new(swim_server::agent::ActionLaneIo::new_action(#lane_name.clone()))
            );
        })
    } else {
        None
    };

    quote! {
        let lifecycle = #lifecycle::create(configuration);
        let (#lane_name, event_stream) = swim_server::agent::lane::model::action::make_lane_model(exec_conf.action_buffer.clone());
        let #task_variable = #task_structure {
            lifecycle,
            name: #lane_name_lit.into(),
            event_stream,
            projection: |agent: &#agent_name| &agent.#lane_name,
        };

        #io
    }
}

fn create_value_lane(lane_data: LaneData) -> proc_macro2::TokenStream {
    let LaneData {
        agent_name,
        is_public,
        lifecycle,
        lane_name,
        task_variable,
        task_structure,
        lane_name_lit,
    } = lane_data;

    let lane_creation = if is_public {
        quote! {
            let (#lane_name, event_stream, deferred) =
                swim_server::agent::lane::model::value::make_lane_model_deferred(std::default::Default::default(), lifecycle.create_strategy(), exec_conf);

            io_map.insert (
                #lane_name_lit.to_string(),
                std::boxed::Box::new(swim_server::agent::ValueLaneIo::new(#lane_name.clone(), deferred))
            );
        }
    } else {
        quote! {
            let (#lane_name, event_stream) =
                swim_server::agent::lane::model::value::make_lane_model(std::default::Default::default(), lifecycle.create_strategy());
        }
    };

    quote! {
        let lifecycle = #lifecycle::create(configuration);

        #lane_creation

        let #task_variable = #task_structure {
            lifecycle,
            name: #lane_name_lit.into(),
            event_stream,
            projection: |agent: &#agent_name| &agent.#lane_name,
        };
    }
}

fn create_map_lane(lane_data: LaneData) -> proc_macro2::TokenStream {
    let LaneData {
        agent_name,
        is_public,
        lifecycle,
        lane_name,
        task_variable,
        task_structure,
        lane_name_lit,
    } = lane_data;

    let lane_creation = if is_public {
        quote! {
            let (#lane_name, event_stream, deferred) =
                swim_server::agent::lane::model::map::make_lane_model_deferred(lifecycle.create_strategy(), exec_conf);

            io_map.insert (
                #lane_name_lit.to_string(), std::boxed::Box::new(swim_server::agent::MapLaneIo::new(#lane_name.clone(), deferred))
            );
        }
    } else {
        quote! {
            let (#lane_name, event_stream) =
                swim_server::agent::lane::model::map::make_lane_model(lifecycle.create_strategy());
        }
    };

    quote! {
        let lifecycle = #lifecycle::create(configuration);

        #lane_creation

        let #task_variable = #task_structure {
            lifecycle,
            name: #lane_name_lit.into(),
            event_stream,
            projection: |agent: &#agent_name| &agent.#lane_name,
        };
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
