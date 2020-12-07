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

use crate::internals::{default_on_start, to_ident};
use crate::utils::{get_task_struct_name, validate_input_ast, InputAstType};
use darling::{ast, FromDeriveInput, FromField, FromMeta};
use proc_macro::TokenStream;
use proc_macro2::{Delimiter, Group, Ident, Literal, Span};
use quote::{quote, ToTokens};
use syn::{parse_macro_input, AttributeArgs, DeriveInput, Path, Type, TypePath};
type AgentName = Ident;

const COMMAND_LANE: &str = "CommandLane";
const ACTION_LANE: &str = "ActionLane";
const VALUE_LANE: &str = "ValueLane";
const MAP_LANE: &str = "MapLane";

#[derive(Debug, FromMeta)]
pub struct AgentAttrs {
    #[darling(map = "to_ident")]
    pub agent: Ident,
    #[darling(default = "default_on_start")]
    #[darling(map = "to_ident")]
    pub on_start: Ident,
}

#[derive(Debug, FromField)]
#[darling(attributes(lifecycle))]
pub struct LifecycleAttrs {
    pub ident: Option<syn::Ident>,
    pub ty: syn::Type,
    #[darling(default)]
    pub public: bool,
    pub name: Option<String>,
}

impl LifecycleAttrs {
    pub fn get_lane_type(&self) -> Option<LaneType> {
        if let Type::Path(TypePath {
            path: Path { segments, .. },
            ..
        }) = &self.ty
        {
            if let Some(path_segment) = segments.last() {
                return match path_segment.ident.to_string().as_str() {
                    COMMAND_LANE => Some(LaneType::Command),
                    ACTION_LANE => Some(LaneType::Action),
                    VALUE_LANE => Some(LaneType::Value),
                    MAP_LANE => Some(LaneType::Map),
                    _ => None,
                };
            }
        }

        None
    }
}

pub enum LaneType {
    Command,
    Action,
    Value,
    Map,
}

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(agent))]
pub struct SwimAgentAttrs {
    pub ident: syn::Ident,
    #[darling(default = "default_config")]
    #[darling(map = "parse_config")]
    pub config: ConfigType,
    pub data: ast::Data<(), LifecycleAttrs>,
    pub generics: syn::Generics,
}

pub fn derive_swim_agent(input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);

    if let Err(error) = validate_input_ast(&input_ast, InputAstType::Agent) {
        return TokenStream::from(quote! {#error});
    }

    let args = match SwimAgentAttrs::from_derive_input(&input_ast) {
        Ok(args) => args,
        Err(e) => {
            return TokenStream::from(e.write_errors());
        }
    };

    let (agent_name, config_type, agent_fields) = get_agent_data(args);

    let lanes = agent_fields
        .iter()
        .map(|agent_field| &agent_field.lane_name);
    let tasks = agent_fields
        .iter()
        .map(|agent_field| &agent_field.task_name);
    let lifecycles_ast = agent_fields
        .iter()
        .map(|agent_field| &agent_field.lifecycle_ast);

    let output_ast = quote! {
        use swim_server::agent::LaneTasks as _;

        #[automatically_derived]
        impl swim_server::agent::SwimAgent<#config_type> for #agent_name {
            fn instantiate<Context: swim_server::agent::AgentContext<Self> + swim_server::agent::context::AgentExecutionContext>(
                configuration: &#config_type,
                exec_conf: &swim_server::agent::lane::channels::AgentExecutionConfig,
            ) -> (
                Self,
                std::vec::Vec<std::boxed::Box<dyn swim_server::agent::LaneTasks<Self, Context>>>,
                std::collections::HashMap<std::string::String, std::boxed::Box<dyn swim_server::agent::LaneIo<Context>>>,
            )
                where
                    Context: swim_server::agent::AgentContext<Self> + swim_server::agent::context::AgentExecutionContext + core::marker::Send + core::marker::Sync + 'static,
            {
                let mut io_map: std::collections::HashMap<std::string::String, std::boxed::Box<dyn swim_server::agent::LaneIo<Context>>> = std::collections::HashMap::new();

                #(#lifecycles_ast)*

                let agent = #agent_name {
                    #(#lanes),*
                };

                let tasks = std::vec![
                    #(#tasks.boxed()),*
                ];

                (agent, tasks, io_map)
            }
        }

    };

    TokenStream::from(output_ast)
}

pub fn derive_agent_lifecycle(args: TokenStream, input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);
    let attr_args = parse_macro_input!(args as AttributeArgs);

    if let Err(error) = validate_input_ast(&input_ast, InputAstType::Lifecycle) {
        return TokenStream::from(quote! {#error});
    }

    let args = match AgentAttrs::from_list(&attr_args) {
        Ok(args) => args,
        Err(e) => {
            return TokenStream::from(e.write_errors());
        }
    };

    let lifecycle_name = &input_ast.ident;
    let agent_name = &args.agent;
    let on_start_func = &args.on_start;

    let output_ast = quote! {
        use futures::FutureExt as _;

        #[derive(core::clone::Clone, core::fmt::Debug)]
        #input_ast

        #[automatically_derived]
        impl swim_server::agent::lifecycle::AgentLifecycle<#agent_name> for #lifecycle_name {
            fn on_start<'a, C>(&'a self, context: &'a C) -> futures::future::BoxFuture<'a, ()>
            where
                C: swim_server::agent::AgentContext<#agent_name> + core::marker::Send + core::marker::Sync + 'a,
            {
                self.#on_start_func(context).boxed()
            }
        }

    };

    TokenStream::from(output_ast)
}

#[derive(Debug)]
pub struct AgentField {
    pub lane_name: Ident,
    pub task_name: Ident,
    pub lifecycle_ast: proc_macro2::TokenStream,
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

pub(crate) struct LaneData<'a> {
    agent_name: &'a Ident,
    is_public: bool,
    lifecycle: &'a Ident,
    lane_name: &'a Ident,
    task_variable: &'a Ident,
    task_structure: &'a Ident,
    lane_name_lit: &'a Literal,
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

pub fn parse_config(value: String) -> ConfigType {
    ConfigType::Struct(to_ident(value))
}
pub fn default_config() -> ConfigType {
    ConfigType::Unit
}

#[derive(Debug)]
pub enum ConfigType {
    Struct(Ident),
    Unit,
}
use syn::export::TokenStream2;

impl ToTokens for ConfigType {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        match self {
            ConfigType::Struct(ident) => ident.to_tokens(tokens),
            ConfigType::Unit => {
                Group::new(Delimiter::Parenthesis, TokenStream2::new()).to_tokens(tokens)
            }
        }
    }
}

pub(crate) fn get_task_var_name(name: &str) -> Ident {
    Ident::new(&format!("{}_task", name), Span::call_site())
}
