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

use crate::utils::{get_task_struct_name, validate_input_ast, Callback, InputAstType};
use crate::utils::{parse_callback, CallbackKind};
use darling::{ast, FromDeriveInput, FromField, FromMeta};
use macro_utilities::{as_const, string_to_ident, ungroup};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use proc_macro2::{Delimiter, Group, Ident, Literal, Span};
use quote::{quote, ToTokens};
use std::ops::Deref;
use syn::{AttributeArgs, DeriveInput, Path, PathSegment, Type, TypePath, Visibility};

type AgentName = Ident;

const COMMAND_LANE: &str = "CommandLane";
const ACTION_LANE: &str = "ActionLane";
const VALUE_LANE: &str = "ValueLane";
const MAP_LANE: &str = "MapLane";
const DEMAND_LANE: &str = "DemandLane";
const DEMAND_MAP_LANE: &str = "DemandMapLane";

#[derive(Debug, FromMeta)]
pub struct AgentAttrs {
    #[darling(map = "string_to_ident")]
    pub agent: Ident,
    #[darling(default)]
    pub on_start: Option<darling::Result<String>>,
}

#[derive(Debug, FromField)]
#[darling(attributes(lifecycle))]
pub struct LifecycleAttrs {
    pub ident: Option<syn::Ident>,
    pub ty: syn::Type,
    pub vis: syn::Visibility,
    pub name: Option<String>,
}

impl LifecycleAttrs {
    pub fn get_lane_type(&self) -> Option<LaneType> {
        match &self.ty {
            Type::Path(TypePath {
                path: Path { segments, .. },
                ..
            }) => Self::map_segment(segments.last()?),
            Type::Group(group) => {
                let last = ungroup(group.elem.deref());
                match last.deref() {
                    Type::Path(TypePath {
                        path: Path { segments, .. },
                        ..
                    }) => Self::map_segment(segments.last()?),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn map_segment(segment: &PathSegment) -> Option<LaneType> {
        match segment.ident.to_string().as_str() {
            COMMAND_LANE => Some(LaneType::Command),
            ACTION_LANE => Some(LaneType::Action),
            VALUE_LANE => Some(LaneType::Value),
            MAP_LANE => Some(LaneType::Map),
            DEMAND_LANE => Some(LaneType::Demand),
            DEMAND_MAP_LANE => Some(LaneType::DemandMap),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum LaneType {
    Command,
    Action,
    Value,
    Map,
    Demand,
    DemandMap,
}

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(agent))]
pub struct SwimAgentAttrs {
    pub ident: syn::Ident,
    #[darling(default = "default_config", map = "parse_config")]
    pub config: ConfigType,
    pub data: ast::Data<(), LifecycleAttrs>,
    pub generics: syn::Generics,
}

pub fn derive_swim_agent(input: DeriveInput) -> Result<TokenStream, TokenStream> {
    if let Err(error) = validate_input_ast(&input, InputAstType::Agent) {
        return Err(TokenStream::from(quote! {#error}));
    }

    let args = match SwimAgentAttrs::from_derive_input(&input) {
        Ok(args) => args,
        Err(e) => {
            return Err(TokenStream::from(e.write_errors()));
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

    let derived = quote! {
        use std::collections::HashMap;
        use std::boxed::Box;

        use swim_server::agent::{LaneTasks, SwimAgent, AgentContext, LaneIo};
        use swim_server::agent::lane::channels::AgentExecutionConfig;
        use swim_server::agent::context::AgentExecutionContext;
        use swim_server::agent::lane::lifecycle::LaneLifecycle;

        #[automatically_derived]
        impl SwimAgent<#config_type> for #agent_name {
            fn instantiate<Context: AgentContext<Self> + AgentExecutionContext>(
                configuration: &#config_type,
                exec_conf: &AgentExecutionConfig,
            ) -> (
                Self,
                Vec<Box<dyn LaneTasks<Self, Context>>>,
                HashMap<String, Box<dyn LaneIo<Context>>>,
            )
                where
                    Context: AgentContext<Self> + AgentExecutionContext + Send + Sync + 'static,
            {
                let mut io_map: HashMap<String, Box<dyn LaneIo<Context>>> = HashMap::new();

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

    Ok(derived.into())
}

pub fn derive_agent_lifecycle(args: AttributeArgs, input: DeriveInput) -> TokenStream {
    if let Err(error) = validate_input_ast(&input, InputAstType::Lifecycle) {
        return TokenStream::from(quote! {#error});
    }

    let args = match AgentAttrs::from_list(&args) {
        Ok(args) => args,
        Err(e) => {
            return TokenStream::from(e.write_errors());
        }
    };

    let lifecycle_name = input.ident.clone();
    let agent_name = &args.agent;
    let on_start_callback =
        parse_callback(&args.on_start, lifecycle_name.clone(), CallbackKind::Start);

    let start_body = match on_start_callback {
        Callback::Default { .. } => {
            quote! {ready(()).boxed()}
        }
        Callback::Custom { func_name, .. } => {
            quote! {self.#func_name(context).boxed()}
        }
    };

    let pub_derived = quote! {
        #[derive(Clone, Debug)]
        #input
    };

    let private_derived = quote! {
        use futures::FutureExt;
        use futures::future::{BoxFuture, ready};

        use swim_server::agent::lifecycle::AgentLifecycle;
        use swim_server::agent::AgentContext;

        #[automatically_derived]
        impl AgentLifecycle<#agent_name> for #lifecycle_name {
            fn starting<'a, C>(&'a self, context: &'a C) -> BoxFuture<'a, ()>
            where
                C: AgentContext<#agent_name> + Send + Sync + 'a,
            {
                #start_body
            }
        }

    };

    let wrapped = as_const("AgentLifecycle", lifecycle_name, private_derived);
    let derived = quote! {
        #pub_derived

        #wrapped
    };

    derived.into()
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

    let ts = match lane_type {
        LaneType::Command => {
            let model = quote!(let (#lane_name, event_stream) = swim_server::agent::lane::model::command::make_lane_model(exec_conf.action_buffer.clone()););

            build_lane_io(
                lane_data,
                quote! {
                    #model

                    io_map.insert (
                        #lane_name_lit.to_string(),
                        Box::new(swim_server::agent::CommandLaneIo::new(#lane_name.clone()))
                    );
                },
                model,
            )
        }
        LaneType::Action => {
            let model = quote!(let (#lane_name, event_stream) = swim_server::agent::lane::model::action::make_lane_model(exec_conf.action_buffer.clone()););

            build_lane_io(
                lane_data,
                quote! {
                    #model

                    io_map.insert (
                        #lane_name_lit.to_string(),
                        Box::new(swim_server::agent::ActionLaneIo::new(#lane_name.clone()))
                    );
                },
                model,
            )
        }
        LaneType::Value => {
            let model = quote! {
                let (#lane_name, observer) = swim_server::agent::lane::model::value::ValueLane::observable(Default::default(), exec_conf.observation_buffer);
                let subscriber = observer.subscriber();
                let event_stream = observer.into_stream();
            };

            build_lane_io(
                lane_data,
                quote! {
                    #model

                    io_map.insert (
                        #lane_name_lit.to_string(),
                        Box::new(swim_server::agent::ValueLaneIo::new(#lane_name.clone(), subscriber))
                    );
                },
                model,
            )
        }
        LaneType::Map => {
            let model = quote! {
                let (#lane_name, subscriber, event_stream) = swim_server::agent::lane::model::map::streamed_map_lane(exec_conf.observation_buffer);
            };

            build_lane_io(
                lane_data,
                quote! {
                    #model

                    io_map.insert (
                        #lane_name_lit.to_string(), Box::new(swim_server::agent::MapLaneIo::new(#lane_name.clone(), subscriber))
                    );
                },
                model,
            )
        }
        LaneType::Demand => build_demand_lane_io(lane_data),
        LaneType::DemandMap => build_demand_map_lane_io(lane_data),
    };

    (ts, task_variable)
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
            let is_public = matches!(field.vis, Visibility::Public(_));

            let (lifecycle_ast, task_name) = create_lane(
                &lane_type,
                is_public,
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

pub struct LaneData<'a> {
    agent_name: &'a Ident,
    is_public: bool,
    lifecycle: &'a Ident,
    lane_name: &'a Ident,
    task_variable: &'a Ident,
    task_structure: &'a Ident,
    lane_name_lit: &'a Literal,
}

fn build_lane_io(
    lane_data: LaneData,
    public_ts: proc_macro2::TokenStream,
    private_ts: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let LaneData {
        agent_name,
        is_public,
        lifecycle,
        lane_name,
        task_variable,
        task_structure,
        lane_name_lit,
    } = lane_data;

    let lane_creation = if is_public { public_ts } else { private_ts };

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

fn build_demand_lane_io(lane_data: LaneData) -> proc_macro2::TokenStream {
    let LaneData {
        agent_name,
        lifecycle,
        lane_name,
        task_variable,
        task_structure,
        lane_name_lit,
        ..
    } = lane_data;

    quote! {
        let buffer_size = exec_conf.action_buffer.clone();
        let (#lane_name, event_stream) = swim_server::agent::lane::model::demand::make_lane_model(buffer_size);
        let (response_tx, response_rx) = tokio::sync::mpsc::channel(buffer_size.get());
        let lifecycle = #lifecycle::create(configuration);

        let #task_variable = #task_structure {
            lifecycle,
            name: #lane_name_lit.into(),
            event_stream,
            projection: |agent: &#agent_name| &agent.#lane_name,
            response_tx,
        };

        io_map.insert (
            #lane_name_lit.to_string(), Box::new(swim_server::agent::DemandLaneIo::new(response_rx))
        );
    }
}

fn build_demand_map_lane_io(lane_data: LaneData) -> proc_macro2::TokenStream {
    let LaneData {
        agent_name,
        is_public,
        lifecycle,
        lane_name,
        task_variable,
        task_structure,
        lane_name_lit,
    } = lane_data;

    quote! {
        let buffer_size = exec_conf.action_buffer.clone();
        let (lifecycle_tx, event_rx) = tokio::sync::mpsc::channel(buffer_size.get());
        let lifecycle = #lifecycle::create(configuration);
        let event_stream = tokio_stream::wrappers::ReceiverStream::new(event_rx);

        let #task_variable = #task_structure {
            lifecycle,
            name: #lane_name_lit.into(),
            event_stream,
            projection: |agent: &#agent_name| &agent.#lane_name,
        };

        let (#lane_name, topic) = swim_server::agent::model::demand_map::make_lane_model(buffer_size, lifecycle_tx);

        if #is_public {
            io_map.insert (
                #lane_name_lit.to_string(), Box::new(swim_server::agent::DemandMapLaneIo::new(#lane_name.clone(), topic))
            );
        }
    }
}

pub fn parse_config(value: String) -> ConfigType {
    ConfigType::Struct(string_to_ident(value))
}

pub fn default_config() -> ConfigType {
    ConfigType::Unit
}

#[derive(Debug)]
pub enum ConfigType {
    Struct(Ident),
    Unit,
}

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

fn get_task_var_name(name: &str) -> Ident {
    Ident::new(&format!("{}_task", name), Span::call_site())
}
