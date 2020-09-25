use crate::args::{LaneType, SwimAgentAttrs};
use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::{quote, ToTokens};

#[derive(Debug)]
pub struct AgentField {
    pub lane_name: Ident,
    pub task_name: Ident,
    pub io_name: Ident,
    pub lifecycle_ast: proc_macro2::TokenStream,
}

#[derive(Debug)]
pub struct IOField<'a> {
    pub lane_name: Literal,
    pub io_task: &'a Ident,
}

impl ToTokens for IOField<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let lane_name = &self.lane_name;
        let io_task = self.io_task;
        // Todo check for None
        *tokens = quote! { #lane_name.to_string(), std::boxed::Box::new(#io_task.unwrap()) as std::boxed::Box<dyn swim_server::agent::LaneIo<Context>>};
    }
}

type ConfigName = Ident;
type AgentName = Ident;

pub fn get_agent_data(args: SwimAgentAttrs) -> (AgentName, ConfigName, Vec<AgentField>) {
    let SwimAgentAttrs {
        ident: agent_name,
        data: fields,
        config: config_name,
    } = args;

    let mut agent_fields = Vec::new();

    fields.map_struct_fields(|field| {
        if let (Some(lane_type), Some(lane_name), Some(lifecycle_name)) =
            (field.get_lane_type(), field.ident, field.name)
        {
            let lifecycle_name = Ident::new(&lifecycle_name, Span::call_site());

            let (lifecycle_ast, task_name, io_name) = create_lane(
                &lane_type,
                field.public,
                &agent_name,
                &lifecycle_name,
                &lane_name,
            );

            agent_fields.push(AgentField {
                lane_name,
                task_name,
                io_name,
                lifecycle_ast,
            });
        }
    });

    (agent_name, config_name, agent_fields)
}

pub fn get_task_struct_name(name: &str) -> Ident {
    Ident::new(&format!("{}Task", name), Span::call_site())
}

fn get_task_var_name(name: &str) -> Ident {
    Ident::new(&format!("{}_task", name), Span::call_site())
}

fn get_io_var_name(name: &str) -> Ident {
    Ident::new(&format!("{}_io", name), Span::call_site())
}

struct LaneData<'a> {
    agent_name: &'a Ident,
    is_public: bool,
    lifecycle: &'a Ident,
    lane_name: &'a Ident,
    task_variable: &'a Ident,
    task_structure: &'a Ident,
    io_variable: &'a Ident,
    lane_name_lit: &'a Literal,
}

fn create_lane(
    lane_type: &LaneType,
    is_public: bool,
    agent_name: &Ident,
    lifecycle: &Ident,
    lane_name: &Ident,
) -> (proc_macro2::TokenStream, Ident, Ident) {
    let lane_name_str = lane_name.to_string();
    let task_variable = get_task_var_name(&lane_name_str);
    let task_structure = get_task_struct_name(&lifecycle.to_string());
    let io_variable = get_io_var_name(&lane_name_str);
    let lane_name_lit = Literal::string(&lane_name_str);

    let lane_data = LaneData {
        agent_name,
        is_public,
        lifecycle,
        lane_name,
        task_variable: &task_variable,
        task_structure: &task_structure,
        io_variable: &io_variable,
        lane_name_lit: &lane_name_lit,
    };

    match lane_type {
        LaneType::Command => (create_action_lane(lane_data), task_variable, io_variable),
        LaneType::Action => (create_action_lane(lane_data), task_variable, io_variable),
        LaneType::Value => (create_value_lane(lane_data), task_variable, io_variable),
        LaneType::Map => (create_map_lane(lane_data), task_variable, io_variable),
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
        io_variable,
        lane_name_lit,
    } = lane_data;

    quote! {
        let lifecycle = #lifecycle::create(configuration);
        let (#lane_name, event_stream) = swim_server::agent::lane::model::action::make_lane_model(configuration.command_buffer_size.clone());
        let #task_variable = #task_structure {
            lifecycle,
            name: #lane_name_lit.into(),
            event_stream,
            projection: |agent: &#agent_name| &agent.#lane_name,
        };

        let #io_variable = if #is_public {
            std::option::Option::Some(swim_server::agent::ActionLaneIo::new_command(#lane_name.clone()))
        } else {
            std::option::Option::None
        };
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
        io_variable,
        lane_name_lit,
    } = lane_data;

    quote! {
        let lifecycle = #lifecycle::create(configuration);

        let (#lane_name, event_stream, deferred) = if #is_public {
            let (lane, event_stream, deferred) =
                swim_server::agent::lane::model::value::make_lane_model_deferred(std::default::Default::default(), lifecycle.create_strategy(), exec_conf);

            (lane, event_stream, std::option::Option::Some(deferred))
        } else {
            let (lane, event_stream) =
                swim_server::agent::lane::model::value::make_lane_model(std::default::Default::default(), lifecycle.create_strategy());

            (lane, event_stream, std::option::Option::None)
        };


        let #task_variable = #task_structure {
            lifecycle,
            name: #lane_name_lit.into(),
            event_stream,
            projection: |agent: &#agent_name| &agent.#lane_name,
        };
        let #io_variable = deferred.map(|d| swim_server::agent::ValueLaneIo::new(#lane_name.clone(), d));
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
        io_variable,
        lane_name_lit,
    } = lane_data;

    quote! {
        let lifecycle = #lifecycle::create(configuration);

        let (#lane_name, event_stream, deferred) = if #is_public {
            let (lane, event_stream, deferred) =
                swim_server::agent::lane::model::map::make_lane_model_deferred(lifecycle.create_strategy(), exec_conf);

            (lane, event_stream, std::option::Option::Some(deferred))
        } else {
            let (lane, event_stream) =
                swim_server::agent::lane::model::map::make_lane_model(lifecycle.create_strategy());

            (lane, event_stream, std::option::Option::None)
        };


        let #task_variable = #task_structure {
            lifecycle,
            name: #lane_name_lit.into(),
            event_stream,
            projection: |agent: &#agent_name| &agent.#lane_name,
        };
        let #io_variable = deferred.map(|d| swim_server::agent::MapLaneIo::new(#lane_name.clone(), d));
    }
}
