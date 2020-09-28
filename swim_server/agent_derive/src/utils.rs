use crate::args::{LaneType, SwimAgentAttrs};
use proc_macro2::{Ident, Literal, Span};
use quote::quote;

#[derive(Debug)]
pub struct AgentField {
    pub lane_name: Ident,
    pub task_name: Ident,
    pub lifecycle_ast: proc_macro2::TokenStream,
}

type ConfigName = Ident;
type AgentName = Ident;

pub fn get_agent_data(args: SwimAgentAttrs) -> (AgentName, ConfigName, Vec<AgentField>) {
    let SwimAgentAttrs {
        ident: agent_name,
        data: fields,
        config: config_name,
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

    (agent_name, config_name, agent_fields)
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
        LaneType::Command => (create_action_lane(lane_data), task_variable),
        LaneType::Action => (create_action_lane(lane_data), task_variable),
        LaneType::Value => (create_value_lane(lane_data), task_variable),
        LaneType::Map => (create_map_lane(lane_data), task_variable),
    }
}

//Todo new command should be new action
//Todo Decide for is_public at compile time
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

    quote! {
        let lifecycle = #lifecycle::create(configuration);
        let (#lane_name, event_stream) = swim_server::agent::lane::model::action::make_lane_model(configuration.get_buffer_size());
        let #task_variable = #task_structure {
            lifecycle,
            name: #lane_name_lit.into(),
            event_stream,
            projection: |agent: &#agent_name| &agent.#lane_name,
        };

        if #is_public {
            io_map.insert (
                #lane_name_lit.to_string(),
                std::boxed::Box::new(swim_server::agent::ActionLaneIo::new_command(#lane_name.clone()))
            );
        }
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

    quote! {
        let lifecycle = #lifecycle::create(configuration);

        let (#lane_name, event_stream) = if #is_public {
            let (lane, event_stream, deferred) =
                swim_server::agent::lane::model::value::make_lane_model_deferred(std::default::Default::default(), lifecycle.create_strategy(), exec_conf);
            io_map.insert (
                #lane_name_lit.to_string(),
                std::boxed::Box::new(swim_server::agent::ValueLaneIo::new(lane.clone(), deferred))
            );

            (lane, event_stream)
        } else {
            let (lane, event_stream) =
                swim_server::agent::lane::model::value::make_lane_model(std::default::Default::default(), lifecycle.create_strategy());

            (lane, event_stream)
        };


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

    quote! {
        let lifecycle = #lifecycle::create(configuration);

        let (#lane_name, event_stream) = if #is_public {
            let (lane, event_stream, deferred) =
                swim_server::agent::lane::model::map::make_lane_model_deferred(lifecycle.create_strategy(), exec_conf);
            io_map.insert (
                #lane_name_lit.to_string(), std::boxed::Box::new(swim_server::agent::MapLaneIo::new(lane.clone(), deferred))
            );

            (lane, event_stream)
        } else {
            let (lane, event_stream) =
                swim_server::agent::lane::model::map::make_lane_model(lifecycle.create_strategy());

            (lane, event_stream)
        };


        let #task_variable = #task_structure {
            lifecycle,
            name: #lane_name_lit.into(),
            event_stream,
            projection: |agent: &#agent_name| &agent.#lane_name,
        };
    }
}
