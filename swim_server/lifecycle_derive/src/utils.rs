use crate::args::{LaneType, SwimAgent};
use proc_macro2::{Ident, Literal, Span};
use quote::quote;

pub(crate) struct AgentField {
    pub(crate) lane_name: Ident,
    pub(crate) task_name: Ident,
    pub(crate) lifecycle_ast: proc_macro2::TokenStream,
}

type ConfigName = Ident;
type AgentName = Ident;

pub(crate) fn get_agent_data(args: SwimAgent) -> (AgentName, ConfigName, Vec<AgentField>) {
    let SwimAgent {
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

            let (lifecycle_ast, task_name) =
                create_lane(&lane_type, &agent_name, &lifecycle_name, &lane_name);

            agent_fields.push(AgentField {
                lane_name,
                task_name,
                lifecycle_ast,
            });
        }
    });

    (agent_name, config_name, agent_fields)
}

pub(crate) fn get_task_struct_name(name: &str) -> Ident {
    Ident::new(&format!("{}Task", name), Span::call_site())
}

fn get_task_var_name(name: &str) -> Ident {
    Ident::new(&format!("{}_task", name), Span::call_site())
}

fn create_lane(
    lane_type: &LaneType,
    agent_name: &Ident,
    lifecycle: &Ident,
    lane_name: &Ident,
) -> (proc_macro2::TokenStream, Ident) {
    match lane_type {
        LaneType::Command => create_command_lane(&agent_name, &lifecycle, &lane_name),
        LaneType::Value => create_value_lane(&agent_name, &lifecycle, &lane_name),
        LaneType::Map => create_map_lane(&agent_name, &lifecycle, &lane_name),
    }
}

fn create_command_lane(
    agent_name: &Ident,
    lifecycle: &Ident,
    lane_name: &Ident,
) -> (proc_macro2::TokenStream, Ident) {
    let lane_name_str = lane_name.to_string();
    let task_var_ident = get_task_var_name(&lane_name_str);
    let task_struct_ident = get_task_struct_name(&lifecycle.to_string());
    let lane_name_lit = Literal::string(&lane_name_str);

    (
        quote! {
            let lifecycle = #lifecycle::create(configuration);
            let (#lane_name, event_stream) = model::action::make_lane_model(configuration.command_buffer_size.clone());
            let #task_var_ident = #task_struct_ident {
                lifecycle,
                name: #lane_name_lit.into(),
                event_stream,
                projection: |agent: &#agent_name| &agent.#lane_name,
            };
        },
        task_var_ident,
    )
}

fn create_value_lane(
    agent_name: &Ident,
    lifecycle: &Ident,
    lane_name: &Ident,
) -> (proc_macro2::TokenStream, Ident) {
    //Todo extract this
    let lane_name_str = lane_name.to_string();
    let task_var_ident = get_task_var_name(&lane_name_str);
    let task_struct_ident = get_task_struct_name(&lifecycle.to_string());
    let lane_name_lit = Literal::string(&lane_name_str);

    (
        quote! {
            let lifecycle = #lifecycle::create(configuration);
            let (#lane_name, event_stream) =
                model::value::make_lane_model(Default::default(), lifecycle.create_strategy());

            let #task_var_ident = #task_struct_ident {
                lifecycle,
                name: #lane_name_lit.into(),
                event_stream,
                projection: |agent: &#agent_name| &agent.#lane_name,
            };
        },
        task_var_ident,
    )
}

fn create_map_lane(
    agent_name: &Ident,
    lifecycle: &Ident,
    lane_name: &Ident,
) -> (proc_macro2::TokenStream, Ident) {
    let lane_name_str = lane_name.to_string();
    let task_var_ident = get_task_var_name(&lane_name_str);
    let task_struct_ident = get_task_struct_name(&lifecycle.to_string());
    let lane_name_lit = Literal::string(&lane_name_str);

    (
        quote! {
            let lifecycle = #lifecycle::create(configuration);
            let (#lane_name, event_stream) = model::map::make_lane_model(lifecycle.create_strategy());

            let #task_var_ident = #task_struct_ident {
                lifecycle,
                name: #lane_name_lit.into(),
                event_stream,
                projection: |agent: &#agent_name| &agent.#lane_name,
            };
        },
        task_var_ident,
    )
}
