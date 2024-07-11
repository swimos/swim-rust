use std::collections::HashMap;
use swimos::agent::agent_lifecycle::HandlerContext;
use swimos::agent::event_handler::{EventHandler, HandlerActionExt};
use swimos::agent::lanes::{CommandLane, JoinValueLane};
use swimos::agent::projections;
use swimos::agent::{lifecycle, AgentLaneModel};

#[derive(AgentLaneModel)]
#[projections]
pub struct BuildingAgent {
    lights: JoinValueLane<String, bool>,
    register_room: CommandLane<String>,
}

#[derive(Clone)]
pub struct BuildingLifecycle;

#[lifecycle(BuildingAgent)]
impl BuildingLifecycle {
    #[on_start]
    pub fn on_start(
        &self,
        context: HandlerContext<BuildingAgent>,
    ) -> impl EventHandler<BuildingAgent> {
        context
            .get_agent_uri()
            .and_then(move |uri| context.effect(move || println!("Starting agent at: {}", uri)))
    }

    #[on_stop]
    pub fn on_stop(
        &self,
        context: HandlerContext<BuildingAgent>,
    ) -> impl EventHandler<BuildingAgent> {
        context
            .get_agent_uri()
            .and_then(move |uri| context.effect(move || println!("Stopping agent at: {}", uri)))
    }

    #[on_command(register_room)]
    pub fn register_room(
        &self,
        context: HandlerContext<BuildingAgent>,
        room_id: &str,
    ) -> impl EventHandler<BuildingAgent> {
        let downlink_room_id = room_id.to_string();
        let handler_room_id = room_id.to_string();
        context
            .get_parameter("name")
            .and_then(move |building_name: Option<String>| {
                let building_name = building_name.expect("Missing building name URI parameter");
                let node = format!("/rooms/{building_name}/{downlink_room_id}");
                context.add_downlink(
                    BuildingAgent::LIGHTS,
                    downlink_room_id,
                    None,
                    node.as_str(),
                    "lights",
                )
            })
            .followed_by(context.effect(move || println!("Registered room: {handler_room_id}")))
    }

    #[on_update(lights)]
    fn lights(
        &self,
        context: HandlerContext<BuildingAgent>,
        _map: &HashMap<String, bool>,
        key: String,
        prev: Option<bool>,
        new_value: &bool,
    ) -> impl EventHandler<BuildingAgent> {
        let new_value = *new_value;
        context.effect(move || println!("Light {key} changed from {prev:?} to {new_value}"))
    }
}
