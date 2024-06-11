use std::collections::HashMap;
use swimos::agent::agent_lifecycle::utility::HandlerContext;
use swimos::agent::event_handler::{EventHandler, HandlerActionExt};
use swimos::agent::lanes::{CommandLane, JoinValueLane};
use swimos::agent::projections;
use swimos::agent::{lifecycle, AgentLaneModel};

#[derive(AgentLaneModel)]
#[projections]
pub struct BuildingAgent {
    lights: JoinValueLane<u64, bool>,
    register_room: CommandLane<u64>,
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
        room_id: &u64,
    ) -> impl EventHandler<BuildingAgent> {
        let room_id = *room_id;
        context
            .get_parameter("name")
            .and_then(move |building_name: Option<String>| {
                let building_name = building_name.expect("Missing building name URI parameter");
                context.add_downlink(
                    BuildingAgent::LIGHTS,
                    room_id,
                    None,
                    format!("/rooms/{building_name}/{room_id}").as_str(),
                    "lights",
                )
            })
            .followed_by(context.effect(move || println!("Registered room: {room_id}")))
    }

    #[on_update(lights)]
    fn lights(
        &self,
        context: HandlerContext<BuildingAgent>,
        _map: &HashMap<u64, bool>,
        key: u64,
        prev: Option<bool>,
        new_value: &bool,
    ) -> impl EventHandler<BuildingAgent> {
        let new_value = *new_value;
        context.effect(move || println!("Light {key} changed from {prev:?} to {new_value}"))
    }
}
