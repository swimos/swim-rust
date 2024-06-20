use std::str::FromStr;
use swimos::agent::agent_lifecycle::HandlerContext;
use swimos::agent::event_handler::{EventHandler, HandlerActionExt};
use swimos::agent::lanes::ValueLane;
use swimos::agent::projections;
use swimos::agent::{lifecycle, AgentLaneModel};

#[derive(AgentLaneModel)]
#[projections]
pub struct RoomAgent {
    lights: ValueLane<bool>,
}

#[derive(Clone)]
pub struct RoomLifecycle;

#[lifecycle(RoomAgent)]
impl RoomLifecycle {
    #[on_start]
    pub fn on_start(&self, context: HandlerContext<RoomAgent>) -> impl EventHandler<RoomAgent> {
        context
            .get_agent_uri()
            .and_then(move |uri| context.effect(move || println!("Starting agent at: {}", uri)))
            .followed_by(context.get_parameter("building").and_then(
                move |building_name: Option<String>| {
                    context
                        .get_parameter("room")
                        .and_then(move |room_id: Option<String>| {
                            let building_name =
                                building_name.expect("Missing building URI parameter");
                            let room_id_str = room_id.expect("Missing room ID URI parameter");
                            let room_id = u64::from_str(room_id_str.as_str())
                                .expect("Expected a u64 room ID");

                            context.send_command(
                                None,
                                format!("/buildings/{building_name}"),
                                "register_room".to_string(),
                                room_id,
                            )
                        })
                },
            ))
    }

    #[on_stop]
    pub fn on_stop(&self, context: HandlerContext<RoomAgent>) -> impl EventHandler<RoomAgent> {
        context
            .get_agent_uri()
            .and_then(move |uri| context.effect(move || println!("Starting agent at: {}", uri)))
    }
}
