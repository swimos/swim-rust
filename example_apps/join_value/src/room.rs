// Copyright 2015-2024 Swim Inc.
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

use swimos::agent::agent_lifecycle::HandlerContext;
use swimos::agent::event_handler::{EventHandler, HandlerActionExt};
use swimos::agent::lanes::ValueLane;
use swimos::agent::projections;
use swimos::agent::{lifecycle, AgentLaneModel};

#[projections]
#[derive(AgentLaneModel)]
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

                            context.send_command(
                                None,
                                format!("/buildings/{building_name}"),
                                "register_room".to_string(),
                                room_id_str,
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
