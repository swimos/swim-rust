// Copyright 2015-2023 Swim Inc.
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

use std::str::FromStr;
use std::time::Duration;

use rand::Rng;

use swimos::agent::stores::ValueStore;
use swimos::{
    agent::agent_lifecycle::HandlerContext,
    agent::event_handler::{EventHandler, HandlerActionExt},
    agent::lanes::ValueLane,
    agent::{lifecycle, projections, AgentLaneModel},
};

use crate::area::{Action, Area};

#[derive(AgentLaneModel)]
#[projections]
pub struct CarAgent {
    speed: ValueLane<u64>,
    #[item(transient)]
    area: ValueStore<Area>,
}

#[derive(Debug, Clone)]
pub struct CarLifecycle;

#[lifecycle(CarAgent)]
impl CarLifecycle {
    #[on_start]
    pub fn on_start(&self, context: HandlerContext<CarAgent>) -> impl EventHandler<CarAgent> {
        let speed_handler = context.schedule_repeatedly(Duration::from_secs(5), move || {
            let mut rng = rand::rngs::OsRng;
            Some(context.set_value(CarAgent::SPEED, rng.gen_range(10..=70)))
        });
        let area_handler = move |car_id: u64| {
            context.schedule_repeatedly(Duration::from_secs(5), move || {
                Some(context.get_value(CarAgent::AREA).and_then(move |old_area| {
                    let new_area = Area::select_random();
                    let handler = if old_area != new_area {
                        // deregister this car with its current area
                        let deregister_handler = context.send_command(
                            None,
                            format!("/area/{old_area}"),
                            "registrations".to_string(),
                            Action::Deregister(car_id),
                        );
                        // register this car with its new assigned area
                        let register_handler = context.send_command(
                            None,
                            format!("/area/{new_area}"),
                            "registrations".to_string(),
                            Action::Register(car_id),
                        );

                        let handler = deregister_handler
                            .followed_by(register_handler)
                            .followed_by(context.set_value(CarAgent::AREA, new_area));
                        Some(handler)
                    } else {
                        // noop handler as the car didn't switch area
                        None
                    };

                    handler.discard()
                }))
            })
        };

        context
            .get_parameter("car_id")
            .map(|param: Option<String>| {
                let car_id = param.expect("Missing car_id URI parameter");
                u64::from_str(car_id.as_str()).expect("Failed to parse car ID into u64")
            })
            .and_then(area_handler)
            .followed_by(speed_handler)
    }
}
