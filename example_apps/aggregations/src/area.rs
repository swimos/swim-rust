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

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use rand::seq::SliceRandom;

use swimos::{
    agent::agent_lifecycle::utility::HandlerContext,
    agent::event_handler::EventHandler,
    agent::event_handler::HandlerActionExt,
    agent::lanes::{CommandLane, JoinValueLane, ValueLane},
    agent::lifecycle,
    agent::projections,
    agent::AgentLaneModel,
};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Area {
    A,
    B,
    C,
    D,
}

impl FromStr for Area {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "A" => Ok(Area::A),
            "B" => Ok(Area::B),
            "C" => Ok(Area::C),
            "D" => Ok(Area::D),
            _ => Err(()),
        }
    }
}

impl Area {
    pub fn random() -> Self {
        [Area::A, Area::B, Area::C, Area::D]
            .choose(&mut rand::thread_rng())
            .copied()
            .expect("Slice was not empty")
    }
}

#[derive(AgentLaneModel)]
#[projections]
pub struct AreaAgent {
    register: CommandLane<u64>,
    deregister: CommandLane<u64>,
    // car_id -> speed
    cars: JoinValueLane<u64, u64>,
    average_speed: ValueLane<f64>,
}

#[derive(Clone, Default)]
pub struct AreaLifecycle {
    area: Arc<Mutex<Option<Area>>>,
}

#[lifecycle(AreaAgent)]
impl AreaLifecycle {
    #[on_start]
    pub fn on_start(&self, context: HandlerContext<AreaAgent>) -> impl EventHandler<AreaAgent> {
        let area = self.area.clone();
        context
            .get_parameter("area")
            .and_then(move |area_id: Option<String>| {
                context.effect(move || {
                    let assigned_area = &mut *area.lock().expect("Mutex poisoned");
                    let area_str = area_id.expect("Missing area URI");
                    *assigned_area = Area::from_str(area_str.as_str()).ok();
                    area_str
                })
            })
            .and_then(move |area: String| {
                context.send_command(None, "/aggregate", "register", area)
            })
    }

    #[on_command(register)]
    pub fn register(
        &self,
        context: HandlerContext<AreaAgent>,
        car_id: &u64,
    ) -> impl EventHandler<AreaAgent> {
        context.add_downlink(
            AreaAgent::CARS,
            *car_id,
            None,
            format!("/cars/{car_id}").as_str(),
            "speed",
        )
    }

    #[on_command(deregister)]
    pub fn deregister(
        &self,
        context: HandlerContext<AreaAgent>,
        car_id: &u64,
    ) -> impl EventHandler<AreaAgent> {
        context.remove_downlink(AreaAgent::CARS, *car_id)
    }

    #[on_update(cars)]
    fn cars(
        &self,
        context: HandlerContext<AreaAgent>,
        speeds: &HashMap<u64, u64>,
        _key: u64,
        _prev: Option<u64>,
        _new_value: &u64,
    ) -> impl EventHandler<AreaAgent> {
        let speeds = speeds.clone();
        context
            .effect(move || speeds.values().sum::<u64>() as f64 / speeds.len() as f64)
            .and_then(move |average: f64| context.set_value(AreaAgent::AVERAGE_SPEED, average))
    }
}
