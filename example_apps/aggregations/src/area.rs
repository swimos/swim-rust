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
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use rand::seq::SliceRandom;

use swimos::agent::event_handler::HandlerActionExt;
use swimos::{
    agent::agent_lifecycle::HandlerContext,
    agent::event_handler::EventHandler,
    agent::lanes::{CommandLane, JoinValueLane, ValueLane},
    agent::lifecycle,
    agent::projections,
    agent::AgentLaneModel,
};
use swimos_form::Form;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Form)]
pub enum Area {
    Arbury,
    CherryHinton,
    KingsHedges,
    Petersfield,
}

impl Area {
    pub fn universe() -> [Area; 4] {
        [
            Area::Arbury,
            Area::CherryHinton,
            Area::KingsHedges,
            Area::Petersfield,
        ]
    }
}

impl Default for Area {
    fn default() -> Self {
        Area::select_random()
    }
}

impl FromStr for Area {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "arbury" => Ok(Area::Arbury),
            "cherryhinton" => Ok(Area::CherryHinton),
            "kingshedges" => Ok(Area::KingsHedges),
            "petersfield" => Ok(Area::Petersfield),
            _ => Err(()),
        }
    }
}

impl Display for Area {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Area::Arbury => write!(f, "arbury"),
            Area::CherryHinton => write!(f, "cherryhinton"),
            Area::KingsHedges => write!(f, "kingshedges"),
            Area::Petersfield => write!(f, "petersfield"),
        }
    }
}

impl Area {
    pub fn select_random() -> Self {
        [
            Area::Arbury,
            Area::CherryHinton,
            Area::KingsHedges,
            Area::Petersfield,
        ]
        .choose(&mut rand::thread_rng())
        .copied()
        .expect("Slice was not empty")
    }
}

#[derive(PartialEq, Copy, Clone, Form)]
pub enum Action {
    Register(u64),
    Deregister(u64),
}

#[derive(AgentLaneModel)]
#[projections]
pub struct AreaAgent {
    registrations: CommandLane<Action>,
    cars: JoinValueLane<u64, u64>,
    average_speed: ValueLane<f64>,
}

#[derive(Clone, Default)]
pub struct AreaLifecycle {
    area: Area,
}

impl AreaLifecycle {
    pub fn new(area: Area) -> AreaLifecycle {
        AreaLifecycle { area }
    }
}

#[lifecycle(AreaAgent)]
impl AreaLifecycle {
    #[on_start]
    pub fn on_start(&self, context: HandlerContext<AreaAgent>) -> impl EventHandler<AreaAgent> {
        context.send_command(None, "/city", "register", self.area.to_string())
    }

    #[on_command(registrations)]
    pub fn registrations(
        &self,
        context: HandlerContext<AreaAgent>,
        action: &Action,
    ) -> impl EventHandler<AreaAgent> {
        match action {
            Action::Register(car_id) => context
                .add_downlink(
                    AreaAgent::CARS,
                    *car_id,
                    None,
                    format!("/cars/{car_id}").as_str(),
                    "speed",
                )
                .boxed_local(),
            Action::Deregister(car_id) => context
                .remove_downlink(AreaAgent::CARS, *car_id)
                .boxed_local(),
        }
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
        let average = speeds.values().sum::<u64>() as f64 / speeds.len() as f64;
        context.set_value(AreaAgent::AVERAGE_SPEED, average)
    }
}
