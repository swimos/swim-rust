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

use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
};

use swim::agent::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{EventHandler, HandlerActionExt},
    lanes::{CommandLane, MapLane, ValueLane},
    lifecycle, projections, AgentLaneModel,
};
use tokio::time::Instant;

use crate::model::vehicle::Vehicle;

#[derive(AgentLaneModel)]
#[projections]
#[agent(convention = "camel")]
pub struct VehicleAgent {
    vehicle: ValueLane<Option<Vehicle>>,
    speeds: MapLane<u64, u32>,
    accelerations: MapLane<u64, u32>,
    add_vehicle: CommandLane<Vehicle>,
}

#[derive(Debug)]
pub struct VehicleLifecycle {
    epoch: Instant,
    history_len: usize,
    last_reported_time: Cell<Option<u64>>,
    timestamps: RefCell<VecDeque<u64>>,
}

impl VehicleLifecycle {
    pub fn new(epoch: Instant, history_len: usize) -> Self {
        VehicleLifecycle {
            epoch,
            history_len,
            last_reported_time: Default::default(),
            timestamps: Default::default(),
        }
    }

    fn update_timestamps(&self, timestamp: u64) -> impl EventHandler<VehicleAgent> + '_ {
        let context: HandlerContext<VehicleAgent> = Default::default();
        context
            .effect(move || {
                let mut guard = self.timestamps.borrow_mut();
                guard.push_back(timestamp);
                if guard.len() > self.history_len {
                    guard.pop_front()
                } else {
                    None
                }
            })
            .and_then(move |to_remove: Option<u64>| to_remove.map(remove_old).discard())
    }
}

fn remove_old(to_remove: u64) -> impl EventHandler<VehicleAgent> {
    let context: HandlerContext<VehicleAgent> = Default::default();
    let remove_speed = context.remove(VehicleAgent::SPEEDS, to_remove);
    let remove_acc = context.remove(VehicleAgent::ACCELERATIONS, to_remove);
    remove_speed.followed_by(remove_acc)
}

#[lifecycle(VehicleAgent, no_clone)]
impl VehicleLifecycle {
    #[on_start]
    fn init(&self, context: HandlerContext<VehicleAgent>) -> impl EventHandler<VehicleAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || println!("Starting vehicle agent at: {}", uri))
        })
    }

    #[on_command(add_vehicle)]
    fn set_vehicle(
        &self,
        context: HandlerContext<VehicleAgent>,
        vehicle: &Vehicle,
    ) -> impl EventHandler<VehicleAgent> {
        context.set_value(VehicleAgent::VEHICLE, Some(vehicle.clone()))
    }

    #[on_set(vehicle)]
    fn on_set_vehicle(
        &self,
        context: HandlerContext<VehicleAgent>,
        new_value: &Option<Vehicle>,
        old_value: Option<Option<Vehicle>>,
    ) -> impl EventHandler<VehicleAgent> + '_ {
        new_value
            .as_ref()
            .map(move |vehicle| {
                let timestamp = timestamp(self.epoch, vehicle);
                let old_timestamp = self.last_reported_time.replace(Some(timestamp));

                let speed = vehicle.speed;
                let update_speed = context.update(VehicleAgent::SPEEDS, timestamp, vehicle.speed);

                let update_acc = old_value
                    .flatten()
                    .zip(old_timestamp)
                    .map(move |(previous, previous_ts)| {
                        let acceleration =
                            compute_acceleration(timestamp, previous_ts, speed, previous.speed);
                        context.update(VehicleAgent::ACCELERATIONS, timestamp, acceleration)
                    })
                    .discard();

                let update_ts = self.update_timestamps(timestamp);

                update_speed.followed_by(update_acc).followed_by(update_ts)
            })
            .discard()
    }
}

fn timestamp(epoch: Instant, vehicle: &Vehicle) -> u64 {
    let now = Instant::now().duration_since(epoch).as_millis() as u64;
    let offset = vehicle.secs_since_report as u64 * 1000;
    now.saturating_sub(offset)
}

fn compute_acceleration(
    timestamp: u64,
    previous_timestamp: u64,
    speed: u32,
    previous_speed: u32,
) -> u32 {
    let accf = (speed as i64).saturating_sub(previous_speed as i64) as f64
        / (timestamp.saturating_sub(previous_timestamp)) as f64;
    accf.round() as u32
}
