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

use std::{collections::HashMap, f64::consts::SQRT_2};

use chrono::Utc;
use parking_lot::RwLock;
use rand::Rng;
use tracing::debug;
use transit_model::{
    route::Route,
    vehicle::{Heading, Vehicle, VehicleResponse},
};

use crate::{
    agency::AgencyWithRoutes,
    vehicles::{generate_vehicles, LOC_UNIT},
};

#[derive(Debug)]
pub struct AgenciesState {
    inner: RwLock<Inner>,
    update_interval: u64,
}

impl AgenciesState {
    pub fn generate(agencies_lst: Vec<AgencyWithRoutes>, update_interval: u64) -> Self {
        let mut agencies = HashMap::new();
        let mut vehicles = HashMap::new();
        for agency in agencies_lst {
            let v = generate_vehicles(&agency);
            vehicles.insert(agency.agency.id.clone(), v);
            agencies.insert(agency.agency.id.clone(), agency);
        }
        let last = Utc::now().timestamp_millis() as u64;
        let inner = Inner {
            agencies,
            vehicles,
            last,
        };
        AgenciesState {
            inner: RwLock::new(inner),
            update_interval,
        }
    }
}

#[derive(Debug)]
struct Inner {
    agencies: HashMap<String, AgencyWithRoutes>,
    vehicles: HashMap<String, Vec<Vehicle>>,
    last: u64,
}

impl AgenciesState {
    pub fn routes_for_agency(&self, agency_id: &str) -> Option<Vec<Route>> {
        let guard = self.inner.read();
        guard
            .agencies
            .get(agency_id)
            .map(|AgencyWithRoutes { routes, .. }| routes.clone())
    }

    pub fn vehicles_for_agency(&self, agency_id: &str) -> Option<(Vec<VehicleResponse>, u64)> {
        let guard = self.inner.read();
        let last = guard.last;
        guard
            .vehicles
            .get(agency_id)
            .map(move |vehicles| (vehicles.iter().cloned().map(to_response).collect(), last))
    }

    pub fn update(&self) {
        let AgenciesState {
            ref inner,
            update_interval,
        } = *self;

        let mut guard = inner.write();
        let Inner { last, vehicles, .. } = &mut *guard;
        let now = Utc::now().timestamp_millis() as u64;

        let diff_secs = (now - *last) / 1000;

        let num_updates = diff_secs / update_interval;

        debug!(now, diff_secs, num_updates, "Updating the server state.");

        let mut r = rand::thread_rng();
        for i in 0..num_updates {
            for (_agency, vehicles_lst) in vehicles.iter_mut() {
                let update_index = r.gen_range(0..vehicles_lst.len());
                for (j, v) in vehicles_lst.iter_mut().enumerate() {
                    let diff = (i + 1) * update_interval;
                    if j == update_index {
                        update(v, diff);
                    } else {
                        v.secs_since_report += diff_secs as u32;
                    }
                }
            }
        }
        *last = now;
    }
}

fn update(vehicle: &mut Vehicle, diff: u64) {
    let Vehicle {
        latitude,
        longitude,
        speed,
        secs_since_report,
        heading,
        ..
    } = vehicle;

    let ang_diff = (*speed as f64 * 1000.0 * diff as f64 * LOC_UNIT) / 3600.0;
    let projected = ang_diff / SQRT_2;
    match heading {
        Heading::N => {
            *latitude += ang_diff;
        }
        Heading::NE => {
            *latitude += projected;
            *longitude += projected;
        }
        Heading::E => {
            *longitude += ang_diff;
        }
        Heading::SE => {
            *latitude -= projected;
            *longitude += projected;
        }
        Heading::S => {
            *latitude -= ang_diff;
        }
        Heading::SW => {
            *latitude -= projected;
            *longitude -= projected;
        }
        Heading::W => {
            *longitude -= ang_diff;
        }
        Heading::NW => {
            *latitude += projected;
            *longitude -= projected;
        }
    }
    *secs_since_report = 0;
}

fn to_response(vehicle: Vehicle) -> VehicleResponse {
    let Vehicle {
        id,
        route_tag,
        dir_id,
        latitude,
        longitude,
        speed,
        secs_since_report,
        heading,
        predictable,
        ..
    } = vehicle;
    VehicleResponse {
        id,
        route_tag,
        dir_id,
        latitude,
        longitude,
        secs_since_report,
        predictable,
        heading: to_degrees(heading),
        speed,
    }
}

fn to_degrees(heading: Heading) -> u32 {
    match heading {
        Heading::N => 90,
        Heading::NE => 45,
        Heading::E => 0,
        Heading::SE => 315,
        Heading::S => 270,
        Heading::SW => 225,
        Heading::W => 180,
        Heading::NW => 135,
    }
}
