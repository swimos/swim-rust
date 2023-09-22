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

use rand::Rng;
use transit_model::vehicle::{Heading, Vehicle};

use crate::agency::AgencyWithRoutes;

const HEADINGS: &[Heading] = &[
    Heading::N,
    Heading::NE,
    Heading::E,
    Heading::SE,
    Heading::S,
    Heading::SW,
    Heading::W,
    Heading::NW,
];

pub const LOC_UNIT: f64 = 0.00001;
const LAT_ORIGIN: f64 = 64.0;
const LON_ORIGIN: f64 = -12.0;

pub fn generate_vehicles(agency: &AgencyWithRoutes) -> Vec<Vehicle> {
    let AgencyWithRoutes { agency, routes } = agency;
    let mut r = rand::thread_rng();
    let n = r.gen_range(5usize..=10);

    let mut vehicles = vec![];
    for i in 0..n {
        let id = format!("bus_{}", i);

        let route = &routes[r.gen_range(0..routes.len())];

        let dir_id = if r.gen_bool(0.5) {
            "outwards"
        } else {
            "inwards"
        }
        .to_string();

        let speed = r.gen_range(0..=30);
        let heading = HEADINGS[r.gen_range(0..HEADINGS.len())];

        let latitude = LAT_ORIGIN + r.gen_range(0i32..5000) as f64 * LOC_UNIT;
        let longitude = LON_ORIGIN + r.gen_range(0i32..5000) as f64 * LOC_UNIT;
        let uri = format!("/vehicles/{}/{}/{}", agency.country, agency.state, id);
        let v = Vehicle {
            id,
            agency: agency.id.clone(),
            uri,
            route_tag: route.tag.clone(),
            dir_id,
            latitude,
            longitude,
            speed,
            secs_since_report: 0,
            heading,
            predictable: r.gen_bool(0.5),
            route_title: route.title.clone(),
        };
        vehicles.push(v);
    }
    vehicles
}
