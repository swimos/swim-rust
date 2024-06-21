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

use transit_model::{bounding_box::BoundingBox, vehicle::VehicleResponse};

#[derive(Default)]
pub struct Statistics {
    pub mean_speed: f64,
    pub n: usize,
    pub bounding_box: Option<BoundingBox>,
}

const MIN_LAT: f64 = -90.0;
const MAX_LAT: f64 = 90.0;
const MIN_LONG: f64 = -180.0;
const MAX_LONG: f64 = 180.0;

impl Statistics {
    pub fn update(mut self, vehicle: &VehicleResponse) -> Self {
        let Statistics {
            mean_speed,
            n,
            bounding_box,
        } = &mut self;

        let next_n = n.checked_add(1).expect("Number of vehicles overflowed.");
        *mean_speed =
            (*n as f64 * *mean_speed + vehicle.speed.unwrap_or_default() as f64) / (next_n as f64);
        *n = next_n;

        if let Some(BoundingBox {
            min_lat,
            max_lat,
            min_lng,
            max_lng,
        }) = bounding_box
        {
            *min_lat = min_lat.min(vehicle.latitude).clamp(MIN_LAT, MAX_LAT);
            *max_lat = max_lat.max(vehicle.latitude).clamp(MIN_LAT, MAX_LAT);
            *min_lng = min_lng.min(vehicle.longitude).clamp(MIN_LONG, MAX_LONG);
            *max_lng = max_lng.max(vehicle.longitude).clamp(MIN_LONG, MAX_LONG);
        } else {
            let lat = vehicle.latitude.clamp(MIN_LAT, MAX_LAT);
            let lng = vehicle.longitude.clamp(MIN_LONG, MAX_LONG);
            *bounding_box = Some(BoundingBox {
                min_lat: lat,
                max_lat: lat,
                min_lng: lng,
                max_lng: lng,
            });
        }

        self
    }
}
