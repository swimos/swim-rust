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

use std::fmt::Display;

use swim::form::Form;

#[derive(Clone, Copy, Debug, Default, PartialEq, Form)]
#[form(tag = "bounds", fields_convention = "camel")]
pub struct BoundingBox {
    pub min_lat: f64,
    pub max_lat: f64,
    pub min_lng: f64,
    pub max_lng: f64,
}

impl Display for BoundingBox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let BoundingBox {
            min_lat,
            max_lat,
            min_lng,
            max_lng,
        } = self;
        write!(
            f,
            "Bounds {{ latitude = [{}, {}), longitude = [{}, {}) }}",
            min_lat, max_lat, min_lng, max_lng
        )
    }
}
