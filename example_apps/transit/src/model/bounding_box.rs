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

use swim::form::Form;

#[derive(Debug, PartialEq, Form)]
#[form(tag = "bounds")]
pub struct BoundingBox {
    #[form(name = "minLat")]
    pub min_latitude: f64,
    #[form(name = "maxLat")]
    pub max_latitude: f64,
    #[form(name = "minLng")]
    pub min_longitude: f64,
    #[form(name = "maxLng")]
    pub max_longitude: f64,
}

impl Default for BoundingBox {
    fn default() -> Self {
        Self { 
            min_latitude: -90.0, 
            max_latitude: 90.0, 
            min_longitude: -180.0, 
            max_longitude: 180.0
        }
    }
}