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

use transit_model::{agency::Agency, route::Route};

pub const STATE1: &str = "AB";
pub const STATE2: &str = "CD";
pub const COUNTRY: &str = "US";

pub fn mock_agencies() -> Vec<AgencyWithRoutes> {
    vec![
        AgencyWithRoutes {
            agency: Agency { index: 0, id: "first".to_string(), state: STATE1.to_string(), country: COUNTRY.to_string() },
            routes: vec![
                Route { tag: "r1".to_string(), title: "Red Route".to_string() },
                Route { tag: "r2".to_string(), title: "Green Route".to_string() },
                Route { tag: "r3".to_string(), title: "Blue Route".to_string() },
            ],
        },
        AgencyWithRoutes {
            agency: Agency { index: 1, id: "second".to_string(), state: STATE1.to_string(), country: COUNTRY.to_string() },
            routes: vec![
                Route { tag: "purple".to_string(), title: "Purple Route".to_string() },
                Route { tag: "brown".to_string(), title: "Brown Route".to_string() },
            ],
        },
        AgencyWithRoutes {
            agency: Agency { index: 2, id: "third".to_string(), state: STATE2.to_string(), country: COUNTRY.to_string() },
            routes: vec![
                Route { tag: "circ".to_string(), title: "Circular Route".to_string() },
                Route { tag: "rad".to_string(), title: "Radial Route".to_string() },
                Route { tag: "spiral".to_string(), title: "Spiral Route".to_string() },
            ],
        }
    ]
}

#[derive(Debug, Clone)]
pub struct AgencyWithRoutes {
    pub agency: Agency,
    pub routes: Vec<Route>,
}