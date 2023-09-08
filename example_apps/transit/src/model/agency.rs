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

use percent_encoding::{PercentEncode, NON_ALPHANUMERIC};
use serde::Deserialize;
use swim::form::Form;

use super::vehicle::{Vehicle, VehicleResponse};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Form)]
#[form(tag = "agency")]
pub struct Agency {
    #[serde(default)]
    pub index: usize,
    pub id: String,
    pub state: String,
    pub country: String,
}

const AGENCIES_CSV: &[u8] = include_bytes!("agencies.csv");

pub fn agencies() -> Vec<Agency> {
    let mut reader = csv::Reader::from_reader(AGENCIES_CSV);
    let agencies_result = reader.deserialize::<Agency>().collect::<Result<_, _>>();

    let mut agencies: Vec<Agency> = agencies_result.expect("CSV data was invalid.");
    for (i, agency) in agencies.iter_mut().enumerate() {
        agency.index = i;
    }
    agencies
}

fn enc<'a>(s: &'a str) -> PercentEncode<'a> {
    percent_encoding::utf8_percent_encode(s, NON_ALPHANUMERIC)
}

impl Agency {
    pub fn uri(&self) -> String {
        format!(
            "/agency/{}/{}/{}",
            enc(&self.country),
            enc(&self.state),
            enc(&self.id)
        )
    }

    pub fn create_vehicle(&self, response: VehicleResponse, route_title: String) -> Vehicle {
        let VehicleResponse {
            id,
            route_tag,
            dir_id,
            latitude,
            longitude,
            speed,
            secs_since_report,
            heading,
            predictable,
        } = response;

        let uri = format!(
            "/vehicle/{}/{}/{}/{}",
            enc(&self.country),
            enc(&self.state),
            enc(&self.id),
            enc(&id)
        );
        Vehicle {
            id,
            agency: self.id.clone(),
            uri,
            route_tag,
            dir_id,
            latitude,
            longitude,
            speed,
            secs_since_report,
            heading,
            predictable,
            route_title,
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn load_agencies() {
        let agencies = super::agencies();
        println!("{:?}", agencies);
        assert_eq!(agencies.len(), 46);
    }
}
