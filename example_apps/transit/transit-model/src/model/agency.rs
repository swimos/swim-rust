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

use percent_encoding::PercentEncode;
use serde::Deserialize;
use swim::form::Form;

use crate::{vehicle::Heading, URL_ENCODE};

use super::{
    route::Route,
    vehicle::{Vehicle, VehicleResponse},
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Form, Hash)]
#[form(tag = "agency")]
pub struct Agency {
    #[serde(default)]
    pub index: usize,
    pub id: String,
    pub state: String,
    pub country: String,
}

fn enc(s: &str) -> PercentEncode<'_> {
    percent_encoding::utf8_percent_encode(s, URL_ENCODE)
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

    pub fn country_uri(&self) -> String {
        format!("/country/{}", enc(&self.country))
    }

    pub fn state_uri(&self) -> String {
        format!("/state/{}/{}", enc(&self.country), enc(&self.state))
    }

    pub fn create_vehicle(&self, route: &Route, response: VehicleResponse) -> Vehicle {
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

        let heading = Heading::try_from(heading).unwrap_or(Heading::N);

        Vehicle {
            id,
            agency: self.id.clone(),
            uri,
            route_tag,
            dir_id,
            latitude,
            longitude,
            speed: speed.unwrap_or_default(),
            secs_since_report,
            heading,
            predictable,
            route_title: route.title.clone(),
        }
    }
}
