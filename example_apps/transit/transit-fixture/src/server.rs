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

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::HeaderName,
    response::IntoResponse,
    routing::get,
    Router,
};
use hyper::{header, StatusCode};
use serde::Deserialize;
use transit_model::{route, vehicle};

use crate::state::AgenciesState;

pub fn make_server_router(state: Arc<AgenciesState>) -> Router {
    let app = Router::new()
        .route("/service/publicXMLFeed", get(handle_request))
        .with_state(state);
    app
}

#[derive(Deserialize)]
#[serde(tag = "command")]
enum Command {
    #[serde(rename = "routeList")]
    RouteList {
        #[serde(rename = "a")]
        agency_id: String,
    },
    #[serde(rename = "vehicleLocations")]
    VehicleLocations {
        #[serde(rename = "a")]
        agency_id: String,
        #[serde(rename = "t")]
        time: i64,
    },
}

const XML_HEADER: (HeaderName, &'static str) = (header::CONTENT_TYPE, "application/xml");
const PLAIN_TXT_HEADER: (HeaderName, &'static str) = (header::CONTENT_TYPE, "text/plain");

async fn handle_request(
    Query(params): Query<Command>,
    State(state): State<Arc<AgenciesState>>,
) -> impl IntoResponse {
    match params {
        Command::RouteList { agency_id } => {
            if let Some(routes) = state.routes_for_agency(&agency_id) {
                (
                    StatusCode::OK,
                    [XML_HEADER],
                    route::produce_xml("NStream 2023".to_string(), routes),
                )
            } else {
                (
                    StatusCode::NOT_FOUND,
                    [PLAIN_TXT_HEADER],
                    format!("No such agency: {}", agency_id),
                )
            }
        }
        Command::VehicleLocations { agency_id, time } => {
            if time != 0 {
                (
                    StatusCode::BAD_REQUEST,
                    [PLAIN_TXT_HEADER],
                    "Predictions not supported.".to_string(),
                )
            } else if let Some((vehicles, last_time)) = state.vehicles_for_agency(&agency_id) {
                (
                    StatusCode::OK,
                    [XML_HEADER],
                    vehicle::produce_xml("NStream 2023".to_string(), vehicles, last_time),
                )
            } else {
                (
                    StatusCode::NOT_FOUND,
                    [PLAIN_TXT_HEADER],
                    format!("No such agency: {}", agency_id),
                )
            }
        }
    }
}
