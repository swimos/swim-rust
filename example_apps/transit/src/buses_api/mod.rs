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

use std::fmt::Display;

use bytes::Bytes;
use reqwest::{Client, StatusCode};
use thiserror::Error;
use tracing::{debug, error};
use transit_model::URL_ENCODE;

use crate::model::{
    agency::Agency,
    route::{load_xml_routes, Route},
    vehicle::{load_xml_vehicles, VehicleResponse},
};

#[derive(Debug, Clone)]
pub struct BusesApi {
    client: Client,
    base_uri: String,
}

pub struct BusesApiError(Box<dyn std::error::Error>);

impl Display for BusesApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<E> From<E> for BusesApiError
where
    E: std::error::Error + 'static,
{
    fn from(value: E) -> Self {
        BusesApiError(Box::new(value))
    }
}

#[derive(Debug, Error)]
#[error("HTTP Request to {uri} returned unexpected response: {code}")]
struct BadStatusCode {
    uri: String,
    code: StatusCode,
}

impl BusesApi {
    pub fn new(base_uri: String, enable_gzip: bool) -> Self {
        debug!(base_uri, enable_gzip, "Initializing Buses API.");
        let client = Client::builder().gzip(enable_gzip).build().unwrap();

        BusesApi { client, base_uri }
    }

    pub async fn get_routes(&self, agency: &Agency) -> Result<Vec<Route>, BusesApiError> {
        let BusesApi { base_uri, .. } = self;
        let Agency { id, .. } = agency;
        let uri = format!(
            "{}?command=routeList&a={}",
            base_uri,
            percent_encoding::utf8_percent_encode(id, URL_ENCODE)
        );
        debug!(id, uri, "Requesting routes for agency.");
        let bytes = self.do_request(uri).await.map_err(|error| {
            error!(id, error = %error, "Failed to get routes for agency.");
            error
        })?;
        let as_str = std::str::from_utf8(bytes.as_ref()).unwrap_or("Bad UTF8");
        Ok(load_xml_routes(bytes.as_ref()).map_err(|error| {
            error!(id, error = %error, as_str, "Failed to parse route XML.");
            error
        })?)
    }

    pub async fn poll_vehicles(
        &self,
        agency: &Agency,
    ) -> Result<Vec<VehicleResponse>, BusesApiError> {
        let BusesApi { base_uri, .. } = self;
        let Agency { id, .. } = agency;
        let uri = format!(
            "{}?command=vehicleLocations&a={}&t=0",
            base_uri,
            percent_encoding::utf8_percent_encode(id, URL_ENCODE)
        );
        debug!(id, uri, "Polling vehicles for agency.");
        let bytes = self.do_request(uri).await.map_err(|error| {
            error!(id, error = %error, "Failed to poll vehicles for agency.");
            error
        })?;
        let (responses, _last_time) = load_xml_vehicles(bytes.as_ref()).map_err(|error| {
            error!(id, error = %error, "Failed to get parse vehicle XML.");
            error
        })?;
        Ok(responses.into_iter().collect())
    }

    async fn do_request(&self, uri: String) -> Result<Bytes, BusesApiError> {
        debug!(uri, "Making HTTP request.");
        let BusesApi { client, .. } = self;
        let request = client.get(&uri).build()?;
        let response = client.execute(request).await?;
        if response.status() != StatusCode::OK {
            return Err(BusesApiError(Box::new(BadStatusCode {
                uri,
                code: response.status(),
            })));
        }
        Ok(response.bytes().await?)
    }
}

const BASE: &str = "https://retro.umoiq.com/service/publicXMLFeed";

impl Default for BusesApi {
    fn default() -> Self {
        Self::new(BASE.to_string(), true)
    }
}
