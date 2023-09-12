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

use bytes::Bytes;
use percent_encoding::NON_ALPHANUMERIC;
use reqwest::Client;

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

impl BusesApi {
    pub(crate) fn new(base_uri: String, enable_gzip: bool) -> Self {
        let client = Client::builder().gzip(enable_gzip).build().unwrap();

        BusesApi { client, base_uri }
    }

    pub async fn get_routes(&self, agency: &Agency) -> Result<Vec<Route>, BusesApiError> {
        let BusesApi { base_uri, .. } = self;
        let Agency { id, .. } = agency;
        let uri = format!(
            "{}?command=routeList&a={}",
            base_uri,
            percent_encoding::utf8_percent_encode(id, NON_ALPHANUMERIC)
        );
        let bytes = self.do_request(uri).await?;
        Ok(load_xml_routes(bytes.as_ref())?)
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
            percent_encoding::utf8_percent_encode(id, NON_ALPHANUMERIC)
        );
        let bytes = self.do_request(uri).await?;
        let responses = load_xml_vehicles(bytes.as_ref())?;
        Ok(responses.into_iter().collect())
    }

    async fn do_request(&self, uri: String) -> Result<Bytes, BusesApiError> {
        let BusesApi { client, .. } = self;
        let request = client.get(uri).build()?;
        let response = client.execute(request).await?;
        Ok(response.bytes().await?)
    }
}

const BASE: &str = "https://retro.umoiq.com/service/publicXMLFeed";

impl Default for BusesApi {
    fn default() -> Self {
        Self::new(BASE.to_string(), true)
    }
}
