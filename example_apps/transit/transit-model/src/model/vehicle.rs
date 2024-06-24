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

use std::io::BufRead;

use quick_xml::DeError;
use serde::{Deserialize, Deserializer, Serialize};
use swimos::model::{Text, ValueKind};
use swimos_form::{
    read::{ExpectedEvent, ReadError, ReadEvent, Recognizer, RecognizerReadable},
    write::{StructuralWritable, StructuralWriter},
    Form, Tag,
};
use thiserror::Error;

#[derive(Deserialize, Serialize)]
#[serde(rename = "body")]
struct Body {
    #[serde(rename = "@copyright")]
    copyright: String,
    #[serde(default)]
    vehicle: Vec<VehicleResponse>,
    #[serde(rename = "lastTime")]
    last_time: Option<LastTime>,
}

/// The current state of a vehicle as reported by the web service.
#[derive(Debug, Clone, PartialEq, Form)]
#[form(tag = "vehicle", fields_convention = "camel")]
pub struct Vehicle {
    /// Unique service identifier for the vehicle.
    pub id: String,
    /// The ID of the agency to which the vehicle belongs.
    pub agency: String,
    /// The Swim node URI of the agent representing the vehicle.
    pub uri: String,
    /// The service identifier of the route which the vehicle is on.
    pub route_tag: String,
    /// ID defining the direction of the vehicle on its route.
    pub dir_id: String,
    /// Last reported latitude of the vehicle.
    pub latitude: f64,
    /// Last reported longitude of the vehicle.
    pub longitude: f64,
    /// Last reported speed of the vehicle.
    pub speed: u32,
    /// Number of seconds since the vehicle reported, relative to the timestamp of the response.
    pub secs_since_report: u32,
    /// Last reported heading of the vehicle.
    pub heading: Heading,
    /// Whether the future state of the vehicle can be predicted.
    pub predictable: bool,
    /// Descriptive title of the route which the vehicle is on.
    pub route_title: String,
}

/// Representation of the type that is returned by the vehicles endpoint of the web servive.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Form)]
pub struct VehicleResponse {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@routeTag")]
    pub route_tag: String,
    #[serde(rename = "@dirTag")]
    #[serde(deserialize_with = "deser_dir_id")]
    pub dir_id: String,
    #[serde(rename = "@lat")]
    pub latitude: f64,
    #[serde(rename = "@lon")]
    pub longitude: f64,
    #[serde(rename = "@secsSinceReport")]
    pub secs_since_report: u32,
    #[serde(rename = "@predictable")]
    pub predictable: bool,
    #[serde(rename = "@heading")]
    pub heading: u32,
    #[serde(rename = "@speedKmHr")]
    pub speed: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct LastTime {
    #[serde(rename = "@time")]
    pub time: u64,
}

/// Parse the XML returned by the vehicles endpoint of the web service.
pub fn load_xml_vehicles<R: BufRead>(
    read: R,
) -> Result<(Vec<VehicleResponse>, Option<u64>), DeError> {
    quick_xml::de::from_reader::<R, Body>(read).map(
        |Body {
             vehicle, last_time, ..
         }| (vehicle, last_time.map(|t| t.time)),
    )
}

/// Enumeration of cardinal and ordinal headings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Tag)]
pub enum Heading {
    N,
    NE,
    E,
    SE,
    S,
    SW,
    W,
    NW,
}

impl StructuralWritable for Heading {
    fn num_attributes(&self) -> usize {
        0
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_text(self.as_ref())
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        self.write_with(writer)
    }
}

pub struct HeadingRecognizer;

impl RecognizerReadable for Heading {
    type Rec = HeadingRecognizer;

    type AttrRec = HeadingRecognizer;

    type BodyRec = HeadingRecognizer;

    fn make_recognizer() -> Self::Rec {
        HeadingRecognizer
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        HeadingRecognizer
    }

    fn make_body_recognizer() -> Self::BodyRec {
        HeadingRecognizer
    }

    fn is_simple() -> bool {
        true
    }
}

impl Recognizer for HeadingRecognizer {
    type Target = Heading;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::TextValue(string) => match string.as_ref() {
                "N" => Some(Ok(Heading::N)),
                "NE" => Some(Ok(Heading::NE)),
                "E" => Some(Ok(Heading::E)),
                "SE" => Some(Ok(Heading::SE)),
                "S" => Some(Ok(Heading::S)),
                "SW" => Some(Ok(Heading::SW)),
                "W" => Some(Ok(Heading::W)),
                "NW" => Some(Ok(Heading::NW)),
                ow => Some(Err(ReadError::Malformatted {
                    text: ow.into(),
                    message: Text::new("Not a valid heading."),
                })),
            },
            ow => Some(Err(
                ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::Text))
            )),
        }
    }

    fn reset(&mut self) {}
}

#[derive(Debug, Error)]
#[error("{0} is not a valid heading.")]
pub struct HeadingOutOfRange(pub u32);

impl TryFrom<u32> for Heading {
    type Error = HeadingOutOfRange;

    fn try_from(heading: u32) -> Result<Self, Self::Error> {
        if heading < 360 && !(23..338).contains(&heading) {
            Ok(Heading::E)
        } else if (23..68).contains(&heading) {
            Ok(Heading::NE)
        } else if (68..113).contains(&heading) {
            Ok(Heading::N)
        } else if (113..158).contains(&heading) {
            Ok(Heading::NW)
        } else if (158..203).contains(&heading) {
            Ok(Heading::W)
        } else if (203..248).contains(&heading) {
            Ok(Heading::SW)
        } else if (248..293).contains(&heading) {
            Ok(Heading::S)
        } else if (293..338).contains(&heading) {
            Ok(Heading::SE)
        } else {
            Err(HeadingOutOfRange(heading))
        }
    }
}

fn deser_dir_id<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let dir_id: String = Deserialize::deserialize(deserializer)?;
    Ok(if dir_id.is_empty() || dir_id.contains("_0") {
        "outbound".to_string()
    } else {
        dir_id
    })
}

#[cfg(test)]
mod tests {

    use super::{load_xml_vehicles, VehicleResponse};

    const VEHICLES_EXAMPLE: &[u8] = include_bytes!("test-data/vehicles.xml");

    fn vehicles() -> Vec<VehicleResponse> {
        vec![
            VehicleResponse {
                id: "Citi2".to_string(),
                route_tag: "asdf".to_string(),
                dir_id: "bloop".to_string(),
                latitude: 64.1511322,
                longitude: -0.2549486,
                speed: Some(10),
                secs_since_report: 12,
                heading: 23,
                predictable: true,
            },
            VehicleResponse {
                id: "Citi3".to_string(),
                route_tag: "jhkl".to_string(),
                dir_id: "sloop".to_string(),
                latitude: 64.1603444,
                longitude: -0.2380486,
                speed: Some(70),
                secs_since_report: 36,
                heading: 4,
                predictable: true,
            },
            VehicleResponse {
                id: "A".to_string(),
                route_tag: "yhdjd".to_string(),
                dir_id: "floop".to_string(),
                latitude: 64.1463333,
                longitude: -0.2469221,
                speed: Some(0),
                secs_since_report: 5,
                heading: 300,
                predictable: true,
            },
            VehicleResponse {
                id: "B".to_string(),
                route_tag: "uuu8".to_string(),
                dir_id: "up".to_string(),
                latitude: 64.1467001,
                longitude: -0.2469456,
                speed: Some(22),
                secs_since_report: 2,
                heading: 110,
                predictable: false,
            },
        ]
    }

    const TIMESTAMP: u64 = 1333098017222;

    #[test]
    fn load_vehicles() {
        let expected = vehicles();

        let (vehicles, time) = load_xml_vehicles(VEHICLES_EXAMPLE).expect("Loading routes failed.");

        assert_eq!(vehicles, expected);
        assert_eq!(time, Some(TIMESTAMP));
    }
}
