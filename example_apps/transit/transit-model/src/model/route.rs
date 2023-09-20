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
use serde::{Deserialize, Serialize};
use swim::form::Form;

#[derive(Deserialize, Serialize)]
#[serde(rename = "body")]
struct Body {
    #[serde(rename = "@copyright")]
    copyright: String,
    #[serde(default)]
    route: Vec<Route>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Form)]
#[form(tag = "route")]
pub struct Route {
    #[serde(rename = "@tag")]
    pub tag: String,
    #[serde(rename = "@title")]
    pub title: String,
}

pub fn load_xml_routes<R: BufRead>(read: R) -> Result<Vec<Route>, DeError> {
    quick_xml::de::from_reader::<R, Body>(read).map(|body| body.route)
}

pub fn produce_xml(copyright: String, routes: Vec<Route>) -> String {
    let body = Body {
        copyright,
        route: routes,
    };
    quick_xml::se::to_string(&body).expect("Invalid routes.")
}

#[cfg(test)]
mod tests {

    use super::{load_xml_routes, Route};

    const ROUTES_EXAMPLE: &[u8] = include_bytes!("test-data/routes.xml");

    #[test]
    fn load_routes() {
        let expected = vec![
            Route {
                tag: "antelope".to_string(),
                title: "Beige Line".to_string(),
            },
            Route {
                tag: "llama".to_string(),
                title: "Chartreuse Line".to_string(),
            },
            Route {
                tag: "zebra".to_string(),
                title: "Black and White Line".to_string(),
            },
        ];
        let result = load_xml_routes(ROUTES_EXAMPLE).expect("Loading routes failed.");
        assert_eq!(result, expected);
    }

    #[test]
    fn produce_routes() {
        let routes = vec![
            Route {
                tag: "antelope".to_string(),
                title: "Beige Line".to_string(),
            },
            Route {
                tag: "llama".to_string(),
                title: "Chartreuse Line".to_string(),
            },
            Route {
                tag: "zebra".to_string(),
                title: "Black and White Line".to_string(),
            },
        ];
        let xml = super::produce_xml("NStream 2023".to_string(), routes);
        println!("{}", xml);
    }
}
