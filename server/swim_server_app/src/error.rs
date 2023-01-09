// Copyright 2015-2021 Swim Inc.
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

use std::{
    error::Error,
    fmt::{Display, Formatter},
};

use swim_api::error::StoreError;
use swim_runtime::{error::tls::TlsError, net::ConnectionError};
use thiserror::Error;

use swim_utilities::{format::comma_sep, routing::route_pattern::RoutePattern};

/// Indicates that the routes specified for plane are ambiguous (overlap with each other).
#[derive(Debug)]
pub enum AmbiguousRoutes {
    Overlapping {
        routes: Vec<RoutePattern>,
    },
    MetaCollision {
        meta_routes: Vec<RoutePattern>,
        routes: Vec<RoutePattern>,
    },
}

impl AmbiguousRoutes {
    pub fn new(routes: Vec<RoutePattern>) -> Self {
        AmbiguousRoutes::Overlapping { routes }
    }

    pub fn collision(meta_routes: Vec<RoutePattern>, routes: Vec<RoutePattern>) -> Self {
        AmbiguousRoutes::MetaCollision {
            meta_routes,
            routes,
        }
    }
}

impl Display for AmbiguousRoutes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AmbiguousRoutes::Overlapping { routes } => {
                write!(
                    f,
                    "Agent route patterns are ambiguous: [{}]",
                    comma_sep(routes)
                )
            }
            AmbiguousRoutes::MetaCollision {
                meta_routes,
                routes,
            } => {
                write!(
                    f,
                    "Agent route patterns [{}] are ambiguous with the meta-agent routes: [{}]",
                    comma_sep(routes),
                    comma_sep(meta_routes),
                )
            }
        }
    }
}

impl Error for AmbiguousRoutes {}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("The server network connection failed.")]
    Networking(#[from] ConnectionError),
    #[error("Opening the store for a plane failed.")]
    Persistence(#[from] StoreError),
}

#[derive(Debug, Error)]
pub enum ServerBuilderError {
    #[error("The specified agent routes are invalid: {0}")]
    BadRoutes(#[from] AmbiguousRoutes),
    #[error("Opening the store failed: {0}")]
    Persistence(#[from] StoreError),
    #[error("Invalid TLS configuration/certificate: {0}")]
    Tls(#[from] TlsError),
}

#[cfg(test)]
mod tests {
    use swim_introspection::route::{lane_pattern, node_pattern};
    use swim_utilities::routing::route_pattern::RoutePattern;

    use super::AmbiguousRoutes;

    #[test]
    fn ambiguous_routes_overlapping_display() {
        let pat1 = RoutePattern::parse_str("/node").expect("Invalid route.");
        let pat2 = RoutePattern::parse_str("/:id").expect("Invalid route.");

        let err = AmbiguousRoutes::new(vec![pat1, pat2]);

        let err_string = err.to_string();

        assert_eq!(
            err_string,
            "Agent route patterns are ambiguous: [/node, /:id]"
        );
    }

    #[test]
    fn ambiguous_routes_meta_display() {
        let pat1 = RoutePattern::parse_str("/node").expect("Invalid route.");
        let pat2 = RoutePattern::parse_str("/:id").expect("Invalid route.");

        let err =
            AmbiguousRoutes::collision(vec![node_pattern(), lane_pattern()], vec![pat1, pat2]);

        let err_string = err.to_string();

        assert_eq!(
            err_string,
            "Agent route patterns [/node, /:id] are ambiguous with the meta-agent routes: [swim:meta:node/:node_uri, swim:meta:node/:node_uri/lane/:lane_name]"
        );
    }
}
