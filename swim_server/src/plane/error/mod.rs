// Copyright 2015-2020 SWIM.AI inc.
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

#[cfg(test)]
mod tests;

use crate::routing::RoutingAddr;
use std::error::Error;
use std::fmt::{Display, Formatter};
use swim_common::routing::RoutingError;
use utilities::route_pattern::RoutePattern;
use utilities::uri::RelativeUri;

/// Error indicating that request to route to a plane-local agent failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoAgentAtRoute(pub RelativeUri);

impl Display for NoAgentAtRoute {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let NoAgentAtRoute(route) = self;
        write!(f, "No agent at route: '{}'", route)
    }
}

impl Error for NoAgentAtRoute {}

/// Error indicating that a routing address is invalid. (Typically, this should not occur and
/// suggests a bug).
#[derive(Debug, Clone, Copy)]
pub struct Unresolvable(pub RoutingAddr);

impl Display for Unresolvable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Unresolvable(addr) = self;
        write!(f, "No active endpoint with ID: {}", addr)
    }
}

impl Error for Unresolvable {}

/// Indicates that ambiguous routes were specified when defining a plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmbiguousRoutes(RoutePattern, RoutePattern);

impl AmbiguousRoutes {
    pub fn new(first: RoutePattern, second: RoutePattern) -> Self {
        AmbiguousRoutes(first, second)
    }
}

impl Display for AmbiguousRoutes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Routes '{}' and '{}' are ambiguous.", &self.0, &self.1)
    }
}

impl Error for AmbiguousRoutes {}

/// General error type for a failed agent resolution.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolutionError {
    /// Local error where we can be sure that there is no agent for a specified route.
    NoAgent(NoAgentAtRoute),
    /// We failed to route a message to a remote endpoint.
    NoRoute(RoutingError),
}

impl Display for ResolutionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolutionError::NoAgent(err) => err.fmt(f),
            ResolutionError::NoRoute(err) => err.fmt(f),
        }
    }
}

impl Error for ResolutionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ResolutionError::NoAgent(err) => Some(err),
            ResolutionError::NoRoute(err) => Some(err),
        }
    }
}
