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

//! # SwimOS routes
//!
//! A route is a URI with no authority that specified the location of a single SwimOS agent instance.
//! It does not specify the host on which the agent can be found and this information must be retrieved
//! from a routing table or provided separately.
//!
//! This crate contains:
//!
//! - A type to represent a [route URI](`RouteUri``).
//! - [Route patterns](`RoutePattern`) that can be used to extract components from the path of a
//! route URI.

mod route_pattern;
mod route_uri;

pub use route_pattern::{ApplyError, ParseError, RoutePattern, UnapplyError};
pub use route_uri::{InvalidRouteUri, RouteUri};
