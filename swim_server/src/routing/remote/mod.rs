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

use crate::routing::error::{ConnectionError, Unresolvable};
use crate::routing::{Route, RoutingAddr, TaggedEnvelope};
use swim_common::request::Request;
use tokio::sync::mpsc;
use url::Url;

pub mod addresses;
mod config;
pub mod pending;
pub mod router;
pub mod table;
pub mod task;
#[cfg(test)]
mod test_fixture;

type EndpointRequest = Request<Result<Route<mpsc::Sender<TaggedEnvelope>>, Unresolvable>>;
type ResolutionRequest = Request<Result<RoutingAddr, ConnectionError>>;

/// Requests that are generated by the remote router to be serviced by the connection manager.
#[derive(Debug)]
pub enum RoutingRequest {
    /// Get channel to route messages to a specified routing address.
    Endpoint {
        addr: RoutingAddr,
        request: EndpointRequest,
    },
    /// Resolve the routing address for a host.
    ResolveUrl {
        host: Url,
        request: ResolutionRequest,
    },
}

const REQUEST_DROPPED: &str = "The receiver of a routing request was dropped before it completed.";
