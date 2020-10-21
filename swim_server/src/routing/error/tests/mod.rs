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

use crate::routing::error::{ConnectionError, ResolutionError, RouterError, Unresolvable};
use crate::routing::RoutingAddr;
use std::io;
use swim_common::ws::error::WebSocketError;
use utilities::uri::RelativeUri;

#[test]
fn unresolvable_display() {
    let err = Unresolvable(RoutingAddr::local(4));

    let string = err.to_string();

    assert_eq!(string, "No active endpoint with ID: Local(4)");
}

#[test]
fn resolution_error_display() {
    let string = ResolutionError::Unresolvable(Unresolvable(RoutingAddr::local(4))).to_string();
    assert_eq!(string, "Address Local(4) could not be resolved.");

    let string = ResolutionError::RouterDropped.to_string();
    assert_eq!(string, "The router channel was dropped.");
}

#[test]
fn router_error_display() {
    let uri: RelativeUri = "/name".parse().unwrap();
    let string = RouterError::NoAgentAtRoute(uri).to_string();
    assert_eq!(string, "No agent at: '/name'");

    let string = RouterError::ConnectionFailure(ConnectionError::ClosedRemotely).to_string();
    assert_eq!(
        string,
        "Failed to route to requested endpoint: 'The connection was closed remotely.'"
    );

    let string = RouterError::RouterDropped.to_string();
    assert_eq!(string, "The router channel was dropped.");
}

#[test]
fn connection_error_display() {
    let string = ConnectionError::ClosedRemotely.to_string();
    assert_eq!(string, "The connection was closed remotely.");

    let string = ConnectionError::Resolution.to_string();
    assert_eq!(string, "The specified host could not be resolved.");

    let string = ConnectionError::Warp("Bad".to_string()).to_string();
    assert_eq!(string, "Warp protocol error: 'Bad'");

    let string = ConnectionError::Websocket(WebSocketError::Protocol).to_string();
    assert_eq!(string, "Web socket error: 'A protocol error occurred.'");

    let string = ConnectionError::Socket(io::ErrorKind::ConnectionRefused).to_string();
    assert_eq!(string, "IO error: 'ConnectionRefused'");
}
