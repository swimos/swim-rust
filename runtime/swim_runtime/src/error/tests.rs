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

use crate::error::{
    CloseError, CloseErrorKind, ConnectionError, IoError, ProtocolError, ProtocolErrorKind,
    ResolutionError,
};
use crate::error::{RouterError, Unresolvable};
use crate::routing::RoutingAddr;
use std::io::ErrorKind;
use std::time::Duration;
use swim_utilities::routing::uri::RelativeUri;

#[test]
fn connection_error_display() {
    let string =
        ConnectionError::Closed(CloseError::new(CloseErrorKind::ClosedRemotely, None)).to_string();
    assert_eq!(string, "The connection was closed remotely.");

    let string = ConnectionError::Resolution("xyz://localtoast:9001/".to_string()).to_string();
    assert_eq!(
        string,
        "Address xyz://localtoast:9001/ could not be resolved."
    );

    let string = ConnectionError::Protocol(ProtocolError::new(
        ProtocolErrorKind::Warp,
        Some("Bad".into()),
    ))
    .to_string();
    assert_eq!(string, "WARP violation. Bad");

    let string = ConnectionError::Protocol(ProtocolError::new(ProtocolErrorKind::WebSocket, None))
        .to_string();
    assert_eq!(string, "WebSocket protocol violation.");

    let string = ConnectionError::Io(IoError::new(ErrorKind::ConnectionRefused, None)).to_string();
    assert_eq!(string, "IO error: ConnectionRefused.");

    let string = ConnectionError::Closed(CloseError::new(CloseErrorKind::Normal, None)).to_string();
    assert_eq!(string, "The connection has been closed.");

    let string = ConnectionError::WriteTimeout(Duration::from_secs(5)).to_string();
    assert_eq!(
        string,
        "Writing to the connection failed to complete within 5s."
    )
}

#[test]
fn unresolvable_display() {
    let err = Unresolvable(RoutingAddr::plane(4));

    let string = err.to_string();

    assert_eq!(string, "No active endpoint with ID: Plane(4)");
}

#[test]
fn resolution_error_display() {
    let string = ResolutionError::unresolvable(RoutingAddr::plane(4)).to_string();
    assert_eq!(string, "Address Plane(4) could not be resolved.");

    let string = ResolutionError::router_dropped().to_string();
    assert_eq!(string, "The router channel was dropped.");
}

#[test]
fn router_error_display() {
    let uri: RelativeUri = "/name".parse().unwrap();
    let string = RouterError::NoAgentAtRoute(uri).to_string();
    assert_eq!(string, "No agent at: '/name'");

    let string = RouterError::ConnectionFailure(ConnectionError::Closed(CloseError::new(
        CloseErrorKind::ClosedRemotely,
        None,
    )))
    .to_string();
    assert_eq!(
        string,
        "Failed to route to requested endpoint: 'The connection was closed remotely.'"
    );

    let string = RouterError::RouterDropped.to_string();
    assert_eq!(string, "The router channel was dropped.");
}
