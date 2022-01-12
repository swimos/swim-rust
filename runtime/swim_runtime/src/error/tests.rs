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

use crate::error::Unresolvable;
use crate::error::{
    CloseError, CloseErrorKind, ConnectionError, IoError, ProtocolError, ProtocolErrorKind,
    ResolutionError, RoutingError,
};
use crate::routing::RoutingAddr;
use std::io::ErrorKind;
use swim_utilities::routing::uri::RelativeUri;

#[test]
fn connection_error_display() {
    let string =
        ConnectionError::Closed(CloseError::new(CloseErrorKind::ClosedRemotely, None)).to_string();
    assert_eq!(string, "The connection was closed remotely.");

    let string = ConnectionError::Resolution(ResolutionError::Host(
        "xyz://localtoast:9001/".parse().unwrap(),
    ))
    .to_string();
    assert_eq!(string, "Failed to resolve host: `xyz://localtoast:9001/`");

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
}

#[test]
fn unresolvable_display() {
    let err = Unresolvable(RoutingAddr::plane(4));

    let string = err.to_string();

    assert_eq!(string, "No active endpoint with ID: Plane(4)");
}

#[test]
fn router_error_display() {
    let uri: RelativeUri = "/name".parse().unwrap();
    let string = RoutingError::Resolution(ResolutionError::Agent(uri)).to_string();
    assert_eq!(string, "Failed to resolve agent URI: `/name`");

    let string = RoutingError::Connection(ConnectionError::Closed(CloseError::new(
        CloseErrorKind::ClosedRemotely,
        None,
    )))
    .to_string();
    assert_eq!(string, "The connection was closed remotely.");

    let string = RoutingError::Dropped.to_string();
    assert_eq!(string, "Router dropped");
}
