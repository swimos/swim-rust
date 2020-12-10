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

use crate::routing::ws::WebSocketError;
use crate::routing::{ConnectionError, ConnectionErrorKind};
use std::io;

#[test]
fn connection_error_display() {
    let string = ConnectionError::new(ConnectionErrorKind::ClosedRemotely).to_string();
    assert_eq!(string, "The connection was closed remotely.");

    let string = ConnectionError::new(ConnectionErrorKind::Resolution).to_string();
    assert_eq!(string, "The specified host could not be resolved.");

    let string = ConnectionError::with_cause(ConnectionErrorKind::Warp, "Bad".into()).to_string();
    assert_eq!(string, "WARP error. Caused by: Bad");

    let string =
        ConnectionError::new(ConnectionErrorKind::Websocket(WebSocketError::Protocol)).to_string();
    assert_eq!(string, "Websocket error: \"A protocol error occurred.\"");

    let string = ConnectionError::new(ConnectionErrorKind::Socket(
        io::ErrorKind::ConnectionRefused,
    ))
    .to_string();
    assert_eq!(string, "IO error: 'ConnectionRefused'");

    let string = ConnectionError::new(ConnectionErrorKind::Closed).to_string();
    assert_eq!(string, "The connection has been closed.");
}
