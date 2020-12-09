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

use crate::routing::server::ServerConnectionError;
use crate::routing::ws::WebSocketError;
use std::io;

#[test]
fn connection_error_display() {
    let string = ServerConnectionError::ClosedRemotely.to_string();
    assert_eq!(string, "The connection was closed remotely.");

    let string = ServerConnectionError::Resolution.to_string();
    assert_eq!(string, "The specified host could not be resolved.");

    let string = ServerConnectionError::Warp("Bad".to_string()).to_string();
    assert_eq!(string, "Warp protocol error: 'Bad'");

    let string = ServerConnectionError::Websocket(WebSocketError::Protocol).to_string();
    assert_eq!(string, "Web socket error: 'A protocol error occurred.'");

    let string = ServerConnectionError::Socket(io::ErrorKind::ConnectionRefused).to_string();
    assert_eq!(string, "IO error: 'ConnectionRefused'");

    let string = ServerConnectionError::Closed.to_string();
    assert_eq!(string, "The connection has been closed.");
}
