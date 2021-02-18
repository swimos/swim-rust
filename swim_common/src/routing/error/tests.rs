// Copyright 2015-2021 SWIM.AI inc.
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

use crate::routing::{
    CloseError, CloseErrorKind, ConnectionError, IoError, ProtocolError, ProtocolErrorKind,
    ResolutionError,
};
use std::io::ErrorKind;

#[test]
fn connection_error_display() {
    let string =
        ConnectionError::Closed(CloseError::new(CloseErrorKind::ClosedRemotely, None)).to_string();
    assert_eq!(string, "The connection was closed remotely.");

    let string = ConnectionError::Resolution(ResolutionError::unresolvable(
        "xyz://localtoast:9001/".into(),
    ))
    .to_string();
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
}
