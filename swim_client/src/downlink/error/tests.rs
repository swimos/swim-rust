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

use crate::downlink::error::{DownlinkError, TransitionError};
use swim_model::Value;
use swim_runtime::error::{CloseError, ConnectionError, RoutingError};
use swim_schema::schema::StandardSchema;

#[test]
fn transition_error_display() {
    let string = TransitionError::ReceiverDropped.to_string();
    assert_eq!(string, "Observer of the update was dropped.");

    let string = TransitionError::SideEffectFailed.to_string();
    assert_eq!(string, "A side effect failed to complete.");

    let string = TransitionError::IllegalTransition("Bad".to_string()).to_string();
    assert_eq!(string, "An illegal transition was attempted: 'Bad'");
}

const MESSAGE: &str = "Boom!";

#[test]
fn downlink_error_display() {
    let string = DownlinkError::DroppedChannel.to_string();
    assert_eq!(
        string,
        "An internal channel was dropped and the downlink is now closed."
    );

    let string = DownlinkError::ClosingFailure.to_string();
    assert_eq!(string, "An error occurred while closing down.");

    let string = DownlinkError::ConnectionFailure(MESSAGE.to_string()).to_string();
    assert_eq!(string, "Connection failure: Boom!");

    let string =
        DownlinkError::ConnectionPoolFailure(ConnectionError::Closed(CloseError::unexpected()))
            .to_string();
    assert_eq!(
        string,
        format!(
            "The connection pool has encountered a failure: {}",
            CloseError::unexpected()
        )
    );

    let string = DownlinkError::InvalidAction.to_string();
    assert_eq!(
        string,
        "An action could not be applied to the internal state."
    );

    let string = DownlinkError::MalformedMessage.to_string();
    assert_eq!(string, "A message did not have the expected shape.");

    let string =
        DownlinkError::SchemaViolation(Value::Int32Value(0), StandardSchema::Nothing).to_string();
    assert_eq!(
        string,
        format!(
            "Received {} but expected a value matching {}.",
            &Value::Int32Value(0),
            &StandardSchema::Nothing
        )
    );

    let string = DownlinkError::TaskPanic(MESSAGE).to_string();
    assert_eq!(string, "The downlink task panicked with: \"Boom!\"");

    let string = DownlinkError::TransitionError.to_string();
    assert_eq!(string, "The downlink state machine produced and error.");
}

#[test]
fn routing_error_to_downlink_error() {
    let err: DownlinkError = RoutingError::CloseError.into();
    assert!(matches!(err, DownlinkError::ClosingFailure));

    let err: DownlinkError = RoutingError::ConnectionError.into();
    assert!(
        matches!(err, DownlinkError::ConnectionFailure(str) if str == "The connection has been lost")
    );

    let err: DownlinkError = RoutingError::RouterDropped.into();
    assert!(matches!(err, DownlinkError::DroppedChannel));

    let err: DownlinkError = RoutingError::HostUnreachable.into();
    assert!(
        matches!(err, DownlinkError::ConnectionFailure(str) if str == "The host is unreachable")
    );

    let err: DownlinkError =
        RoutingError::PoolError(ConnectionError::Closed(CloseError::unexpected())).into();
    assert!(
        matches!(err, DownlinkError::ConnectionPoolFailure(ConnectionError::Closed(e)) if e == CloseError::unexpected())
    );
}
