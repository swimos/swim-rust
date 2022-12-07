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

use super::*;
use crate::downlink::{Command, Message};
use swim_model::{Value, ValueKind};
use swim_schema::schema::StandardSchema;

#[test]
fn event_dl_init() {
    let machine = EventStateMachine::new(StandardSchema::Anything, SchemaViolations::Ignore);

    let (_, cmd) = machine.initialize();
    assert_eq!(cmd, Some(Command::Link));
}

#[test]
fn event_dl_linked() {
    let machine = EventStateMachine::new(StandardSchema::Anything, SchemaViolations::Ignore);

    let EventResult { result, terminate } = machine.handle_warp_message(&mut (), Message::Linked);

    assert!(!terminate);
    assert!(matches!(result, Ok(None)));
}

#[test]
fn event_dl_synced() {
    let machine = EventStateMachine::new(StandardSchema::Anything, SchemaViolations::Ignore);

    let EventResult { result, terminate } = machine.handle_warp_message(&mut (), Message::Synced);

    assert!(!terminate);
    assert!(matches!(result, Ok(None)));
}

#[test]
fn event_dl_unlinked() {
    let machine = EventStateMachine::new(StandardSchema::Anything, SchemaViolations::Ignore);

    let EventResult { result, terminate } = machine.handle_warp_message(&mut (), Message::Unlinked);

    assert!(terminate);
    assert!(matches!(result, Ok(None)));
}

#[test]
fn event_dl_valid_message() {
    let machine = EventStateMachine::new(
        StandardSchema::OfKind(ValueKind::Int32),
        SchemaViolations::Ignore,
    );

    let EventResult { result, terminate } =
        machine.handle_warp_message(&mut (), Message::Action(Value::from(2)));

    assert!(!terminate);
    assert!(matches!(result, Ok(Some(v)) if v == Value::from(2)));
}

#[test]
fn event_dl_ignore_invalid_message() {
    let machine = EventStateMachine::new(
        StandardSchema::OfKind(ValueKind::Int32),
        SchemaViolations::Ignore,
    );

    let EventResult { result, terminate } =
        machine.handle_warp_message(&mut (), Message::Action(Value::from("hello")));

    assert!(!terminate);
    assert!(matches!(result, Ok(None)));
}

#[test]
fn event_dl_error_on_invalid_message() {
    let machine = EventStateMachine::new(
        StandardSchema::OfKind(ValueKind::Int32),
        SchemaViolations::Report,
    );

    let EventResult { result, terminate } =
        machine.handle_warp_message(&mut (), Message::Action(Value::from("hello")));

    assert!(terminate);
    assert!(matches!(
        result,
        Err(DownlinkError::SchemaViolation(
            Value::Text(_),
            StandardSchema::OfKind(ValueKind::Int32)
        ))
    ));
}
