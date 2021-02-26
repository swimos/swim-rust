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

use super::*;
use crate::downlink::Command;
use swim_common::form::{Form, ValidatedForm};
use swim_common::model::ValueKind;

#[test]
fn test_init() {
    let machine = CommandStateMachine::new(i32::schema());
    let (_, cmd) = machine.initialize();
    assert!(cmd.is_none());
}

#[test]
fn test_handle_value_action_valid() {
    let action = 3.into_value();

    let machine = CommandStateMachine::new(i32::schema());

    let mut state = ();
    let result = machine.handle_request(&mut state, action.clone());

    if let Ok(Response {
        event: None,
        command: Some(Command::Action(act)),
    }) = result
    {
        assert_eq!(act, action);
    } else {
        panic!("Unexpected response: {:?}", result);
    }
}

#[test]
fn test_handle_value_action_invalid() {
    let action = 3.into_value();
    let mut state = ();

    let machine = CommandStateMachine::new(StandardSchema::OfKind(ValueKind::Text));
    let result = machine.handle_request(&mut state, action.clone());

    assert!(matches!(
        result,
        Ok(Response {
            event: None,
            command: None
        })
    ));
}
