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

use crate::downlink::model::command::CommandStateMachine;
use crate::downlink::model::map::MapModification;
use crate::downlink::{Command, DownlinkState, Operation, Response, StateMachine};
use swim_common::form::{Form, ValidatedForm};
use swim_common::model::{Attr, Item, Value};
use std::sync::Arc;

#[test]
fn test_handle_value_action_valid() {
    let action = 3.into_value();

    let machine = CommandStateMachine::new(i32::schema());
    let response = machine
        .handle_operation(
            &mut DownlinkState::Unlinked,
            &mut (),
            Operation::Action(action.clone()),
        )
        .unwrap();

    assert_eq!(response, Response::for_command(Command::Action(action)));
}

#[test]
fn test_handle_value_action_invalid() {
    let action = 3.into_value();

    let machine = CommandStateMachine::new(String::schema());
    let response = machine
        .handle_operation(
            &mut DownlinkState::Unlinked,
            &mut (),
            Operation::Action(action.clone()),
        )
        .unwrap();

    assert_eq!(response, Response::none());
}

#[test]
fn test_handle_map_action_valid() {
    let action = MapModification::Update("Foo".to_string(), Arc::new(3)).into_value();

    let machine = CommandStateMachine::new(<MapModification<String, i32> as ValidatedForm>::schema());
    let response = machine
        .handle_operation(
            &mut DownlinkState::Unlinked,
            &mut (),
            Operation::Action(action.clone()),
        )
        .unwrap();

    let header = Attr::of(("update", Value::record(vec![Item::slot("key", "Foo")])));
    let body = Item::of(3);
    let expected = Value::Record(vec![header], vec![body]);

    assert_eq!(response, Response::for_command(Command::Action(expected)));
}

#[test]
fn test_handle_map_action_invalid_key() {
    let action = MapModification::Update("Foo".to_string(), Arc::new(3)).into_value();

    let machine = CommandStateMachine::new(MapModification::<i32, i32>::schema());
    let response = machine
        .handle_operation(
            &mut DownlinkState::Unlinked,
            &mut (),
            Operation::Action(action.clone()),
        )
        .unwrap();

    assert_eq!(response, Response::none());
}

#[test]
fn test_handle_map_action_invalid_value() {
    let action = MapModification::Update("Foo".to_string(), Arc::new(3)).into_value();

    let machine = CommandStateMachine::new(MapModification::<String, String>::schema());
    let response = machine
        .handle_operation(
            &mut DownlinkState::Unlinked,
            &mut (),
            Operation::Action(action.clone()),
        )
        .unwrap();

    assert_eq!(response, Response::none());
}
