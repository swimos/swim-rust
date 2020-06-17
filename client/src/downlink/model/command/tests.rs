use crate::downlink::model::command::CommandStateMachine;
use crate::downlink::model::map::MapModification;
use crate::downlink::{Command, DownlinkState, Operation, Response, StateMachine};
use common::model::{Attr, Item, Value};
use swim_form::{Form, ValidatedForm};

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
    let action = MapModification::Insert("Foo".to_string(), 3).into_value();

    let machine = CommandStateMachine::new(MapModification::<String, i32>::schema());
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
    let action = MapModification::Insert("Foo".to_string(), 3).into_value();

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
    let action = MapModification::Insert("Foo".to_string(), 3).into_value();

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
