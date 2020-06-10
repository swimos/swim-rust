use crate::downlink::model::command::CommandStateMachine;
use crate::downlink::{Command, DownlinkState, Operation, Response, StateMachine};
use common::model::Value;
use form::ValidatedForm;

#[test]
fn test_handle_value_action_valid() {
    let action = 3.into_value();
    let machine = CommandStateMachine::new(i32::schema());

    let response = machine
        .handle_operation(
            &mut DownlinkState::Synced,
            &mut (),
            Operation::Action(Value::Int32Value(3)),
        )
        .unwrap();

    assert_eq!(
        Response::for_command(Command::Action(Value::Int32Value(3))),
        response
    );
}

#[test]
fn test_handle_value_action_invalid() {
    let action = 3.into_value();
    let machine = CommandStateMachine::new(String::schema());

    let response = machine
        .handle_operation(
            &mut DownlinkState::Synced,
            &mut (),
            Operation::Action(Value::Int32Value(3)),
        )
        .unwrap();

    assert_eq!(Response::none(), response);
}
