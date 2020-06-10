use crate::configuration::downlink::{BackpressureMode, DownlinkParams, OnInvalidMessage};
use crate::downlink::model::command;
use crate::downlink::model::command::CommandStateMachine;
use crate::downlink::{Command, DownlinkState, Operation, Response, StateMachine};
use crate::router::RoutingError;
use common::model::schema::StandardSchema;
use common::model::Value;
use common::sink::item::ItemSender;
use form::ValidatedForm;
use tokio::sync::mpsc;
use tokio::time::Duration;

#[test]
fn test_handle_action_valid() {
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
fn test_handle_action_invalid() {
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
