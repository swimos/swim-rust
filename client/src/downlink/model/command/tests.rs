use crate::configuration::downlink::{BackpressureMode, DownlinkParams, OnInvalidMessage};
use crate::downlink::model::command;
use crate::downlink::model::command::CommandStateMachine;
use crate::downlink::{BasicResponse, BasicStateMachine, Command};
use crate::router::RoutingError;
use common::model::schema::StandardSchema;
use common::model::Value;
use common::sink::item::ItemSender;
use form::ValidatedForm;
use tokio::sync::mpsc;
use tokio::time::Duration;

#[tokio::test]
async fn test_create_command_downlink() {
    let (tx, mut rx) = mpsc::channel(5);
    let params = DownlinkParams::new_buffered(
        BackpressureMode::Propagate,
        5,
        Duration::from_micros(100),
        5,
        OnInvalidMessage::Terminate,
        256,
    )
    .unwrap();

    command::create_downlink(
        StandardSchema::Anything,
        tx.map_err_into::<RoutingError>(),
        &params,
    );

    assert_eq!(rx.recv().await.unwrap(), Command::Sync);
}

#[test]
fn test_handle_action_valid() {
    let machine = CommandStateMachine::new(i32::schema());
    let response = machine.handle_action(&mut (), Value::Int32Value(3));
    assert_eq!(BasicResponse::of((), Value::Int32Value(3)), response);
}

#[test]
fn test_handle_action_invalid() {
    let machine = CommandStateMachine::new(String::schema());
    let response = machine.handle_action(&mut (), Value::Int32Value(3));

    assert_eq!(BasicResponse::none(), response);
}
