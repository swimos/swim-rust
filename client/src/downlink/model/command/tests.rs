use crate::configuration::downlink::{BackpressureMode, DownlinkParams, OnInvalidMessage};
use crate::downlink::model::command;
use crate::downlink::model::command::CommandStateMachine;
use crate::downlink::model::map::TypedMapModification;
use crate::downlink::{BasicResponse, BasicStateMachine, Command};
use crate::router::RoutingError;
use common::model::schema::StandardSchema;
use common::model::{Attr, Item, Value};
use common::sink::item::ItemSender;
use form::{Form, ValidatedForm};
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

    assert_eq!(Command::Sync, rx.recv().await.unwrap());
}

#[test]
fn test_handle_value_action_valid() {
    let action = 3.into_value();
    let machine = CommandStateMachine::new(i32::schema());
    let response = machine.handle_action(&mut (), action);
    assert_eq!(response, BasicResponse::of((), Value::Int32Value(3)));
}

#[test]
fn test_handle_value_action_invalid() {
    let action = 3.into_value();
    let machine = CommandStateMachine::new(String::schema());
    let response = machine.handle_action(&mut (), action);
    assert_eq!(response, BasicResponse::none());
}

#[test]
fn test_handle_map_action_valid() {
    let action = TypedMapModification::Insert("Foo".to_string(), 3).into_value();
    let machine = CommandStateMachine::new(TypedMapModification::<String, i32>::schema());
    let response = machine.handle_action(&mut (), action);

    let header = Attr::of(("update", Value::record(vec![Item::slot("key", "Foo")])));
    let body = Item::of(3);
    let expected = Value::Record(vec![header], vec![body]);

    assert_eq!(response, BasicResponse::of((), expected));
}

#[test]
fn test_handle_map_action_invalid_key() {
    let action = TypedMapModification::Insert("Foo".to_string(), 3).into_value();
    let machine = CommandStateMachine::new(TypedMapModification::<i32, i32>::schema());
    let response = machine.handle_action(&mut (), action);
    assert_eq!(response, BasicResponse::none());
}

#[test]
fn test_handle_map_action_invalid_value() {
    let action = TypedMapModification::Insert("Foo".to_string(), 3).into_value();
    let machine = CommandStateMachine::new(TypedMapModification::<String, String>::schema());
    let response = machine.handle_action(&mut (), action);
    assert_eq!(response, BasicResponse::none());
}
