use crate::downlink::model::event::EventStateMachine;
use crate::downlink::model::map::MapModification;
use crate::downlink::Event;
use crate::downlink::{Command, DownlinkState, Message, Operation, Response, StateMachine};
use common::model::schema::StandardSchema;
use common::model::{Attr, Item, Value};
use form::{Form, ValidatedForm};

#[test]
fn test_handle_start_event() {
    let machine = EventStateMachine::new(StandardSchema::Anything);

    let response = machine
        .handle_operation(&mut DownlinkState::Unlinked, &mut (), Operation::Start)
        .unwrap();

    assert_eq!(response, Response::for_command(Command::Link));
}

#[test]
fn test_handle_start_event_already_running() {
    let machine = EventStateMachine::new(StandardSchema::Anything);

    let response = machine
        .handle_operation(&mut DownlinkState::Linked, &mut (), Operation::Start)
        .unwrap();

    assert_eq!(response, Response::none());
}

#[test]
fn test_handle_unlinked_event() {
    let machine = EventStateMachine::new(StandardSchema::Anything);

    let response = machine
        .handle_operation(
            &mut DownlinkState::Linked,
            &mut (),
            Operation::Message(Message::Unlinked),
        )
        .unwrap();

    assert_eq!(response, Response::none().then_terminate());
}

#[test]
fn test_handle_error_event() {
    let machine = EventStateMachine::new(StandardSchema::Anything);

    let response = machine.handle_operation(
        &mut DownlinkState::Linked,
        &mut (),
        Operation::Message(Message::BadEnvelope("Bad".to_string())),
    );

    assert!(response.is_err());
}

#[test]
fn test_handle_invalid_event() {
    let machine = EventStateMachine::new(StandardSchema::Anything);

    let response = machine
        .handle_operation(
            &mut DownlinkState::Linked,
            &mut (),
            Operation::Action(5.into_value()),
        )
        .unwrap();

    assert_eq!(response, Response::none());
}

#[test]
fn test_handle_invalid_synced_event() {
    let machine = EventStateMachine::new(StandardSchema::Anything);

    let response = machine
        .handle_operation(
            &mut DownlinkState::Linked,
            &mut (),
            Operation::Message(Message::Synced),
        )
        .unwrap();

    assert_eq!(response, Response::none());
}

#[test]
fn test_handle_recv_value_event_valid() {
    let machine = EventStateMachine::new(String::schema());

    let response = machine
        .handle_operation(
            &mut DownlinkState::Unlinked,
            &mut (),
            Operation::Message(Message::Action("Test".to_string().into_value())),
        )
        .unwrap();

    assert_eq!(
        response,
        Response::for_event(Event::Remote("Test".to_string().into_value()))
    );
}

#[test]
fn test_handle_recv_value_event_invalid() {
    let machine = EventStateMachine::new(i32::schema());

    let response = machine
        .handle_operation(
            &mut DownlinkState::Unlinked,
            &mut (),
            Operation::Message(Message::Action("Test".to_string().into_value())),
        )
        .unwrap();

    assert_eq!(response, Response::none());
}

#[test]
fn test_handle_recv_map_event_valid() {
    let machine = EventStateMachine::new(MapModification::<i32, String>::schema());

    let header = Attr::of(("update", Value::record(vec![Item::slot("key", 3)])));
    let body = Item::of("Test");
    let value = Value::Record(vec![header], vec![body]);

    let response = machine
        .handle_operation(
            &mut DownlinkState::Unlinked,
            &mut (),
            Operation::Message(Message::Action(value.clone())),
        )
        .unwrap();

    assert_eq!(response, Response::for_event(Event::Remote(value)));
}

#[test]
fn test_handle_recv_map_event_invalid_key() {
    let machine = EventStateMachine::new(MapModification::<String, String>::schema());

    let header = Attr::of(("update", Value::record(vec![Item::slot("key", 3)])));
    let body = Item::of("Test");
    let value = Value::Record(vec![header], vec![body]);

    let response = machine
        .handle_operation(
            &mut DownlinkState::Unlinked,
            &mut (),
            Operation::Message(Message::Action(value.clone())),
        )
        .unwrap();

    assert_eq!(response, Response::none());
}

#[test]
fn test_handle_recv_map_event_invalid_value() {
    let machine = EventStateMachine::new(MapModification::<i32, i32>::schema());

    let header = Attr::of(("update", Value::record(vec![Item::slot("key", 3)])));
    let body = Item::of("Test");
    let value = Value::Record(vec![header], vec![body]);

    let response = machine
        .handle_operation(
            &mut DownlinkState::Unlinked,
            &mut (),
            Operation::Message(Message::Action(value.clone())),
        )
        .unwrap();

    assert_eq!(response, Response::none());
}
