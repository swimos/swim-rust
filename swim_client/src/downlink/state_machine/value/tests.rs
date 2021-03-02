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

use tokio::sync::oneshot;

use super::*;
use crate::downlink::error::UpdateFailure;
use crate::downlink::state_machine::{DownlinkStateMachine, EventResult};
use crate::downlink::{Command, DownlinkState, Message};
use std::sync::Arc;
use swim_common::model::ValueKind;
use swim_common::request::Request;

const STATES: [DownlinkState; 3] = [
    DownlinkState::Unlinked,
    DownlinkState::Linked,
    DownlinkState::Synced,
];

fn unvalidated(init: Value) -> ValueStateMachine {
    ValueStateMachine::new(init, StandardSchema::Anything)
}

#[test]
fn init_downlink() {
    let machine = unvalidated(Value::from(0));
    let ((dl_state, state), start) = machine.initialize();
    assert_eq!(dl_state, DownlinkState::Unlinked);
    assert_eq!(&*state, &Value::from(0));
    assert_eq!(start, Some(Command::Sync));
}

fn linked_response(start_state: DownlinkState) {
    let mut state = (start_state, SharedValue::new(Value::from(0)));

    let machine = unvalidated(Value::from(0));
    let EventResult { result, terminate } = machine.handle_event(&mut state, Message::Linked);

    assert!(result.is_ok());
    assert!(!terminate);
    let response = result.unwrap();

    assert_eq!(state.0, DownlinkState::Linked);
    assert!(response.is_none());
}

#[test]
fn linked_message() {
    for start in STATES.iter() {
        linked_response(*start);
    }
}

fn synced_response(start_state: DownlinkState) {
    let mut state = (start_state, SharedValue::new(Value::from(2)));

    let machine = unvalidated(Value::from(0));
    let EventResult { result, terminate } = machine.handle_event(&mut state, Message::Synced);

    let (dl_state, data_state) = state;

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(&*data_state, &Value::from(2));
    assert!(!terminate);
    if start_state == DownlinkState::Synced {
        assert!(matches!(result, Ok(None)));
    } else {
        assert!(matches!(result, Ok(Some(v)) if *v == Value::from(2)));
    }
}

#[test]
fn synced_message() {
    for start in STATES.iter() {
        synced_response(*start);
    }
}

fn unlinked_response(start_state: DownlinkState) {
    let mut state = (start_state, SharedValue::new(Value::from(2)));

    let machine = unvalidated(Value::from(0));
    let EventResult { result, terminate } = machine.handle_event(&mut state, Message::Unlinked);

    let (dl_state, _data_state) = state;

    assert!(matches!(result, Ok(None)));
    assert!(terminate);
    assert_eq!(dl_state, DownlinkState::Unlinked);
}

#[test]
fn unlinked_message() {
    for start in STATES.iter() {
        unlinked_response(*start);
    }
}

#[test]
fn update_message_unlinked() {
    let mut state = (DownlinkState::Unlinked, SharedValue::new(Value::from(1)));

    let machine = unvalidated(Value::from(0));
    let EventResult { result, terminate } =
        machine.handle_event(&mut state, Message::Action(Value::Int32Value(3)));

    let (dl_state, data_state) = state;

    assert!(matches!(result, Ok(None)));
    assert!(!terminate);
    assert_eq!(dl_state, DownlinkState::Unlinked);
    assert_eq!(&*data_state, &Value::from(1));
}

#[test]
fn update_message_linked() {
    let mut state = (DownlinkState::Linked, SharedValue::new(Value::from(1)));

    let machine = unvalidated(Value::from(0));

    let EventResult { result, terminate } =
        machine.handle_event(&mut state, Message::Action(Value::Int32Value(3)));

    let (dl_state, data_state) = state;

    assert!(matches!(result, Ok(None)));
    assert!(!terminate);
    assert_eq!(dl_state, DownlinkState::Linked);
    assert_eq!(&*data_state, &Value::from(3));
}

#[test]
fn update_message_synced() {
    let mut state = (DownlinkState::Synced, SharedValue::new(Value::from(1)));

    let machine = unvalidated(Value::from(0));

    let EventResult { result, terminate } =
        machine.handle_event(&mut state, Message::Action(Value::Int32Value(3)));

    let (dl_state, data_state) = state;

    assert!(matches!(result, Ok(Some(v)) if *v == Value::from(3)));
    assert!(!terminate);
    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(&*data_state, &Value::from(3));
}

fn make_get() -> (Action, oneshot::Receiver<Result<Arc<Value>, DownlinkError>>) {
    let (tx, rx) = oneshot::channel();
    (Action::get(Request::new(tx)), rx)
}

#[test]
fn get_action() {
    let mut state = (DownlinkState::Synced, SharedValue::new(Value::from(13)));

    let machine = unvalidated(Value::from(0));
    let (action, mut rx) = make_get();

    let result = machine.handle_request(&mut state, action);

    let (dl_state, data_state) = state;

    assert!(result.is_ok());
    let response = result.unwrap();

    assert_eq!(
        response,
        Response {
            event: None,
            command: None
        }
    );

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(*data_state, Value::from(13));

    let result = rx.try_recv();

    assert!(matches!(result, Ok(Ok(v)) if Arc::ptr_eq(&v, &data_state)));
}

#[test]
fn dropped_get() {
    let mut state = (DownlinkState::Synced, SharedValue::new(Value::from(13)));

    let machine = unvalidated(Value::from(0));
    let (action, _) = make_get();

    let result = machine.handle_request(&mut state, action);

    let (dl_state, data_state) = state;

    assert!(result.is_ok());
    let response = result.unwrap();

    assert_eq!(
        response,
        Response {
            event: None,
            command: None
        }
    );

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(*data_state, Value::from(13));
}

fn make_set(n: i32) -> (Action, oneshot::Receiver<Result<(), DownlinkError>>) {
    let (tx, rx) = oneshot::channel();
    (
        Action::set_and_await(Value::Int32Value(n), Request::new(tx)),
        rx,
    )
}

#[test]
fn set_action() {
    let mut state = (DownlinkState::Synced, SharedValue::new(Value::from(13)));

    let machine = unvalidated(Value::from(0));
    let (action, mut rx) = make_set(67);

    let result = machine.handle_request(&mut state, action);

    let (dl_state, data_state) = state;

    assert!(result.is_ok());
    let response = result.unwrap();

    assert!(
        matches!(response, Response{ event: Some(ev), command: Some(Command::Action(cmd)) } if *ev == *data_state && *cmd == *data_state)
    );

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(*data_state, Value::from(67));

    let result = rx.try_recv();

    assert!(matches!(result, Ok(Ok(_))));
}

#[test]
fn invalid_set_action() {
    let mut state = (DownlinkState::Synced, SharedValue::new(Value::text("")));

    let machine = ValueStateMachine::new(Value::text(""), StandardSchema::OfKind(ValueKind::Text));
    let (action, mut rx) = make_set(67);

    let result = machine.handle_request(&mut state, action);

    let (dl_state, data_state) = state;

    assert!(result.is_ok());
    let response = result.unwrap();

    assert_eq!(
        response,
        Response {
            event: None,
            command: None
        }
    );

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(*data_state, Value::from(""));

    let result = rx.try_recv();

    assert!(matches!(
        result,
        Ok(Err(DownlinkError::SchemaViolation(
            Value::Int32Value(67),
            StandardSchema::OfKind(ValueKind::Text)
        )))
    ));
}

#[test]
fn dropped_set_action() {
    let mut state = (DownlinkState::Synced, SharedValue::new(Value::from(13)));

    let machine = unvalidated(Value::from(0));
    let (action, _) = make_set(67);

    let result = machine.handle_request(&mut state, action);

    let (dl_state, data_state) = state;

    assert!(result.is_ok());
    let response = result.unwrap();

    assert!(
        matches!(response, Response{ event: Some(ev), command: Some(Command::Action(cmd)) } if *ev == *data_state && *cmd == *data_state)
    );

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(*data_state, Value::from(67));
}

fn make_update() -> (Action, oneshot::Receiver<Result<Arc<Value>, DownlinkError>>) {
    let (tx, rx) = oneshot::channel();
    (
        Action::update_and_await(
            |v| match v {
                Value::Int32Value(n) => Value::Int32Value(n * 2),
                ow => ow.clone(),
            },
            Request::new(tx),
        ),
        rx,
    )
}

fn make_bad(n: i32) -> (Action, oneshot::Receiver<Result<Arc<Value>, DownlinkError>>) {
    let (tx, rx) = oneshot::channel();
    (
        Action::update_and_await(move |_| Value::Int32Value(n), Request::new(tx)),
        rx,
    )
}

#[test]
fn update_action() {
    let mut state = (DownlinkState::Synced, SharedValue::new(Value::from(13)));

    let machine = unvalidated(Value::from(0));
    let (action, mut rx) = make_update();

    let old = state.1.clone();

    let result = machine.handle_request(&mut state, action);

    let (dl_state, data_state) = state;

    assert!(result.is_ok());
    let response = result.unwrap();

    assert!(
        matches!(response, Response{ event: Some(ev), command: Some(Command::Action(cmd)) } if *ev == *data_state && *cmd == *data_state)
    );

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(*data_state, Value::from(26));

    let result = rx.try_recv();

    assert!(matches!(result, Ok(Ok(v)) if *v == *old));
}

#[test]
fn invalid_update_action() {
    let mut state = (DownlinkState::Synced, SharedValue::new(Value::text("")));

    let machine = ValueStateMachine::new(Value::text(""), StandardSchema::OfKind(ValueKind::Text));
    let (action, mut rx) = make_bad(67);

    let result = machine.handle_request(&mut state, action);

    let (dl_state, data_state) = state;

    assert!(result.is_ok());
    let response = result.unwrap();

    assert_eq!(
        response,
        Response {
            event: None,
            command: None
        }
    );

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(*data_state, Value::from(""));

    let result = rx.try_recv();

    assert!(matches!(
        result,
        Ok(Err(DownlinkError::SchemaViolation(
            Value::Int32Value(67),
            StandardSchema::OfKind(ValueKind::Text)
        )))
    ));
}

#[test]
fn dropped_update_action() {
    let mut state = (DownlinkState::Synced, SharedValue::new(Value::from(13)));

    let machine = unvalidated(Value::from(0));
    let (action, _) = make_update();

    let result = machine.handle_request(&mut state, action);

    let (dl_state, data_state) = state;

    assert!(result.is_ok());
    let response = result.unwrap();

    assert!(
        matches!(response, Response{ event: Some(ev), command: Some(Command::Action(cmd)) } if *ev == *data_state && *cmd == *data_state)
    );

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(*data_state, Value::from(26));
}

fn make_try_update() -> (
    Action,
    oneshot::Receiver<Result<UpdateResult<Arc<Value>>, DownlinkError>>,
) {
    let (tx, rx) = oneshot::channel();
    (
        Action::try_update_and_await(
            |v| match v {
                Value::Int32Value(n) if n % 2 == 1 => Ok(Value::Int32Value(n * 2)),
                _ => Err(UpdateFailure("Failed".to_string())),
            },
            Request::new(tx),
        ),
        rx,
    )
}

fn make_try_bad(
    n: i32,
) -> (
    Action,
    oneshot::Receiver<Result<UpdateResult<Arc<Value>>, DownlinkError>>,
) {
    let (tx, rx) = oneshot::channel();
    (
        Action::try_update_and_await(move |_| Ok(Value::Int32Value(n)), Request::new(tx)),
        rx,
    )
}

#[test]
fn successful_try_update_action() {
    let mut state = (DownlinkState::Synced, SharedValue::new(Value::from(13)));

    let machine = unvalidated(Value::from(0));
    let (action, mut rx) = make_try_update();

    let old = state.1.clone();

    let result = machine.handle_request(&mut state, action);

    let (dl_state, data_state) = state;
    assert!(result.is_ok());
    let response = result.unwrap();

    assert!(
        matches!(response, Response{ event: Some(ev), command: Some(Command::Action(cmd)) } if *ev == *data_state && *cmd == *data_state)
    );

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(*data_state, Value::from(26));

    let result = rx.try_recv();

    assert!(matches!(result, Ok(Ok(Ok(v))) if *v == *old));
}

#[test]
fn unsuccessful_try_update_action() {
    let mut state = (DownlinkState::Synced, SharedValue::new(Value::from(14)));

    let machine = unvalidated(Value::from(0));
    let (action, mut rx) = make_try_update();

    let result = machine.handle_request(&mut state, action);

    let (dl_state, data_state) = state;
    assert!(result.is_ok());
    let response = result.unwrap();

    assert_eq!(
        response,
        Response {
            event: None,
            command: None
        }
    );

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(*data_state, Value::from(14));

    let result = rx.try_recv();

    assert!(matches!(result, Ok(Ok(Err(_)))));
}

#[test]
fn invalid_try_update_action() {
    let mut state = (DownlinkState::Synced, SharedValue::new(Value::text("")));

    let machine = ValueStateMachine::new(Value::text(""), StandardSchema::OfKind(ValueKind::Text));
    let (action, mut rx) = make_try_bad(7);

    let result = machine.handle_request(&mut state, action);

    let (dl_state, data_state) = state;

    assert!(result.is_ok());
    let response = result.unwrap();

    assert_eq!(
        response,
        Response {
            event: None,
            command: None
        }
    );

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(*data_state, Value::from(""));

    let result = rx.try_recv();

    assert!(matches!(
        result,
        Ok(Err(DownlinkError::SchemaViolation(
            Value::Int32Value(7),
            StandardSchema::OfKind(ValueKind::Text)
        )))
    ));
}

#[test]
fn dropped_try_update_action() {
    let mut state = (DownlinkState::Synced, SharedValue::new(Value::from(13)));

    let machine = unvalidated(Value::from(0));
    let (action, _) = make_try_update();

    let result = machine.handle_request(&mut state, action);

    let (dl_state, data_state) = state;
    assert!(result.is_ok());
    let response = result.unwrap();

    assert!(
        matches!(response, Response{ event: Some(ev), command: Some(Command::Action(cmd)) } if *ev == *data_state && *cmd == *data_state)
    );

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(*data_state, Value::from(26));
}

#[test]
fn invalid_message_unlinked() {
    let mut state = (DownlinkState::Unlinked, SharedValue::new(Value::from(1)));

    let schema = StandardSchema::OfKind(ValueKind::Int32);
    let machine = ValueStateMachine::new(Value::from(0), schema);

    let EventResult { result, terminate } =
        machine.handle_event(&mut state, Message::Action(Value::from("a")));

    let (dl_state, data_state) = state;

    assert!(!terminate);
    assert!(matches!(result, Ok(None)));

    assert_eq!(dl_state, DownlinkState::Unlinked);
    assert_eq!(*data_state, Value::from(1));
}

#[test]
fn invalid_message_linked() {
    let mut state = (DownlinkState::Linked, SharedValue::new(Value::from(1)));

    let schema = StandardSchema::OfKind(ValueKind::Int32);
    let machine = ValueStateMachine::new(Value::from(0), schema);

    let EventResult { result, terminate } =
        machine.handle_event(&mut state, Message::Action(Value::from("a")));

    assert!(
        matches!(result, Err(DownlinkError::SchemaViolation(Value::Text(t), StandardSchema::OfKind(ValueKind::Int32))) if t == "a")
    );
    assert!(terminate);
}

#[test]
fn extant_message_linked() {
    let mut state = (DownlinkState::Linked, SharedValue::new(Value::from(1)));

    let schema = StandardSchema::OfKind(ValueKind::Int32);
    let machine = ValueStateMachine::new(Value::from(0), schema);

    let EventResult { result, terminate } =
        machine.handle_event(&mut state, Message::Action(Value::Extant));

    assert_eq!(result, Ok(None));
    assert!(!terminate);
}

#[test]
fn invalid_message_synced() {
    let mut state = (DownlinkState::Synced, SharedValue::new(Value::from(1)));

    let schema = StandardSchema::OfKind(ValueKind::Int32);
    let machine = ValueStateMachine::new(Value::from(0), schema);

    let EventResult { result, terminate } =
        machine.handle_event(&mut state, Message::Action(Value::from("a")));

    assert!(
        matches!(result, Err(DownlinkError::SchemaViolation(Value::Text(t), StandardSchema::OfKind(ValueKind::Int32))) if t == "a")
    );
    assert!(terminate);
}
