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

use tokio::sync::oneshot;

use super::*;
use crate::downlink::{DownlinkState, Operation, Response, StateMachine};
use swim_common::model::ValueKind;
use swim_common::request::Request;

const STATES: [DownlinkState; 3] = [
    DownlinkState::Unlinked,
    DownlinkState::Linked,
    DownlinkState::Synced,
];

#[test]
fn start_downlink() {
    for s in STATES.iter() {
        let mut state = *s;
        let mut model = ValueModel::new(Value::from(0));
        let machine = ValueStateMachine::unvalidated(Value::from(0));
        let maybe_response = machine.handle_operation(&mut state, &mut model, Operation::Start);

        assert!(maybe_response.is_ok());
        let response = maybe_response.unwrap();

        let Response {
            event,
            command,
            error,
            terminate,
        } = response;

        assert!(!terminate);
        assert!(error.is_none());
        assert!(event.is_none());

        assert_eq!(state, *s);

        match command {
            Some(cmd) => {
                assert_ne!(*s, DownlinkState::Synced);
                assert_eq!(cmd, Command::Sync);
            }
            _ => {
                assert_eq!(*s, DownlinkState::Synced);
            }
        }
    }
}

fn linked_response(start_state: DownlinkState) {
    let mut state = start_state;
    let mut model = ValueModel::new(Value::from(0));
    let machine = ValueStateMachine::unvalidated(Value::from(0));
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Message(Message::Linked));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Linked);
    assert_eq!(response, Response::none());
}

#[test]
fn linked_message() {
    for start in STATES.iter() {
        linked_response(*start);
    }
}

fn only_event(response: &Response<Arc<Value>, Arc<Value>>) -> &Arc<Value> {
    match response {
        Response {
            event: Some(Event::Remote(ev)),
            command: None,
            error: None,
            terminate: false,
        } => ev,
        _ => panic!("Response does not consist of just an event."),
    }
}

fn synced_response(start_state: DownlinkState) {
    let mut state = start_state;
    let mut model = ValueModel::new(Value::from(7));
    let machine = ValueStateMachine::unvalidated(Value::from(0));
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Message(Message::Synced));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    if start_state == DownlinkState::Synced {
        assert_eq!(response, Response::none());
    } else {
        let ev = only_event(&response);
        assert!(Arc::ptr_eq(ev, &model.state));
    }
}

#[test]
fn synced_message() {
    for start in STATES.iter() {
        synced_response(*start);
    }
}

fn unlinked_response(start_state: DownlinkState) {
    let mut state = start_state;
    let mut model = ValueModel::new(Value::from(7));
    let machine = ValueStateMachine::unvalidated(Value::from(0));
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Unlinked),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Unlinked);
    assert_eq!(response, Response::none().then_terminate());
}

#[test]
fn unlinked_message() {
    for start in STATES.iter() {
        unlinked_response(*start);
    }
}

#[test]
fn update_message_unlinked() {
    let mut state = DownlinkState::Unlinked;
    let mut model = ValueModel::new(Value::from(1));
    let machine = ValueStateMachine::unvalidated(Value::from(0));
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(Value::Int32Value(3))),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Unlinked);
    assert_eq!(model.state, Arc::new(Value::Int32Value(1)));
    assert_eq!(response, Response::none());
}

#[test]
fn update_message_linked() {
    let mut state = DownlinkState::Linked;
    let mut model = ValueModel::new(Value::from(1));
    let machine = ValueStateMachine::unvalidated(Value::from(0));
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(Value::Int32Value(3))),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Linked);
    assert_eq!(model.state, Arc::new(Value::Int32Value(3)));
    assert_eq!(response, Response::none());
}

#[test]
fn update_message_synced() {
    let mut state = DownlinkState::Synced;
    let mut model = ValueModel::new(Value::from(1));
    let machine = ValueStateMachine::unvalidated(Value::from(0));
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(Value::Int32Value(3))),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = Arc::new(Value::Int32Value(3));
    assert_eq!(&model.state, &expected);
    let ev = only_event(&response);
    assert!(Arc::ptr_eq(ev, &model.state));
}

fn make_get() -> (Action, oneshot::Receiver<Result<Arc<Value>, DownlinkError>>) {
    let (tx, rx) = oneshot::channel();
    (Action::get(Request::new(tx)), rx)
}

#[test]
fn get_action() {
    let mut state = DownlinkState::Synced;
    let mut model = ValueModel::new(Value::from(13));
    let machine = ValueStateMachine::unvalidated(Value::from(0));
    let (action, mut rx) = make_get();
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = Arc::new(Value::Int32Value(13));
    assert_eq!(&model.state, &expected);
    assert_eq!(response, Response::none());

    let result = rx.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_ok());
    let get_val = response.unwrap();
    assert!(Arc::ptr_eq(&get_val, &model.state));
}

#[test]
fn dropped_get() {
    let mut state = DownlinkState::Synced;
    let mut model = ValueModel::new(Value::from(13));
    let machine = ValueStateMachine::unvalidated(Value::from(0));
    let (action, rx) = make_get();
    drop(rx);
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = Arc::new(Value::Int32Value(13));
    assert_eq!(&model.state, &expected);
    assert_eq!(
        response,
        with_error(Response::none(), TransitionError::ReceiverDropped)
    );
}

fn with_error<Ev, Cmd>(mut response: Response<Ev, Cmd>, err: TransitionError) -> Response<Ev, Cmd> {
    response.error = Some(err);
    response
}

fn make_set(n: i32) -> (Action, oneshot::Receiver<Result<(), DownlinkError>>) {
    let (tx, rx) = oneshot::channel();
    (
        Action::set_and_await(Value::Int32Value(n), Request::new(tx)),
        rx,
    )
}

fn event_and_cmd(
    response: Response<Arc<Value>, Arc<Value>>,
) -> (Arc<Value>, Arc<Value>, Option<TransitionError>) {
    match response {
        Response {
            event: Some(Event::Local(ev)),
            command: Some(Command::Action(cmd)),
            error,
            terminate: false,
        } => (ev, cmd, error),
        _ => panic!("Response does not consist of and event and a command."),
    }
}

#[test]
fn set_action() {
    let mut state = DownlinkState::Synced;
    let mut model = ValueModel::new(Value::from(13));
    let machine = ValueStateMachine::unvalidated(Value::from(0));
    let (action, mut rx) = make_set(67);
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = Arc::new(Value::Int32Value(67));
    assert_eq!(&model.state, &expected);
    let (ev, cmd, err) = event_and_cmd(response);
    assert!(Arc::ptr_eq(&ev, &model.state));
    assert!(Arc::ptr_eq(&cmd, &model.state));
    assert!(err.is_none());

    assert!(rx.try_recv().is_ok());
}

#[test]
fn invalid_set_action() {
    let mut state = DownlinkState::Synced;
    let schema = StandardSchema::OfKind(ValueKind::Text);
    let mut model = ValueModel::new(Value::text(""));
    let machine = ValueStateMachine::new(Value::text(""), schema.clone());
    let (action, mut rx) = make_set(67);
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = Arc::new(Value::text(""));
    assert_eq!(&model.state, &expected);
    assert_eq!(response, Response::none());

    let expected_error = DownlinkError::SchemaViolation(Value::Int32Value(67), schema);
    assert_eq!(rx.try_recv(), Ok(Err(expected_error)));
}

#[test]
fn dropped_set_action() {
    let mut state = DownlinkState::Synced;
    let mut model = ValueModel::new(Value::from(13));
    let machine = ValueStateMachine::unvalidated(Value::from(0));
    let (action, rx) = make_set(67);

    drop(rx);

    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = Arc::new(Value::Int32Value(67));
    assert_eq!(&model.state, &expected);
    let (ev, cmd, err) = event_and_cmd(response);
    assert!(Arc::ptr_eq(&ev, &model.state));
    assert!(Arc::ptr_eq(&cmd, &model.state));
    assert!(err.is_some());
    assert_eq!(err.unwrap(), TransitionError::ReceiverDropped);
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
    let mut state = DownlinkState::Synced;
    let mut model = ValueModel::new(Value::from(13));
    let old = model.state.clone();
    let machine = ValueStateMachine::unvalidated(Value::from(0));
    let (action, mut rx) = make_update();
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = Arc::new(Value::Int32Value(26));
    assert_eq!(&model.state, &expected);

    let (ev, cmd, err) = event_and_cmd(response);
    assert!(Arc::ptr_eq(&ev, &model.state));
    assert!(Arc::ptr_eq(&cmd, &model.state));
    assert!(err.is_none());

    let result = rx.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_ok());
    let upd_val = response.unwrap();
    assert!(Arc::ptr_eq(&upd_val, &old));
}

#[test]
fn invalid_update_action() {
    let mut state = DownlinkState::Synced;
    let schema = StandardSchema::OfKind(ValueKind::Text);
    let mut model = ValueModel::new(Value::text(""));
    let machine = ValueStateMachine::new(Value::text(""), schema.clone());
    let (action, mut rx) = make_bad(7);
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = Arc::new(Value::text(""));
    assert_eq!(&model.state, &expected);

    assert_eq!(response, Response::none());

    let expected_err = DownlinkError::SchemaViolation(Value::Int32Value(7), schema);

    let result = rx.try_recv();
    assert_eq!(result, Ok(Err(expected_err)))
}

#[test]
fn dropped_update_action() {
    let mut state = DownlinkState::Synced;
    let mut model = ValueModel::new(Value::from(13));
    let machine = ValueStateMachine::unvalidated(Value::from(0));
    let (action, rx) = make_update();

    drop(rx);

    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = Arc::new(Value::Int32Value(26));
    assert_eq!(&model.state, &expected);

    let (ev, cmd, err) = event_and_cmd(response);
    assert!(Arc::ptr_eq(&ev, &model.state));
    assert!(Arc::ptr_eq(&cmd, &model.state));
    assert!(err.is_some());
    assert_eq!(err.unwrap(), TransitionError::ReceiverDropped);
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
    let mut state = DownlinkState::Synced;
    let mut model = ValueModel::new(Value::from(13));
    let old = model.state.clone();
    let machine = ValueStateMachine::unvalidated(Value::from(0));
    let (action, mut rx) = make_try_update();
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = Arc::new(Value::Int32Value(26));
    assert_eq!(&model.state, &expected);

    let (ev, cmd, err) = event_and_cmd(response);
    assert!(Arc::ptr_eq(&ev, &model.state));
    assert!(Arc::ptr_eq(&cmd, &model.state));
    assert!(err.is_none());

    let result = rx.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_ok());
    let upd_res = response.unwrap();
    assert!(upd_res.is_ok());
    let upd_val = upd_res.unwrap();
    assert!(Arc::ptr_eq(&upd_val, &old));
}

#[test]
fn unsuccessful_try_update_action() {
    let mut state = DownlinkState::Synced;
    let mut model = ValueModel::new(Value::from(14));
    let old = model.state.clone();
    let machine = ValueStateMachine::unvalidated(Value::from(0));
    let (action, mut rx) = make_try_update();
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);

    assert!(Arc::ptr_eq(&model.state, &old));

    assert_eq!(response, Response::none());

    let result = rx.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_ok());
    let upd_res = response.unwrap();
    assert!(upd_res.is_err());
}

#[test]
fn invalid_try_update_action() {
    let mut state = DownlinkState::Synced;
    let schema = StandardSchema::OfKind(ValueKind::Text);
    let mut model = ValueModel::new(Value::text(""));
    let machine = ValueStateMachine::new(Value::text(""), schema.clone());
    let (action, mut rx) = make_try_bad(7);
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = Arc::new(Value::text(""));
    assert_eq!(&model.state, &expected);

    assert_eq!(response, Response::none());

    let expected_err = DownlinkError::SchemaViolation(Value::Int32Value(7), schema);

    let result = rx.try_recv();
    assert_eq!(result, Ok(Err(expected_err)))
}

#[test]
fn dropped_try_update_action() {
    let mut state = DownlinkState::Synced;
    let mut model = ValueModel::new(Value::from(13));
    let machine = ValueStateMachine::unvalidated(Value::from(0));
    let (action, rx) = make_try_update();

    drop(rx);

    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = Arc::new(Value::Int32Value(26));
    assert_eq!(&model.state, &expected);

    let (ev, cmd, err) = event_and_cmd(response);
    assert!(Arc::ptr_eq(&ev, &model.state));
    assert!(Arc::ptr_eq(&cmd, &model.state));
    assert!(err.is_some());
    assert_eq!(err.unwrap(), TransitionError::ReceiverDropped);
}

#[test]
fn invalid_message_unlinked() {
    let mut state = DownlinkState::Unlinked;
    let mut model = ValueModel::new(Value::from(1));

    let schema = StandardSchema::OfKind(ValueKind::Extant).negate();
    let machine = ValueStateMachine::new(Value::from(0), schema);
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(Value::Extant)),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Unlinked);
    assert_eq!(model.state, Arc::new(Value::Int32Value(1)));
    assert_eq!(response, Response::none());
}

#[test]
fn invalid_message_linked() {
    let mut state = DownlinkState::Linked;
    let mut model = ValueModel::new(Value::from(1));

    let schema = StandardSchema::OfKind(ValueKind::Int32);
    let machine = ValueStateMachine::new(Value::from(0), schema.clone());
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(Value::Text(String::from("foo")))),
    );

    assert!(maybe_response.is_err());
    let error = maybe_response.err().unwrap();

    assert_eq!(state, DownlinkState::Linked);
    assert_eq!(model.state, Arc::new(Value::Int32Value(1)));
    assert_eq!(
        error,
        DownlinkError::SchemaViolation(Value::Text(String::from("foo")), schema)
    );
}

#[test]
fn extant_message_linked() {
    let mut state = DownlinkState::Linked;
    let mut model = ValueModel::new(Value::from(1));

    let schema = StandardSchema::OfKind(ValueKind::Int32);
    let machine = ValueStateMachine::new(Value::from(0), schema.clone());
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(Value::Extant)),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Linked);
    assert_eq!(model.state, Arc::new(Value::Int32Value(1)));
    assert_eq!(response, Response::none());
}

#[test]
fn invalid_message_synced() {
    let mut state = DownlinkState::Synced;
    let mut model = ValueModel::new(Value::from(1));

    let schema = StandardSchema::OfKind(ValueKind::Extant).negate();
    let machine = ValueStateMachine::new(Value::from(0), schema.clone());
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(Value::Extant)),
    );

    assert!(maybe_response.is_err());
    let error = maybe_response.err().unwrap();

    assert_eq!(state, DownlinkState::Synced);
    assert_eq!(model.state, Arc::new(Value::Int32Value(1)));
    assert_eq!(error, DownlinkError::SchemaViolation(Value::Extant, schema));
}
