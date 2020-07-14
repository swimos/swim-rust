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

use hamcrest2::assert_that;
use hamcrest2::prelude::*;
use tokio::sync::oneshot;

use super::*;
use crate::downlink::{DownlinkState, Operation, Response, StateMachine};
use common::model::ValueKind;
use common::request::Request;

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

        assert_that!(&maybe_response, ok());
        let response = maybe_response.unwrap();

        let Response {
            event,
            command,
            error,
            terminate,
        } = response;

        assert!(!terminate);
        assert_that!(error, none());
        assert_that!(event, none());

        assert_that!(state, eq(*s));

        match command {
            Some(cmd) => {
                assert_that!(*s, not(eq(DownlinkState::Synced)));
                assert_that!(cmd, eq(Command::Sync));
            }
            _ => {
                assert_that!(*s, eq(DownlinkState::Synced));
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

    assert_that!(&maybe_response, ok());
    let response = maybe_response.unwrap();

    assert_that!(state, eq(DownlinkState::Linked));
    assert_that!(response, eq(Response::none()));
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

    assert_that!(&maybe_response, ok());
    let response = maybe_response.unwrap();

    assert_that!(state, eq(DownlinkState::Synced));
    if start_state == DownlinkState::Synced {
        assert_that!(response, eq(Response::none()));
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

    assert_that!(&maybe_response, ok());
    let response = maybe_response.unwrap();

    assert_that!(state, eq(DownlinkState::Unlinked));
    assert_that!(response, eq(Response::none().then_terminate()));
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

    assert_that!(&maybe_response, ok());
    let response = maybe_response.unwrap();

    assert_that!(state, eq(DownlinkState::Unlinked));
    assert_that!(model.state, eq(Arc::new(Value::Int32Value(1))));
    assert_that!(response, eq(Response::none()));
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

    assert_that!(&maybe_response, ok());
    let response = maybe_response.unwrap();

    assert_that!(state, eq(DownlinkState::Linked));
    assert_that!(model.state, eq(Arc::new(Value::Int32Value(3))));
    assert_that!(response, eq(Response::none()));
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

    assert_that!(&maybe_response, ok());
    let response = maybe_response.unwrap();

    assert_that!(state, eq(DownlinkState::Synced));
    let expected = Arc::new(Value::Int32Value(3));
    assert_that!(&model.state, eq(&expected));
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

    assert_that!(&maybe_response, ok());
    let response = maybe_response.unwrap();

    assert_that!(state, eq(DownlinkState::Synced));
    let expected = Arc::new(Value::Int32Value(13));
    assert_that!(&model.state, eq(&expected));
    assert_that!(response, eq(Response::none()));

    let result = rx.try_recv();
    assert_that!(&result, ok());
    let response = result.unwrap();
    assert_that!(&response, ok());
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

    assert_that!(&maybe_response, ok());
    let response = maybe_response.unwrap();

    assert_that!(state, eq(DownlinkState::Synced));
    let expected = Arc::new(Value::Int32Value(13));
    assert_that!(&model.state, eq(&expected));
    assert_that!(
        response,
        eq(with_error(
            Response::none(),
            TransitionError::ReceiverDropped
        ))
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

    assert_that!(&maybe_response, ok());
    let response = maybe_response.unwrap();

    assert_that!(state, eq(DownlinkState::Synced));
    let expected = Arc::new(Value::Int32Value(67));
    assert_that!(&model.state, eq(&expected));
    let (ev, cmd, err) = event_and_cmd(response);
    assert!(Arc::ptr_eq(&ev, &model.state));
    assert!(Arc::ptr_eq(&cmd, &model.state));
    assert_that!(err, none());

    assert_that!(rx.try_recv(), ok());
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

    assert_that!(&maybe_response, ok());
    let response = maybe_response.unwrap();

    assert_that!(state, eq(DownlinkState::Synced));
    let expected = Arc::new(Value::text(""));
    assert_that!(&model.state, eq(&expected));
    assert_that!(response, eq(Response::none()));

    let expected_error = DownlinkError::SchemaViolation(Value::Int32Value(67), schema);
    assert_that!(rx.try_recv(), eq(Ok(Err(expected_error))));
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

    assert_that!(&maybe_response, ok());
    let response = maybe_response.unwrap();

    assert_that!(state, eq(DownlinkState::Synced));
    let expected = Arc::new(Value::Int32Value(67));
    assert_that!(&model.state, eq(&expected));
    let (ev, cmd, err) = event_and_cmd(response);
    assert!(Arc::ptr_eq(&ev, &model.state));
    assert!(Arc::ptr_eq(&cmd, &model.state));
    assert_that!(&err, some());
    assert_that!(err.unwrap(), eq(TransitionError::ReceiverDropped));
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

    assert_that!(&maybe_response, ok());
    let response = maybe_response.unwrap();

    assert_that!(state, eq(DownlinkState::Synced));
    let expected = Arc::new(Value::Int32Value(26));
    assert_that!(&model.state, eq(&expected));

    let (ev, cmd, err) = event_and_cmd(response);
    assert!(Arc::ptr_eq(&ev, &model.state));
    assert!(Arc::ptr_eq(&cmd, &model.state));
    assert_that!(err, none());

    let result = rx.try_recv();
    assert_that!(&result, ok());
    let response = result.unwrap();
    assert_that!(&response, ok());
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

    assert_that!(&maybe_response, ok());
    let response = maybe_response.unwrap();

    assert_that!(state, eq(DownlinkState::Synced));
    let expected = Arc::new(Value::text(""));
    assert_that!(&model.state, eq(&expected));

    assert_that!(response, eq(Response::none()));

    let expected_err = DownlinkError::SchemaViolation(Value::Int32Value(7), schema);

    let result = rx.try_recv();
    assert_that!(result, eq(Ok(Err(expected_err))))
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

    assert_that!(&maybe_response, ok());
    let response = maybe_response.unwrap();

    assert_that!(state, eq(DownlinkState::Synced));
    let expected = Arc::new(Value::Int32Value(26));
    assert_that!(&model.state, eq(&expected));

    let (ev, cmd, err) = event_and_cmd(response);
    assert!(Arc::ptr_eq(&ev, &model.state));
    assert!(Arc::ptr_eq(&cmd, &model.state));
    assert_that!(&err, some());
    assert_that!(err.unwrap(), eq(TransitionError::ReceiverDropped));
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

    assert_that!(&maybe_response, ok());
    let response = maybe_response.unwrap();

    assert_that!(state, eq(DownlinkState::Synced));
    let expected = Arc::new(Value::Int32Value(26));
    assert_that!(&model.state, eq(&expected));

    let (ev, cmd, err) = event_and_cmd(response);
    assert!(Arc::ptr_eq(&ev, &model.state));
    assert!(Arc::ptr_eq(&cmd, &model.state));
    assert_that!(err, none());

    let result = rx.try_recv();
    assert_that!(&result, ok());
    let response = result.unwrap();
    assert_that!(&response, ok());
    let upd_res = response.unwrap();
    assert_that!(&upd_res, ok());
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

    assert_that!(&maybe_response, ok());
    let response = maybe_response.unwrap();

    assert_that!(state, eq(DownlinkState::Synced));

    assert!(Arc::ptr_eq(&model.state, &old));

    assert_that!(response, eq(Response::none()));

    let result = rx.try_recv();
    assert_that!(&result, ok());
    let response = result.unwrap();
    assert_that!(&response, ok());
    let upd_res = response.unwrap();
    assert_that!(&upd_res, err());
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

    assert_that!(&maybe_response, ok());
    let response = maybe_response.unwrap();

    assert_that!(state, eq(DownlinkState::Synced));
    let expected = Arc::new(Value::text(""));
    assert_that!(&model.state, eq(&expected));

    assert_that!(response, eq(Response::none()));

    let expected_err = DownlinkError::SchemaViolation(Value::Int32Value(7), schema);

    let result = rx.try_recv();
    assert_that!(result, eq(Ok(Err(expected_err))))
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

    assert_that!(&maybe_response, ok());
    let response = maybe_response.unwrap();

    assert_that!(state, eq(DownlinkState::Synced));
    let expected = Arc::new(Value::Int32Value(26));
    assert_that!(&model.state, eq(&expected));

    let (ev, cmd, err) = event_and_cmd(response);
    assert!(Arc::ptr_eq(&ev, &model.state));
    assert!(Arc::ptr_eq(&cmd, &model.state));
    assert_that!(&err, some());
    assert_that!(err.unwrap(), eq(TransitionError::ReceiverDropped));
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

    assert_that!(&maybe_response, ok());
    let response = maybe_response.unwrap();

    assert_that!(state, eq(DownlinkState::Unlinked));
    assert_that!(model.state, eq(Arc::new(Value::Int32Value(1))));
    assert_that!(response, eq(Response::none()));
}

#[test]
fn invalid_message_linked() {
    let mut state = DownlinkState::Linked;
    let mut model = ValueModel::new(Value::from(1));

    let schema = StandardSchema::OfKind(ValueKind::Extant).negate();

    let machine = ValueStateMachine::new(Value::from(0), schema.clone());
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(Value::Extant)),
    );

    assert_that!(&maybe_response, err());
    let error = maybe_response.err().unwrap();

    assert_that!(state, eq(DownlinkState::Linked));
    assert_that!(model.state, eq(Arc::new(Value::Int32Value(1))));
    assert_that!(
        error,
        eq(DownlinkError::SchemaViolation(Value::Extant, schema))
    );
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

    assert_that!(&maybe_response, err());
    let error = maybe_response.err().unwrap();

    assert_that!(state, eq(DownlinkState::Synced));
    assert_that!(model.state, eq(Arc::new(Value::Int32Value(1))));
    assert_that!(
        error,
        eq(DownlinkError::SchemaViolation(Value::Extant, schema))
    );
}
