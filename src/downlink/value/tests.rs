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

fn make_model(state: DownlinkState, n: i32) -> Model<Arc<Value>> {
    Model {
        state,
        data_state: Arc::new(Value::Int32Value(n)),
    }
}

const STATES: [DownlinkState; 3] = [
    DownlinkState::Unlinked,
    DownlinkState::Linked,
    DownlinkState::Synced,
];

#[test]
fn start_downlink() {
    for s in STATES.iter() {
        let mut model = make_model(*s, 0);
        let response = StateMachine::handle_operation(&mut model, Operation::Start);

        let Response {
            event,
            command,
            error,
            terminate,
        } = response;

        assert!(!terminate);
        assert_that!(error, none());
        assert_that!(event, none());

        assert_that!(model.state, eq(*s));

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
    let mut model = make_model(start_state, 0);
    let response = StateMachine::handle_operation(&mut model, Operation::Message(Message::Linked));

    assert_that!(model.state, eq(DownlinkState::Linked));
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
            event: Some(Event(ev, false)),
            command: None,
            error: None,
            terminate: false,
        } => ev,
        _ => panic!("Response does not consist of just an event."),
    }
}

fn synced_response(start_state: DownlinkState) {
    let mut model = make_model(start_state, 7);
    let response = StateMachine::handle_operation(&mut model, Operation::Message(Message::Synced));

    assert_that!(model.state, eq(DownlinkState::Synced));
    if start_state == DownlinkState::Synced {
        assert_that!(response, eq(Response::none()));
    } else {
        let ev = only_event(&response);
        assert!(Arc::ptr_eq(ev, &model.data_state));
    }
}

#[test]
fn synced_message() {
    for start in STATES.iter() {
        synced_response(*start);
    }
}

fn unlinked_response(start_state: DownlinkState) {
    let mut model = make_model(start_state, 7);
    let response =
        StateMachine::handle_operation(&mut model, Operation::Message(Message::Unlinked));

    assert_that!(model.state, eq(DownlinkState::Unlinked));
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
    let mut model = make_model(DownlinkState::Unlinked, 1);
    let response = StateMachine::handle_operation(
        &mut model,
        Operation::Message(Message::Action(Value::Int32Value(3))),
    );

    assert_that!(model.state, eq(DownlinkState::Unlinked));
    assert_that!(model.data_state, eq(Arc::new(Value::Int32Value(1))));
    assert_that!(response, eq(Response::none()));
}

#[test]
fn update_message_linked() {
    let mut model = make_model(DownlinkState::Linked, 1);
    let response = StateMachine::handle_operation(
        &mut model,
        Operation::Message(Message::Action(Value::Int32Value(3))),
    );

    assert_that!(model.state, eq(DownlinkState::Linked));
    assert_that!(model.data_state, eq(Arc::new(Value::Int32Value(3))));
    assert_that!(response, eq(Response::none()));
}

#[test]
fn update_message_synced() {
    let mut model = make_model(DownlinkState::Synced, 1);
    let response = StateMachine::handle_operation(
        &mut model,
        Operation::Message(Message::Action(Value::Int32Value(3))),
    );

    assert_that!(model.state, eq(DownlinkState::Synced));
    let expected = Arc::new(Value::Int32Value(3));
    assert_that!(&model.data_state, eq(&expected));
    let ev = only_event(&response);
    assert!(Arc::ptr_eq(ev, &model.data_state));
}

fn make_get() -> (Action, oneshot::Receiver<Arc<Value>>) {
    let (tx, rx) = oneshot::channel();
    (Action::get(Request::new(tx)), rx)
}

#[test]
fn get_action() {
    let mut model = make_model(DownlinkState::Synced, 13);
    let (action, mut rx) = make_get();
    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    assert_that!(model.state, eq(DownlinkState::Synced));
    let expected = Arc::new(Value::Int32Value(13));
    assert_that!(&model.data_state, eq(&expected));
    assert_that!(response, eq(Response::none()));

    let result = rx.try_recv();
    assert_that!(&result, ok());
    let get_val = result.unwrap();
    assert!(Arc::ptr_eq(&get_val, &model.data_state));
}

#[test]
fn dropped_get() {
    let mut model = make_model(DownlinkState::Synced, 13);
    let (action, rx) = make_get();
    drop(rx);
    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    assert_that!(model.state, eq(DownlinkState::Synced));
    let expected = Arc::new(Value::Int32Value(13));
    assert_that!(&model.data_state, eq(&expected));
    assert_that!(
        response,
        eq(Response::none().with_error(TransitionError::ReceiverDropped))
    );
}

fn make_set(n: i32) -> (Action, oneshot::Receiver<()>) {
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
            event: Some(Event(ev, true)),
            command: Some(Command::Action(cmd)),
            error,
            terminate: false,
        } => (ev, cmd, error),
        _ => panic!("Response does not consist of and event and a command."),
    }
}

#[test]
fn set_action() {
    let mut model = make_model(DownlinkState::Synced, 13);
    let (action, mut rx) = make_set(67);
    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    assert_that!(model.state, eq(DownlinkState::Synced));
    let expected = Arc::new(Value::Int32Value(67));
    assert_that!(&model.data_state, eq(&expected));
    let (ev, cmd, err) = event_and_cmd(response);
    assert!(Arc::ptr_eq(&ev, &model.data_state));
    assert!(Arc::ptr_eq(&cmd, &model.data_state));
    assert_that!(err, none());

    assert_that!(rx.try_recv(), ok());
}

#[test]
fn dropped_set_action() {
    let mut model = make_model(DownlinkState::Synced, 13);
    let (action, rx) = make_set(67);

    drop(rx);

    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    assert_that!(model.state, eq(DownlinkState::Synced));
    let expected = Arc::new(Value::Int32Value(67));
    assert_that!(&model.data_state, eq(&expected));
    let (ev, cmd, err) = event_and_cmd(response);
    assert!(Arc::ptr_eq(&ev, &model.data_state));
    assert!(Arc::ptr_eq(&cmd, &model.data_state));
    assert_that!(err, some());
    assert_that!(err.unwrap(), eq(TransitionError::ReceiverDropped));
}

fn make_update() -> (Action, oneshot::Receiver<Arc<Value>>) {
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

#[test]
fn update_action() {
    let mut model = make_model(DownlinkState::Synced, 13);
    let (action, mut rx) = make_update();
    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    assert_that!(model.state, eq(DownlinkState::Synced));
    let expected = Arc::new(Value::Int32Value(26));
    assert_that!(&model.data_state, eq(&expected));

    let (ev, cmd, err) = event_and_cmd(response);
    assert!(Arc::ptr_eq(&ev, &model.data_state));
    assert!(Arc::ptr_eq(&cmd, &model.data_state));
    assert_that!(err, none());

    let result = rx.try_recv();
    assert_that!(&result, ok());
    let upd_val = result.unwrap();
    assert!(Arc::ptr_eq(&upd_val, &model.data_state));
}

#[test]
fn dropped_update_action() {
    let mut model = make_model(DownlinkState::Synced, 13);
    let (action, rx) = make_update();

    drop(rx);

    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    assert_that!(model.state, eq(DownlinkState::Synced));
    let expected = Arc::new(Value::Int32Value(26));
    assert_that!(&model.data_state, eq(&expected));

    let (ev, cmd, err) = event_and_cmd(response);
    assert!(Arc::ptr_eq(&ev, &model.data_state));
    assert!(Arc::ptr_eq(&cmd, &model.data_state));
    assert_that!(err, some());
    assert_that!(err.unwrap(), eq(TransitionError::ReceiverDropped));
}
