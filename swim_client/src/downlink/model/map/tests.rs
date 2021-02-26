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

use num_bigint::{BigInt, BigUint};
use tokio::sync::oneshot;

use super::*;
use crate::downlink::{
    error::UpdateFailure, DownlinkState, Event, Operation, Response, StateMachine,
};
use swim_common::form::{Form, FormErr, ValidatedForm};
use swim_common::model::schema::Schema;
use swim_common::model::{Attr, Item, ValueKind};
use swim_common::request::Request;

fn make_model_with(key: i32, value: String) -> MapModel {
    let k = Value::Int32Value(key);
    let v = Arc::new(Value::text(value));
    MapModel {
        state: OrdMap::from(vec![(k, v)]),
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
        let mut state = *s;
        let machine = MapStateMachine::unvalidated();
        let mut model = machine.init_state();
        let response = machine.handle_operation(&mut state, &mut model, Operation::Start);

        assert!(response.is_ok());

        let Response {
            event,
            command,
            error,
            terminate,
        } = response.unwrap();

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
    let machine = MapStateMachine::unvalidated();
    let mut model = machine.init_state();
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

fn only_event(response: &Response<ViewWithEvent, UntypedMapModification<Value>>) -> &ViewWithEvent {
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
    let machine = MapStateMachine::unvalidated();
    let mut model = make_model_with(7, "hello".to_owned());
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Message(Message::Synced));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    if start_state == DownlinkState::Synced {
        assert_eq!(response, Response::none());
    } else {
        let ViewWithEvent { view, event } = only_event(&response);
        assert_eq!(event, &MapEvent::Initial);
        assert!(view.ptr_eq(&model.state));
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
    let machine = MapStateMachine::unvalidated();
    let mut model = make_model_with(7, "hello".to_owned());
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
fn insert_message_unlinked() {
    let k = Value::Int32Value(4);
    let v = Value::text("hello");

    let mut state = DownlinkState::Unlinked;
    let machine = MapStateMachine::unvalidated();
    let mut model = MapModel::new();
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Update(
            k,
            Arc::new(v),
        ))),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Unlinked);
    assert_eq!(model.state.len(), 0);
    assert_eq!(response, Response::none());
}

#[test]
fn remove_message_unlinked() {
    let k = Value::Int32Value(4);
    let v = Value::text("hello");

    let mut state = DownlinkState::Unlinked;
    let machine = MapStateMachine::unvalidated();
    let mut model = make_model_with(4, "hello".to_owned());
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Remove(k.clone()))),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    let expected = ValMap::from(vec![(k, Arc::new(v))]);

    assert_eq!(state, DownlinkState::Unlinked);
    assert_eq!(model.state, expected);
    assert_eq!(response, Response::none());
}

#[test]
fn take_message_unlinked() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");

    let mut state = DownlinkState::Unlinked;
    let mut model = MapModel {
        state: ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    };
    let machine = MapStateMachine::unvalidated();

    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Take(1))),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    let expected = ValMap::from(vec![
        (k1.clone(), Arc::new(v1.clone())),
        (k2.clone(), Arc::new(v2.clone())),
    ]);

    assert_eq!(state, DownlinkState::Unlinked);
    assert_eq!(model.state, expected);
    assert_eq!(response, Response::none());
}

#[test]
fn skip_message_unlinked() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");

    let mut state = DownlinkState::Unlinked;
    let mut model = MapModel {
        state: ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    };

    let machine = MapStateMachine::unvalidated();

    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Drop(1))),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    let expected = ValMap::from(vec![
        (k1.clone(), Arc::new(v1.clone())),
        (k2.clone(), Arc::new(v2.clone())),
    ]);

    assert_eq!(state, DownlinkState::Unlinked);
    assert_eq!(model.state, expected);
    assert_eq!(response, Response::none());
}

#[test]
fn clear_message_unlinked() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");

    let mut state = DownlinkState::Unlinked;
    let mut model = MapModel {
        state: ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    };
    let machine = MapStateMachine::unvalidated();

    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Clear)),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    let expected = ValMap::from(vec![
        (k1.clone(), Arc::new(v1.clone())),
        (k2.clone(), Arc::new(v2.clone())),
    ]);

    assert_eq!(state, DownlinkState::Unlinked);
    assert_eq!(model.state, expected);
    assert_eq!(response, Response::none());
}

#[test]
fn insert_message_linked() {
    let k = Value::Int32Value(4);
    let v = Arc::new(Value::text("hello"));

    let mut state = DownlinkState::Linked;
    let mut model = MapModel::new();
    let machine = MapStateMachine::unvalidated();
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Update(
            k.clone(),
            v.clone(),
        ))),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    let expected = ValMap::from(vec![(k, v)]);

    assert_eq!(state, DownlinkState::Linked);
    assert_eq!(model.state, expected);
    assert_eq!(response, Response::none());
}

#[test]
fn remove_message_linked() {
    let k = Value::Int32Value(4);

    let mut state = DownlinkState::Linked;
    let mut model = make_model_with(4, "hello".to_owned());
    let machine = MapStateMachine::unvalidated();
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Remove(k.clone()))),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Linked);
    assert_eq!(model.state.len(), 0);
    assert_eq!(response, Response::none());
}

#[test]
fn take_message_linked() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");

    let mut state = DownlinkState::Linked;
    let mut model = MapModel {
        state: ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    };
    let machine = MapStateMachine::unvalidated();
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Take(1))),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    let expected = ValMap::from(vec![(k1.clone(), Arc::new(v1.clone()))]);

    assert_eq!(state, DownlinkState::Linked);
    assert_eq!(model.state, expected);
    assert_eq!(response, Response::none());
}

#[test]
fn skip_message_linked() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");

    let mut state = DownlinkState::Linked;
    let mut model = MapModel {
        state: ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    };
    let machine = MapStateMachine::unvalidated();
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Drop(1))),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    let expected = ValMap::from(vec![(k2.clone(), Arc::new(v2.clone()))]);

    assert_eq!(state, DownlinkState::Linked);
    assert_eq!(model.state, expected);
    assert_eq!(response, Response::none());
}

#[test]
fn clear_message_linked() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");

    let mut state = DownlinkState::Linked;
    let mut model = MapModel {
        state: ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    };
    let machine = MapStateMachine::unvalidated();
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Clear)),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Linked);
    assert_eq!(model.state.len(), 0);
    assert_eq!(response, Response::none());
}

#[test]
fn insert_message_synced() {
    let k = Value::Int32Value(4);
    let v = Arc::new(Value::text("hello"));

    let mut state = DownlinkState::Synced;
    let mut model = MapModel::new();
    let machine = MapStateMachine::unvalidated();
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Update(
            k.clone(),
            v.clone(),
        ))),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    let expected = ValMap::from(vec![(k.clone(), v)]);

    assert_eq!(state, DownlinkState::Synced);
    assert_eq!(&model.state, &expected);

    let ViewWithEvent { view, event } = only_event(&response);
    assert!(view.ptr_eq(&model.state));
    let expected_event = MapEvent::Update(k);
    assert_eq!(event, &expected_event);
}

#[test]
fn remove_message_synced() {
    let k = Value::Int32Value(4);

    let mut state = DownlinkState::Synced;
    let mut model = make_model_with(4, "hello".to_owned());
    let machine = MapStateMachine::unvalidated();
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Remove(k.clone()))),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    assert_eq!(model.state.len(), 0);

    let ViewWithEvent { view, event } = only_event(&response);
    assert!(view.ptr_eq(&model.state));
    let expected_event = MapEvent::Remove(k);
    assert_eq!(event, &expected_event);
}

#[test]
fn take_message_synced() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");

    let mut state = DownlinkState::Synced;
    let mut model = MapModel {
        state: ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    };
    let machine = MapStateMachine::unvalidated();
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Take(1))),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    let expected = ValMap::from(vec![(k1.clone(), Arc::new(v1.clone()))]);

    assert_eq!(state, DownlinkState::Synced);
    assert_eq!(&model.state, &expected);

    let ViewWithEvent { view, event } = only_event(&response);
    assert!(view.ptr_eq(&model.state));
    let expected_event = MapEvent::Take(1);
    assert_eq!(event, &expected_event);
}

#[test]
fn skip_message_synced() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");

    let mut state = DownlinkState::Synced;
    let mut model = MapModel {
        state: ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    };
    let machine = MapStateMachine::unvalidated();
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Drop(1))),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    let expected = ValMap::from(vec![(k2.clone(), Arc::new(v2.clone()))]);

    assert_eq!(state, DownlinkState::Synced);
    assert_eq!(&model.state, &expected);

    let ViewWithEvent { view, event } = only_event(&response);
    assert!(view.ptr_eq(&model.state));
    let expected_event = MapEvent::Drop(1);
    assert_eq!(event, &expected_event);
}

#[test]
fn clear_message_synced() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");

    let mut state = DownlinkState::Synced;
    let mut model = MapModel {
        state: ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    };
    let machine = MapStateMachine::unvalidated();

    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Clear)),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    assert_eq!(model.state.len(), 0);

    let ViewWithEvent { view, event } = only_event(&response);
    assert!(view.ptr_eq(&model.state));
    let expected_event = MapEvent::Clear;
    assert_eq!(event, &expected_event);
}

fn make_get_map() -> (MapAction, oneshot::Receiver<Result<ValMap, DownlinkError>>) {
    let (tx, rx) = oneshot::channel();
    (MapAction::get_map(Request::new(tx)), rx)
}

fn make_get(
    key: i32,
) -> (
    MapAction,
    oneshot::Receiver<Result<Option<Arc<Value>>, DownlinkError>>,
) {
    let (tx, rx) = oneshot::channel();
    (MapAction::get(Value::Int32Value(key), Request::new(tx)), rx)
}

#[test]
fn get_action() {
    let k = Value::Int32Value(13);
    let v = Value::text("stuff");

    let mut state = DownlinkState::Synced;
    let mut model = make_model_with(13, "stuff".to_owned());
    let machine = MapStateMachine::unvalidated();
    let (action, mut rx) = make_get_map();
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = ValMap::from(vec![(k, v)]);
    assert_eq!(&model.state, &expected);
    assert_eq!(response, Response::none());

    let result = rx.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_ok());
    let get_val = response.unwrap();
    assert!(get_val.ptr_eq(&model.state));
}

#[test]
fn get_by_defined_key_action() {
    let k = Value::Int32Value(13);
    let v = Value::text("stuff");

    let mut state = DownlinkState::Synced;
    let mut model = make_model_with(13, "stuff".to_owned());
    let machine = MapStateMachine::unvalidated();
    let (action, mut rx) = make_get(13);
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = ValMap::from(vec![(k.clone(), v)]);
    assert_eq!(&model.state, &expected);
    assert_eq!(response, Response::none());

    let result = rx.try_recv();
    assert!(result.is_ok());
    let maybe_get_val = result.unwrap();
    let response = maybe_get_val.unwrap();
    assert!(response.is_some());
    let get_val = response.unwrap();
    assert!(Arc::ptr_eq(&get_val, model.state.get(&k).unwrap()));
}

#[test]
fn get_by_undefined_key_action() {
    let k = Value::Int32Value(13);
    let v = Value::text("stuff");

    let mut state = DownlinkState::Synced;
    let mut model = make_model_with(13, "stuff".to_owned());
    let machine = MapStateMachine::unvalidated();
    let (action, mut rx) = make_get(-1);
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = ValMap::from(vec![(k.clone(), v)]);
    assert_eq!(&model.state, &expected);
    assert_eq!(response, Response::none());

    let result = rx.try_recv();
    assert!(result.is_ok());
    let maybe_get_val = result.unwrap();
    assert_eq!(maybe_get_val, Ok(None));
}

fn make_update(
    key: i32,
    value: String,
) -> (
    MapAction,
    oneshot::Receiver<Result<Option<Arc<Value>>, DownlinkError>>,
) {
    let (tx, rx) = oneshot::channel();
    (
        MapAction::update_and_await(Value::Int32Value(key), Value::text(value), Request::new(tx)),
        rx,
    )
}

fn event_and_cmd(
    response: Response<ViewWithEvent, UntypedMapModification<Value>>,
) -> (
    ViewWithEvent,
    UntypedMapModification<Value>,
    Option<TransitionError>,
) {
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
fn insert_to_undefined_action() {
    let k = Value::Int32Value(13);
    let v = Value::text("stuff");

    let mut state = DownlinkState::Synced;
    let mut model = MapModel::new();
    let machine = MapStateMachine::unvalidated();
    let (action, mut rx) = make_update(13, "stuff".to_owned());
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = ValMap::from(vec![(k.clone(), v.clone())]);
    assert_eq!(&model.state, &expected);

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.state));
    assert_eq!(event, MapEvent::Update(k.clone()));
    match cmd {
        UntypedMapModification::Update(cmd_k, cmd_v) => {
            assert_eq!(&cmd_k, &k);
            assert!(Arc::ptr_eq(&cmd_v, model.state.get(&k).unwrap()));
        }
        ow => {
            panic!("{:?} is not an insertion.", ow);
        }
    }
    assert!(err.is_none());

    let result = rx.try_recv();
    assert!(result.is_ok());
    let maybe_old_val = result.unwrap();
    assert_eq!(maybe_old_val, Ok(None));
}

#[test]
fn insert_action_dropped_listener() {
    let k = Value::Int32Value(13);
    let v = Value::text("stuff");

    let mut state = DownlinkState::Synced;
    let mut model = MapModel::new();
    let machine = MapStateMachine::unvalidated();
    let (action, rx) = make_update(13, "stuff".to_owned());

    drop(rx);

    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();
    assert_eq!(state, DownlinkState::Synced);
    let expected = ValMap::from(vec![(k.clone(), v.clone())]);
    assert_eq!(&model.state, &expected);

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.state));
    assert_eq!(event, MapEvent::Update(k.clone()));
    match cmd {
        UntypedMapModification::Update(cmd_k, cmd_v) => {
            assert_eq!(&cmd_k, &k);
            assert!(Arc::ptr_eq(&cmd_v, model.state.get(&k).unwrap()));
        }
        ow => {
            panic!("{:?} is not an insertion.", ow);
        }
    }
    assert_eq!(err, Some(TransitionError::ReceiverDropped));
}

#[test]
fn insert_to_defined_action() {
    let original_val = "original".to_owned();
    let new_val = "stuff".to_owned();

    let k = Value::Int32Value(13);

    let mut state = DownlinkState::Synced;
    let mut model = make_model_with(13, original_val.clone());
    let machine = MapStateMachine::unvalidated();

    let (action, mut rx) = make_update(13, new_val.clone());
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = ValMap::from(vec![(k.clone(), Value::text(new_val.clone()))]);
    assert_eq!(&model.state, &expected);

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.state));
    assert_eq!(event, MapEvent::Update(k.clone()));
    match cmd {
        UntypedMapModification::Update(cmd_k, cmd_v) => {
            assert_eq!(&cmd_k, &k);
            assert!(Arc::ptr_eq(&cmd_v, model.state.get(&k).unwrap()));
        }
        ow => {
            panic!("{:?} is not an insertion.", ow);
        }
    }
    assert!(err.is_none());

    let result = rx.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_ok());
    let maybe_old_val = response.unwrap();
    assert!(maybe_old_val.is_some());
    let old_val = maybe_old_val.unwrap();
    assert_eq!(old_val, Arc::new(Value::text(original_val)))
}

#[test]
fn invalid_key_insert_action() {
    let original_val = "original".to_owned();
    let key_schema = StandardSchema::OfKind(ValueKind::Int32);
    let val_schema = StandardSchema::OfKind(ValueKind::Text);

    let k = Value::Int32Value(13);

    let mut state = DownlinkState::Synced;
    let mut model = make_model_with(13, original_val.clone());
    let machine = MapStateMachine::new(key_schema.clone(), val_schema);

    let (tx, mut rx) = oneshot::channel();
    let action = MapAction::Update {
        key: Value::BooleanValue(false),
        value: Value::text("updated"),
        old: Some(Request::new(tx)),
    };

    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = ValMap::from(vec![(k.clone(), Value::text(original_val))]);
    assert_eq!(&model.state, &expected);

    assert_eq!(response, Response::none());

    let expected_err = DownlinkError::SchemaViolation(Value::BooleanValue(false), key_schema);
    let result = rx.try_recv();
    assert_eq!(result, Ok(Err(expected_err)))
}

#[test]
fn invalid_value_insert_action() {
    let original_val = "original".to_owned();
    let key_schema = StandardSchema::OfKind(ValueKind::Int32);
    let val_schema = StandardSchema::OfKind(ValueKind::Text);

    let k = Value::Int32Value(13);

    let mut state = DownlinkState::Synced;
    let mut model = make_model_with(13, original_val.clone());
    let machine = MapStateMachine::new(key_schema, val_schema.clone());

    let (tx, mut rx) = oneshot::channel();
    let action = MapAction::Update {
        key: k.clone(),
        value: Value::BooleanValue(false),
        old: Some(Request::new(tx)),
    };

    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = ValMap::from(vec![(k.clone(), Value::text(original_val))]);
    assert_eq!(&model.state, &expected);

    assert_eq!(response, Response::none());

    let expected_err = DownlinkError::SchemaViolation(Value::BooleanValue(false), val_schema);
    let result = rx.try_recv();
    assert_eq!(result, Ok(Err(expected_err)))
}

fn make_remove(
    key: i32,
) -> (
    MapAction,
    oneshot::Receiver<Result<Option<Arc<Value>>, DownlinkError>>,
) {
    let (tx, rx) = oneshot::channel();
    (
        MapAction::remove_and_await(Value::Int32Value(key), Request::new(tx)),
        rx,
    )
}

#[test]
fn remove_undefined_action() {
    let mut state = DownlinkState::Synced;
    let mut model = MapModel::new();
    let machine = MapStateMachine::unvalidated();
    let (action, mut rx) = make_remove(43);

    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    assert_eq!(model.state.len(), 0);
    assert_eq!(response, Response::none());

    let result = rx.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_ok());
    let maybe_old_val = response.unwrap();
    assert!(maybe_old_val.is_none());
}

#[test]
fn remove_action_dropped_listener() {
    let mut state = DownlinkState::Synced;
    let mut model = MapModel::new();
    let machine = MapStateMachine::unvalidated();
    let (action, rx) = make_remove(43);

    drop(rx);

    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    assert_eq!(model.state.len(), 0);
    assert_eq!(
        response,
        with_error(Response::none(), TransitionError::ReceiverDropped)
    );
}

fn with_error<Ev, Cmd>(mut response: Response<Ev, Cmd>, err: TransitionError) -> Response<Ev, Cmd> {
    response.error = Some(err);
    response
}

#[test]
fn remove_defined_action() {
    let k = Value::Int32Value(13);
    let original_val = "original".to_owned();

    let mut state = DownlinkState::Synced;
    let mut model = make_model_with(13, original_val.clone());
    let machine = MapStateMachine::unvalidated();

    let (action, mut rx) = make_remove(13);
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);

    assert_eq!(model.state.len(), 0);

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.state));
    assert_eq!(event, MapEvent::Remove(k.clone()));
    match cmd {
        UntypedMapModification::Remove(cmd_k) => {
            assert_eq!(&cmd_k, &k);
        }
        ow => {
            panic!("{:?} is not a removal.", ow);
        }
    }
    assert!(err.is_none());

    let result = rx.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_ok());
    let maybe_old_val = response.unwrap();
    assert!(maybe_old_val.is_some());
    let old_val = maybe_old_val.unwrap();
    assert_eq!(old_val, Arc::new(Value::text(original_val)))
}

#[test]
fn invalid_remove_action() {
    let original_val = "original".to_owned();
    let key_schema = StandardSchema::OfKind(ValueKind::Int32);
    let val_schema = StandardSchema::OfKind(ValueKind::Text);

    let k = Value::Int32Value(13);

    let mut state = DownlinkState::Synced;
    let mut model = make_model_with(13, original_val.clone());
    let machine = MapStateMachine::new(key_schema.clone(), val_schema);

    let (tx, mut rx) = oneshot::channel();
    let action = MapAction::Remove {
        key: Value::BooleanValue(false),
        old: Some(Request::new(tx)),
    };

    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = ValMap::from(vec![(k.clone(), Value::text(original_val))]);
    assert_eq!(&model.state, &expected);

    assert_eq!(response, Response::none());

    let expected_err = DownlinkError::SchemaViolation(Value::BooleanValue(false), key_schema);
    let result = rx.try_recv();
    assert_eq!(result, Ok(Err(expected_err)))
}

fn make_take(
    n: usize,
) -> (
    MapAction,
    oneshot::Receiver<Result<ValMap, DownlinkError>>,
    oneshot::Receiver<Result<ValMap, DownlinkError>>,
) {
    let (tx_bef, rx_bef) = oneshot::channel();
    let (tx_aft, rx_aft) = oneshot::channel();
    (
        MapAction::take_and_await(n, Request::new(tx_bef), Request::new(tx_aft)),
        rx_bef,
        rx_aft,
    )
}

#[test]
fn take_action() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");

    let mut state = DownlinkState::Synced;
    let mut model = MapModel {
        state: ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    };
    let machine = MapStateMachine::unvalidated();

    let expected_before = model.state.clone();

    let (action, mut rx_before, mut rx_after) = make_take(1);

    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    let expected = ValMap::from(vec![(k1.clone(), Arc::new(v1.clone()))]);

    assert_eq!(state, DownlinkState::Synced);
    assert_eq!(&model.state, &expected);

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.state));
    assert_eq!(event, MapEvent::Take(1));
    assert_eq!(cmd, UntypedMapModification::Take(1));
    assert!(err.is_none());

    let result_before = rx_before.try_recv();
    assert!(result_before.is_ok());
    let response_before = result_before.unwrap();
    assert!(response_before.is_ok());
    let before_val = response_before.unwrap();
    assert!(before_val.ptr_eq(&expected_before));

    let result_after = rx_after.try_recv();
    assert!(result_after.is_ok());
    let response_after = result_after.unwrap();
    assert!(response_after.is_ok());
    let after_val = response_after.unwrap();
    assert!(after_val.ptr_eq(&model.state));
}

#[test]
fn take_action_dropped_before() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");

    let mut state = DownlinkState::Synced;
    let mut model = MapModel {
        state: ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    };
    let machine = MapStateMachine::unvalidated();

    let (action, rx_before, mut rx_after) = make_take(1);

    drop(rx_before);

    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    let expected = ValMap::from(vec![(k1.clone(), Arc::new(v1.clone()))]);

    assert_eq!(state, DownlinkState::Synced);
    assert_eq!(&model.state, &expected);

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.state));
    assert_eq!(event, MapEvent::Take(1));
    assert_eq!(cmd, UntypedMapModification::Take(1));
    assert_eq!(err, Some(TransitionError::ReceiverDropped));

    let result_after = rx_after.try_recv();
    assert!(result_after.is_ok());
    let response_after = result_after.unwrap();
    assert!(response_after.is_ok());
    let after_val = response_after.unwrap();
    assert!(after_val.ptr_eq(&model.state));
}

#[test]
fn take_action_dropped_after() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");

    let mut state = DownlinkState::Synced;
    let mut model = MapModel {
        state: ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    };
    let machine = MapStateMachine::unvalidated();

    let expected_before = model.state.clone();

    let (action, mut rx_before, rx_after) = make_take(1);

    drop(rx_after);

    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    let expected = ValMap::from(vec![(k1.clone(), Arc::new(v1.clone()))]);

    assert_eq!(state, DownlinkState::Synced);
    assert_eq!(&model.state, &expected);

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.state));
    assert_eq!(event, MapEvent::Take(1));
    assert_eq!(cmd, UntypedMapModification::Take(1));
    assert_eq!(err, Some(TransitionError::ReceiverDropped));

    let result_before = rx_before.try_recv();
    assert!(result_before.is_ok());
    let response_before = result_before.unwrap();
    assert!(response_before.is_ok());
    let before_val = response_before.unwrap();
    assert!(before_val.ptr_eq(&expected_before));
}

#[test]
fn take_action_both_dropped() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");

    let mut state = DownlinkState::Synced;
    let mut model = MapModel {
        state: ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    };
    let machine = MapStateMachine::unvalidated();

    let (action, rx_before, rx_after) = make_take(1);

    drop(rx_before);
    drop(rx_after);

    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    let expected = ValMap::from(vec![(k1.clone(), Arc::new(v1.clone()))]);

    assert_eq!(state, DownlinkState::Synced);
    assert_eq!(&model.state, &expected);

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.state));
    assert_eq!(event, MapEvent::Take(1));
    assert_eq!(cmd, UntypedMapModification::Take(1));
    assert_eq!(err, Some(TransitionError::ReceiverDropped));
}

fn make_skip(
    n: usize,
) -> (
    MapAction,
    oneshot::Receiver<Result<ValMap, DownlinkError>>,
    oneshot::Receiver<Result<ValMap, DownlinkError>>,
) {
    let (tx_bef, rx_bef) = oneshot::channel();
    let (tx_aft, rx_aft) = oneshot::channel();
    (
        MapAction::drop_and_await(n, Request::new(tx_bef), Request::new(tx_aft)),
        rx_bef,
        rx_aft,
    )
}

#[test]
fn skip_action() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");

    let mut state = DownlinkState::Synced;
    let mut model = MapModel {
        state: ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    };
    let machine = MapStateMachine::unvalidated();

    let expected_before = model.state.clone();

    let (action, mut rx_before, mut rx_after) = make_skip(1);

    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    let expected = ValMap::from(vec![(k2.clone(), Arc::new(v2.clone()))]);

    assert_eq!(state, DownlinkState::Synced);
    assert_eq!(&model.state, &expected);

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.state));
    assert_eq!(event, MapEvent::Drop(1));
    assert_eq!(cmd, UntypedMapModification::Drop(1));
    assert!(err.is_none());

    let result_before = rx_before.try_recv();
    assert!(result_before.is_ok());
    let response_before = result_before.unwrap();
    assert!(response_before.is_ok());
    let before_val = response_before.unwrap();
    assert!(before_val.ptr_eq(&expected_before));

    let result_after = rx_after.try_recv();
    assert!(result_after.is_ok());
    let response_after = result_after.unwrap();
    assert!(response_after.is_ok());
    let after_val = response_after.unwrap();
    assert!(after_val.ptr_eq(&model.state));
}

#[test]
fn skip_action_dropped_before() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");

    let mut state = DownlinkState::Synced;
    let mut model = MapModel {
        state: ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    };
    let machine = MapStateMachine::unvalidated();

    let (action, rx_before, mut rx_after) = make_skip(1);

    drop(rx_before);

    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    let expected = ValMap::from(vec![(k2.clone(), Arc::new(v2.clone()))]);

    assert_eq!(state, DownlinkState::Synced);
    assert_eq!(&model.state, &expected);

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.state));
    assert_eq!(event, MapEvent::Drop(1));
    assert_eq!(cmd, UntypedMapModification::Drop(1));
    assert_eq!(err, Some(TransitionError::ReceiverDropped));

    let result_after = rx_after.try_recv();
    assert!(result_after.is_ok());
    let response_after = result_after.unwrap();
    assert!(response_after.is_ok());
    let after_val = response_after.unwrap();
    assert!(after_val.ptr_eq(&model.state));
}

#[test]
fn skip_action_dropped_after() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");

    let mut state = DownlinkState::Synced;
    let mut model = MapModel {
        state: ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    };
    let machine = MapStateMachine::unvalidated();

    let expected_before = model.state.clone();

    let (action, mut rx_before, rx_after) = make_skip(1);

    drop(rx_after);

    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    let expected = ValMap::from(vec![(k2.clone(), Arc::new(v2.clone()))]);

    assert_eq!(state, DownlinkState::Synced);
    assert_eq!(&model.state, &expected);

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.state));
    assert_eq!(event, MapEvent::Drop(1));
    assert_eq!(cmd, UntypedMapModification::Drop(1));
    assert_eq!(err, Some(TransitionError::ReceiverDropped));

    let result_before = rx_before.try_recv();
    assert!(result_before.is_ok());
    let response_before = result_before.unwrap();
    assert!(response_before.is_ok());
    let before_val = response_before.unwrap();
    assert!(before_val.ptr_eq(&expected_before));
}

#[test]
fn skip_action_dropped_both() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");

    let mut state = DownlinkState::Synced;
    let mut model = MapModel {
        state: ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    };
    let machine = MapStateMachine::unvalidated();

    let (action, rx_before, rx_after) = make_skip(1);

    drop(rx_before);
    drop(rx_after);

    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    let expected = ValMap::from(vec![(k2.clone(), Arc::new(v2.clone()))]);

    assert_eq!(state, DownlinkState::Synced);
    assert_eq!(&model.state, &expected);

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.state));
    assert_eq!(event, MapEvent::Drop(1));
    assert_eq!(cmd, UntypedMapModification::Drop(1));
    assert_eq!(err, Some(TransitionError::ReceiverDropped));
}

fn make_clear() -> (MapAction, oneshot::Receiver<Result<ValMap, DownlinkError>>) {
    let (tx_bef, rx_bef) = oneshot::channel();
    (MapAction::clear_and_await(Request::new(tx_bef)), rx_bef)
}

#[test]
fn clear_action() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");

    let mut state = DownlinkState::Synced;
    let mut model = MapModel {
        state: ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    };
    let machine = MapStateMachine::unvalidated();

    let expected_before = model.state.clone();

    let (action, mut rx_before) = make_clear();

    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    assert_eq!(model.state.len(), 0);

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.state));
    assert_eq!(event, MapEvent::Clear);
    assert_eq!(cmd, UntypedMapModification::Clear);
    assert!(err.is_none());

    let result_before = rx_before.try_recv();
    assert!(result_before.is_ok());
    let response_before = result_before.unwrap();
    assert!(response_before.is_ok());
    let before_val = response_before.unwrap();
    assert!(before_val.ptr_eq(&expected_before));
}

#[test]
fn clear_action_dropped_receiver() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");

    let mut state = DownlinkState::Synced;
    let mut model = MapModel {
        state: ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    };
    let machine = MapStateMachine::unvalidated();

    let (action, rx_before) = make_clear();

    drop(rx_before);

    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    assert_eq!(model.state.len(), 0);

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.state));
    assert_eq!(event, MapEvent::Clear);
    assert_eq!(cmd, UntypedMapModification::Clear);
    assert_eq!(err, Some(TransitionError::ReceiverDropped));
}

#[test]
pub fn clear_to_value() {
    let expected = Value::of_attr("clear");
    assert_eq!(
        Form::into_value(UntypedMapModification::<Value>::Clear),
        expected
    );
    assert_eq!(
        Form::as_value(&UntypedMapModification::<Value>::Clear),
        expected
    );
}

type MapModResult = Result<UntypedMapModification<Value>, FormErr>;

#[test]
pub fn clear_from_value() {
    let rep = Value::of_attr("clear");
    let result1: MapModResult = Form::try_from_value(&rep);
    assert_eq!(result1, Ok(UntypedMapModification::<Value>::Clear));
    let result2: MapModResult = Form::try_convert(rep);
    assert_eq!(result2, Ok(UntypedMapModification::<Value>::Clear));
}

#[test]
pub fn take_to_value() {
    let expected = Value::of_attr(("take", 3));
    assert_eq!(
        Form::into_value(UntypedMapModification::<Value>::Take(3)),
        expected
    );
    assert_eq!(
        Form::as_value(&UntypedMapModification::<Value>::Take(3)),
        expected
    );
}

#[test]
pub fn take_from_value() {
    let rep = Value::of_attr(("take", 3));
    let result1: MapModResult = Form::try_from_value(&rep);
    assert_eq!(result1, Ok(UntypedMapModification::Take(3)));
    let result2: MapModResult = Form::try_convert(rep);
    assert_eq!(result2, Ok(UntypedMapModification::Take(3)));
}

#[test]
pub fn skip_to_value() {
    let expected = Value::of_attr(("drop", 5));
    assert_eq!(
        Form::into_value(UntypedMapModification::<Value>::Drop(5)),
        expected
    );
    assert_eq!(
        Form::as_value(&UntypedMapModification::<Value>::Drop(5)),
        expected
    );
}

#[test]
pub fn skip_from_value() {
    let rep = Value::of_attr(("drop", 5));
    let result1: MapModResult = Form::try_from_value(&rep);
    assert_eq!(result1, Ok(UntypedMapModification::Drop(5)));
    let result2: MapModResult = Form::try_convert(rep);
    assert_eq!(result2, Ok(UntypedMapModification::Drop(5)));
}

#[test]
pub fn remove_to_value() {
    let expected = Value::of_attr(("remove", Value::record(vec![Item::slot("key", "hello")])));
    assert_eq!(
        Form::into_value(UntypedMapModification::<Value>::Remove(Value::text(
            "hello"
        ))),
        expected
    );
    assert_eq!(
        Form::as_value(&UntypedMapModification::<Value>::Remove(Value::text(
            "hello"
        ))),
        expected
    );
}

#[test]
pub fn remove_from_value() {
    let rep = Value::of_attr(("remove", Value::record(vec![Item::slot("key", "hello")])));
    let result1: MapModResult = Form::try_from_value(&rep);
    assert_eq!(
        result1,
        Ok(UntypedMapModification::<Value>::Remove(Value::text(
            "hello"
        )))
    );
    let result2: MapModResult = Form::try_convert(rep);
    assert_eq!(
        result2,
        Ok(UntypedMapModification::Remove(Value::text("hello")))
    );
}

#[test]
pub fn simple_insert_to_value() {
    let attr = Attr::of(("update", Value::record(vec![Item::slot("key", "hello")])));
    let body = Item::ValueItem(Value::Int32Value(2));
    let expected = Value::Record(vec![attr], vec![body]);
    assert_eq!(
        Form::into_value(UntypedMapModification::Update(
            Value::text("hello"),
            Arc::new(Value::Int32Value(2))
        )),
        expected
    );
    assert_eq!(
        Form::as_value(&UntypedMapModification::Update(
            Value::text("hello"),
            Arc::new(Value::Int32Value(2))
        )),
        expected
    );
}

#[test]
pub fn simple_insert_from_value() {
    let attr = Attr::of(("update", Value::record(vec![Item::slot("key", "hello")])));
    let body = Item::ValueItem(Value::Int32Value(2));
    let rep = Value::Record(vec![attr], vec![body]);
    let result1: MapModResult = Form::try_from_value(&rep);
    assert_eq!(
        result1,
        Ok(UntypedMapModification::Update(
            Value::text("hello"),
            Arc::new(Value::Int32Value(2))
        ))
    );
    let result2: MapModResult = Form::try_convert(rep);
    assert_eq!(
        result2,
        Ok(UntypedMapModification::Update(
            Value::text("hello"),
            Arc::new(Value::Int32Value(2))
        ))
    );
}

#[test]
pub fn complex_insert_to_value() {
    let body = Arc::new(Value::Record(
        vec![Attr::of(("complex", 0))],
        vec![Item::slot("a", true)],
    ));
    let attr = Attr::of(("update", Value::record(vec![Item::slot("key", "hello")])));
    let expected = Value::Record(
        vec![attr, Attr::of(("complex", 0))],
        vec![Item::slot("a", true)],
    );
    assert_eq!(
        Form::into_value(UntypedMapModification::Update(
            Value::text("hello"),
            body.clone()
        )),
        expected
    );
    assert_eq!(
        Form::as_value(&UntypedMapModification::Update(
            Value::text("hello"),
            body.clone()
        )),
        expected
    );
}

#[test]
pub fn complex_insert_from_value() {
    let body = Arc::new(Value::Record(
        vec![Attr::of(("complex", 0))],
        vec![Item::slot("a", true)],
    ));
    let attr = Attr::of(("update", Value::record(vec![Item::slot("key", "hello")])));
    let rep = Value::Record(
        vec![attr, Attr::of(("complex", 0))],
        vec![Item::slot("a", true)],
    );
    let result1: MapModResult = Form::try_from_value(&rep);
    assert_eq!(
        result1,
        Ok(UntypedMapModification::Update(
            Value::text("hello"),
            body.clone()
        ))
    );
    let result2: MapModResult = Form::try_convert(rep);
    assert_eq!(
        result2,
        Ok(UntypedMapModification::Update(
            Value::text("hello"),
            body.clone()
        ))
    );
}

#[test]
pub fn map_modification_schema() {
    let clear = Value::of_attr("clear");
    let take = Value::of_attr(("take", 3));
    let skip = Value::of_attr(("drop", 5));
    let remove = Value::of_attr(("remove", Value::record(vec![Item::slot("key", "hello")])));

    let attr = Attr::of(("update", Value::record(vec![Item::slot("key", "hello")])));
    let body = Item::ValueItem(Value::Int32Value(2));
    let simple_insert = Value::Record(vec![attr], vec![body]);

    let attr = Attr::of(("update", Value::record(vec![Item::slot("key", "hello")])));
    let complex_insert = Value::Record(
        vec![attr, Attr::of(("complex", 0))],
        vec![Item::slot("a", true)],
    );

    let schema = <UntypedMapModification<Value> as ValidatedForm>::schema();

    assert!(schema.matches(&clear));
    assert!(schema.matches(&take));
    assert!(schema.matches(&skip));
    assert!(schema.matches(&remove));
    assert!(schema.matches(&simple_insert));
    assert!(schema.matches(&complex_insert));
}

#[test]
fn invalid_insert_key_unlinked() {
    let k = Value::Extant;
    let v = Arc::new(Value::text("hello"));

    let mut state = DownlinkState::Unlinked;
    let machine = MapStateMachine::new(
        StandardSchema::OfKind(ValueKind::Int32),
        StandardSchema::OfKind(ValueKind::Text),
    );
    let mut model = MapModel::new();
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Update(k, v))),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Unlinked);
    assert_eq!(model.state.len(), 0);
    assert_eq!(response, Response::none());
}

#[test]
fn invalid_insert_value_unlinked() {
    let k = Value::Int32Value(1);
    let v = Arc::new(Value::Extant);

    let mut state = DownlinkState::Unlinked;
    let machine = MapStateMachine::new(
        StandardSchema::OfKind(ValueKind::Int32),
        StandardSchema::OfKind(ValueKind::Text),
    );
    let mut model = MapModel::new();
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Update(k, v))),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Unlinked);
    assert_eq!(model.state.len(), 0);
    assert_eq!(response, Response::none());
}

#[test]
fn invalid_remove_unlinked() {
    let k = Value::Extant;

    let mut state = DownlinkState::Unlinked;
    let machine = MapStateMachine::new(
        StandardSchema::OfKind(ValueKind::Int32),
        StandardSchema::OfKind(ValueKind::Text),
    );
    let mut model = MapModel::new();
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Remove(k))),
    );

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Unlinked);
    assert_eq!(model.state.len(), 0);
    assert_eq!(response, Response::none());
}

#[test]
fn invalid_insert_key_linked() {
    let k = Value::Extant;
    let v = Arc::new(Value::text("hello"));

    let mut state = DownlinkState::Linked;
    let machine = MapStateMachine::new(
        StandardSchema::OfKind(ValueKind::Int32),
        StandardSchema::OfKind(ValueKind::Text),
    );
    let mut model = MapModel::new();
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Update(k, v))),
    );

    assert!(maybe_response.is_err());
    let error = maybe_response.err().unwrap();

    assert_eq!(state, DownlinkState::Linked);
    assert_eq!(model.state.len(), 0);
    assert_eq!(
        error,
        DownlinkError::SchemaViolation(Value::Extant, StandardSchema::OfKind(ValueKind::Int32))
    );
}

#[test]
fn invalid_insert_value_linked() {
    let k = Value::Int32Value(1);
    let v = Arc::new(Value::Extant);

    let mut state = DownlinkState::Linked;
    let machine = MapStateMachine::new(
        StandardSchema::OfKind(ValueKind::Int32),
        StandardSchema::OfKind(ValueKind::Text),
    );
    let mut model = MapModel::new();
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Update(k, v))),
    );

    assert!(maybe_response.is_err());
    let error = maybe_response.err().unwrap();

    assert_eq!(state, DownlinkState::Linked);
    assert_eq!(model.state.len(), 0);
    assert_eq!(
        error,
        DownlinkError::SchemaViolation(Value::Extant, StandardSchema::OfKind(ValueKind::Text))
    );
}

#[test]
fn invalid_remove_linked() {
    let k = Value::Extant;

    let mut state = DownlinkState::Linked;
    let machine = MapStateMachine::new(
        StandardSchema::OfKind(ValueKind::Int32),
        StandardSchema::OfKind(ValueKind::Text),
    );
    let mut model = MapModel::new();
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Remove(k))),
    );

    assert!(maybe_response.is_err());
    let error = maybe_response.err().unwrap();

    assert_eq!(state, DownlinkState::Linked);
    assert_eq!(model.state.len(), 0);
    assert_eq!(
        error,
        DownlinkError::SchemaViolation(Value::Extant, StandardSchema::OfKind(ValueKind::Int32))
    );
}

#[test]
fn invalid_insert_key_synced() {
    let k = Value::Extant;
    let v = Arc::new(Value::text("hello"));

    let mut state = DownlinkState::Synced;
    let machine = MapStateMachine::new(
        StandardSchema::OfKind(ValueKind::Int32),
        StandardSchema::OfKind(ValueKind::Text),
    );
    let mut model = MapModel::new();
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Update(k, v))),
    );

    assert!(maybe_response.is_err());
    let error = maybe_response.err().unwrap();

    assert_eq!(state, DownlinkState::Synced);
    assert_eq!(model.state.len(), 0);
    assert_eq!(
        error,
        DownlinkError::SchemaViolation(Value::Extant, StandardSchema::OfKind(ValueKind::Int32))
    );
}

#[test]
fn invalid_insert_value_synced() {
    let k = Value::Int32Value(1);
    let v = Arc::new(Value::Extant);

    let mut state = DownlinkState::Synced;
    let machine = MapStateMachine::new(
        StandardSchema::OfKind(ValueKind::Int32),
        StandardSchema::OfKind(ValueKind::Text),
    );
    let mut model = MapModel::new();
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Update(k, v))),
    );

    assert!(maybe_response.is_err());
    let error = maybe_response.err().unwrap();

    assert_eq!(state, DownlinkState::Synced);
    assert_eq!(model.state.len(), 0);
    assert_eq!(
        error,
        DownlinkError::SchemaViolation(Value::Extant, StandardSchema::OfKind(ValueKind::Text))
    );
}

#[test]
fn invalid_remove_synced() {
    let k = Value::Extant;

    let mut state = DownlinkState::Synced;
    let machine = MapStateMachine::new(
        StandardSchema::OfKind(ValueKind::Int32),
        StandardSchema::OfKind(ValueKind::Text),
    );
    let mut model = MapModel::new();
    let maybe_response = machine.handle_operation(
        &mut state,
        &mut model,
        Operation::Message(Message::Action(UntypedMapModification::Remove(k))),
    );

    assert!(maybe_response.is_err());
    let error = maybe_response.err().unwrap();

    assert_eq!(state, DownlinkState::Synced);
    assert_eq!(model.state.len(), 0);
    assert_eq!(
        error,
        DownlinkError::SchemaViolation(Value::Extant, StandardSchema::OfKind(ValueKind::Int32))
    );
}

fn make_modify(
    key: i32,
) -> (
    MapAction,
    oneshot::Receiver<Result<Option<Arc<Value>>, DownlinkError>>,
    oneshot::Receiver<Result<Option<Arc<Value>>, DownlinkError>>,
) {
    make_modify_raw(Value::from(key))
}

fn make_modify_raw(
    key: Value,
) -> (
    MapAction,
    oneshot::Receiver<Result<Option<Arc<Value>>, DownlinkError>>,
    oneshot::Receiver<Result<Option<Arc<Value>>, DownlinkError>>,
) {
    let upd_fn = |maybe_v: &Option<&Value>| match maybe_v {
        Some(Value::Text(t)) if !t.is_empty() => Some(Value::text(t.as_str().to_uppercase())),
        _ => None,
    };
    let (tx_before, rx_before) = oneshot::channel();
    let (tx_after, rx_after) = oneshot::channel();
    (
        MapAction::modify_and_await(key, upd_fn, Request::new(tx_before), Request::new(tx_after)),
        rx_before,
        rx_after,
    )
}

fn make_modify_bad(
    key: i32,
) -> (
    MapAction,
    oneshot::Receiver<Result<Option<Arc<Value>>, DownlinkError>>,
    oneshot::Receiver<Result<Option<Arc<Value>>, DownlinkError>>,
) {
    let upd_fn = |maybe_v: &Option<&Value>| match maybe_v {
        Some(Value::Text(t)) if !t.is_empty() => Some(Value::BooleanValue(true)),
        _ => None,
    };
    let (tx_before, rx_before) = oneshot::channel();
    let (tx_after, rx_after) = oneshot::channel();
    (
        MapAction::modify_and_await(
            Value::from(key),
            upd_fn,
            Request::new(tx_before),
            Request::new(tx_after),
        ),
        rx_before,
        rx_after,
    )
}

fn make_try_update(
    key: i32,
) -> (
    MapAction,
    oneshot::Receiver<Result<UpdateResult<Option<Arc<Value>>>, DownlinkError>>,
    oneshot::Receiver<Result<UpdateResult<Option<Arc<Value>>>, DownlinkError>>,
) {
    make_try_update_raw(Value::from(key))
}

fn make_try_update_raw(
    key: Value,
) -> (
    MapAction,
    oneshot::Receiver<Result<UpdateResult<Option<Arc<Value>>>, DownlinkError>>,
    oneshot::Receiver<Result<UpdateResult<Option<Arc<Value>>>, DownlinkError>>,
) {
    let upd_fn = |maybe_v: &Option<&Value>| match maybe_v {
        Some(Value::Text(t)) if t.len() > 1 => Ok(Some(Value::text(t.as_str().to_uppercase()))),
        Some(Value::Text(t)) if t.is_empty() => Ok(None),
        _ => Err(UpdateFailure("Update failed.".to_string())),
    };
    let (tx_before, rx_before) = oneshot::channel();
    let (tx_after, rx_after) = oneshot::channel();
    (
        MapAction::try_modify_and_await(
            key,
            upd_fn,
            Request::new(tx_before),
            Request::new(tx_after),
        ),
        rx_before,
        rx_after,
    )
}

fn make_try_update_bad(
    key: i32,
) -> (
    MapAction,
    oneshot::Receiver<Result<UpdateResult<Option<Arc<Value>>>, DownlinkError>>,
    oneshot::Receiver<Result<UpdateResult<Option<Arc<Value>>>, DownlinkError>>,
) {
    let upd_fn = |maybe_v: &Option<&Value>| match maybe_v {
        Some(Value::Text(t)) if !t.is_empty() => Ok(Some(Value::BooleanValue(true))),
        _ => Err(UpdateFailure("Update failed.".to_string())),
    };
    let (tx_before, rx_before) = oneshot::channel();
    let (tx_after, rx_after) = oneshot::channel();
    (
        MapAction::try_modify_and_await(
            Value::from(key),
            upd_fn,
            Request::new(tx_before),
            Request::new(tx_after),
        ),
        rx_before,
        rx_after,
    )
}

#[test]
fn update_to_defined_action() {
    let original_val = "original".to_string();
    let expected_val = original_val.to_uppercase();

    let k = Value::Int32Value(13);

    let mut state = DownlinkState::Synced;
    let mut model = make_model_with(13, original_val.clone());
    let machine = MapStateMachine::unvalidated();

    let (action, mut rx_before, mut rx_after) = make_modify(13);
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = ValMap::from(vec![(k.clone(), Value::text(expected_val.clone()))]);
    assert_eq!(&model.state, &expected);

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.state));
    assert_eq!(event, MapEvent::Update(k.clone()));
    match cmd {
        UntypedMapModification::Update(cmd_k, cmd_v) => {
            assert_eq!(&cmd_k, &k);
            assert!(Arc::ptr_eq(&cmd_v, model.state.get(&k).unwrap()));
        }
        ow => {
            panic!("{:?} is not an insertion.", ow);
        }
    }
    assert!(err.is_none());

    let result = rx_before.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_ok());
    let maybe_old_val = response.unwrap();
    assert!(maybe_old_val.is_some());
    let old_val = maybe_old_val.unwrap();
    assert_eq!(old_val, Arc::new(Value::text(original_val)));

    let result = rx_after.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_ok());
    let maybe_new_val = response.unwrap();
    assert!(maybe_new_val.is_some());
    let new_val = maybe_new_val.unwrap();
    assert_eq!(new_val, Arc::new(Value::text(expected_val)));
}

#[test]
fn update_to_undefined_action() {
    let original_val = "".to_string();

    let k = Value::Int32Value(13);

    let mut state = DownlinkState::Synced;
    let mut model = make_model_with(13, original_val.clone());
    let machine = MapStateMachine::unvalidated();

    let (action, mut rx_before, mut rx_after) = make_modify(13);
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = ValMap::new();
    assert_eq!(&model.state, &expected);

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.state));
    assert_eq!(event, MapEvent::Remove(k.clone()));
    match cmd {
        UntypedMapModification::Remove(cmd_k) => {
            assert_eq!(&cmd_k, &k);
        }
        ow => {
            panic!("{:?} is not an removal.", ow);
        }
    }
    assert!(err.is_none());

    let result = rx_before.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_ok());
    let maybe_old_val = response.unwrap();
    assert!(maybe_old_val.is_some());
    let old_val = maybe_old_val.unwrap();
    assert_eq!(old_val, Arc::new(Value::text(original_val)));

    let result = rx_after.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_ok());
    let maybe_new_val = response.unwrap();
    assert!(maybe_new_val.is_none());
}

#[test]
fn update_action_with_invalid_key() {
    let original_val = "original".to_string();

    let k = Value::from(13);
    let key_schema = StandardSchema::OfKind(ValueKind::Int32);

    let mut state = DownlinkState::Synced;
    let mut model = make_model_with(13, original_val.clone());
    let machine = MapStateMachine::new(key_schema.clone(), StandardSchema::OfKind(ValueKind::Text));

    let (action, mut rx_before, mut rx_after) = make_modify_raw(Value::text("a"));
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = ValMap::from(vec![(k.clone(), Value::text(original_val))]);
    assert_eq!(&model.state, &expected);

    assert_eq!(response, Response::none());

    let expected_err = DownlinkError::SchemaViolation(Value::text("a"), key_schema);

    let result = rx_before.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_err());
    let error = response.err().unwrap();
    assert_eq!(&error, &expected_err);

    let result = rx_after.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_err());
    let error = response.err().unwrap();
    assert_eq!(&error, &expected_err);
}

#[test]
fn update_action_with_invalid_value() {
    let original_val = "original".to_string();

    let k = Value::from(13);
    let value_schema = StandardSchema::OfKind(ValueKind::Text);

    let mut state = DownlinkState::Synced;
    let mut model = make_model_with(13, original_val.clone());
    let machine = MapStateMachine::new(
        StandardSchema::OfKind(ValueKind::Int32),
        value_schema.clone(),
    );

    let (action, mut rx_before, mut rx_after) = make_modify_bad(13);
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = ValMap::from(vec![(k.clone(), Value::text(original_val))]);
    assert_eq!(&model.state, &expected);

    assert_eq!(response, Response::none());

    let expected_err = DownlinkError::SchemaViolation(Value::BooleanValue(true), value_schema);

    let result = rx_before.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_err());
    let error = response.err().unwrap();
    assert_eq!(&error, &expected_err);

    let result = rx_after.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_err());
    let error = response.err().unwrap();
    assert_eq!(&error, &expected_err);
}

#[test]
fn update_action_dropped_receiver() {
    let original_val = "original".to_string();
    let expected_val = original_val.to_uppercase();

    let k = Value::Int32Value(13);

    let mut state = DownlinkState::Synced;
    let mut model = make_model_with(13, original_val.clone());
    let machine = MapStateMachine::unvalidated();

    let (action, rx_before, mut rx_after) = make_modify(13);

    drop(rx_before);

    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = ValMap::from(vec![(k.clone(), Value::text(expected_val.clone()))]);
    assert_eq!(&model.state, &expected);

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.state));
    assert_eq!(event, MapEvent::Update(k.clone()));
    match cmd {
        UntypedMapModification::Update(cmd_k, cmd_v) => {
            assert_eq!(&cmd_k, &k);
            assert!(Arc::ptr_eq(&cmd_v, model.state.get(&k).unwrap()));
        }
        ow => {
            panic!("{:?} is not an insertion.", ow);
        }
    }
    assert_eq!(err, Some(TransitionError::ReceiverDropped));

    let result = rx_after.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_ok());
    let maybe_new_val = response.unwrap();
    assert!(maybe_new_val.is_some());
    let old_val = maybe_new_val.unwrap();
    assert_eq!(old_val, Arc::new(Value::text(expected_val)));
}

#[test]
fn try_update_to_successful_defined_action() {
    let original_val = "original".to_string();
    let expected_val = original_val.to_uppercase();

    let k = Value::Int32Value(13);

    let mut state = DownlinkState::Synced;
    let mut model = make_model_with(13, original_val.clone());
    let machine = MapStateMachine::unvalidated();

    let (action, mut rx_before, mut rx_after) = make_try_update(13);
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = ValMap::from(vec![(k.clone(), Value::text(&expected_val))]);
    assert_eq!(&model.state, &expected);

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.state));
    assert_eq!(event, MapEvent::Update(k.clone()));
    match cmd {
        UntypedMapModification::Update(cmd_k, cmd_v) => {
            assert_eq!(&cmd_k, &k);
            assert!(Arc::ptr_eq(&cmd_v, model.state.get(&k).unwrap()));
        }
        ow => {
            panic!("{:?} is not an insertion.", ow);
        }
    }
    assert!(err.is_none());

    let result = rx_before.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_ok());
    let maybe_old_val = response.unwrap();
    assert!(maybe_old_val.is_ok());
    let old_result = maybe_old_val.unwrap();
    assert!(old_result.is_some());
    let old_val = old_result.unwrap();
    assert_eq!(old_val, Arc::new(Value::text(original_val)));

    let result = rx_after.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_ok());
    let maybe_new_val = response.unwrap();
    assert!(maybe_new_val.is_ok());
    let new_result = maybe_new_val.unwrap();
    assert!(new_result.is_some());
    let new_val = new_result.unwrap();
    assert_eq!(new_val, Arc::new(Value::text(&expected_val)));
}

#[test]
fn try_update_to_undefined_action() {
    let original_val = "".to_string();

    let k = Value::Int32Value(13);

    let mut state = DownlinkState::Synced;
    let mut model = make_model_with(13, original_val.clone());
    let machine = MapStateMachine::unvalidated();

    let (action, mut rx_before, mut rx_after) = make_try_update(13);
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = ValMap::new();
    assert_eq!(&model.state, &expected);

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.state));
    assert_eq!(event, MapEvent::Remove(k.clone()));
    match cmd {
        UntypedMapModification::Remove(cmd_k) => {
            assert_eq!(&cmd_k, &k);
        }
        ow => {
            panic!("{:?} is not an removal.", ow);
        }
    }
    assert!(err.is_none());

    let result = rx_before.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_ok());
    let maybe_old_val = response.unwrap();
    assert!(maybe_old_val.is_ok());
    let old_result = maybe_old_val.unwrap();
    assert!(old_result.is_some());
    let old_val = old_result.unwrap();
    assert_eq!(old_val, Arc::new(Value::text(original_val)));

    let result = rx_after.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_ok());
    let new_result = response.unwrap();
    assert!(new_result.is_ok());
    let maybe_new_val = new_result.unwrap();
    assert!(maybe_new_val.is_none());
}

#[test]
fn try_update_to_failed_action() {
    let original_val = "a".to_string();

    let k = Value::Int32Value(13);

    let mut state = DownlinkState::Synced;
    let mut model = make_model_with(13, original_val.clone());
    let machine = MapStateMachine::unvalidated();

    let (action, mut rx_before, mut rx_after) = make_try_update(13);
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = ValMap::from(vec![(k.clone(), Value::text(original_val))]);
    assert_eq!(&model.state, &expected);

    assert_eq!(response, Response::none());

    let result = rx_before.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_ok());
    let maybe_old_val = response.unwrap();
    assert!(maybe_old_val.is_err());

    let result = rx_after.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_ok());
    let maybe_new_val = response.unwrap();
    assert!(maybe_new_val.is_err());
}

#[test]
fn try_update_action_with_invalid_key() {
    let original_val = "original".to_string();

    let k = Value::from(13);
    let key_schema = StandardSchema::OfKind(ValueKind::Int32);

    let mut state = DownlinkState::Synced;
    let mut model = make_model_with(13, original_val.clone());
    let machine = MapStateMachine::new(key_schema.clone(), StandardSchema::OfKind(ValueKind::Text));

    let (action, mut rx_before, mut rx_after) = make_try_update_raw(Value::text("a"));
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = ValMap::from(vec![(k.clone(), Value::text(original_val))]);
    assert_eq!(&model.state, &expected);

    assert_eq!(response, Response::none());

    let expected_err = DownlinkError::SchemaViolation(Value::text("a"), key_schema);

    let result = rx_before.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_err());
    let error = response.err().unwrap();
    assert_eq!(&error, &expected_err);

    let result = rx_after.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_err());
    let error = response.err().unwrap();
    assert_eq!(&error, &expected_err);
}

#[test]
fn try_update_action_with_invalid_value() {
    let original_val = "original".to_string();

    let k = Value::from(13);
    let value_schema = StandardSchema::OfKind(ValueKind::Text);

    let mut state = DownlinkState::Synced;
    let mut model = make_model_with(13, original_val.clone());
    let machine = MapStateMachine::new(
        StandardSchema::OfKind(ValueKind::Int32),
        value_schema.clone(),
    );

    let (action, mut rx_before, mut rx_after) = make_try_update_bad(13);
    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = ValMap::from(vec![(k.clone(), Value::text(original_val))]);
    assert_eq!(&model.state, &expected);

    assert_eq!(response, Response::none());

    let expected_err = DownlinkError::SchemaViolation(Value::BooleanValue(true), value_schema);

    let result = rx_before.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_err());
    let error = response.err().unwrap();
    assert_eq!(&error, &expected_err);

    let result = rx_after.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_err());
    let error = response.err().unwrap();
    assert_eq!(&error, &expected_err);
}

#[test]
fn try_update_action_with_dropped_receiver() {
    let original_val = "original".to_string();
    let expected_val = original_val.to_uppercase();

    let k = Value::Int32Value(13);

    let mut state = DownlinkState::Synced;
    let mut model = make_model_with(13, original_val.clone());
    let machine = MapStateMachine::unvalidated();

    let (action, mut rx_before, rx_after) = make_try_update(13);

    drop(rx_after);

    let maybe_response =
        machine.handle_operation(&mut state, &mut model, Operation::Action(action));

    assert!(maybe_response.is_ok());
    let response = maybe_response.unwrap();

    assert_eq!(state, DownlinkState::Synced);
    let expected = ValMap::from(vec![(k.clone(), Value::text(expected_val.clone()))]);
    assert_eq!(&model.state, &expected);

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.state));
    assert_eq!(event, MapEvent::Update(k.clone()));
    match cmd {
        UntypedMapModification::Update(cmd_k, cmd_v) => {
            assert_eq!(&cmd_k, &k);
            assert!(Arc::ptr_eq(&cmd_v, model.state.get(&k).unwrap()));
        }
        ow => {
            panic!("{:?} is not an insertion.", ow);
        }
    }
    assert_eq!(err, Some(TransitionError::ReceiverDropped));

    let result = rx_before.try_recv();
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_ok());
    let maybe_old_val = response.unwrap();
    assert!(maybe_old_val.is_ok());
    let old_result = maybe_old_val.unwrap();
    assert!(old_result.is_some());
    let old_val = old_result.unwrap();
    assert_eq!(old_val, Arc::new(Value::text(original_val)));
}

#[test]
fn test_val_map_i32() {
    let mut map = ValMap::new();

    map.insert(Value::Int32Value(1), Arc::new(Value::Int32Value(10)));
    map.insert(Value::Int32Value(2), Arc::new(Value::Int32Value(20)));
    map.insert(Value::Int32Value(3), Arc::new(Value::Int32Value(30)));

    assert_eq!(
        map.get(&Value::Int32Value(1)).unwrap(),
        &Arc::new(Value::Int32Value(10))
    );

    assert_eq!(
        map.get(&Value::Int64Value(2)).unwrap(),
        &Arc::new(Value::Int32Value(20))
    );

    assert_eq!(
        map.get(&Value::UInt32Value(3)).unwrap(),
        &Arc::new(Value::Int32Value(30))
    );

    assert_eq!(
        map.get(&Value::UInt64Value(1)).unwrap(),
        &Arc::new(Value::Int32Value(10))
    );

    assert_eq!(
        map.get(&Value::Float64Value(2.0)).unwrap(),
        &Arc::new(Value::Int32Value(20))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(3))).unwrap(),
        &Arc::new(Value::Int32Value(30))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(1u32))).unwrap(),
        &Arc::new(Value::Int32Value(10))
    );
}

#[test]
fn test_val_map_i64() {
    let mut map = ValMap::new();

    map.insert(Value::Int64Value(1), Arc::new(Value::Int64Value(10)));
    map.insert(Value::Int64Value(2), Arc::new(Value::Int64Value(20)));
    map.insert(Value::Int64Value(3), Arc::new(Value::Int64Value(30)));

    assert_eq!(
        map.get(&Value::Int32Value(1)).unwrap(),
        &Arc::new(Value::Int64Value(10))
    );

    assert_eq!(
        map.get(&Value::Int64Value(2)).unwrap(),
        &Arc::new(Value::Int64Value(20))
    );

    assert_eq!(
        map.get(&Value::UInt32Value(3)).unwrap(),
        &Arc::new(Value::Int64Value(30))
    );

    assert_eq!(
        map.get(&Value::UInt64Value(1)).unwrap(),
        &Arc::new(Value::Int64Value(10))
    );

    assert_eq!(
        map.get(&Value::Float64Value(2.0)).unwrap(),
        &Arc::new(Value::Int64Value(20))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(3))).unwrap(),
        &Arc::new(Value::Int64Value(30))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(1u32))).unwrap(),
        &Arc::new(Value::Int64Value(10))
    );
}

#[test]
fn test_val_map_u32() {
    let mut map = ValMap::new();

    map.insert(Value::UInt32Value(1), Arc::new(Value::UInt32Value(10)));
    map.insert(Value::UInt32Value(2), Arc::new(Value::UInt32Value(20)));
    map.insert(Value::UInt32Value(3), Arc::new(Value::UInt32Value(30)));

    assert_eq!(
        map.get(&Value::Int32Value(1)).unwrap(),
        &Arc::new(Value::UInt32Value(10))
    );

    assert_eq!(
        map.get(&Value::Int64Value(2)).unwrap(),
        &Arc::new(Value::UInt32Value(20))
    );

    assert_eq!(
        map.get(&Value::UInt32Value(3)).unwrap(),
        &Arc::new(Value::UInt32Value(30))
    );

    assert_eq!(
        map.get(&Value::UInt64Value(1)).unwrap(),
        &Arc::new(Value::UInt32Value(10))
    );

    assert_eq!(
        map.get(&Value::Float64Value(2.0)).unwrap(),
        &Arc::new(Value::UInt32Value(20))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(3))).unwrap(),
        &Arc::new(Value::UInt32Value(30))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(1u32))).unwrap(),
        &Arc::new(Value::UInt32Value(10))
    );
}

#[test]
fn test_val_map_u64() {
    let mut map = ValMap::new();

    map.insert(Value::UInt64Value(1), Arc::new(Value::UInt64Value(10)));
    map.insert(Value::UInt64Value(2), Arc::new(Value::UInt64Value(20)));
    map.insert(Value::UInt64Value(3), Arc::new(Value::UInt64Value(30)));

    assert_eq!(
        map.get(&Value::Int32Value(1)).unwrap(),
        &Arc::new(Value::UInt64Value(10))
    );

    assert_eq!(
        map.get(&Value::Int64Value(2)).unwrap(),
        &Arc::new(Value::UInt64Value(20))
    );

    assert_eq!(
        map.get(&Value::UInt32Value(3)).unwrap(),
        &Arc::new(Value::UInt64Value(30))
    );

    assert_eq!(
        map.get(&Value::UInt64Value(1)).unwrap(),
        &Arc::new(Value::UInt64Value(10))
    );

    assert_eq!(
        map.get(&Value::Float64Value(2.0)).unwrap(),
        &Arc::new(Value::UInt64Value(20))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(3))).unwrap(),
        &Arc::new(Value::UInt64Value(30))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(1u32))).unwrap(),
        &Arc::new(Value::UInt64Value(10))
    );
}

#[test]
fn test_val_map_f64() {
    let mut map = ValMap::new();

    map.insert(
        Value::Float64Value(1.0),
        Arc::new(Value::Float64Value(10.0)),
    );
    map.insert(
        Value::Float64Value(2.0),
        Arc::new(Value::Float64Value(20.0)),
    );
    map.insert(
        Value::Float64Value(3.0),
        Arc::new(Value::Float64Value(30.0)),
    );

    assert_eq!(
        map.get(&Value::Int32Value(1)).unwrap(),
        &Arc::new(Value::Float64Value(10.0))
    );

    assert_eq!(
        map.get(&Value::Int64Value(2)).unwrap(),
        &Arc::new(Value::Float64Value(20.0))
    );

    assert_eq!(
        map.get(&Value::UInt32Value(3)).unwrap(),
        &Arc::new(Value::Float64Value(30.0))
    );

    assert_eq!(
        map.get(&Value::UInt64Value(1)).unwrap(),
        &Arc::new(Value::Float64Value(10.0))
    );

    assert_eq!(
        map.get(&Value::Float64Value(2.0)).unwrap(),
        &Arc::new(Value::Float64Value(20.0))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(3))).unwrap(),
        &Arc::new(Value::Float64Value(30.0))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(1u32))).unwrap(),
        &Arc::new(Value::Float64Value(10.0))
    );
}

#[test]
fn test_val_map_big_int() {
    let mut map = ValMap::new();

    map.insert(
        Value::BigInt(BigInt::from(1)),
        Arc::new(Value::BigInt(BigInt::from(10))),
    );
    map.insert(
        Value::BigInt(BigInt::from(2)),
        Arc::new(Value::BigInt(BigInt::from(20))),
    );
    map.insert(
        Value::BigInt(BigInt::from(3)),
        Arc::new(Value::BigInt(BigInt::from(30))),
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(1))).unwrap(),
        &Arc::new(Value::BigInt(BigInt::from(10)))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(2))).unwrap(),
        &Arc::new(Value::BigInt(BigInt::from(20)))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(3))).unwrap(),
        &Arc::new(Value::BigInt(BigInt::from(30)))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(1))).unwrap(),
        &Arc::new(Value::BigInt(BigInt::from(10)))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(2))).unwrap(),
        &Arc::new(Value::BigInt(BigInt::from(20)))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(3))).unwrap(),
        &Arc::new(Value::BigInt(BigInt::from(30)))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(1))).unwrap(),
        &Arc::new(Value::BigInt(BigInt::from(10)))
    );
}

#[test]
fn test_val_map_big_uint() {
    let mut map = ValMap::new();

    map.insert(
        Value::BigUint(BigUint::from(1u32)),
        Arc::new(Value::BigUint(BigUint::from(10u32))),
    );
    map.insert(
        Value::BigUint(BigUint::from(2u32)),
        Arc::new(Value::BigUint(BigUint::from(20u32))),
    );
    map.insert(
        Value::BigUint(BigUint::from(3u32)),
        Arc::new(Value::BigUint(BigUint::from(30u32))),
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(1u32))).unwrap(),
        &Arc::new(Value::BigUint(BigUint::from(10u32)))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(2u32))).unwrap(),
        &Arc::new(Value::BigUint(BigUint::from(20u32)))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(3u32))).unwrap(),
        &Arc::new(Value::BigUint(BigUint::from(30u32)))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(1u32))).unwrap(),
        &Arc::new(Value::BigUint(BigUint::from(10u32)))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(2u32))).unwrap(),
        &Arc::new(Value::BigUint(BigUint::from(20u32)))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(3u32))).unwrap(),
        &Arc::new(Value::BigUint(BigUint::from(30u32)))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(1u32))).unwrap(),
        &Arc::new(Value::BigUint(BigUint::from(10u32)))
    );
}

#[test]
fn test_val_map_mixed() {
    let mut map = ValMap::new();

    map.insert(Value::UInt64Value(3), Arc::new(Value::UInt64Value(30)));
    map.insert(Value::Int32Value(6), Arc::new(Value::Int32Value(60)));
    map.insert(
        Value::BigInt(BigInt::from(1)),
        Arc::new(Value::BigInt(BigInt::from(10))),
    );
    map.insert(Value::UInt32Value(7), Arc::new(Value::UInt32Value(70)));
    map.insert(
        Value::BigUint(BigUint::from(5u32)),
        Arc::new(Value::BigUint(BigUint::from(50u32))),
    );
    map.insert(
        Value::Float64Value(2.0),
        Arc::new(Value::Float64Value(20.0)),
    );
    map.insert(Value::Int64Value(4), Arc::new(Value::Int64Value(40)));

    assert_eq!(
        map.get(&Value::Float64Value(1.0)).unwrap(),
        &Arc::new(Value::BigInt(BigInt::from(10)))
    );

    assert_eq!(
        map.get(&Value::Int32Value(2)).unwrap(),
        &Arc::new(Value::Float64Value(20.0))
    );

    assert_eq!(
        map.get(&Value::UInt32Value(3)).unwrap(),
        &Arc::new(Value::UInt64Value(30))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(4))).unwrap(),
        &Arc::new(Value::Int64Value(40))
    );

    assert_eq!(
        map.get(&Value::UInt64Value(5)).unwrap(),
        &Arc::new(Value::BigUint(BigUint::from(50u32)))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(6u32))).unwrap(),
        &Arc::new(Value::Int32Value(60))
    );

    assert_eq!(
        map.get(&Value::Int64Value(7)).unwrap(),
        &Arc::new(Value::UInt32Value(70))
    );
}
