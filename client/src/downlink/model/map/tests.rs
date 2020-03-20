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

fn make_model(state: DownlinkState, contents: ValMap) -> Model<ValMap> {
    Model {
        state,
        data_state: contents,
    }
}

fn make_empty_model(state: DownlinkState) -> Model<ValMap> {
    make_model(state, ValMap::new())
}

fn make_model_with(state: DownlinkState, key: i32, value: String) -> Model<ValMap> {
    let k = Value::Int32Value(key);
    let v = Arc::new(Value::Text(value));
    make_model(state, OrdMap::from(vec![(k, v)]))
}

const STATES: [DownlinkState; 3] = [
    DownlinkState::Unlinked,
    DownlinkState::Linked,
    DownlinkState::Synced,
];

#[test]
fn start_downlink() {
    for s in STATES.iter() {
        let mut model = make_empty_model(*s);
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
    let mut model = make_empty_model(start_state);
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

fn only_event(response: &Response<ViewWithEvent, MapModification<Arc<Value>>>) -> &ViewWithEvent {
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
    let mut model = make_model_with(start_state, 7, "hello".to_owned());
    let response = StateMachine::handle_operation(&mut model, Operation::Message(Message::Synced));

    assert_that!(model.state, eq(DownlinkState::Synced));
    if start_state == DownlinkState::Synced {
        assert_that!(response, eq(Response::none()));
    } else {
        let ViewWithEvent { view, event } = only_event(&response);
        assert_that!(event, eq(&MapEvent::Initial));
        assert!(view.ptr_eq(&model.data_state));
    }
}

#[test]
fn synced_message() {
    for start in STATES.iter() {
        synced_response(*start);
    }
}

fn unlinked_response(start_state: DownlinkState) {
    let mut model = make_model_with(start_state, 7, "hello".to_owned());
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
fn insert_message_unlinked() {
    let k = Value::Int32Value(4);
    let v = Value::Text("hello".to_owned());

    let mut model = make_empty_model(DownlinkState::Unlinked);
    let response = StateMachine::handle_operation(
        &mut model,
        Operation::Message(Message::Action(MapModification::Insert(k, v))),
    );

    assert_that!(model.state, eq(DownlinkState::Unlinked));
    assert_that!(model.data_state.len(), eq(0));
    assert_that!(response, eq(Response::none()));
}

#[test]
fn remove_message_unlinked() {
    let k = Value::Int32Value(4);
    let v = Value::Text("hello".to_owned());

    let mut model = make_model_with(DownlinkState::Unlinked, 4, "hello".to_owned());
    let response = StateMachine::handle_operation(
        &mut model,
        Operation::Message(Message::Action(MapModification::Remove(k.clone()))),
    );

    let expected = ValMap::from(vec![(k, Arc::new(v))]);

    assert_that!(model.state, eq(DownlinkState::Unlinked));
    assert_that!(model.data_state, eq(expected));
    assert_that!(response, eq(Response::none()));
}

#[test]
fn take_message_unlinked() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::Text("hello".to_owned());
    let v2 = Value::Text("world".to_owned());

    let mut model = make_model(
        DownlinkState::Unlinked,
        ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    );
    let response = StateMachine::handle_operation(
        &mut model,
        Operation::Message(Message::Action(MapModification::Take(1))),
    );

    let expected = ValMap::from(vec![
        (k1.clone(), Arc::new(v1.clone())),
        (k2.clone(), Arc::new(v2.clone())),
    ]);

    assert_that!(model.state, eq(DownlinkState::Unlinked));
    assert_that!(model.data_state, eq(expected));
    assert_that!(response, eq(Response::none()));
}

#[test]
fn skip_message_unlinked() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::Text("hello".to_owned());
    let v2 = Value::Text("world".to_owned());

    let mut model = make_model(
        DownlinkState::Unlinked,
        ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    );
    let response = StateMachine::handle_operation(
        &mut model,
        Operation::Message(Message::Action(MapModification::Skip(1))),
    );

    let expected = ValMap::from(vec![
        (k1.clone(), Arc::new(v1.clone())),
        (k2.clone(), Arc::new(v2.clone())),
    ]);

    assert_that!(model.state, eq(DownlinkState::Unlinked));
    assert_that!(model.data_state, eq(expected));
    assert_that!(response, eq(Response::none()));
}

#[test]
fn clear_message_unlinked() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::Text("hello".to_owned());
    let v2 = Value::Text("world".to_owned());

    let mut model = make_model(
        DownlinkState::Unlinked,
        ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    );
    let response = StateMachine::handle_operation(
        &mut model,
        Operation::Message(Message::Action(MapModification::Clear)),
    );

    let expected = ValMap::from(vec![
        (k1.clone(), Arc::new(v1.clone())),
        (k2.clone(), Arc::new(v2.clone())),
    ]);

    assert_that!(model.state, eq(DownlinkState::Unlinked));
    assert_that!(model.data_state, eq(expected));
    assert_that!(response, eq(Response::none()));
}

#[test]
fn insert_message_linked() {
    let k = Value::Int32Value(4);
    let v = Value::Text("hello".to_owned());

    let mut model = make_empty_model(DownlinkState::Linked);
    let response = StateMachine::handle_operation(
        &mut model,
        Operation::Message(Message::Action(MapModification::Insert(
            k.clone(),
            v.clone(),
        ))),
    );

    let expected = ValMap::from(vec![(k, Arc::new(v))]);

    assert_that!(model.state, eq(DownlinkState::Linked));
    assert_that!(model.data_state, eq(expected));
    assert_that!(response, eq(Response::none()));
}

#[test]
fn remove_message_linked() {
    let k = Value::Int32Value(4);

    let mut model = make_model_with(DownlinkState::Linked, 4, "hello".to_owned());
    let response = StateMachine::handle_operation(
        &mut model,
        Operation::Message(Message::Action(MapModification::Remove(k.clone()))),
    );

    assert_that!(model.state, eq(DownlinkState::Linked));
    assert_that!(model.data_state.len(), eq(0));
    assert_that!(response, eq(Response::none()));
}

#[test]
fn take_message_linked() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::Text("hello".to_owned());
    let v2 = Value::Text("world".to_owned());

    let mut model = make_model(
        DownlinkState::Linked,
        ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    );
    let response = StateMachine::handle_operation(
        &mut model,
        Operation::Message(Message::Action(MapModification::Take(1))),
    );

    let expected = ValMap::from(vec![(k1.clone(), Arc::new(v1.clone()))]);

    assert_that!(model.state, eq(DownlinkState::Linked));
    assert_that!(model.data_state, eq(expected));
    assert_that!(response, eq(Response::none()));
}

#[test]
fn skip_message_linked() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::Text("hello".to_owned());
    let v2 = Value::Text("world".to_owned());

    let mut model = make_model(
        DownlinkState::Linked,
        ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    );
    let response = StateMachine::handle_operation(
        &mut model,
        Operation::Message(Message::Action(MapModification::Skip(1))),
    );

    let expected = ValMap::from(vec![(k2.clone(), Arc::new(v2.clone()))]);

    assert_that!(model.state, eq(DownlinkState::Linked));
    assert_that!(model.data_state, eq(expected));
    assert_that!(response, eq(Response::none()));
}

#[test]
fn clear_message_linked() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::Text("hello".to_owned());
    let v2 = Value::Text("world".to_owned());

    let mut model = make_model(
        DownlinkState::Linked,
        ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    );
    let response = StateMachine::handle_operation(
        &mut model,
        Operation::Message(Message::Action(MapModification::Clear)),
    );

    assert_that!(model.state, eq(DownlinkState::Linked));
    assert_that!(model.data_state.len(), eq(0));
    assert_that!(response, eq(Response::none()));
}

#[test]
fn insert_message_synced() {
    let k = Value::Int32Value(4);
    let v = Value::Text("hello".to_owned());

    let mut model = make_empty_model(DownlinkState::Synced);
    let response = StateMachine::handle_operation(
        &mut model,
        Operation::Message(Message::Action(MapModification::Insert(
            k.clone(),
            v.clone(),
        ))),
    );

    let expected = ValMap::from(vec![(k.clone(), Arc::new(v))]);

    assert_that!(model.state, eq(DownlinkState::Synced));
    assert_that!(&model.data_state, eq(&expected));

    let ViewWithEvent { view, event } = only_event(&response);
    assert!(view.ptr_eq(&model.data_state));
    let expected_event = MapEvent::Insert(k);
    assert_that!(event, eq(&expected_event));
}

#[test]
fn remove_message_synced() {
    let k = Value::Int32Value(4);

    let mut model = make_model_with(DownlinkState::Synced, 4, "hello".to_owned());
    let response = StateMachine::handle_operation(
        &mut model,
        Operation::Message(Message::Action(MapModification::Remove(k.clone()))),
    );

    assert_that!(model.state, eq(DownlinkState::Synced));
    assert_that!(model.data_state.len(), eq(0));

    let ViewWithEvent { view, event } = only_event(&response);
    assert!(view.ptr_eq(&model.data_state));
    let expected_event = MapEvent::Remove(k);
    assert_that!(event, eq(&expected_event));
}

#[test]
fn take_message_synced() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::Text("hello".to_owned());
    let v2 = Value::Text("world".to_owned());

    let mut model = make_model(
        DownlinkState::Synced,
        ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    );
    let response = StateMachine::handle_operation(
        &mut model,
        Operation::Message(Message::Action(MapModification::Take(1))),
    );

    let expected = ValMap::from(vec![(k1.clone(), Arc::new(v1.clone()))]);

    assert_that!(model.state, eq(DownlinkState::Synced));
    assert_that!(&model.data_state, eq(&expected));

    let ViewWithEvent { view, event } = only_event(&response);
    assert!(view.ptr_eq(&model.data_state));
    let expected_event = MapEvent::Take(1);
    assert_that!(event, eq(&expected_event));
}

#[test]
fn skip_message_synced() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::Text("hello".to_owned());
    let v2 = Value::Text("world".to_owned());

    let mut model = make_model(
        DownlinkState::Synced,
        ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    );
    let response = StateMachine::handle_operation(
        &mut model,
        Operation::Message(Message::Action(MapModification::Skip(1))),
    );

    let expected = ValMap::from(vec![(k2.clone(), Arc::new(v2.clone()))]);

    assert_that!(model.state, eq(DownlinkState::Synced));
    assert_that!(&model.data_state, eq(&expected));

    let ViewWithEvent { view, event } = only_event(&response);
    assert!(view.ptr_eq(&model.data_state));
    let expected_event = MapEvent::Skip(1);
    assert_that!(event, eq(&expected_event));
}

#[test]
fn clear_message_synced() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::Text("hello".to_owned());
    let v2 = Value::Text("world".to_owned());

    let mut model = make_model(
        DownlinkState::Synced,
        ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    );
    let response = StateMachine::handle_operation(
        &mut model,
        Operation::Message(Message::Action(MapModification::Clear)),
    );

    assert_that!(model.state, eq(DownlinkState::Synced));
    assert_that!(model.data_state.len(), eq(0));

    let ViewWithEvent { view, event } = only_event(&response);
    assert!(view.ptr_eq(&model.data_state));
    let expected_event = MapEvent::Clear;
    assert_that!(event, eq(&expected_event));
}

fn make_get_map() -> (MapAction, oneshot::Receiver<ValMap>) {
    let (tx, rx) = oneshot::channel();
    (MapAction::get_map(Request::new(tx)), rx)
}

fn make_get(key: i32) -> (MapAction, oneshot::Receiver<Option<Arc<Value>>>) {
    let (tx, rx) = oneshot::channel();
    (MapAction::get(Value::Int32Value(key), Request::new(tx)), rx)
}

#[test]
fn get_action() {
    let k = Value::Int32Value(13);
    let v = Value::Text("stuff".to_owned());

    let mut model = make_model_with(DownlinkState::Synced, 13, "stuff".to_owned());
    let (action, mut rx) = make_get_map();
    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    assert_that!(model.state, eq(DownlinkState::Synced));
    let expected = ValMap::from(vec![(k, v)]);
    assert_that!(&model.data_state, eq(&expected));
    assert_that!(response, eq(Response::none()));

    let result = rx.try_recv();
    assert_that!(&result, ok());
    let get_val = result.unwrap();
    assert!(get_val.ptr_eq(&model.data_state));
}

#[test]
fn get_by_defined_key_action() {
    let k = Value::Int32Value(13);
    let v = Value::Text("stuff".to_owned());

    let mut model = make_model_with(DownlinkState::Synced, 13, "stuff".to_owned());
    let (action, mut rx) = make_get(13);
    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    assert_that!(model.state, eq(DownlinkState::Synced));
    let expected = ValMap::from(vec![(k.clone(), v)]);
    assert_that!(&model.data_state, eq(&expected));
    assert_that!(response, eq(Response::none()));

    let result = rx.try_recv();
    assert_that!(&result, ok());
    let maybe_get_val = result.unwrap();
    assert_that!(&maybe_get_val, some());
    let get_val = maybe_get_val.unwrap();
    assert!(Arc::ptr_eq(&get_val, model.data_state.get(&k).unwrap()));
}

#[test]
fn get_by_undefined_key_action() {
    let k = Value::Int32Value(13);
    let v = Value::Text("stuff".to_owned());

    let mut model = make_model_with(DownlinkState::Synced, 13, "stuff".to_owned());
    let (action, mut rx) = make_get(-1);
    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    assert_that!(model.state, eq(DownlinkState::Synced));
    let expected = ValMap::from(vec![(k.clone(), v)]);
    assert_that!(&model.data_state, eq(&expected));
    assert_that!(response, eq(Response::none()));

    let result = rx.try_recv();
    assert_that!(&result, ok());
    let maybe_get_val = result.unwrap();
    assert_that!(&maybe_get_val, none());
}

fn make_insert(key: i32, value: String) -> (MapAction, oneshot::Receiver<Option<Arc<Value>>>) {
    let (tx, rx) = oneshot::channel();
    (
        MapAction::insert_and_await(Value::Int32Value(key), Value::Text(value), Request::new(tx)),
        rx,
    )
}

fn event_and_cmd(
    response: Response<ViewWithEvent, MapModification<Arc<Value>>>,
) -> (
    ViewWithEvent,
    MapModification<Arc<Value>>,
    Option<TransitionError>,
) {
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
fn insert_to_undefined_action() {
    let k = Value::Int32Value(13);
    let v = Value::Text("stuff".to_owned());

    let mut model = make_empty_model(DownlinkState::Synced);
    let (action, mut rx) = make_insert(13, "stuff".to_owned());
    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    assert_that!(model.state, eq(DownlinkState::Synced));
    let expected = ValMap::from(vec![(k.clone(), v.clone())]);
    assert_that!(&model.data_state, eq(&expected));

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.data_state));
    assert_that!(event, eq(MapEvent::Insert(k.clone())));
    match cmd {
        MapModification::Insert(cmd_k, cmd_v) => {
            assert_that!(&cmd_k, eq(&k));
            assert!(Arc::ptr_eq(&cmd_v, model.data_state.get(&k).unwrap()));
        }
        ow => {
            panic!("{:?} is not an insertion.", ow);
        }
    }
    assert_that!(err, none());

    let result = rx.try_recv();
    assert_that!(&result, ok());
    let maybe_old_val = result.unwrap();
    assert_that!(&maybe_old_val, none());
}

#[test]
fn insert_action_dropped_listener() {
    let k = Value::Int32Value(13);
    let v = Value::Text("stuff".to_owned());

    let mut model = make_empty_model(DownlinkState::Synced);
    let (action, rx) = make_insert(13, "stuff".to_owned());

    drop(rx);

    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    assert_that!(model.state, eq(DownlinkState::Synced));
    let expected = ValMap::from(vec![(k.clone(), v.clone())]);
    assert_that!(&model.data_state, eq(&expected));

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.data_state));
    assert_that!(event, eq(MapEvent::Insert(k.clone())));
    match cmd {
        MapModification::Insert(cmd_k, cmd_v) => {
            assert_that!(&cmd_k, eq(&k));
            assert!(Arc::ptr_eq(&cmd_v, model.data_state.get(&k).unwrap()));
        }
        ow => {
            panic!("{:?} is not an insertion.", ow);
        }
    }
    assert_that!(err, eq(Some(TransitionError::ReceiverDropped)));
}

#[test]
fn insert_to_defined_action() {
    let original_val = "original".to_owned();
    let new_val = "stuff".to_owned();

    let k = Value::Int32Value(13);

    let mut model = make_model_with(DownlinkState::Synced, 13, original_val.clone());

    let (action, mut rx) = make_insert(13, new_val.clone());
    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    assert_that!(model.state, eq(DownlinkState::Synced));
    let expected = ValMap::from(vec![(k.clone(), Value::text(new_val.clone()))]);
    assert_that!(&model.data_state, eq(&expected));

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.data_state));
    assert_that!(event, eq(MapEvent::Insert(k.clone())));
    match cmd {
        MapModification::Insert(cmd_k, cmd_v) => {
            assert_that!(&cmd_k, eq(&k));
            assert!(Arc::ptr_eq(&cmd_v, model.data_state.get(&k).unwrap()));
        }
        ow => {
            panic!("{:?} is not an insertion.", ow);
        }
    }
    assert_that!(err, none());

    let result = rx.try_recv();
    assert_that!(&result, ok());
    let maybe_old_val = result.unwrap();
    assert_that!(&maybe_old_val, some());
    let old_val = maybe_old_val.unwrap();
    assert_that!(old_val, eq(Arc::new(Value::Text(original_val.clone()))))
}

fn make_remove(key: i32) -> (MapAction, oneshot::Receiver<Option<Arc<Value>>>) {
    let (tx, rx) = oneshot::channel();
    (
        MapAction::remove_and_await(Value::Int32Value(key), Request::new(tx)),
        rx,
    )
}

#[test]
fn remove_undefined_action() {
    let mut model = make_empty_model(DownlinkState::Synced);
    let (action, mut rx) = make_remove(43);

    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    assert_that!(model.state, eq(DownlinkState::Synced));
    assert_that!(model.data_state.len(), eq(0));
    assert_that!(response, eq(Response::none()));

    let result = rx.try_recv();
    assert_that!(&result, ok());
    let maybe_old_val = result.unwrap();
    assert_that!(&maybe_old_val, none());
}

#[test]
fn remove_action_dropped_listener() {
    let mut model = make_empty_model(DownlinkState::Synced);
    let (action, rx) = make_remove(43);

    drop(rx);

    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    assert_that!(model.state, eq(DownlinkState::Synced));
    assert_that!(model.data_state.len(), eq(0));
    assert_that!(
        response,
        eq(Response::none().with_error(TransitionError::ReceiverDropped))
    );
}

#[test]
fn remove_defined_action() {
    let k = Value::Int32Value(13);
    let original_val = "original".to_owned();

    let mut model = make_model_with(DownlinkState::Synced, 13, original_val.clone());

    let (action, mut rx) = make_remove(13);
    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    assert_that!(model.state, eq(DownlinkState::Synced));

    assert_that!(model.data_state.len(), eq(0));

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.data_state));
    assert_that!(event, eq(MapEvent::Remove(k.clone())));
    match cmd {
        MapModification::Remove(cmd_k) => {
            assert_that!(&cmd_k, eq(&k));
        }
        ow => {
            panic!("{:?} is not a removal.", ow);
        }
    }
    assert_that!(err, none());

    let result = rx.try_recv();
    assert_that!(&result, ok());
    let maybe_old_val = result.unwrap();
    assert_that!(&maybe_old_val, some());
    let old_val = maybe_old_val.unwrap();
    assert_that!(old_val, eq(Arc::new(Value::Text(original_val))))
}

fn make_take(
    n: usize,
) -> (
    MapAction,
    oneshot::Receiver<ValMap>,
    oneshot::Receiver<ValMap>,
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
    let v1 = Value::Text("hello".to_owned());
    let v2 = Value::Text("world".to_owned());

    let mut model = make_model(
        DownlinkState::Synced,
        ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    );

    let expected_before = model.data_state.clone();

    let (action, mut rx_before, mut rx_after) = make_take(1);

    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    let expected = ValMap::from(vec![(k1.clone(), Arc::new(v1.clone()))]);

    assert_that!(model.state, eq(DownlinkState::Synced));
    assert_that!(&model.data_state, eq(&expected));

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.data_state));
    assert_that!(event, eq(MapEvent::Take(1)));
    assert_that!(cmd, eq(MapModification::Take(1)));
    assert_that!(err, none());

    let result_before = rx_before.try_recv();
    assert_that!(&result_before, ok());
    let before_val = result_before.unwrap();
    assert!(before_val.ptr_eq(&expected_before));

    let result_after = rx_after.try_recv();
    assert_that!(&result_after, ok());
    let after_val = result_after.unwrap();
    assert!(after_val.ptr_eq(&model.data_state));
}

#[test]
fn take_action_dropped_before() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::Text("hello".to_owned());
    let v2 = Value::Text("world".to_owned());

    let mut model = make_model(
        DownlinkState::Synced,
        ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    );

    let (action, rx_before, mut rx_after) = make_take(1);

    drop(rx_before);

    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    let expected = ValMap::from(vec![(k1.clone(), Arc::new(v1.clone()))]);

    assert_that!(model.state, eq(DownlinkState::Synced));
    assert_that!(&model.data_state, eq(&expected));

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.data_state));
    assert_that!(event, eq(MapEvent::Take(1)));
    assert_that!(cmd, eq(MapModification::Take(1)));
    assert_that!(err, eq(Some(TransitionError::ReceiverDropped)));

    let result_after = rx_after.try_recv();
    assert_that!(&result_after, ok());
    let after_val = result_after.unwrap();
    assert!(after_val.ptr_eq(&model.data_state));
}

#[test]
fn take_action_dropped_after() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::Text("hello".to_owned());
    let v2 = Value::Text("world".to_owned());

    let mut model = make_model(
        DownlinkState::Synced,
        ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    );

    let expected_before = model.data_state.clone();

    let (action, mut rx_before, rx_after) = make_take(1);

    drop(rx_after);

    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    let expected = ValMap::from(vec![(k1.clone(), Arc::new(v1.clone()))]);

    assert_that!(model.state, eq(DownlinkState::Synced));
    assert_that!(&model.data_state, eq(&expected));

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.data_state));
    assert_that!(event, eq(MapEvent::Take(1)));
    assert_that!(cmd, eq(MapModification::Take(1)));
    assert_that!(err, eq(Some(TransitionError::ReceiverDropped)));

    let result_before = rx_before.try_recv();
    assert_that!(&result_before, ok());
    let before_val = result_before.unwrap();
    assert!(before_val.ptr_eq(&expected_before));
}

#[test]
fn take_action_both_dropped() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::Text("hello".to_owned());
    let v2 = Value::Text("world".to_owned());

    let mut model = make_model(
        DownlinkState::Synced,
        ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    );

    let (action, rx_before, rx_after) = make_take(1);

    drop(rx_before);
    drop(rx_after);

    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    let expected = ValMap::from(vec![(k1.clone(), Arc::new(v1.clone()))]);

    assert_that!(model.state, eq(DownlinkState::Synced));
    assert_that!(&model.data_state, eq(&expected));

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.data_state));
    assert_that!(event, eq(MapEvent::Take(1)));
    assert_that!(cmd, eq(MapModification::Take(1)));
    assert_that!(err, eq(Some(TransitionError::ReceiverDropped)));
}

fn make_skip(
    n: usize,
) -> (
    MapAction,
    oneshot::Receiver<ValMap>,
    oneshot::Receiver<ValMap>,
) {
    let (tx_bef, rx_bef) = oneshot::channel();
    let (tx_aft, rx_aft) = oneshot::channel();
    (
        MapAction::skip_and_await(n, Request::new(tx_bef), Request::new(tx_aft)),
        rx_bef,
        rx_aft,
    )
}

#[test]
fn skip_action() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::Text("hello".to_owned());
    let v2 = Value::Text("world".to_owned());

    let mut model = make_model(
        DownlinkState::Synced,
        ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    );

    let expected_before = model.data_state.clone();

    let (action, mut rx_before, mut rx_after) = make_skip(1);

    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    let expected = ValMap::from(vec![(k2.clone(), Arc::new(v2.clone()))]);

    assert_that!(model.state, eq(DownlinkState::Synced));
    assert_that!(&model.data_state, eq(&expected));

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.data_state));
    assert_that!(event, eq(MapEvent::Skip(1)));
    assert_that!(cmd, eq(MapModification::Skip(1)));
    assert_that!(err, none());

    let result_before = rx_before.try_recv();
    assert_that!(&result_before, ok());
    let before_val = result_before.unwrap();
    assert!(before_val.ptr_eq(&expected_before));

    let result_after = rx_after.try_recv();
    assert_that!(&result_after, ok());
    let after_val = result_after.unwrap();
    assert!(after_val.ptr_eq(&model.data_state));
}

#[test]
fn skip_action_dropped_before() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::Text("hello".to_owned());
    let v2 = Value::Text("world".to_owned());

    let mut model = make_model(
        DownlinkState::Synced,
        ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    );

    let (action, rx_before, mut rx_after) = make_skip(1);

    drop(rx_before);

    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    let expected = ValMap::from(vec![(k2.clone(), Arc::new(v2.clone()))]);

    assert_that!(model.state, eq(DownlinkState::Synced));
    assert_that!(&model.data_state, eq(&expected));

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.data_state));
    assert_that!(event, eq(MapEvent::Skip(1)));
    assert_that!(cmd, eq(MapModification::Skip(1)));
    assert_that!(err, eq(Some(TransitionError::ReceiverDropped)));

    let result_after = rx_after.try_recv();
    assert_that!(&result_after, ok());
    let after_val = result_after.unwrap();
    assert!(after_val.ptr_eq(&model.data_state));
}

#[test]
fn skip_action_dropped_after() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::Text("hello".to_owned());
    let v2 = Value::Text("world".to_owned());

    let mut model = make_model(
        DownlinkState::Synced,
        ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    );

    let expected_before = model.data_state.clone();

    let (action, mut rx_before, rx_after) = make_skip(1);

    drop(rx_after);

    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    let expected = ValMap::from(vec![(k2.clone(), Arc::new(v2.clone()))]);

    assert_that!(model.state, eq(DownlinkState::Synced));
    assert_that!(&model.data_state, eq(&expected));

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.data_state));
    assert_that!(event, eq(MapEvent::Skip(1)));
    assert_that!(cmd, eq(MapModification::Skip(1)));
    assert_that!(err, eq(Some(TransitionError::ReceiverDropped)));

    let result_before = rx_before.try_recv();
    assert_that!(&result_before, ok());
    let before_val = result_before.unwrap();
    assert!(before_val.ptr_eq(&expected_before));
}

#[test]
fn skip_action_dropped_both() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::Text("hello".to_owned());
    let v2 = Value::Text("world".to_owned());

    let mut model = make_model(
        DownlinkState::Synced,
        ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    );

    let (action, rx_before, rx_after) = make_skip(1);

    drop(rx_before);
    drop(rx_after);

    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    let expected = ValMap::from(vec![(k2.clone(), Arc::new(v2.clone()))]);

    assert_that!(model.state, eq(DownlinkState::Synced));
    assert_that!(&model.data_state, eq(&expected));

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.data_state));
    assert_that!(event, eq(MapEvent::Skip(1)));
    assert_that!(cmd, eq(MapModification::Skip(1)));
    assert_that!(err, eq(Some(TransitionError::ReceiverDropped)));
}

fn make_clear() -> (MapAction, oneshot::Receiver<ValMap>) {
    let (tx_bef, rx_bef) = oneshot::channel();
    (MapAction::clear_and_await(Request::new(tx_bef)), rx_bef)
}

#[test]
fn clear_action() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::Text("hello".to_owned());
    let v2 = Value::Text("world".to_owned());

    let mut model = make_model(
        DownlinkState::Synced,
        ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    );

    let expected_before = model.data_state.clone();

    let (action, mut rx_before) = make_clear();

    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    assert_that!(model.state, eq(DownlinkState::Synced));
    assert_that!(model.data_state.len(), eq(0));

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.data_state));
    assert_that!(event, eq(MapEvent::Clear));
    assert_that!(cmd, eq(MapModification::Clear));
    assert_that!(err, none());

    let result_before = rx_before.try_recv();
    assert_that!(&result_before, ok());
    let before_val = result_before.unwrap();
    assert!(before_val.ptr_eq(&expected_before));
}

#[test]
fn clear_action_dropped_receiver() {
    let k1 = Value::Int32Value(4);
    let k2 = Value::Int32Value(6);
    let v1 = Value::Text("hello".to_owned());
    let v2 = Value::Text("world".to_owned());

    let mut model = make_model(
        DownlinkState::Synced,
        ValMap::from(vec![
            (k1.clone(), Arc::new(v1.clone())),
            (k2.clone(), Arc::new(v2.clone())),
        ]),
    );

    let (action, rx_before) = make_clear();

    drop(rx_before);

    let response = StateMachine::handle_operation(&mut model, Operation::Action(action));

    assert_that!(model.state, eq(DownlinkState::Synced));
    assert_that!(model.data_state.len(), eq(0));

    let (ViewWithEvent { view, event }, cmd, err) = event_and_cmd(response);

    assert!(view.ptr_eq(&model.data_state));
    assert_that!(event, eq(MapEvent::Clear));
    assert_that!(cmd, eq(MapModification::Clear));
    assert_that!(err, eq(Some(TransitionError::ReceiverDropped)));
}

#[test]
pub fn clear_to_value() {
    let expected = Value::of_attr("clear");
    assert_that!(&Form::into_value(MapModification::Clear), eq(&expected));
    assert_that!(&Form::as_value(&MapModification::Clear), eq(&expected));
}

type MapModResult = Result<MapModification<Value>, FormDeserializeErr>;

#[test]
pub fn clear_from_value() {
    let rep = Value::of_attr("clear");
    let result1: MapModResult = Form::try_from_value(&rep);
    assert_that!(result1, eq(Ok(MapModification::Clear)));
    let result2: MapModResult = Form::try_convert(rep);
    assert_that!(result2, eq(Ok(MapModification::Clear)));
}

#[test]
pub fn take_to_value() {
    let expected = Value::of_attr(("take", 3));
    assert_that!(&Form::into_value(MapModification::Take(3)), eq(&expected));
    assert_that!(&Form::as_value(&MapModification::Take(3)), eq(&expected));
}

#[test]
pub fn take_from_value() {
    let rep = Value::of_attr(("take", 3));
    let result1: MapModResult = Form::try_from_value(&rep);
    assert_that!(result1, eq(Ok(MapModification::Take(3))));
    let result2: MapModResult = Form::try_convert(rep);
    assert_that!(result2, eq(Ok(MapModification::Take(3))));
}

#[test]
pub fn skip_to_value() {
    let expected = Value::of_attr(("drop", 5));
    assert_that!(&Form::into_value(MapModification::Skip(5)), eq(&expected));
    assert_that!(&Form::as_value(&MapModification::Skip(5)), eq(&expected));
}

#[test]
pub fn skip_from_value() {
    let rep = Value::of_attr(("drop", 5));
    let result1: MapModResult = Form::try_from_value(&rep);
    assert_that!(result1, eq(Ok(MapModification::Skip(5))));
    let result2: MapModResult = Form::try_convert(rep);
    assert_that!(result2, eq(Ok(MapModification::Skip(5))));
}

#[test]
pub fn remove_to_value() {
    let expected = Value::of_attr(("remove", Value::record(vec![Item::slot("key", "hello")])));
    assert_that!(
        &Form::into_value(MapModification::Remove(Value::text("hello"))),
        eq(&expected)
    );
    assert_that!(
        &Form::as_value(&MapModification::Remove(Value::text("hello"))),
        eq(&expected)
    );
}

#[test]
pub fn remove_from_value() {
    let rep = Value::of_attr(("remove", Value::record(vec![Item::slot("key", "hello")])));
    let result1: MapModResult = Form::try_from_value(&rep);
    assert_that!(
        result1,
        eq(Ok(MapModification::Remove(Value::text("hello"))))
    );
    let result2: MapModResult = Form::try_convert(rep);
    assert_that!(
        result2,
        eq(Ok(MapModification::Remove(Value::text("hello"))))
    );
}

#[test]
pub fn simple_insert_to_value() {
    let attr = Attr::of(("insert", Value::record(vec![Item::slot("key", "hello")])));
    let body = Item::ValueItem(Value::Int32Value(2));
    let expected = Value::Record(vec![attr], vec![body]);
    assert_that!(
        &Form::into_value(MapModification::Insert(
            Value::text("hello"),
            Value::Int32Value(2)
        )),
        eq(&expected)
    );
    assert_that!(
        &Form::as_value(&MapModification::Insert(
            Value::text("hello"),
            Value::Int32Value(2)
        )),
        eq(&expected)
    );
}

#[test]
pub fn simple_insert_from_value() {
    let attr = Attr::of(("insert", Value::record(vec![Item::slot("key", "hello")])));
    let body = Item::ValueItem(Value::Int32Value(2));
    let rep = Value::Record(vec![attr], vec![body]);
    let result1: MapModResult = Form::try_from_value(&rep);
    assert_that!(
        result1,
        eq(Ok(MapModification::Insert(
            Value::text("hello"),
            Value::Int32Value(2)
        )))
    );
    let result2: MapModResult = Form::try_convert(rep);
    assert_that!(
        result2,
        eq(Ok(MapModification::Insert(
            Value::text("hello"),
            Value::Int32Value(2)
        )))
    );
}

#[test]
pub fn complex_insert_to_value() {
    let body = Value::Record(vec![Attr::of(("complex", 0))], vec![Item::slot("a", true)]);
    let attr = Attr::of(("insert", Value::record(vec![Item::slot("key", "hello")])));
    let expected = Value::Record(
        vec![attr, Attr::of(("complex", 0))],
        vec![Item::slot("a", true)],
    );
    assert_that!(
        &Form::into_value(MapModification::Insert(Value::text("hello"), body.clone())),
        eq(&expected)
    );
    assert_that!(
        &Form::as_value(&MapModification::Insert(Value::text("hello"), body.clone())),
        eq(&expected)
    );
}

#[test]
pub fn complex_insert_from_value() {
    let body = Value::Record(vec![Attr::of(("complex", 0))], vec![Item::slot("a", true)]);
    let attr = Attr::of(("insert", Value::record(vec![Item::slot("key", "hello")])));
    let rep = Value::Record(
        vec![attr, Attr::of(("complex", 0))],
        vec![Item::slot("a", true)],
    );
    let result1: MapModResult = Form::try_from_value(&rep);
    assert_that!(
        result1,
        eq(Ok(MapModification::Insert(
            Value::text("hello"),
            body.clone()
        )))
    );
    let result2: MapModResult = Form::try_convert(rep);
    assert_that!(
        result2,
        eq(Ok(MapModification::Insert(
            Value::text("hello"),
            body.clone()
        )))
    );
}
