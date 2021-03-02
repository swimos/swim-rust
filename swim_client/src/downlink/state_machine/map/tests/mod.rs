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
use crate::downlink::model::map::{MapEvent, UntypedMapModification};
use crate::downlink::state_machine::{DownlinkStateMachine, EventResult};
use crate::downlink::{Command, DownlinkState, Message};
use swim_common::model::ValueKind;
use swim_common::request::Request;

fn make_model_with(key: i32, value: String) -> ValMap {
    let k = Value::Int32Value(key);
    let v = Arc::new(Value::text(value));
    ValMap::from(vec![(k, v)])
}

const STATES: [DownlinkState; 3] = [
    DownlinkState::Unlinked,
    DownlinkState::Linked,
    DownlinkState::Synced,
];

fn unvalidated() -> MapStateMachine {
    MapStateMachine::new(StandardSchema::Anything, StandardSchema::Anything)
}

#[test]
fn start_downlink() {
    let machine = unvalidated();
    let ((dl_state, data_state), cmd) = machine.initialize();
    assert_eq!(dl_state, DownlinkState::Unlinked);
    assert!(data_state.is_empty());
    assert!(matches!(cmd, Some(Command::Sync)));
}

fn linked_response(start_state: DownlinkState) {
    let machine = unvalidated();
    let mut state = (start_state, ValMap::new());

    let EventResult { result, terminate } = machine.handle_event(&mut state, Message::Linked);

    let (dl_state, data_state) = state;

    assert!(!terminate);
    assert_eq!(result, Ok(None));

    assert_eq!(dl_state, DownlinkState::Linked);
    assert!(data_state.is_empty());
}

#[test]
fn linked_message() {
    for start in STATES.iter() {
        linked_response(*start);
    }
}

fn synced_response(start_state: DownlinkState) {
    let machine = unvalidated();
    let init = make_model_with(7, "hello".to_owned());
    let mut state = (start_state, init.clone());

    let EventResult { result, terminate } = machine.handle_event(&mut state, Message::Synced);

    let (dl_state, data_state) = state;

    assert!(!terminate);
    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(data_state, init);

    if start_state == DownlinkState::Synced {
        assert_eq!(result, Ok(None));
    } else if let Ok(Some(ViewWithEvent { view, event })) = result {
        assert_eq!(view, init);
        assert!(matches!(event, MapEvent::Initial));
    } else {
        panic!("Unexpected response: {:?}", result);
    }
}

#[test]
fn synced_message() {
    for start in STATES.iter() {
        synced_response(*start);
    }
}

fn unlinked_response(start_state: DownlinkState) {
    let machine = unvalidated();
    let mut state = (start_state, ValMap::new());

    let EventResult { result, terminate } = machine.handle_event(&mut state, Message::Unlinked);

    let (dl_state, _data_state) = state;

    assert!(terminate);
    assert_eq!(result, Ok(None));

    assert_eq!(dl_state, DownlinkState::Unlinked);
}

#[test]
fn unlinked_message() {
    for start in STATES.iter() {
        unlinked_response(*start);
    }
}

fn messages() -> impl Iterator<Item = Message<UntypedMapModification<Value>>> {
    vec![
        UntypedMapModification::Update(4.into(), Arc::new("hello".into())),
        UntypedMapModification::Remove(4.into()),
        UntypedMapModification::Drop(1),
        UntypedMapModification::Take(1),
        UntypedMapModification::Clear,
    ]
    .into_iter()
    .map(Message::Action)
}

#[test]
fn messages_when_unlinked() {
    for message in messages() {
        let machine = unvalidated();
        let mut state = (DownlinkState::Unlinked, ValMap::new());

        let EventResult { result, terminate } = machine.handle_event(&mut state, message);

        let (dl_state, data_state) = state;

        assert!(!terminate);
        assert_eq!(result, Ok(None));

        assert_eq!(dl_state, DownlinkState::Unlinked);
        assert!(data_state.is_empty());
    }
}

fn update_response(synced: bool) {
    let init = if synced {
        DownlinkState::Synced
    } else {
        DownlinkState::Linked
    };

    let k = Value::from(4);
    let v = Arc::new(Value::from("hello"));
    let expected = ValMap::from(vec![(k.clone(), v.clone())]);

    let machine = unvalidated();
    let mut state = (init, ValMap::new());

    let message = Message::Action(UntypedMapModification::Update(k.clone(), v));

    let EventResult { result, terminate } = machine.handle_event(&mut state, message);

    let (dl_state, data_state) = state;

    assert!(!terminate);

    if synced {
        if let Ok(Some(ViewWithEvent { view, event })) = result {
            assert_eq!(view, expected);
            assert!(matches!(event, MapEvent::Update(key) if key == k));
        } else {
            panic!("Unexpected response: {:?}", result);
        }
    } else {
        assert_eq!(result, Ok(None));
    }

    assert_eq!(dl_state, init);
    assert_eq!(data_state, expected);
}

#[test]
fn update_when_linked_or_synced() {
    update_response(false);
    update_response(true);
}

fn remove_response(synced: bool) {
    let init = if synced {
        DownlinkState::Synced
    } else {
        DownlinkState::Linked
    };

    let k = Value::from(4);
    let v = Arc::new(Value::from("hello"));
    let start = ValMap::from(vec![(k.clone(), v.clone())]);

    let machine = unvalidated();
    let mut state = (init, start);

    let message = Message::Action(UntypedMapModification::Remove(k.clone()));

    let EventResult { result, terminate } = machine.handle_event(&mut state, message);

    let (dl_state, data_state) = state;

    assert!(!terminate);

    if synced {
        if let Ok(Some(ViewWithEvent { view, event })) = result {
            assert!(view.is_empty());
            assert!(matches!(event, MapEvent::Remove(key) if key == k));
        } else {
            panic!("Unexpected response: {:?}", result);
        }
    } else {
        assert_eq!(result, Ok(None));
    }

    assert_eq!(dl_state, init);
    assert!(data_state.is_empty());
}

#[test]
fn remove_when_linked_or_synced() {
    remove_response(false);
    remove_response(true);
}

fn take_response(synced: bool) {
    let init = if synced {
        DownlinkState::Synced
    } else {
        DownlinkState::Linked
    };

    let k1 = Value::from(4);
    let k2 = Value::from(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");
    let start = ValMap::from(vec![(k1.clone(), v1.clone()), (k2.clone(), v2.clone())]);

    let expected = ValMap::from(vec![(k1.clone(), v1.clone())]);

    let machine = unvalidated();
    let mut state = (init, start);

    let message = Message::Action(UntypedMapModification::Take(1));

    let EventResult { result, terminate } = machine.handle_event(&mut state, message);

    let (dl_state, data_state) = state;

    assert!(!terminate);

    if synced {
        if let Ok(Some(ViewWithEvent { view, event })) = result {
            assert_eq!(view, expected);
            assert!(matches!(event, MapEvent::Take(1)));
        } else {
            panic!("Unexpected response: {:?}", result);
        }
    } else {
        assert_eq!(result, Ok(None));
    }

    assert_eq!(dl_state, init);
    assert_eq!(data_state, expected);
}

#[test]
fn take_when_linked_or_synced() {
    take_response(false);
    take_response(true);
}

fn drop_response(synced: bool) {
    let init = if synced {
        DownlinkState::Synced
    } else {
        DownlinkState::Linked
    };

    let k1 = Value::from(4);
    let k2 = Value::from(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");
    let start = ValMap::from(vec![(k1.clone(), v1.clone()), (k2.clone(), v2.clone())]);

    let expected = ValMap::from(vec![(k2.clone(), v2.clone())]);

    let machine = unvalidated();
    let mut state = (init, start);

    let message = Message::Action(UntypedMapModification::Drop(1));

    let EventResult { result, terminate } = machine.handle_event(&mut state, message);

    let (dl_state, data_state) = state;

    assert!(!terminate);

    if synced {
        if let Ok(Some(ViewWithEvent { view, event })) = result {
            assert_eq!(view, expected);
            assert!(matches!(event, MapEvent::Drop(1)));
        } else {
            panic!("Unexpected response: {:?}", result);
        }
    } else {
        assert_eq!(result, Ok(None));
    }

    assert_eq!(dl_state, init);
    assert_eq!(data_state, expected);
}

#[test]
fn drop_message_when_linked_or_synced() {
    drop_response(false);
    drop_response(true);
}

fn clear_response(synced: bool) {
    let init = if synced {
        DownlinkState::Synced
    } else {
        DownlinkState::Linked
    };

    let k1 = Value::from(4);
    let k2 = Value::from(6);
    let v1 = Value::text("hello");
    let v2 = Value::text("world");
    let start = ValMap::from(vec![(k1.clone(), v1.clone()), (k2.clone(), v2.clone())]);

    let expected = ValMap::new();

    let machine = unvalidated();
    let mut state = (init, start);

    let message = Message::Action(UntypedMapModification::Clear);

    let EventResult { result, terminate } = machine.handle_event(&mut state, message);

    let (dl_state, data_state) = state;

    assert!(!terminate);

    if synced {
        if let Ok(Some(ViewWithEvent { view, event })) = result {
            assert_eq!(view, expected);
            assert!(matches!(event, MapEvent::Clear));
        } else {
            panic!("Unexpected response: {:?}", result);
        }
    } else {
        assert_eq!(result, Ok(None));
    }

    assert_eq!(dl_state, init);
    assert_eq!(data_state, expected);
}

#[test]
fn clear_when_linked_or_synced() {
    clear_response(false);
    clear_response(true);
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
    let k = Value::from(4);
    let v = Arc::new(Value::from("hello"));
    let start = ValMap::from(vec![(k.clone(), v.clone())]);

    let machine = unvalidated();
    let mut state = (DownlinkState::Synced, start.clone());

    let (request, mut rx) = make_get_map();

    let result = machine.handle_request(&mut state, request);

    let (dl_state, data_state) = state;

    assert!(matches!(
        result,
        Ok(Response {
            event: None,
            command: None
        })
    ));

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(data_state, start);

    let result = rx.try_recv();

    assert!(matches!(result, Ok(Ok(m)) if m == start));
}

#[test]
fn get_key_action() {
    let k = Value::from(4);
    let v = Arc::new(Value::from("hello"));
    let start = ValMap::from(vec![(k.clone(), v.clone())]);

    let machine = unvalidated();
    let mut state = (DownlinkState::Synced, start.clone());

    let (request, mut rx) = make_get(4);

    let result = machine.handle_request(&mut state, request);

    let (dl_state, data_state) = state;

    assert!(matches!(
        result,
        Ok(Response {
            event: None,
            command: None
        })
    ));

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(data_state, start);

    let result = rx.try_recv();

    assert!(matches!(result, Ok(Ok(Some(v))) if *v == Value::from("hello")));
}

#[test]
fn get_absent_key_action() {
    let k = Value::from(4);
    let v = Arc::new(Value::from("hello"));
    let start = ValMap::from(vec![(k.clone(), v.clone())]);

    let machine = unvalidated();
    let mut state = (DownlinkState::Synced, start.clone());

    let (request, mut rx) = make_get(5);

    let result = machine.handle_request(&mut state, request);

    let (dl_state, data_state) = state;

    assert!(matches!(
        result,
        Ok(Response {
            event: None,
            command: None
        })
    ));

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(data_state, start);

    let result = rx.try_recv();

    assert!(matches!(result, Ok(Ok(None))));
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

#[test]
fn insert_action() {
    let k = Value::from(4);
    let v = Arc::new(Value::from("hello"));
    let start = ValMap::new();
    let expected = ValMap::from(vec![(k.clone(), v.clone())]);

    let machine = unvalidated();
    let mut state = (DownlinkState::Synced, start.clone());

    let (request, mut rx) = make_update(4, "hello".to_string());

    let result = machine.handle_request(&mut state, request);

    let (dl_state, data_state) = state;

    if let Ok(Response {
        event:
            Some(ViewWithEvent {
                view,
                event: MapEvent::Update(key_event),
            }),
        command: Some(Command::Action(UntypedMapModification::Update(key_upd, value))),
    }) = result
    {
        assert_eq!(view, expected);
        assert_eq!(key_event, k);
        assert_eq!(key_upd, k);
        assert_eq!(value, v);
    } else {
        panic!("Unexpected response: {:?}", result);
    }

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(data_state, expected);

    let result = rx.try_recv();

    assert!(matches!(result, Ok(Ok(None))));
}

#[test]
fn update_action() {
    let k = Value::from(4);
    let v1 = Arc::new(Value::from("hello"));
    let v2 = Arc::new(Value::from("world"));
    let start = ValMap::from(vec![(k.clone(), v1.clone())]);
    let expected = ValMap::from(vec![(k.clone(), v2.clone())]);

    let machine = unvalidated();
    let mut state = (DownlinkState::Synced, start.clone());

    let (request, mut rx) = make_update(4, "world".to_string());

    let result = machine.handle_request(&mut state, request);

    let (dl_state, data_state) = state;

    if let Ok(Response {
        event:
            Some(ViewWithEvent {
                view,
                event: MapEvent::Update(key_event),
            }),
        command: Some(Command::Action(UntypedMapModification::Update(key_upd, value))),
    }) = result
    {
        assert_eq!(view, expected);
        assert_eq!(key_event, k);
        assert_eq!(key_upd, k);
        assert_eq!(value, v2);
    } else {
        panic!("Unexpected response: {:?}", result);
    }

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(data_state, expected);

    let result = rx.try_recv();

    assert!(matches!(result, Ok(Ok(Some(v))) if v == v1));
}

#[test]
fn update_action_dropped() {
    let k = Value::from(4);
    let v1 = Arc::new(Value::from("hello"));
    let v2 = Arc::new(Value::from("world"));
    let start = ValMap::from(vec![(k.clone(), v1.clone())]);
    let expected = ValMap::from(vec![(k.clone(), v2.clone())]);

    let machine = unvalidated();
    let mut state = (DownlinkState::Synced, start.clone());

    let (request, _) = make_update(4, "world".to_string());

    let result = machine.handle_request(&mut state, request);

    let (dl_state, data_state) = state;

    if let Ok(Response {
        event:
            Some(ViewWithEvent {
                view,
                event: MapEvent::Update(key_event),
            }),
        command: Some(Command::Action(UntypedMapModification::Update(key_upd, value))),
    }) = result
    {
        assert_eq!(view, expected);
        assert_eq!(key_event, k);
        assert_eq!(key_upd, k);
        assert_eq!(value, v2);
    } else {
        panic!("Unexpected response: {:?}", result);
    }

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(data_state, expected);
}

#[test]
fn insert_action_invalid_key() {
    let start = ValMap::new();

    let machine = MapStateMachine::new(
        StandardSchema::OfKind(ValueKind::Int32),
        StandardSchema::OfKind(ValueKind::Text),
    );
    let mut state = (DownlinkState::Synced, start.clone());

    let (tx, mut rx) = oneshot::channel();

    let request = MapAction::Update {
        key: Value::BooleanValue(false),
        value: Value::text("updated"),
        old: Some(Request::new(tx)),
    };

    let result = machine.handle_request(&mut state, request);

    let (dl_state, data_state) = state;

    assert!(matches!(
        result,
        Ok(Response {
            event: None,
            command: None
        })
    ));

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(data_state, start);

    let result = rx.try_recv();

    assert!(matches!(
        result,
        Ok(Err(DownlinkError::SchemaViolation(
            Value::BooleanValue(false),
            StandardSchema::OfKind(ValueKind::Int32)
        )))
    ));
}

#[test]
fn insert_action_invalid_value() {
    let k = Value::from(4);
    let start = ValMap::new();

    let machine = MapStateMachine::new(
        StandardSchema::OfKind(ValueKind::Int32),
        StandardSchema::OfKind(ValueKind::Text),
    );
    let mut state = (DownlinkState::Synced, start.clone());

    let (tx, mut rx) = oneshot::channel();

    let request = MapAction::Update {
        key: k.clone(),
        value: Value::BooleanValue(false),
        old: Some(Request::new(tx)),
    };

    let result = machine.handle_request(&mut state, request);

    let (dl_state, data_state) = state;

    assert!(matches!(
        result,
        Ok(Response {
            event: None,
            command: None
        })
    ));

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(data_state, start);

    let result = rx.try_recv();

    assert!(matches!(
        result,
        Ok(Err(DownlinkError::SchemaViolation(
            Value::BooleanValue(false),
            StandardSchema::OfKind(ValueKind::Text)
        )))
    ));
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
fn remove_action_undefined() {
    let start = ValMap::new();

    let machine = unvalidated();
    let mut state = (DownlinkState::Synced, start.clone());

    let (request, mut rx) = make_remove(4);

    let result = machine.handle_request(&mut state, request);

    let (dl_state, data_state) = state;

    assert!(matches!(
        result,
        Ok(Response {
            event: None,
            command: None
        })
    ));

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(data_state, start);

    let result = rx.try_recv();

    assert!(matches!(result, Ok(Ok(None))));
}

#[test]
fn remove_action_defined() {
    let k = Value::from(4);
    let v = Arc::new(Value::from("hello"));
    let start = ValMap::from(vec![(k.clone(), v.clone())]);
    let expected = ValMap::new();

    let machine = unvalidated();
    let mut state = (DownlinkState::Synced, start.clone());

    let (request, mut rx) = make_remove(4);

    let result = machine.handle_request(&mut state, request);

    let (dl_state, data_state) = state;

    if let Ok(Response {
        event:
            Some(ViewWithEvent {
                view,
                event: MapEvent::Remove(key_event),
            }),
        command: Some(Command::Action(UntypedMapModification::Remove(key_upd))),
    }) = result
    {
        assert_eq!(view, expected);
        assert_eq!(key_event, k);
        assert_eq!(key_upd, k);
    } else {
        panic!("Unexpected response: {:?}", result);
    }

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(data_state, expected);

    let result = rx.try_recv();

    assert!(matches!(result, Ok(Ok(Some(value))) if value == v));
}

#[test]
fn remove_action_dropped() {
    let k = Value::from(4);
    let v = Arc::new(Value::from("hello"));
    let start = ValMap::from(vec![(k.clone(), v.clone())]);
    let expected = ValMap::new();

    let machine = unvalidated();
    let mut state = (DownlinkState::Synced, start.clone());

    let (request, _) = make_remove(4);

    let result = machine.handle_request(&mut state, request);

    let (dl_state, data_state) = state;

    if let Ok(Response {
        event:
            Some(ViewWithEvent {
                view,
                event: MapEvent::Remove(key_event),
            }),
        command: Some(Command::Action(UntypedMapModification::Remove(key_upd))),
    }) = result
    {
        assert_eq!(view, expected);
        assert_eq!(key_event, k);
        assert_eq!(key_upd, k);
    } else {
        panic!("Unexpected response: {:?}", result);
    }

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(data_state, expected);
}

#[test]
fn remove_action_invalid() {
    let start = ValMap::new();

    let machine = MapStateMachine::new(
        StandardSchema::OfKind(ValueKind::Int32),
        StandardSchema::OfKind(ValueKind::Text),
    );
    let mut state = (DownlinkState::Synced, start.clone());

    let (tx, mut rx) = oneshot::channel();

    let request = MapAction::Remove {
        key: Value::BooleanValue(false),
        old: Some(Request::new(tx)),
    };

    let result = machine.handle_request(&mut state, request);

    let (dl_state, data_state) = state;

    assert!(matches!(
        result,
        Ok(Response {
            event: None,
            command: None
        })
    ));

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(data_state, start);

    let result = rx.try_recv();

    assert!(matches!(
        result,
        Ok(Err(DownlinkError::SchemaViolation(
            Value::BooleanValue(false),
            StandardSchema::OfKind(ValueKind::Int32)
        )))
    ));
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
    let k1 = Value::from(4);
    let v1 = Arc::new(Value::from("hello"));
    let k2 = Value::from(5);
    let v2 = Arc::new(Value::from("world"));
    let start = ValMap::from(vec![(k1.clone(), v1.clone()), (k2.clone(), v2.clone())]);
    let expected = ValMap::from(vec![(k1.clone(), v1.clone())]);

    let machine = unvalidated();
    let mut state = (DownlinkState::Synced, start.clone());

    let (request, mut rx_before, mut rx_after) = make_take(1);

    let result = machine.handle_request(&mut state, request);

    let (dl_state, data_state) = state;

    if let Ok(Response {
        event:
            Some(ViewWithEvent {
                view,
                event: MapEvent::Take(1),
            }),
        command: Some(Command::Action(UntypedMapModification::Take(1))),
    }) = result
    {
        assert_eq!(view, expected);
    } else {
        panic!("Unexpected response: {:?}", result);
    }

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(data_state, expected);

    let result_before = rx_before.try_recv();
    let result_after = rx_after.try_recv();

    assert!(matches!(result_before, Ok(Ok(m)) if m == start));
    assert!(matches!(result_after, Ok(Ok(m)) if m == expected));
}

#[test]
fn take_action_dropped() {
    let k1 = Value::from(4);
    let v1 = Arc::new(Value::from("hello"));
    let k2 = Value::from(5);
    let v2 = Arc::new(Value::from("world"));
    let start = ValMap::from(vec![(k1.clone(), v1.clone()), (k2.clone(), v2.clone())]);
    let expected = ValMap::from(vec![(k1.clone(), v1.clone())]);

    let machine = unvalidated();
    let mut state = (DownlinkState::Synced, start.clone());

    let (request, _, _) = make_take(1);

    let result = machine.handle_request(&mut state, request);

    let (dl_state, data_state) = state;

    if let Ok(Response {
        event:
            Some(ViewWithEvent {
                view,
                event: MapEvent::Take(1),
            }),
        command: Some(Command::Action(UntypedMapModification::Take(1))),
    }) = result
    {
        assert_eq!(view, expected);
    } else {
        panic!("Unexpected response: {:?}", result);
    }

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(data_state, expected);
}

fn make_drop(
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
fn drop_action() {
    let k1 = Value::from(4);
    let v1 = Arc::new(Value::from("hello"));
    let k2 = Value::from(5);
    let v2 = Arc::new(Value::from("world"));
    let start = ValMap::from(vec![(k1.clone(), v1.clone()), (k2.clone(), v2.clone())]);
    let expected = ValMap::from(vec![(k2.clone(), v2.clone())]);

    let machine = unvalidated();
    let mut state = (DownlinkState::Synced, start.clone());

    let (request, mut rx_before, mut rx_after) = make_drop(1);

    let result = machine.handle_request(&mut state, request);

    let (dl_state, data_state) = state;

    if let Ok(Response {
        event:
            Some(ViewWithEvent {
                view,
                event: MapEvent::Drop(1),
            }),
        command: Some(Command::Action(UntypedMapModification::Drop(1))),
    }) = result
    {
        assert_eq!(view, expected);
    } else {
        panic!("Unexpected response: {:?}", result);
    }

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(data_state, expected);

    let result_before = rx_before.try_recv();
    let result_after = rx_after.try_recv();

    assert!(matches!(result_before, Ok(Ok(m)) if m == start));
    assert!(matches!(result_after, Ok(Ok(m)) if m == expected));
}

#[test]
fn drop_action_dropped() {
    let k1 = Value::from(4);
    let v1 = Arc::new(Value::from("hello"));
    let k2 = Value::from(5);
    let v2 = Arc::new(Value::from("world"));
    let start = ValMap::from(vec![(k1.clone(), v1.clone()), (k2.clone(), v2.clone())]);
    let expected = ValMap::from(vec![(k2.clone(), v2.clone())]);

    let machine = unvalidated();
    let mut state = (DownlinkState::Synced, start.clone());

    let (request, _, _) = make_drop(1);

    let result = machine.handle_request(&mut state, request);

    let (dl_state, data_state) = state;

    if let Ok(Response {
        event:
            Some(ViewWithEvent {
                view,
                event: MapEvent::Drop(1),
            }),
        command: Some(Command::Action(UntypedMapModification::Drop(1))),
    }) = result
    {
        assert_eq!(view, expected);
    } else {
        panic!("Unexpected response: {:?}", result);
    }

    assert_eq!(dl_state, DownlinkState::Synced);
    assert_eq!(data_state, expected);
}

fn make_clear() -> (MapAction, oneshot::Receiver<Result<ValMap, DownlinkError>>) {
    let (tx_bef, rx_bef) = oneshot::channel();
    (MapAction::clear_and_await(Request::new(tx_bef)), rx_bef)
}

#[test]
fn clear_action() {
    let k1 = Value::from(4);
    let v1 = Arc::new(Value::from("hello"));
    let k2 = Value::from(5);
    let v2 = Arc::new(Value::from("world"));
    let start = ValMap::from(vec![(k1.clone(), v1.clone()), (k2.clone(), v2.clone())]);

    let machine = unvalidated();
    let mut state = (DownlinkState::Synced, start.clone());

    let (request, mut rx) = make_clear();

    let result = machine.handle_request(&mut state, request);

    let (dl_state, data_state) = state;

    if let Ok(Response {
        event: Some(ViewWithEvent {
            view,
            event: MapEvent::Clear,
        }),
        command: Some(Command::Action(UntypedMapModification::Clear)),
    }) = result
    {
        assert!(view.is_empty());
    } else {
        panic!("Unexpected response: {:?}", result);
    }

    assert_eq!(dl_state, DownlinkState::Synced);
    assert!(data_state.is_empty());

    let result = rx.try_recv();

    assert!(matches!(result, Ok(Ok(m)) if m == start));
}

#[test]
fn clear_action_dropped() {
    let k1 = Value::from(4);
    let v1 = Arc::new(Value::from("hello"));
    let k2 = Value::from(5);
    let v2 = Arc::new(Value::from("world"));
    let start = ValMap::from(vec![(k1.clone(), v1.clone()), (k2.clone(), v2.clone())]);

    let machine = unvalidated();
    let mut state = (DownlinkState::Synced, start.clone());

    let (request, _) = make_clear();

    let result = machine.handle_request(&mut state, request);

    let (dl_state, data_state) = state;

    if let Ok(Response {
        event: Some(ViewWithEvent {
            view,
            event: MapEvent::Clear,
        }),
        command: Some(Command::Action(UntypedMapModification::Clear)),
    }) = result
    {
        assert!(view.is_empty());
    } else {
        panic!("Unexpected response: {:?}", result);
    }

    assert_eq!(dl_state, DownlinkState::Synced);
    assert!(data_state.is_empty());
}
