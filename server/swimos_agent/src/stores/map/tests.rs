// Copyright 2015-2023 Swim Inc.
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

use std::collections::HashMap;
use std::fmt::Debug;

use bytes::BytesMut;
use swimos_api::{
    agent::AgentConfig,
    protocol::{
        agent::{MapStoreResponse, MapStoreResponseDecoder},
        map::MapOperation,
    },
};
use swimos_model::Text;
use swimos_recon::parser::{parse_recognize, Span};
use swimos_utilities::routing::route_uri::RouteUri;
use tokio_util::codec::Decoder;

use crate::{
    agent_model::WriteResult,
    event_handler::{EventHandlerError, HandlerAction, Modification, StepResult},
    item::MapItem,
    lanes::map::MapLaneEvent,
    meta::AgentMetadata,
    stores::{
        map::{
            MapStoreClear, MapStoreGet, MapStoreGetMap, MapStoreRemove, MapStoreUpdate,
            MapStoreTransformEntry,
        },
        MapStore, StoreItem,
    },
    test_context::dummy_context,
};

const ID: u64 = 567;

const K1: i32 = 6;
const K2: i32 = -5672;
const K3: i32 = 287857;

const ABSENT: i32 = 500;

const V1: &str = "first";
const V2: &str = "second";
const V3: &str = "third";

fn init() -> HashMap<i32, Text> {
    [(K1, V1), (K2, V2), (K3, V3)]
        .into_iter()
        .map(|(k, v)| (k, Text::new(v)))
        .collect()
}

#[test]
fn get_from_map_store() {
    let store = MapStore::new(ID, init());

    let value = store.get(&K1, |v| v.cloned());
    assert_eq!(value, Some(Text::new(V1)));

    let value = store.get(&ABSENT, |v| v.cloned());
    assert!(value.is_none());
}

#[test]
fn get_map_store() {
    let store = MapStore::new(ID, init());

    let value = store.get_map(Clone::clone);
    assert_eq!(value, init());
}

#[test]
fn update_map_store() {
    let store = MapStore::new(ID, init());

    store.update(K2, Text::new("altered"));

    store.get_map(|m| {
        assert_eq!(m.len(), 3);
        assert_eq!(m.get(&K1), Some(&Text::new(V1)));
        assert_eq!(m.get(&K2), Some(&Text::new("altered")));
        assert_eq!(m.get(&K3), Some(&Text::new(V3)));
    });

    store.update(ABSENT, Text::new("added"));

    store.get_map(|m| {
        assert_eq!(m.len(), 4);
        assert_eq!(m.get(&K1), Some(&Text::new(V1)));
        assert_eq!(m.get(&K2), Some(&Text::new("altered")));
        assert_eq!(m.get(&K3), Some(&Text::new(V3)));
        assert_eq!(m.get(&ABSENT), Some(&Text::new("added")));
    });
}

#[test]
fn remove_from_map_store() {
    let store = MapStore::new(ID, init());

    store.remove(&K2);

    store.get_map(|m| {
        assert_eq!(m.len(), 2);
        assert_eq!(m.get(&K1), Some(&Text::new(V1)));
        assert_eq!(m.get(&K3), Some(&Text::new(V3)));
    });
}

#[test]
fn clear_map_store() {
    let store = MapStore::new(ID, init());

    store.clear();

    store.get_map(|m| {
        assert!(m.is_empty());
    });
}

#[test]
fn write_to_buffer_no_data() {
    let store = MapStore::new(ID, init());
    let mut buffer = BytesMut::new();

    let result = store.write_to_buffer(&mut buffer);
    assert_eq!(result, WriteResult::NoData);
    assert!(buffer.is_empty());
}

#[test]
fn write_to_buffer_one_update() {
    let store = MapStore::new(ID, init());

    store.update(K2, Text::new("altered"));

    let mut buffer = BytesMut::new();

    let result = store.write_to_buffer(&mut buffer);
    assert_eq!(result, WriteResult::Done);

    let mut decoder = MapStoreResponseDecoder::default();
    let MapStoreResponse { message } = decoder
        .decode(&mut buffer)
        .expect("Invalid frame.")
        .expect("Incomplete frame.");

    match message {
        MapOperation::Update { key, value } => {
            assert_eq!(key.as_ref(), b"-5672");
            assert_eq!(value.as_ref(), b"altered");
        }
        ow => {
            panic!("Unexpected operation: {:?}", ow);
        }
    }
}

#[test]
fn write_to_buffer_one_remove() {
    let store = MapStore::new(ID, init());

    store.remove(&K2);

    let mut buffer = BytesMut::new();

    let result = store.write_to_buffer(&mut buffer);
    assert_eq!(result, WriteResult::Done);

    let mut decoder = MapStoreResponseDecoder::default();
    let MapStoreResponse { message } = decoder
        .decode(&mut buffer)
        .expect("Invalid frame.")
        .expect("Incomplete frame.");

    match message {
        MapOperation::Remove { key } => {
            assert_eq!(key.as_ref(), b"-5672");
        }
        ow => {
            panic!("Unexpected operation: {:?}", ow);
        }
    }
}

#[test]
fn write_to_buffer_clear() {
    let store = MapStore::new(ID, init());

    store.clear();

    let mut buffer = BytesMut::new();

    let result = store.write_to_buffer(&mut buffer);
    assert_eq!(result, WriteResult::Done);

    let mut decoder = MapStoreResponseDecoder::default();
    let MapStoreResponse { message } = decoder
        .decode(&mut buffer)
        .expect("Invalid frame.")
        .expect("Incomplete frame.");

    assert!(matches!(message, MapOperation::Clear));
}

#[derive(Debug)]
struct Operations {
    events: Vec<MapOperation<i32, Text>>,
}

fn consume_events(store: &MapStore<i32, Text>) -> Operations {
    let mut events = vec![];

    let mut decoder = MapStoreResponseDecoder::default();
    let mut buffer = BytesMut::new();

    loop {
        let result = store.write_to_buffer(&mut buffer);

        if matches!(result, WriteResult::NoData) {
            break;
        }

        let MapStoreResponse { message } = decoder
            .decode(&mut buffer)
            .expect("Invalid frame.")
            .expect("Incomplete frame.");

        events.push(interpret(message));

        if matches!(result, WriteResult::Done) {
            break;
        }
    }

    Operations { events }
}

fn interpret(op: MapOperation<BytesMut, BytesMut>) -> MapOperation<i32, Text> {
    match op {
        MapOperation::Update { key, value } => {
            let key_str = std::str::from_utf8(key.as_ref()).expect("Bad key bytes.");
            let val_str = std::str::from_utf8(value.as_ref()).expect("Bad value bytes.");
            let key = parse_recognize::<i32>(Span::new(key_str), false).expect("Bad key recon.");
            let value =
                parse_recognize::<Text>(Span::new(val_str), false).expect("Bad value recon.");
            MapOperation::Update { key, value }
        }
        MapOperation::Remove { key } => {
            let key_str = std::str::from_utf8(key.as_ref()).expect("Bad key bytes.");
            let key = parse_recognize::<i32>(Span::new(key_str), false).expect("Bad key recon.");
            MapOperation::Remove { key }
        }
        MapOperation::Clear => MapOperation::Clear,
    }
}

#[test]
fn write_multiple_events_to_buffer() {
    let store = MapStore::new(ID, init());

    store.update(ABSENT, Text::new("added"));
    store.remove(&K1);
    store.update(K3, Text::new("altered"));

    let Operations { events } = consume_events(&store);

    let expected = vec![
        MapOperation::Update {
            key: ABSENT,
            value: Text::new("added"),
        },
        MapOperation::Remove { key: K1 },
        MapOperation::Update {
            key: K3,
            value: Text::new("altered"),
        },
    ];
    assert_eq!(events, expected);
}

#[test]
fn updates_to_one_key_overwrite() {
    let store = MapStore::new(ID, init());

    store.update(ABSENT, Text::new("added"));
    store.update(K3, Text::new("altered"));
    store.update(ABSENT, Text::new("changed"));

    let Operations { events } = consume_events(&store);

    let expected = vec![
        MapOperation::Update {
            key: ABSENT,
            value: Text::new("changed"),
        },
        MapOperation::Update {
            key: K3,
            value: Text::new("altered"),
        },
    ];
    assert_eq!(events, expected);
}

#[test]
fn clear_resets_event_queue() {
    let store = MapStore::new(ID, init());

    store.update(ABSENT, Text::new("added"));
    store.remove(&K1);
    store.update(K3, Text::new("altered"));
    store.clear();

    let Operations { events } = consume_events(&store);

    let expected = vec![MapOperation::Clear];
    assert_eq!(events, expected);
}

const CONFIG: AgentConfig = AgentConfig::DEFAULT;
const NODE_URI: &str = "/node";

fn make_uri() -> RouteUri {
    RouteUri::try_from(NODE_URI).expect("Bad URI.")
}

fn make_meta<'a>(
    uri: &'a RouteUri,
    route_params: &'a HashMap<String, String>,
) -> AgentMetadata<'a> {
    AgentMetadata::new(uri, route_params, &CONFIG)
}

struct TestAgent {
    store: MapStore<i32, Text>,
}

const STORE_ID: u64 = 9;

impl Default for TestAgent {
    fn default() -> Self {
        TestAgent {
            store: MapStore::new(STORE_ID, Default::default()),
        }
    }
}

impl TestAgent {
    fn with_init() -> Self {
        let init: HashMap<_, _> = [(K1, V1), (K2, V2), (K3, V3)]
            .into_iter()
            .map(|(k, v)| (k, Text::new(v)))
            .collect();

        TestAgent {
            store: MapStore::new(STORE_ID, init),
        }
    }
}

impl TestAgent {
    pub const STORE: fn(&TestAgent) -> &MapStore<i32, Text> = |agent| &agent.store;
}

fn check_result<T: Eq + Debug>(
    result: StepResult<T>,
    written: bool,
    trigger_handler: bool,
    complete: Option<T>,
) {
    let expected_mod = if written {
        if trigger_handler {
            Some(Modification::of(STORE_ID))
        } else {
            Some(Modification::no_trigger(STORE_ID))
        }
    } else {
        None
    };
    match (result, complete) {
        (
            StepResult::Complete {
                modified_item,
                result,
            },
            Some(expected),
        ) => {
            assert_eq!(modified_item, expected_mod);
            assert_eq!(result, expected);
        }
        (StepResult::Continue { modified_item }, None) => {
            assert_eq!(modified_item, expected_mod);
        }
        ow => {
            panic!("Unexpected result: {:?}", ow);
        }
    }
}

#[test]
fn map_store_update_event_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();

    let mut handler = MapStoreUpdate::new(TestAgent::STORE, K1, Text::new(V1));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, true, true, Some(()));

    agent.store.get_map(|map| {
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(&K1), Some(&Text::new(V1)));
    });

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn map_store_remove_event_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::with_init();

    let mut handler = MapStoreRemove::new(TestAgent::STORE, K1);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, true, true, Some(()));

    agent.store.get_map(|map| {
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&K2), Some(&Text::new(V2)));
        assert_eq!(map.get(&K3), Some(&Text::new(V3)));
    });

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn map_store_clear_event_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::with_init();

    let mut handler = MapStoreClear::new(TestAgent::STORE);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, true, true, Some(()));

    agent.store.get_map(|map| {
        assert!(map.is_empty());
    });

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn map_store_get_event_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::with_init();

    let mut handler = MapStoreGet::new(TestAgent::STORE, K1);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, false, false, Some(Some(Text::new(V1))));

    let mut handler = MapStoreGet::new(TestAgent::STORE, ABSENT);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, false, false, Some(None));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn map_store_get_map_event_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::with_init();

    let mut handler = MapStoreGetMap::new(TestAgent::STORE);

    let expected = init();

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, false, false, Some(expected));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn map_store_with_event_handler_update() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::with_init();

    let mut handler = MapStoreTransformEntry::new(TestAgent::STORE, K1, |maybe: Option<&Text>| {
        maybe.map(|v| Text::from(v.as_str().to_uppercase()))
    });

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, true, true, Some(()));

    agent.store.get_map(|map| {
        assert_eq!(map.len(), 3);
        assert_eq!(map.get(&K1), Some(&Text::from(V1.to_uppercase())));
        assert_eq!(map.get(&K2), Some(&Text::new(V2)));
        assert_eq!(map.get(&K3), Some(&Text::new(V3)));
    });

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));

    let event = agent.store.read_with_prev(|event, _| event);
    assert_eq!(event, Some(MapLaneEvent::Update(K1, Some(Text::new(V1)))));
}

#[test]
fn map_lane_with_event_handler_remove() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::with_init();

    let mut handler = MapStoreTransformEntry::new(TestAgent::STORE, K1, |_: Option<&Text>| None);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, true, true, Some(()));

    agent.store.get_map(|map| {
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&K2), Some(&Text::new(V2)));
        assert_eq!(map.get(&K3), Some(&Text::new(V3)));
    });

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));

    let event = agent.store.read_with_prev(|event, _| event);
    assert_eq!(event, Some(MapLaneEvent::Remove(K1, Text::new(V1))));
}
