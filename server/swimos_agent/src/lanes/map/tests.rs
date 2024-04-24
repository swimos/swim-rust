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
        agent::{MapLaneResponse, MapLaneResponseDecoder},
        map::MapOperation,
    },
};
use swimos_recon::parser::{parse_recognize, Span};
use swimos_utilities::routing::route_uri::RouteUri;
use tokio_util::codec::Decoder;
use uuid::Uuid;

use crate::{
    agent_model::WriteResult,
    event_handler::{
        EventHandlerError, HandlerAction, HandlerFuture, Modification, Spawner, StepResult,
    },
    item::MapItem,
    lanes::{
        map::{
            MapLane, MapLaneClear, MapLaneEvent, MapLaneGet, MapLaneGetMap, MapLaneRemove,
            MapLaneSync, MapLaneUpdate, MapLaneTransformEntry,
        },
        LaneItem,
    },
    meta::AgentMetadata,
    test_context::dummy_context,
};

const ID: u64 = 74;

const K1: i32 = 5;
const K2: i32 = 78;
const K3: i32 = -4;

const ABSENT: i32 = 93;

const V1: &str = "first";
const V2: &str = "second";
const V3: &str = "third";

fn init() -> HashMap<i32, String> {
    [(K1, V1), (K2, V2), (K3, V3)]
        .into_iter()
        .map(|(k, v)| (k, v.to_owned()))
        .collect()
}

struct NoSpawn;

impl<Context> Spawner<Context> for NoSpawn {
    fn spawn_suspend(&self, _: HandlerFuture<Context>) {
        panic!("No suspended futures expected.");
    }
}

#[test]
fn get_from_map_lane() {
    let lane = MapLane::new(ID, init());

    let value = lane.get(&K1, |v| v.cloned());
    assert_eq!(value.as_deref(), Some(V1));

    let value = lane.get(&ABSENT, |v| v.cloned());
    assert!(value.is_none());
}

#[test]
fn get_map_lane() {
    let lane = MapLane::new(ID, init());

    let value = lane.get_map(Clone::clone);
    assert_eq!(value, init());
}

#[test]
fn update_map_lane() {
    let lane = MapLane::new(ID, init());

    lane.update(K2, "altered".to_owned());

    lane.get_map(|m| {
        assert_eq!(m.len(), 3);
        assert_eq!(m.get(&K1).map(String::as_str), Some(V1));
        assert_eq!(m.get(&K2).map(String::as_str), Some("altered"));
        assert_eq!(m.get(&K3).map(String::as_str), Some(V3));
    });

    lane.update(ABSENT, "added".to_owned());

    lane.get_map(|m| {
        assert_eq!(m.len(), 4);
        assert_eq!(m.get(&K1).map(String::as_str), Some(V1));
        assert_eq!(m.get(&K2).map(String::as_str), Some("altered"));
        assert_eq!(m.get(&K3).map(String::as_str), Some(V3));
        assert_eq!(m.get(&ABSENT).map(String::as_str), Some("added"));
    });
}

#[test]
fn remove_from_map_lane() {
    let lane = MapLane::new(ID, init());

    lane.remove(&K2);

    lane.get_map(|m| {
        assert_eq!(m.len(), 2);
        assert_eq!(m.get(&K1).map(String::as_str), Some(V1));
        assert_eq!(m.get(&K3).map(String::as_str), Some(V3));
    });
}

#[test]
fn clear_map_lane() {
    let lane = MapLane::new(ID, init());

    lane.clear();

    lane.get_map(|m| {
        assert!(m.is_empty());
    });
}

#[test]
fn write_to_buffer_no_data() {
    let lane = MapLane::new(ID, init());
    let mut buffer = BytesMut::new();

    let result = lane.write_to_buffer(&mut buffer);
    assert_eq!(result, WriteResult::NoData);
    assert!(buffer.is_empty());
}

#[test]
fn write_to_buffer_one_update() {
    let lane = MapLane::new(ID, init());

    lane.update(K2, "altered".to_owned());

    let mut buffer = BytesMut::new();

    let result = lane.write_to_buffer(&mut buffer);
    assert_eq!(result, WriteResult::Done);

    let mut decoder = MapLaneResponseDecoder::default();
    let content = decoder
        .decode(&mut buffer)
        .expect("Invalid frame.")
        .expect("Incomplete frame.");

    match content {
        MapLaneResponse::StandardEvent(operation) => match operation {
            MapOperation::Update { key, value } => {
                assert_eq!(key.as_ref(), b"78");
                assert_eq!(value.as_ref(), b"altered");
            }
            ow => {
                panic!("Unexpected operation: {:?}", ow);
            }
        },
        _ => panic!("Unexpected synced."),
    }
}

#[test]
fn write_to_buffer_one_remove() {
    let lane = MapLane::new(ID, init());

    lane.remove(&K2);

    let mut buffer = BytesMut::new();

    let result = lane.write_to_buffer(&mut buffer);
    assert_eq!(result, WriteResult::Done);

    let mut decoder = MapLaneResponseDecoder::default();
    let content = decoder
        .decode(&mut buffer)
        .expect("Invalid frame.")
        .expect("Incomplete frame.");

    match content {
        MapLaneResponse::StandardEvent(operation) => match operation {
            MapOperation::Remove { key } => {
                assert_eq!(key.as_ref(), b"78");
            }
            ow => {
                panic!("Unexpected operation: {:?}", ow);
            }
        },
        _ => panic!("Unexpected synced."),
    }
}

#[test]
fn write_to_buffer_clear() {
    let lane = MapLane::new(ID, init());

    lane.clear();

    let mut buffer = BytesMut::new();

    let result = lane.write_to_buffer(&mut buffer);
    assert_eq!(result, WriteResult::Done);

    let mut decoder = MapLaneResponseDecoder::default();
    let content = decoder
        .decode(&mut buffer)
        .expect("Invalid frame.")
        .expect("Incomplete frame.");

    match content {
        MapLaneResponse::StandardEvent(operation) => {
            assert!(matches!(operation, MapOperation::Clear));
        }
        _ => panic!("Unexpected synced."),
    }
}

#[derive(Debug)]
struct Operations {
    events: Vec<MapOperation<i32, String>>,
    sync: HashMap<Uuid, Vec<MapOperation<i32, String>>>,
}

fn consume_events(lane: &MapLane<i32, String>) -> Operations {
    let mut events = vec![];
    let mut sync_pending = HashMap::new();
    let mut sync = HashMap::new();

    let mut decoder = MapLaneResponseDecoder::default();
    let mut buffer = BytesMut::new();

    loop {
        let result = lane.write_to_buffer(&mut buffer);

        if matches!(result, WriteResult::NoData) {
            break;
        }

        let content = decoder
            .decode(&mut buffer)
            .expect("Invalid frame.")
            .expect("Incomplete frame.");

        match content {
            MapLaneResponse::StandardEvent(operation) => {
                events.push(interpret(operation));
            }
            MapLaneResponse::SyncEvent(id, operation) => {
                sync_pending
                    .entry(id)
                    .or_insert_with(Vec::new)
                    .push(interpret(operation));
            }
            MapLaneResponse::Synced(id) => {
                assert!(!sync.contains_key(&id));
                let ops = sync_pending.remove(&id).unwrap_or_default();
                sync.insert(id, ops);
            }
            MapLaneResponse::Initialized => {}
        }

        if matches!(result, WriteResult::Done) {
            break;
        }
    }
    assert!(sync_pending.is_empty());

    Operations { events, sync }
}

fn interpret(op: MapOperation<BytesMut, BytesMut>) -> MapOperation<i32, String> {
    match op {
        MapOperation::Update { key, value } => {
            let key_str = std::str::from_utf8(key.as_ref()).expect("Bad key bytes.");
            let val_str = std::str::from_utf8(value.as_ref()).expect("Bad value bytes.");
            let key = parse_recognize::<i32>(Span::new(key_str), false).expect("Bad key recon.");
            let value =
                parse_recognize::<String>(Span::new(val_str), false).expect("Bad value recon.");
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
    let lane = MapLane::new(ID, init());

    lane.update(ABSENT, "added".to_owned());
    lane.remove(&K1);
    lane.update(K3, "altered".to_owned());

    let Operations { events, sync } = consume_events(&lane);

    assert!(sync.is_empty());

    let expected = vec![
        MapOperation::Update {
            key: ABSENT,
            value: "added".to_owned(),
        },
        MapOperation::Remove { key: K1 },
        MapOperation::Update {
            key: K3,
            value: "altered".to_owned(),
        },
    ];
    assert_eq!(events, expected);
}

#[test]
fn updates_to_one_key_overwrite() {
    let lane = MapLane::new(ID, init());

    lane.update(ABSENT, "added".to_owned());
    lane.update(K3, "altered".to_string());
    lane.update(ABSENT, "changed".to_owned());

    let Operations { events, sync } = consume_events(&lane);

    assert!(sync.is_empty());

    let expected = vec![
        MapOperation::Update {
            key: ABSENT,
            value: "changed".to_owned(),
        },
        MapOperation::Update {
            key: K3,
            value: "altered".to_owned(),
        },
    ];
    assert_eq!(events, expected);
}

#[test]
fn clear_resets_event_queue() {
    let lane = MapLane::new(ID, init());

    lane.update(ABSENT, "added".to_owned());
    lane.remove(&K1);
    lane.update(K3, "altered".to_owned());
    lane.clear();

    let Operations { events, sync } = consume_events(&lane);

    assert!(sync.is_empty());

    let expected = vec![MapOperation::Clear];
    assert_eq!(events, expected);
}

const SYNC_ID1: Uuid = Uuid::from_u128(8578393934);
const SYNC_ID2: Uuid = Uuid::from_u128(2847474);

fn to_updates(sync_messages: &[MapOperation<i32, String>]) -> HashMap<i32, String> {
    let mut map = HashMap::new();
    for op in sync_messages {
        match op {
            MapOperation::Update { key, value } => {
                assert!(!map.contains_key(key));
                map.insert(*key, value.clone());
            }
            ow => panic!("Unexpected event: {:?}", ow),
        }
    }
    map
}

#[test]
fn sync_lane_state() {
    let lane = MapLane::new(ID, init());

    lane.sync(SYNC_ID1);

    let Operations { events, sync } = consume_events(&lane);
    assert!(events.is_empty());
    assert_eq!(sync.len(), 1);

    let sync_map = to_updates(sync.get(&SYNC_ID1).expect("Incorrect Sync ID."));

    let expected: HashMap<_, _> = [(K1, V1), (K2, V2), (K3, V3)]
        .into_iter()
        .map(|(k, v)| (k, v.to_owned()))
        .collect();
    assert_eq!(sync_map, expected);
}

#[test]
fn sync_twice_lane_state() {
    let lane = MapLane::new(ID, init());

    lane.sync(SYNC_ID1);
    lane.sync(SYNC_ID2);

    let Operations { events, sync } = consume_events(&lane);
    assert!(events.is_empty());
    assert_eq!(sync.len(), 2);

    let sync_map1 = to_updates(sync.get(&SYNC_ID1).expect("Incorrect Sync ID."));
    let sync_map2 = to_updates(sync.get(&SYNC_ID2).expect("Incorrect Sync ID."));

    let expected: HashMap<_, _> = [(K1, V1), (K2, V2), (K3, V3)]
        .into_iter()
        .map(|(k, v)| (k, v.to_owned()))
        .collect();
    assert_eq!(sync_map1, expected);
    assert_eq!(sync_map2, expected);
}

#[test]
fn sync_lane_state_and_event() {
    let lane = MapLane::new(ID, init());

    lane.sync(SYNC_ID1);
    lane.update(ABSENT, "added".to_owned());

    let Operations { events, sync } = consume_events(&lane);

    let expected_events = vec![MapOperation::Update {
        key: ABSENT,
        value: "added".to_owned(),
    }];
    assert_eq!(events, expected_events);

    assert_eq!(sync.len(), 1);

    let sync_map = to_updates(sync.get(&SYNC_ID1).expect("Incorrect Sync ID."));

    let expected_sync: HashMap<_, _> = [(K1, V1), (K2, V2), (K3, V3)]
        .into_iter()
        .map(|(k, v)| (k, v.to_owned()))
        .collect();
    assert_eq!(sync_map, expected_sync);
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
    lane: MapLane<i32, String>,
}

const LANE_ID: u64 = 9;

impl Default for TestAgent {
    fn default() -> Self {
        TestAgent {
            lane: MapLane::new(LANE_ID, Default::default()),
        }
    }
}

impl TestAgent {
    fn with_init() -> Self {
        let init: HashMap<_, _> = [(K1, V1), (K2, V2), (K3, V3)]
            .into_iter()
            .map(|(k, v)| (k, v.to_owned()))
            .collect();

        TestAgent {
            lane: MapLane::new(LANE_ID, init),
        }
    }
}

impl TestAgent {
    pub const LANE: fn(&TestAgent) -> &MapLane<i32, String> = |agent| &agent.lane;
}

fn check_result<T: Eq + Debug>(
    result: StepResult<T>,
    written: bool,
    trigger_handler: bool,
    complete: Option<T>,
) {
    let expected_mod = if written {
        if trigger_handler {
            Some(Modification::of(LANE_ID))
        } else {
            Some(Modification::no_trigger(LANE_ID))
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
fn map_lane_update_event_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();

    let mut handler = MapLaneUpdate::new(TestAgent::LANE, K1, V1.to_owned());

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, true, true, Some(()));

    agent.lane.get_map(|map| {
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(&K1).map(String::as_str), Some(V1));
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
fn map_lane_remove_event_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::with_init();

    let mut handler = MapLaneRemove::new(TestAgent::LANE, K1);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, true, true, Some(()));

    agent.lane.get_map(|map| {
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&K2).map(String::as_str), Some(V2));
        assert_eq!(map.get(&K3).map(String::as_str), Some(V3));
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
fn map_lane_clear_event_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::with_init();

    let mut handler = MapLaneClear::new(TestAgent::LANE);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, true, true, Some(()));

    agent.lane.get_map(|map| {
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
fn map_lane_get_event_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::with_init();

    let mut handler = MapLaneGet::new(TestAgent::LANE, K1);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, false, false, Some(Some(V1.to_owned())));

    let mut handler = MapLaneGet::new(TestAgent::LANE, ABSENT);

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
fn map_lane_get_map_event_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::with_init();

    let mut handler = MapLaneGetMap::new(TestAgent::LANE);

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
fn map_lane_sync_event_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::with_init();

    let mut handler = MapLaneSync::new(TestAgent::LANE, SYNC_ID1);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, true, false, Some(()));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));

    let Operations { events, sync } = consume_events(&agent.lane);

    assert!(events.is_empty());
    assert_eq!(sync.len(), 1);

    let sync_map = to_updates(sync.get(&SYNC_ID1).expect("Incorrect Sync ID."));

    let expected: HashMap<_, _> = [(K1, V1), (K2, V2), (K3, V3)]
        .into_iter()
        .map(|(k, v)| (k, v.to_owned()))
        .collect();
    assert_eq!(sync_map, expected);
}

#[test]
fn map_lane_transform_entry_handler_update() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::with_init();

    let mut handler = MapLaneTransformEntry::new(TestAgent::LANE, K1, |maybe: Option<&String>| {
        maybe.map(|v| v.to_uppercase())
    });

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, true, true, Some(()));

    agent.lane.get_map(|map| {
        assert_eq!(map.len(), 3);
        assert_eq!(map.get(&K1), Some(&V1.to_uppercase()));
        assert_eq!(map.get(&K2).map(String::as_str), Some(V2));
        assert_eq!(map.get(&K3).map(String::as_str), Some(V3));
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

    let event = agent.lane.read_with_prev(|event, _| event);
    assert_eq!(event, Some(MapLaneEvent::Update(K1, Some(V1.to_owned()))));
}

#[test]
fn map_lane_transform_entry_handler_remove() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::with_init();

    let mut handler = MapLaneTransformEntry::new(TestAgent::LANE, K1, |_: Option<&String>| None);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, true, true, Some(()));

    agent.lane.get_map(|map| {
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&K2).map(String::as_str), Some(V2));
        assert_eq!(map.get(&K3).map(String::as_str), Some(V3));
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

    let event = agent.lane.read_with_prev(|event, _| event);
    assert_eq!(event, Some(MapLaneEvent::Remove(K1, V1.to_owned())));
}
