// Copyright 2015-2021 Swim Inc.
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

use std::fmt::Debug;

use bytes::BytesMut;
use swim_api::{
    agent::AgentConfig,
    protocol::agent::{LaneResponse, ValueLaneResponseDecoder},
};
use swim_utilities::routing::uri::RelativeUri;
use tokio_util::codec::Decoder;
use uuid::Uuid;

use crate::{
    agent_model::WriteResult,
    event_handler::{
        EventHandlerError, HandlerAction, HandlerFuture, Modification, Spawner, StepResult,
    },
    lanes::{
        value::{ValueLaneGet, ValueLaneSync},
        Lane,
    },
    meta::AgentMetadata,
    test_context::dummy_context,
};

use super::{ValueLane, ValueLaneSet};

const ID: u64 = 74;

struct NoSpawn;

impl<Context> Spawner<Context> for NoSpawn {
    fn spawn_suspend(&self, _: HandlerFuture<Context>) {
        panic!("No suspended futures expected.");
    }
}

#[test]
fn not_dirty_initially() {
    let lane = ValueLane::new(ID, 123);

    assert!(!lane.dirty.get());
}

#[test]
fn read_from_value_lane() {
    let lane = ValueLane::new(ID, 123);

    let result = lane.read(|n| *n);

    assert_eq!(result, 123);
}

#[test]
fn read_from_value_lane_with_no_prev() {
    let lane = ValueLane::new(ID, 123);

    let (prev, result) = lane.read_with_prev(|prev, n| (prev, *n));

    assert!(prev.is_none());
    assert_eq!(result, 123);
}

#[test]
fn write_to_value_lane() {
    let lane = ValueLane::new(ID, 123);

    lane.set(89);
    assert!(lane.dirty.get());
    assert_eq!(lane.read(|n| *n), 89);
    assert_eq!(lane.read_with_prev(|prev, n| (prev, *n)), (Some(123), 89));
}

#[test]
fn write_to_buffer_not_dirty() {
    let lane = ValueLane::new(ID, 123);
    let mut buffer = BytesMut::new();

    let result = lane.write_to_buffer(&mut buffer);
    assert_eq!(result, WriteResult::NoData);
    assert!(buffer.is_empty());
}

#[test]
fn write_to_buffer_dirty() {
    let lane = ValueLane::new(ID, 123);
    lane.set(6373);
    let mut buffer = BytesMut::new();

    let result = lane.write_to_buffer(&mut buffer);
    assert_eq!(result, WriteResult::Done);
    assert!(!lane.dirty.get());

    let mut decoder = ValueLaneResponseDecoder::default();
    let content = decoder
        .decode(&mut buffer)
        .expect("Invalid frame.")
        .expect("Incomplete frame.");

    if let LaneResponse::StandardEvent(value) = content {
        assert_eq!(value.as_ref(), b"6373");
    } else {
        panic!("Unexpected response.");
    }
}

const SYNC_ID1: Uuid = Uuid::from_u128(63737383);
const SYNC_ID2: Uuid = Uuid::from_u128(183737);

#[test]
fn write_to_buffer_with_sync_while_clean() {
    let lane = ValueLane::new(ID, 123);
    lane.sync(SYNC_ID1);
    assert!(!lane.dirty.get());

    let mut buffer = BytesMut::new();

    let result = lane.write_to_buffer(&mut buffer);
    assert_eq!(result, WriteResult::Done);

    let mut decoder = ValueLaneResponseDecoder::default();
    let first = decoder
        .decode(&mut buffer)
        .expect("Invalid frame.")
        .expect("Incomplete frame.");
    let second = decoder
        .decode(&mut buffer)
        .expect("Invalid frame.")
        .expect("Incomplete frame.");

    match (first, second) {
        (LaneResponse::SyncEvent(id, value), LaneResponse::Synced(id2)) => {
            assert_eq!(id, SYNC_ID1);
            assert_eq!(id2, SYNC_ID1);
            assert_eq!(value.as_ref(), b"123");
        }
        _ => panic!("Unexpected responses."),
    }
}

#[test]
fn write_to_buffer_with_multiple_syncs_while_clean() {
    let lane = ValueLane::new(ID, 123);
    lane.sync(SYNC_ID1);
    lane.sync(SYNC_ID2);
    assert!(!lane.dirty.get());

    let mut buffer = BytesMut::new();

    let result = lane.write_to_buffer(&mut buffer);
    assert_eq!(result, WriteResult::DataStillAvailable);

    let mut decoder = ValueLaneResponseDecoder::default();

    let frames = std::iter::repeat_with(|| {
        decoder
            .decode(&mut buffer)
            .expect("Invalid frame.")
            .expect("Incomplete frame.")
    })
    .take(2)
    .collect::<Vec<_>>();

    match frames.as_slice() {
        [LaneResponse::SyncEvent(id1, body), LaneResponse::Synced(id2)] => {
            assert_eq!(id1, &SYNC_ID1);
            assert_eq!(id2, &SYNC_ID1);
            assert_eq!(body.as_ref(), b"123");
        }
        _ => {
            panic!("Unexpected responses.");
        }
    }

    let result = lane.write_to_buffer(&mut buffer);
    assert_eq!(result, WriteResult::Done);

    let frames = std::iter::repeat_with(|| {
        decoder
            .decode(&mut buffer)
            .expect("Invalid frame.")
            .expect("Incomplete frame.")
    })
    .take(2)
    .collect::<Vec<_>>();

    match frames.as_slice() {
        [LaneResponse::SyncEvent(id1, body), LaneResponse::Synced(id2)] => {
            assert_eq!(id1, &SYNC_ID2);
            assert_eq!(id2, &SYNC_ID2);
            assert_eq!(body.as_ref(), b"123");
        }
        _ => {
            panic!("Unexpected responses.");
        }
    }
}

#[test]
fn write_to_buffer_with_sync_while_dirty() {
    let lane: ValueLane<i32> = ValueLane::new(ID, 123);
    lane.set(6373);
    lane.sync(SYNC_ID1);
    assert!(lane.dirty.get());

    let mut buffer = BytesMut::new();

    let result = lane.write_to_buffer(&mut buffer);
    assert_eq!(result, WriteResult::DataStillAvailable);

    let mut decoder = ValueLaneResponseDecoder::default();
    let frames = std::iter::repeat_with(|| {
        decoder
            .decode(&mut buffer)
            .expect("Invalid frame.")
            .expect("Incomplete frame.")
    })
    .take(2)
    .collect::<Vec<_>>();

    assert!(lane.dirty.get());

    match frames.as_slice() {
        [LaneResponse::SyncEvent(id1, value), LaneResponse::Synced(id2)] => {
            assert_eq!(id1, &SYNC_ID1);
            assert_eq!(id2, &SYNC_ID1);
            assert_eq!(value.as_ref(), b"6373");
        }
        _ => {
            panic!("Unexpected response.");
        }
    }

    let result = lane.write_to_buffer(&mut buffer);
    assert_eq!(result, WriteResult::Done);

    let frame = decoder
        .decode(&mut buffer)
        .expect("Invalid frame.")
        .expect("Incomplete frame.");

    assert!(!lane.dirty.get());
    if let LaneResponse::StandardEvent(value) = frame {
        assert_eq!(value.as_ref(), b"6373");
    } else {
        panic!("Unexpected response.");
    }
}

const CONFIG: AgentConfig = AgentConfig::DEFAULT;
const NODE_URI: &str = "/node";

fn make_uri() -> RelativeUri {
    RelativeUri::try_from(NODE_URI).expect("Bad URI.")
}

fn make_meta(uri: &RelativeUri) -> AgentMetadata<'_> {
    AgentMetadata::new(uri, &CONFIG)
}

struct TestAgent {
    lane: ValueLane<i32>,
}

const LANE_ID: u64 = 9;

impl Default for TestAgent {
    fn default() -> Self {
        Self {
            lane: ValueLane::new(LANE_ID, 0),
        }
    }
}

impl TestAgent {
    const LANE: fn(&TestAgent) -> &ValueLane<i32> = |agent| &agent.lane;
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
                modified_lane,
                result,
            },
            Some(expected),
        ) => {
            assert_eq!(modified_lane, expected_mod);
            assert_eq!(result, expected);
        }
        (StepResult::Continue { modified_lane }, None) => {
            assert_eq!(modified_lane, expected_mod);
        }
        ow => {
            panic!("Unexpected result: {:?}", ow);
        }
    }
}

#[test]
fn value_lane_set_event_handler() {
    let uri = make_uri();
    let meta = make_meta(&uri);
    let agent = TestAgent::default();

    let mut handler = ValueLaneSet::new(TestAgent::LANE, 84);

    let result = handler.step(dummy_context(), meta, &agent);
    check_result(result, true, true, Some(()));

    assert!(agent.lane.dirty.get());
    assert_eq!(agent.lane.read(|n| *n), 84);

    let result = handler.step(dummy_context(), meta, &agent);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn value_lane_get_event_handler() {
    let uri = make_uri();
    let meta = make_meta(&uri);
    let agent = TestAgent::default();

    let mut handler = ValueLaneGet::new(TestAgent::LANE);

    let result = handler.step(dummy_context(), meta, &agent);
    check_result(result, false, false, Some(0));

    let result = handler.step(dummy_context(), meta, &agent);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn value_lane_sync_event_handler() {
    let uri = make_uri();
    let meta = make_meta(&uri);
    let agent = TestAgent::default();

    let mut handler = ValueLaneSync::new(TestAgent::LANE, SYNC_ID1);

    let result = handler.step(dummy_context(), meta, &agent);
    check_result(result, true, false, Some(()));

    let result = handler.step(dummy_context(), meta, &agent);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));

    let mut buffer = BytesMut::new();

    let result = agent.lane.write_to_buffer(&mut buffer);
    assert_eq!(result, WriteResult::Done);

    let mut decoder = ValueLaneResponseDecoder::default();

    let frames = std::iter::repeat_with(|| {
        decoder
            .decode(&mut buffer)
            .expect("Invalid frame.")
            .expect("Incomplete frame.")
    })
    .take(2)
    .collect::<Vec<_>>();

    match frames.as_slice() {
        [LaneResponse::SyncEvent(id1, body), LaneResponse::Synced(id2)] => {
            assert_eq!(id1, &SYNC_ID1);
            assert_eq!(id2, &SYNC_ID1);
            assert_eq!(body.as_ref(), b"0");
        }
        _ => {
            panic!("Unexpected response.");
        }
    }
}
