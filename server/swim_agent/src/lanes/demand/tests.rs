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

use bytes::BytesMut;
use swim_api::{
    agent::AgentConfig,
    protocol::agent::{LaneResponse, ValueLaneResponseDecoder},
};
use swim_utilities::routing::route_uri::RouteUri;
use tokio_util::codec::Decoder;
use uuid::Uuid;

use crate::{
    agent_model::WriteResult,
    event_handler::{
        check_step::{check_is_complete, check_is_continue},
        ActionContext, ConstHandler, EventHandlerError, HandlerAction, Modification,
        ModificationFlags, StepResult,
    },
    lanes::{
        demand::{Cue, Demand, DemandLaneSync},
        LaneItem,
    },
    meta::AgentMetadata,
    test_context::dummy_context,
};

use super::DemandLane;

const CONFIG: AgentConfig = AgentConfig::DEFAULT;
const NODE_URI: &str = "/node";
const LANE_ID: u64 = 38;
const SYNC_ID: Uuid = Uuid::from_u128(77);
const SYNC_ID2: Uuid = Uuid::from_u128(929);

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
    lane: DemandLane<i32>,
}

impl Default for TestAgent {
    fn default() -> Self {
        Self {
            lane: DemandLane::new(LANE_ID),
        }
    }
}

impl TestAgent {
    const LANE: fn(&TestAgent) -> &DemandLane<i32> = |agent| &agent.lane;
}

#[test]
fn cue_event_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();

    let mut handler = Cue::new(TestAgent::LANE);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );

    check_is_complete(result, LANE_ID, &(), ModificationFlags::all());

    assert!(agent.lane.cued.get());

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
fn demand_lane_sync_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();

    let mut handler = DemandLaneSync::new(TestAgent::LANE, SYNC_ID);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );

    check_is_complete(result, LANE_ID, &(), ModificationFlags::all());

    assert!(!agent.lane.cued.get());
    let guard = agent.lane.inner.borrow();
    assert_eq!(
        guard.sync_queue.iter().copied().collect::<Vec<_>>(),
        vec![SYNC_ID]
    );

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
fn demand_event_handler_no_modify() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();

    let mut handler = Demand::new(TestAgent::LANE, ConstHandler::from(34));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );

    assert!(matches!(
        result,
        StepResult::Continue {
            modified_item: None,
        }
    ));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );

    check_is_complete(result, LANE_ID, &(), ModificationFlags::DIRTY);

    let guard = agent.lane.inner.borrow();
    assert_eq!(guard.computed_value, Some(34));

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

#[derive(Clone, Copy)]
enum TestHandler {
    Init,
    Ready,
    Done,
}

impl HandlerAction<TestAgent> for TestHandler {
    type Completion = i32;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<TestAgent>,
        _meta: AgentMetadata,
        _context: &TestAgent,
    ) -> StepResult<Self::Completion> {
        match *self {
            TestHandler::Init => {
                *self = TestHandler::Ready;
                StepResult::Continue {
                    modified_item: Some(Modification::of(0)),
                }
            }
            TestHandler::Ready => {
                *self = TestHandler::Done;
                StepResult::Complete {
                    modified_item: None,
                    result: 123,
                }
            }
            TestHandler::Done => StepResult::after_done(),
        }
    }
}

#[test]
fn demand_event_handler_with_mod() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();

    let mut handler = Demand::new(TestAgent::LANE, TestHandler::Init);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );

    check_is_continue(result, 0, ModificationFlags::all());

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );

    assert!(matches!(
        result,
        StepResult::Continue {
            modified_item: None,
        }
    ));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );

    check_is_complete(result, LANE_ID, &(), ModificationFlags::DIRTY);

    let guard = agent.lane.inner.borrow();
    assert_eq!(guard.computed_value, Some(123));

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

fn stage_data(lane: &DemandLane<i32>) {
    let mut guard = lane.inner.borrow_mut();
    guard.computed_value = Some(27);
}

fn read_buffer(buffer: &mut BytesMut) -> Vec<LaneResponse<i32>> {
    let mut decoder = ValueLaneResponseDecoder::default();
    let mut results = vec![];
    while !buffer.is_empty() {
        let mut r = decoder.decode(buffer).expect("Decode failed.");
        if r.is_none() {
            r = decoder.decode_eof(buffer).expect("Decode failed.");
        }
        let msg = r.expect("Incomplete record");
        results.push(match msg {
            LaneResponse::StandardEvent(body) => LaneResponse::StandardEvent(
                std::str::from_utf8(body.as_ref())
                    .expect("Bad utf.")
                    .parse::<i32>()
                    .expect("Bad body."),
            ),
            LaneResponse::Initialized => LaneResponse::Initialized,
            LaneResponse::SyncEvent(id, body) => LaneResponse::SyncEvent(
                id,
                std::str::from_utf8(body.as_ref())
                    .expect("Bad utf.")
                    .parse::<i32>()
                    .expect("Bad body."),
            ),
            LaneResponse::Synced(id) => LaneResponse::Synced(id),
        });
    }
    results
}

#[test]
fn write_cued() {
    let lane = DemandLane::new(LANE_ID);
    lane.cue();
    stage_data(&lane);

    let mut buffer = BytesMut::new();
    assert_eq!(lane.write_to_buffer(&mut buffer), WriteResult::Done);

    let messages = read_buffer(&mut buffer);

    assert_eq!(messages, vec![LaneResponse::StandardEvent(27)])
}

#[test]
fn write_sync() {
    let lane = DemandLane::new(LANE_ID);
    lane.sync(SYNC_ID);
    stage_data(&lane);

    let mut buffer = BytesMut::new();
    assert_eq!(lane.write_to_buffer(&mut buffer), WriteResult::Done);

    let messages = read_buffer(&mut buffer);

    assert_eq!(
        messages,
        vec![
            LaneResponse::SyncEvent(SYNC_ID, 27),
            LaneResponse::Synced(SYNC_ID),
        ]
    )
}

#[test]
fn write_multiple() {
    let lane = DemandLane::new(LANE_ID);
    lane.sync(SYNC_ID);
    lane.cue();
    lane.sync(SYNC_ID2);
    stage_data(&lane);

    let mut buffer = BytesMut::new();
    let mut result = WriteResult::DataStillAvailable;

    while result == WriteResult::DataStillAvailable {
        result = lane.write_to_buffer(&mut buffer);
    }

    let messages = read_buffer(&mut buffer);

    assert_eq!(
        messages,
        vec![
            LaneResponse::SyncEvent(SYNC_ID, 27),
            LaneResponse::Synced(SYNC_ID),
            LaneResponse::SyncEvent(SYNC_ID2, 27),
            LaneResponse::Synced(SYNC_ID2),
            LaneResponse::StandardEvent(27),
        ]
    )
}
