// Copyright 2015-2024 Swim Inc.
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
use swimos_agent_protocol::LaneResponse;
use tokio_util::codec::Decoder;
use uuid::Uuid;

use swimos_agent_protocol::encoding::lane::RawValueLaneResponseDecoder;
use swimos_api::agent::AgentConfig;
use swimos_utilities::routing::RouteUri;

use crate::lanes::supply::{Supply, SupplyLaneSync};
use crate::{
    agent_model::WriteResult,
    event_handler::{EventHandlerError, HandlerAction, Modification, StepResult},
    lanes::LaneItem,
    meta::AgentMetadata,
    test_context::dummy_context,
};

use super::SupplyLane;

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
    lane: SupplyLane<i32>,
}

impl Default for TestAgent {
    fn default() -> Self {
        Self {
            lane: SupplyLane::new(LANE_ID),
        }
    }
}

impl TestAgent {
    const LANE: fn(&TestAgent) -> &SupplyLane<i32> = |agent| &agent.lane;
}

#[test]
fn supply_event_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();

    let mut handler = Supply::new(TestAgent::LANE, 13);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );

    let _modification = Modification::no_trigger(LANE_ID);
    assert!(matches!(
        result,
        StepResult::Complete {
            modified_item: Some(_modification),
            result: ()
        }
    ));

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
fn supply_lane_sync_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();

    let mut handler = SupplyLaneSync::new(TestAgent::LANE, SYNC_ID);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );

    let _modification = Modification::no_trigger(LANE_ID);
    assert!(matches!(
        result,
        StepResult::Complete {
            modified_item: Some(_modification),
            result: ()
        }
    ));

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

fn read_buffer(buffer: &mut BytesMut) -> Vec<LaneResponse<i32>> {
    let mut decoder = RawValueLaneResponseDecoder::default();
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
fn write_supplied() {
    let lane = SupplyLane::new(LANE_ID);
    lane.push(13);

    let mut buffer = BytesMut::new();
    assert_eq!(lane.write_to_buffer(&mut buffer), WriteResult::Done);

    let messages = read_buffer(&mut buffer);

    assert_eq!(messages, vec![LaneResponse::StandardEvent(13)]);
    assert!(read_buffer(&mut buffer).is_empty());
}

#[test]
fn write_sync() {
    let lane = SupplyLane::<i32>::new(LANE_ID);
    lane.sync(SYNC_ID);

    let mut buffer = BytesMut::new();
    assert_eq!(lane.write_to_buffer(&mut buffer), WriteResult::Done);

    let messages = read_buffer(&mut buffer);

    assert_eq!(messages, vec![LaneResponse::Synced(SYNC_ID),]);
    assert!(read_buffer(&mut buffer).is_empty());
}

#[test]
fn write_multiple() {
    let lane = SupplyLane::new(LANE_ID);
    lane.push(13);
    lane.sync(SYNC_ID);
    lane.push(14);
    lane.sync(SYNC_ID2);
    lane.push(15);

    let mut buffer = BytesMut::new();
    let mut result = WriteResult::DataStillAvailable;

    while result == WriteResult::DataStillAvailable {
        result = lane.write_to_buffer(&mut buffer);
    }

    let messages = read_buffer(&mut buffer);

    assert_eq!(
        messages,
        vec![
            LaneResponse::Synced(SYNC_ID),
            LaneResponse::Synced(SYNC_ID2),
            LaneResponse::StandardEvent(13),
            LaneResponse::StandardEvent(14),
            LaneResponse::StandardEvent(15),
        ]
    );

    assert!(read_buffer(&mut buffer).is_empty());
}

#[test]
fn read_empty() {
    let lane = SupplyLane::<i32>::new(LANE_ID);
    let mut buffer = BytesMut::new();

    lane.write_to_buffer(&mut buffer);
    assert!(read_buffer(&mut buffer).is_empty());
}
