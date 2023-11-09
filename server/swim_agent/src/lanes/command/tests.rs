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

use crate::{
    agent_model::WriteResult,
    event_handler::{
        check_step::check_is_complete, EventHandlerError, HandlerAction, HandlerFuture,
        ModificationFlags, Spawner, StepResult,
    },
    lanes::{command::DoCommand, LaneItem},
    meta::AgentMetadata,
    test_context::dummy_context,
};

use super::CommandLane;

const LANE_ID: u64 = 38;

struct NoSpawn;

impl<Context> Spawner<Context> for NoSpawn {
    fn spawn_suspend(&self, _: HandlerFuture<Context>) {
        panic!("No suspended futures expected.");
    }
}

#[test]
fn send_command() {
    let lane = CommandLane::<i32>::new(LANE_ID);

    assert_eq!(lane.with_prev(Clone::clone), None);

    lane.command(45);
    assert_eq!(lane.with_prev(Clone::clone), Some(45));
}

#[test]
fn write_command_to_buffer() {
    let lane = CommandLane::<i32>::new(LANE_ID);
    lane.command(45);

    let mut buffer = BytesMut::new();
    let result = lane.write_to_buffer(&mut buffer);

    assert!(matches!(result, WriteResult::Done));

    let mut decoder = ValueLaneResponseDecoder::default();
    let content = decoder
        .decode(&mut buffer)
        .expect("Invalid frame.")
        .expect("Incomplete frame.");

    if let LaneResponse::StandardEvent(value) = content {
        assert_eq!(value.as_ref(), b"45");
    } else {
        panic!("Unexpected response.");
    }
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
    lane: CommandLane<i32>,
}

impl Default for TestAgent {
    fn default() -> Self {
        Self {
            lane: CommandLane::new(LANE_ID),
        }
    }
}

impl TestAgent {
    const LANE: fn(&TestAgent) -> &CommandLane<i32> = |agent| &agent.lane;
}

#[test]
fn command_event_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();

    let mut handler = DoCommand::new(TestAgent::LANE, 546);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_is_complete(result, LANE_ID, &(), ModificationFlags::all());

    assert_eq!(agent.lane.with_prev(Clone::clone), Some(546));

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
