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

use bytes::BytesMut;
use swim_api::{
    agent::AgentConfig,
    protocol::agent::{LaneResponse, ValueLaneResponseDecoder},
};
use swim_utilities::routing::uri::RelativeUri;
use tokio_util::codec::Decoder;

use crate::{
    agent_model::WriteResult,
    event_handler::{
        EventHandlerError, HandlerAction, HandlerFuture, Modification, Spawner, StepResult,
    },
    lanes::{command::DoCommand, Lane},
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

const CONFIG: AgentConfig = AgentConfig {};
const NODE_URI: &str = "/node";

fn make_uri() -> RelativeUri {
    RelativeUri::try_from(NODE_URI).expect("Bad URI.")
}

fn make_meta(uri: &RelativeUri) -> AgentMetadata<'_> {
    AgentMetadata::new(uri, &CONFIG)
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
    let meta = make_meta(&uri);
    let agent = TestAgent::default();

    let mut handler = DoCommand::new(TestAgent::LANE, 546);

    let result = handler.step(dummy_context(), meta, &agent);

    assert!(matches!(
        result,
        StepResult::Complete {
            modified_lane: Some(Modification {
                lane_id: LANE_ID,
                trigger_handler: true
            }),
            result: ()
        }
    ));

    assert_eq!(agent.lane.with_prev(Clone::clone), Some(546));

    let result = handler.step(dummy_context(), meta, &agent);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}
