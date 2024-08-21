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
use swimos_agent_protocol::encoding::ad_hoc::AdHocCommandDecoder;
use swimos_agent_protocol::CommandMessage;
use swimos_api::{address::Address, agent::AgentConfig};
use swimos_utilities::{encoding::BytesStr, routing::RouteUri};
use tokio_util::codec::Decoder;

use crate::{
    event_handler::{EventHandlerError, HandlerAction, StepResult},
    meta::AgentMetadata,
    test_context::dummy_context,
};

use super::SendCommand;

const HOST: &str = "localhost:8080";
const NODE: &str = "/node";
const LANE: &str = "lane";

struct FakeAgent;

fn make_uri() -> RouteUri {
    RouteUri::try_from("/self").expect("Bad URI.")
}

fn make_meta<'a>(
    uri: &'a RouteUri,
    route_params: &'a HashMap<String, String>,
) -> AgentMetadata<'a> {
    AgentMetadata::new(uri, route_params, &AgentConfig::DEFAULT)
}

#[test]
fn write_command_to_buffer() {
    let mut join_lane_init = HashMap::new();
    let mut ad_hoc_buffer = BytesMut::new();
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let address = Address::new(Some(HOST), NODE, LANE);
    let command = 23;
    let mut handler = SendCommand::new(address, command, true);
    {
        let mut action_context = dummy_context(&mut join_lane_init, &mut ad_hoc_buffer);

        match handler.step(&mut action_context, meta, &FakeAgent) {
            StepResult::Complete { modified_item, .. } => {
                assert!(modified_item.is_none());
            }
            ow => panic!("Unexpected step result: {:?}", ow),
        }
    }
    let msg = decode_message(&mut ad_hoc_buffer);
    assert_eq!(msg, CommandMessage::ad_hoc(address, command, true));
    {
        let mut action_context = dummy_context(&mut join_lane_init, &mut ad_hoc_buffer);
        let result = handler.step(&mut action_context, meta, &FakeAgent);
        assert!(matches!(
            result,
            StepResult::Fail(EventHandlerError::SteppedAfterComplete)
        ));
    }
}

fn decode_message(buffer: &mut BytesMut) -> CommandMessage<BytesStr, i32> {
    let mut decoder = AdHocCommandDecoder::<BytesStr, i32>::default();
    let cmd = decoder
        .decode(buffer)
        .expect("Decoding failed.")
        .expect("Incomplete record.");
    assert!(buffer.is_empty());
    cmd
}
