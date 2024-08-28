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

use std::{cell::RefCell, collections::HashMap};

use bytes::BytesMut;
use swimos_agent_protocol::{encoding::command::CommandMessageDecoder, CommandMessage};
use swimos_api::{
    address::Address,
    agent::{AgentConfig, WarpLaneKind},
    error::{CommanderRegistrationError, DynamicRegistrationError},
};
use swimos_model::{Text, Value};
use swimos_utilities::routing::RouteUri;
use tokio_util::codec::Decoder;

use crate::{
    agent_model::downlink::BoxDownlinkChannelFactory,
    commander::Commander,
    event_handler::{
        ActionContext, DownlinkSpawnOnDone, EventHandlerError, HandlerAction, HandlerFuture,
        LaneSpawnOnDone, LaneSpawner, LinkSpawner, Spawner, StepResult,
    },
    AgentMetadata,
};

use super::RegisterCommander;

struct FakeAgent;

struct TestSpawner {
    inner: RefCell<HashMap<Address<Text>, Result<u16, CommanderRegistrationError>>>,
}

impl TestSpawner {
    pub fn new(address: Address<Text>, result: Result<u16, CommanderRegistrationError>) -> Self {
        TestSpawner {
            inner: RefCell::new([(address, result)].into_iter().collect()),
        }
    }
}

impl LinkSpawner<FakeAgent> for TestSpawner {
    fn spawn_downlink(
        &self,
        _path: Address<Text>,
        _make_channel: BoxDownlinkChannelFactory<FakeAgent>,
        _on_done: DownlinkSpawnOnDone<FakeAgent>,
    ) {
        panic!("Downlinks not supported.");
    }

    fn register_commander(&self, path: Address<Text>) -> Result<u16, CommanderRegistrationError> {
        self.inner
            .borrow_mut()
            .remove(&path)
            .unwrap_or(Err(CommanderRegistrationError::CommanderIdOverflow))
    }
}

impl LaneSpawner<FakeAgent> for TestSpawner {
    fn spawn_warp_lane(
        &self,
        _name: &str,
        _kind: WarpLaneKind,
        _on_done: LaneSpawnOnDone<FakeAgent>,
    ) -> Result<(), DynamicRegistrationError> {
        panic!("Dynamic lanes not supported.");
    }
}

impl Spawner<FakeAgent> for TestSpawner {
    fn spawn_suspend(&self, _fut: HandlerFuture<FakeAgent>) {
        panic!("Spawning futures not supported.");
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

fn run_handler<H>(
    mut handler: H,
    spawner: &TestSpawner,
    buffer: &mut BytesMut,
    agent: &FakeAgent,
    meta: AgentMetadata<'_>,
) -> Result<H::Completion, EventHandlerError>
where
    H: HandlerAction<FakeAgent>,
{
    let mut join_lane_init = HashMap::new();
    loop {
        let mut action_context =
            ActionContext::new(spawner, spawner, spawner, &mut join_lane_init, buffer);
        match handler.step(&mut action_context, meta, agent) {
            StepResult::Continue { modified_item } => {
                assert!(modified_item.is_none());
            }
            StepResult::Fail(err) => {
                assert!(join_lane_init.is_empty());
                break Err(err);
            }
            StepResult::Complete {
                modified_item,
                result,
            } => {
                assert!(modified_item.is_none());
                assert!(join_lane_init.is_empty());
                break Ok(result);
            }
        }
    }
}

#[test]
fn create_commander() {
    let address = Address::new(
        Some(Text::new("ws://remote:8080")),
        Text::new("/target"),
        Text::new("lane"),
    );
    let spawner = TestSpawner::new(address.clone(), Ok(7));
    let handler = RegisterCommander::new(address.clone());
    let mut buffer = BytesMut::new();
    let agent = FakeAgent;
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let commander =
        run_handler(handler, &spawner, &mut buffer, &agent, meta).expect("Failed unexpectedly.");

    assert_eq!(commander.id, 7);

    let mut decoder = CommandMessageDecoder::<Text, String>::default();

    let message = decoder
        .decode_eof(&mut buffer)
        .expect("Invalid buffer contents.")
        .expect("No message.");

    assert_eq!(message, CommandMessage::Register { address, id: 7 });
}

#[test]
fn fail_create_commander() {
    let address = Address::new(
        Some(Text::new("ws://remote:8080")),
        Text::new("/target"),
        Text::new("lane"),
    );
    let spawner = TestSpawner::new(
        address.clone(),
        Err(CommanderRegistrationError::CommanderIdOverflow),
    );
    let handler = RegisterCommander::new(address.clone());
    let mut buffer = BytesMut::new();
    let agent = FakeAgent;
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let error =
        run_handler(handler, &spawner, &mut buffer, &agent, meta).expect_err("Should fail.");

    assert!(matches!(
        error,
        EventHandlerError::FailedCommanderRegistration(
            CommanderRegistrationError::CommanderIdOverflow
        )
    ));

    assert!(buffer.is_empty());
}

#[test]
fn send_registered_command_with_overwrite() {
    let address = Address::new(
        Some(Text::new("ws://remote:8080")),
        Text::new("/target"),
        Text::new("lane"),
    );
    let spawner = TestSpawner::new(address.clone(), Ok(7));
    let mut buffer = BytesMut::new();
    let agent = FakeAgent;
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let commander: Commander<FakeAgent> = Commander::new(4);
    let handler = commander.send("hello");
    run_handler(handler, &spawner, &mut buffer, &agent, meta).expect("Failed unexpectedly.");

    let mut decoder = CommandMessageDecoder::<Text, Value>::default();

    let message = decoder
        .decode_eof(&mut buffer)
        .expect("Invalid buffer contents.")
        .expect("No message.");

    assert_eq!(
        message,
        CommandMessage::<Text, Value>::Registered {
            target: 4,
            command: Value::text("hello"),
            overwrite_permitted: true
        }
    );
}

#[test]
fn send_registered_command_without_overwrite() {
    let address = Address::new(
        Some(Text::new("ws://remote:8080")),
        Text::new("/target"),
        Text::new("lane"),
    );
    let spawner = TestSpawner::new(address.clone(), Ok(7));
    let mut buffer = BytesMut::new();
    let agent = FakeAgent;
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let commander: Commander<FakeAgent> = Commander::new(4);
    let handler = commander.send_queued("hello");
    run_handler(handler, &spawner, &mut buffer, &agent, meta).expect("Failed unexpectedly.");

    let mut decoder = CommandMessageDecoder::<Text, Value>::default();

    let message = decoder
        .decode_eof(&mut buffer)
        .expect("Invalid buffer contents.")
        .expect("No message.");

    assert_eq!(
        message,
        CommandMessage::<Text, Value>::Registered {
            target: 4,
            command: Value::text("hello"),
            overwrite_permitted: false
        }
    );
}
