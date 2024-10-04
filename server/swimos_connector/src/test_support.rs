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

use crate::ConnectorAgent;
use bytes::BytesMut;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{Stream, StreamExt};
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use swimos_agent::agent_model::downlink::BoxDownlinkChannelFactory;
use swimos_agent::event_handler::{
    ActionContext, DownlinkSpawnOnDone, EventHandler, EventHandlerError, HandlerFuture,
    LaneSpawnOnDone, LaneSpawner, LinkSpawner, LocalBoxEventHandler, Spawner, StepResult,
};
use swimos_agent::AgentMetadata;
use swimos_api::address::Address;
use swimos_api::agent::{
    AgentConfig, AgentContext, DownlinkKind, HttpLaneRequestChannel, LaneConfig, StoreKind,
    WarpLaneKind,
};
use swimos_api::error::{
    AgentRuntimeError, CommanderRegistrationError, DownlinkRuntimeError, DynamicRegistrationError,
    OpenStoreError,
};
use swimos_model::Text;
use swimos_utilities::byte_channel::{ByteReader, ByteWriter};
use swimos_utilities::routing::RouteUri;

pub struct TestContext;

impl AgentContext for TestContext {
    fn command_channel(&self) -> BoxFuture<'static, Result<ByteWriter, DownlinkRuntimeError>> {
        panic!("Ad hoc commands not supported.");
    }

    fn add_lane(
        &self,
        _name: &str,
        _lane_kind: WarpLaneKind,
        _config: LaneConfig,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), AgentRuntimeError>> {
        panic!("Adding lanes not supported.");
    }

    fn add_http_lane(
        &self,
        _name: &str,
    ) -> BoxFuture<'static, Result<HttpLaneRequestChannel, AgentRuntimeError>> {
        panic!("Adding lanes not supported.");
    }

    fn open_downlink(
        &self,
        _host: Option<&str>,
        _node: &str,
        _lane: &str,
        _kind: DownlinkKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), DownlinkRuntimeError>> {
        panic!("Opening downlinks not supported.");
    }

    fn add_store(
        &self,
        _name: &str,
        _kind: StoreKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), OpenStoreError>> {
        panic!("Opening stores not supported.");
    }
}

const CONFIG: AgentConfig = AgentConfig::DEFAULT;
const NODE_URI: &str = "/node";

pub fn make_uri() -> RouteUri {
    RouteUri::try_from(NODE_URI).expect("Bad URI.")
}

pub fn make_meta<'a>(
    uri: &'a RouteUri,
    route_params: &'a HashMap<String, String>,
) -> AgentMetadata<'a> {
    AgentMetadata::new(uri, route_params, &CONFIG)
}

#[derive(Default)]
pub struct TestSpawner {
    futures: FuturesUnordered<HandlerFuture<ConnectorAgent>>,
}

impl Stream for TestSpawner {
    type Item = LocalBoxEventHandler<'static, ConnectorAgent>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.futures.poll_next_unpin(cx)
    }
}

impl TestSpawner {
    pub fn is_empty(&self) -> bool {
        self.futures.is_empty()
    }
}

impl Spawner<ConnectorAgent> for TestSpawner {
    fn spawn_suspend(&self, fut: HandlerFuture<ConnectorAgent>) {
        self.futures.push(fut);
    }

    fn schedule_timer(&self, _at: tokio::time::Instant, _id: u64) {
        panic!("Unexpected timer.");
    }
}

impl LinkSpawner<ConnectorAgent> for TestSpawner {
    fn spawn_downlink(
        &self,
        _path: Address<Text>,
        _make_channel: BoxDownlinkChannelFactory<ConnectorAgent>,
        _on_done: DownlinkSpawnOnDone<ConnectorAgent>,
    ) {
        panic!("Spawning downlinks not supported.");
    }

    fn register_commander(&self, _path: Address<Text>) -> Result<u16, CommanderRegistrationError> {
        panic!("Registering commanders not supported.");
    }
}

impl LaneSpawner<ConnectorAgent> for TestSpawner {
    fn spawn_warp_lane(
        &self,
        _name: &str,
        _kind: WarpLaneKind,
        _on_done: LaneSpawnOnDone<ConnectorAgent>,
    ) -> Result<(), DynamicRegistrationError> {
        panic!("Spawning lanes not supported.");
    }
}

pub fn run_handler<F, H>(
    spawner: &TestSpawner,
    command_buffer: &mut BytesMut,
    agent: &ConnectorAgent,
    mut handler: H,
    on_err: F,
) where
    H: EventHandler<ConnectorAgent>,
    F: FnOnce(EventHandlerError),
{
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let mut join_lane_init = HashMap::new();

    let mut action_context = ActionContext::new(
        spawner,
        spawner,
        spawner,
        &mut join_lane_init,
        command_buffer,
    );

    loop {
        match handler.step(&mut action_context, meta, agent) {
            StepResult::Continue { .. } => {}
            StepResult::Fail(err) => {
                on_err(err);
                break;
            }
            StepResult::Complete { .. } => {
                break;
            }
        }
    }
}

pub fn fail(err: EventHandlerError) {
    panic!("{:?}", err)
}
