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
use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use swimos_api::{
    address::Address,
    agent::{
        AgentContext, DownlinkKind, HttpLaneRequestChannel, LaneConfig, StoreKind, WarpLaneKind,
    },
    error::{
        AgentRuntimeError, CommanderRegistrationError, DownlinkRuntimeError,
        DynamicRegistrationError, OpenStoreError,
    },
};
use swimos_model::Text;
use swimos_utilities::byte_channel::{ByteReader, ByteWriter};

use crate::{
    agent_model::downlink::BoxDownlinkChannelFactory,
    event_handler::{
        ActionContext, BoxJoinLaneInit, DownlinkSpawnOnDone, DownlinkSpawner, EventHandler,
        HandlerAction, HandlerFuture, LaneSpawnOnDone, LaneSpawner, Spawner, StepResult,
    },
    meta::AgentMetadata,
    test_util::TestDownlinkContext,
};

struct NoSpawn;
pub struct NoDownlinks;
pub struct DummyAgentContext;

const NO_SPAWN: NoSpawn = NoSpawn;
pub const NO_DYN_LANES: NoDynamicLanes = NoDynamicLanes;
pub const NO_DOWNLINKS: NoDownlinks = NoDownlinks;

impl<Context> DownlinkSpawner<Context> for NoDownlinks {
    fn spawn_downlink(
        &self,
        _path: Address<Text>,
        _make_channel: BoxDownlinkChannelFactory<Context>,
        _on_done: DownlinkSpawnOnDone<Context>,
    ) {
        panic!("Opening downlinks not supported.")
    }
}

pub struct NoDynamicLanes;

impl<Context> LaneSpawner<Context> for NoDynamicLanes {
    fn spawn_warp_lane(
        &self,
        _name: &str,
        _kind: WarpLaneKind,
        _on_done: LaneSpawnOnDone<Context>,
    ) -> Result<(), DynamicRegistrationError> {
        panic!("Spawning dynamic lanes not supported.");
    }
}

pub fn dummy_context<'a, Context>(
    join_lane_init: &'a mut HashMap<u64, BoxJoinLaneInit<'static, Context>>,
    ad_hoc_buffer: &'a mut BytesMut,
) -> ActionContext<'a, Context> {
    ActionContext::new(
        &NO_SPAWN,
        &NO_DOWNLINKS,
        &NO_DYN_LANES,
        join_lane_init,
        ad_hoc_buffer,
    )
}

impl<Context> Spawner<Context> for NoSpawn {
    fn spawn_suspend(&self, _: HandlerFuture<Context>) {
        panic!("No suspended futures expected.");
    }
}

impl AgentContext for DummyAgentContext {
    fn command_channel(&self) -> BoxFuture<'static, Result<ByteWriter, DownlinkRuntimeError>> {
        panic!("Dummy context used.");
    }

    fn add_lane(
        &self,
        _name: &str,
        _lane_kind: WarpLaneKind,
        _config: LaneConfig,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), AgentRuntimeError>> {
        panic!("Dummy context used.");
    }

    fn open_downlink(
        &self,
        _host: Option<&str>,
        _node: &str,
        _lane: &str,
        _kind: DownlinkKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), DownlinkRuntimeError>> {
        panic!("Dummy context used.");
    }

    fn add_store(
        &self,
        _name: &str,
        _kind: StoreKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), OpenStoreError>> {
        panic!("Dummy context used.");
    }

    fn add_http_lane(
        &self,
        _name: &str,
    ) -> BoxFuture<'static, Result<HttpLaneRequestChannel, AgentRuntimeError>> {
        panic!("Dummy context used.");
    }

    fn register_command_endpoint(
        &self,
        _host: Option<&str>,
        _node: &str,
        _lane: &str,
    ) -> BoxFuture<'static, Result<u16, CommanderRegistrationError>> {
        panic!("Dummy context used.");
    }
}

pub async fn run_with_futures<H, Agent>(
    context: &TestDownlinkContext<Agent>,
    agent: &Agent,
    meta: AgentMetadata<'_>,
    inits: &mut HashMap<u64, BoxJoinLaneInit<'static, Agent>>,
    ad_hoc_buffer: &mut BytesMut,
    mut handler: H,
) -> H::Completion
where
    H: HandlerAction<Agent>,
{
    let pending = FuturesUnordered::new();

    let result = loop {
        let mut action_context =
            ActionContext::new(&pending, context, context, inits, ad_hoc_buffer);
        match handler.step(&mut action_context, meta, agent) {
            StepResult::Continue { .. } => {}
            StepResult::Fail(err) => panic!("Handler failed: {:?}", err),
            StepResult::Complete { result, .. } => {
                break result;
            }
        };
    };

    run_event_handlers(context, agent, meta, inits, ad_hoc_buffer, pending).await;

    result
}

pub async fn run_event_handlers<'a, Agent>(
    context: &TestDownlinkContext<Agent>,
    agent: &Agent,
    meta: AgentMetadata<'_>,
    inits: &mut HashMap<u64, BoxJoinLaneInit<'static, Agent>>,
    ad_hoc_buffer: &mut BytesMut,
    mut handlers: FuturesUnordered<HandlerFuture<Agent>>,
) {
    let mut dl_handlers = context.handle_dl_requests(agent);
    while !dl_handlers.is_empty() || !handlers.is_empty() {
        for h in dl_handlers.drain(..) {
            run_event_handler(context, agent, meta, inits, ad_hoc_buffer, &handlers, h);
        }
        if !handlers.is_empty() {
            while let Some(h) = handlers.next().await {
                run_event_handler(context, agent, meta, inits, ad_hoc_buffer, &handlers, h);
            }
        }
        dl_handlers.extend(context.handle_dl_requests(agent));
    }
}

fn run_event_handler<Agent, H>(
    context: &TestDownlinkContext<Agent>,
    agent: &Agent,
    meta: AgentMetadata<'_>,
    inits: &mut HashMap<u64, BoxJoinLaneInit<'static, Agent>>,
    ad_hoc_buffer: &mut BytesMut,
    handlers: &FuturesUnordered<HandlerFuture<Agent>>,
    mut handler: H,
) where
    H: EventHandler<Agent>,
{
    loop {
        let mut action_context =
            ActionContext::new(handlers, context, context, inits, ad_hoc_buffer);
        match handler.step(&mut action_context, meta, agent) {
            StepResult::Continue { .. } => {}
            StepResult::Fail(err) => panic!("Handler failed: {:?}", err),
            StepResult::Complete { .. } => break,
        };
    }
}
