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
    agent::{
        AgentContext, DownlinkKind, HttpLaneRequestChannel, LaneConfig, StoreKind, WarpLaneKind,
    },
    error::{AgentRuntimeError, DownlinkRuntimeError, OpenStoreError},
};
use swimos_utilities::byte_channel::{ByteReader, ByteWriter};

use crate::{
    agent_model::downlink::BoxDownlinkChannel,
    event_handler::{
        ActionContext, BoxJoinLaneInit, DownlinkSpawner, HandlerAction, HandlerFuture, Spawner,
        StepResult,
    },
    meta::AgentMetadata,
};

struct NoSpawn;
pub struct DummyAgentContext;

pub fn no_downlink<Context>(_dl: BoxDownlinkChannel<Context>) -> Result<(), DownlinkRuntimeError> {
    panic!("Launching downlinks no supported.");
}

const NO_SPAWN: NoSpawn = NoSpawn;
const NO_AGENT: DummyAgentContext = DummyAgentContext;

pub fn dummy_context<'a, Context>(
    join_lane_init: &'a mut HashMap<u64, BoxJoinLaneInit<'static, Context>>,
    ad_hoc_buffer: &'a mut BytesMut,
) -> ActionContext<'a, Context> {
    ActionContext::new(
        &NO_SPAWN,
        &NO_AGENT,
        &no_downlink,
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
    fn ad_hoc_commands(&self) -> BoxFuture<'static, Result<ByteWriter, DownlinkRuntimeError>> {
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
}

pub async fn run_with_futures<H, Agent>(
    agent_context: &dyn AgentContext,
    downlink_spawner: &dyn DownlinkSpawner<Agent>,
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
        let mut action_context = ActionContext::new(
            &pending,
            agent_context,
            downlink_spawner,
            inits,
            ad_hoc_buffer,
        );
        match handler.step(&mut action_context, meta, agent) {
            StepResult::Continue { .. } => {}
            StepResult::Fail(err) => panic!("Handler failed: {:?}", err),
            StepResult::Complete { result, .. } => {
                break result;
            }
        };
    };

    run_event_handlers(
        agent_context,
        downlink_spawner,
        agent,
        meta,
        inits,
        ad_hoc_buffer,
        pending,
    )
    .await;

    result
}

pub async fn run_event_handlers<'a, Agent>(
    agent_context: &dyn AgentContext,
    downlink_spawner: &dyn DownlinkSpawner<Agent>,
    agent: &Agent,
    meta: AgentMetadata<'_>,
    inits: &mut HashMap<u64, BoxJoinLaneInit<'static, Agent>>,
    ad_hoc_buffer: &mut BytesMut,
    mut handlers: FuturesUnordered<HandlerFuture<Agent>>,
) {
    if !handlers.is_empty() {
        while let Some(mut h) = handlers.next().await {
            loop {
                let mut action_context = ActionContext::new(
                    &handlers,
                    agent_context,
                    downlink_spawner,
                    inits,
                    ad_hoc_buffer,
                );
                match h.step(&mut action_context, meta, agent) {
                    StepResult::Continue { .. } => {}
                    StepResult::Fail(err) => panic!("Handler failed: {:?}", err),
                    StepResult::Complete { .. } => break,
                };
            }
        }
    }
}
