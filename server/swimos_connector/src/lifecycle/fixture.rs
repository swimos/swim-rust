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
use futures::{stream::FuturesUnordered, StreamExt};
use swimos_agent::{
    agent_model::downlink::BoxDownlinkChannelFactory,
    event_handler::{
        ActionContext, DownlinkSpawnOnDone, EventHandler, EventHandlerError, HandlerFuture,
        LaneSpawnOnDone, LaneSpawner, LinkSpawner, Spawner, StepResult,
    },
};
use swimos_api::{
    address::Address,
    agent::WarpLaneKind,
    error::{CommanderRegistrationError, DynamicRegistrationError},
};
use swimos_model::Text;

use crate::{
    test_support::{make_meta, make_uri},
    ConnectorAgent,
};

#[derive(Default)]
pub struct TestSpawner {
    futures: FuturesUnordered<HandlerFuture<ConnectorAgent>>,
    downlinks: RefCell<Vec<DownlinkRecord>>,
}

impl TestSpawner {
    fn take_downlinks(&self) -> Vec<DownlinkRecord> {
        let mut guard = self.downlinks.borrow_mut();
        std::mem::take(&mut *guard)
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

pub struct DownlinkRecord {
    pub path: Address<Text>,
    pub make_channel: BoxDownlinkChannelFactory<ConnectorAgent>,
    pub _on_done: DownlinkSpawnOnDone<ConnectorAgent>,
}

impl std::fmt::Debug for DownlinkRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownlinkRecord")
            .field("path", &self.path)
            .field("make_channel", &"...")
            .field("on_done", &"...")
            .finish()
    }
}

impl LinkSpawner<ConnectorAgent> for TestSpawner {
    fn spawn_downlink(
        &self,
        path: Address<Text>,
        make_channel: BoxDownlinkChannelFactory<ConnectorAgent>,
        on_done: DownlinkSpawnOnDone<ConnectorAgent>,
    ) {
        self.downlinks.borrow_mut().push(DownlinkRecord {
            path,
            make_channel,
            _on_done: on_done,
        })
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

pub async fn run_handle_with_futs<H>(
    agent: &ConnectorAgent,
    handler: H,
) -> Result<Vec<DownlinkRecord>, Box<dyn std::error::Error + Send>>
where
    H: EventHandler<ConnectorAgent>,
{
    let mut spawner = TestSpawner::default();
    run_handler(&spawner, agent, handler)?;
    while !spawner.futures.is_empty() {
        match spawner.futures.next().await {
            Some(h) => {
                run_handler(&spawner, agent, h)?;
            }
            None => break,
        }
    }
    Ok(spawner.take_downlinks())
}

pub fn run_handler<H>(
    spawner: &TestSpawner,
    agent: &ConnectorAgent,
    mut handler: H,
) -> Result<(), Box<dyn std::error::Error + Send>>
where
    H: EventHandler<ConnectorAgent>,
{
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let mut join_lane_init = HashMap::new();
    let mut command_buffer = BytesMut::new();

    let mut action_context = ActionContext::new(
        spawner,
        spawner,
        spawner,
        &mut join_lane_init,
        &mut command_buffer,
    );

    loop {
        match handler.step(&mut action_context, meta, agent) {
            StepResult::Continue { .. } => {}
            StepResult::Fail(EventHandlerError::EffectError(err)) => return Err(err),
            StepResult::Fail(err) => panic!("{:?}", err),
            StepResult::Complete { .. } => {
                break;
            }
        }
    }
    Ok(())
}
