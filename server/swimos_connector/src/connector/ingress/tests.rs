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

use bytes::BytesMut;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::{convert::Infallible, sync::Arc};
use swimos_agent::agent_model::downlink::BoxDownlinkChannelFactory;
use swimos_agent::event_handler::{
    ActionContext, DownlinkSpawnOnDone, EventHandler, HandlerAction, HandlerFuture,
    LaneSpawnOnDone, LaneSpawner, LinkSpawner, Spawner, StepResult,
};
use swimos_agent::AgentMetadata;
use swimos_api::address::Address;
use swimos_api::agent::WarpLaneKind;
use swimos_api::error::{CommanderRegistrationError, DynamicRegistrationError};
use swimos_model::Text;

use crate::test_support::{make_meta, make_uri};
use crate::{ConnectorAgent, ConnectorStream};

#[derive(Debug)]
struct Handler {
    collector: Arc<Mutex<Vec<usize>>>,
    n: usize,
}

impl Handler {
    fn new(collector: &Arc<Mutex<Vec<usize>>>, n: usize) -> Self {
        Handler {
            collector: collector.clone(),
            n,
        }
    }
}

impl HandlerAction<ConnectorAgent> for Handler {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<ConnectorAgent>,
        _meta: AgentMetadata,
        _context: &ConnectorAgent,
    ) -> StepResult<Self::Completion> {
        self.collector.lock().push(self.n);
        StepResult::done(())
    }
}

fn make_stream(state: &Arc<Mutex<Vec<usize>>>) -> impl ConnectorStream<Infallible> + 'static {
    let handlers = vec![
        Ok(Handler::new(state, 1)),
        Ok(Handler::new(state, 2)),
        Ok(Handler::new(state, 3)),
    ];
    futures::stream::iter(handlers)
}

#[tokio::test]
async fn drive_connector_stream() {
    let state = Arc::new(Mutex::new(vec![]));
    let mut spawner = TestSpawner::default();
    let agent = ConnectorAgent::default();
    let handler = super::suspend_connector(make_stream(&state));

    run_handler(&spawner, &agent, handler);

    let mut n = 0;
    while !spawner.futures.is_empty() {
        n += 1;
        let h = spawner.futures.next().await.expect("Expected future.");
        run_handler(&spawner, &agent, h);
    }

    assert_eq!(n, 4);
    let guard = state.lock();
    assert_eq!(guard.as_ref(), vec![1, 2, 3])
}

#[derive(Default)]
struct TestSpawner {
    futures: FuturesUnordered<HandlerFuture<ConnectorAgent>>,
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

fn run_handler<H>(spawner: &TestSpawner, agent: &ConnectorAgent, mut handler: H)
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
            StepResult::Fail(err) => panic!("{:?}", err),
            StepResult::Complete { .. } => {
                break;
            }
        }
    }
}
