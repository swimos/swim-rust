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
    ActionContext, DownlinkSpawnOnDone, EventHandler, EventHandlerError, HandlerAction,
    HandlerFuture, LaneSpawnOnDone, LaneSpawner, LinkSpawner, Spawner, StepResult,
};
use swimos_agent::AgentMetadata;
use swimos_api::address::Address;
use swimos_api::agent::WarpLaneKind;
use swimos_api::error::{CommanderRegistrationError, DynamicRegistrationError};
use swimos_model::Text;

use crate::generic::GenericConnectorAgent;
use crate::test_support::{make_meta, make_uri};
use crate::ConnectorStream;

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

impl HandlerAction<GenericConnectorAgent> for Handler {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<GenericConnectorAgent>,
        _meta: AgentMetadata,
        _context: &GenericConnectorAgent,
    ) -> StepResult<Self::Completion> {
        self.collector.lock().push(self.n);
        StepResult::done(())
    }
}

fn make_stream(
    state: &Arc<Mutex<Vec<usize>>>,
) -> impl ConnectorStream<GenericConnectorAgent, Infallible> + 'static {
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
    let agent = GenericConnectorAgent::default();
    let handler = super::suspend_connector(make_stream(&state));

    run_handler(&spawner, &mut BytesMut::default(), &agent, handler, fail);

    let mut n = 0;
    while !spawner.futures.is_empty() {
        n += 1;
        let h = spawner.futures.next().await.expect("Expected future.");
        run_handler(&spawner, &mut BytesMut::default(), &agent, h, fail);
    }

    assert_eq!(n, 4);
    let guard = state.lock();
    assert_eq!(guard.as_ref(), vec![1, 2, 3])
}

#[derive(Default)]
pub struct TestSpawner<A> {
    futures: FuturesUnordered<HandlerFuture<A>>,
}

impl<A> Spawner<A> for TestSpawner<A> {
    fn spawn_suspend(&self, fut: HandlerFuture<A>) {
        self.futures.push(fut);
    }

    fn schedule_timer(&self, _at: tokio::time::Instant, _id: u64) {
        panic!("Unexpected timer.");
    }
}

impl<A> LinkSpawner<A> for TestSpawner<A> {
    fn spawn_downlink(
        &self,
        _path: Address<Text>,
        _make_channel: BoxDownlinkChannelFactory<A>,
        _on_done: DownlinkSpawnOnDone<A>,
    ) {
        panic!("Spawning downlinks not supported.");
    }

    fn register_commander(&self, _path: Address<Text>) -> Result<u16, CommanderRegistrationError> {
        panic!("Registering commanders not supported.");
    }
}

impl<A> LaneSpawner<A> for TestSpawner<A> {
    fn spawn_warp_lane(
        &self,
        _name: &str,
        _kind: WarpLaneKind,
        _on_done: LaneSpawnOnDone<A>,
    ) -> Result<(), DynamicRegistrationError> {
        panic!("Spawning lanes not supported.");
    }
}

pub fn fail(err: EventHandlerError) {
    panic!("{:?}", err)
}

pub fn run_handler<H, A, F>(
    spawner: &TestSpawner<A>,
    command_buffer: &mut BytesMut,
    agent: &A,
    mut handler: H,
    on_err: F,
) where
    H: EventHandler<A>,
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
