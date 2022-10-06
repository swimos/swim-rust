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

use std::{num::NonZeroUsize, sync::Arc};

use futures::{
    future::{ready, BoxFuture},
    stream::FuturesUnordered,
    FutureExt, StreamExt,
};
use parking_lot::Mutex;
use swim_api::{
    agent::{AgentConfig, AgentContext, LaneConfig, UplinkKind},
    downlink::DownlinkKind,
    error::{AgentRuntimeError, DownlinkRuntimeError, OpenStoreError},
    store::StoreKind,
};
use swim_model::{address::Address, Text};
use swim_utilities::{
    algebra::non_zero_usize,
    io::byte_channel::{byte_channel, ByteReader, ByteWriter},
    routing::uri::RelativeUri,
};

use crate::{
    config::{MapDownlinkConfig, ValueDownlinkConfig},
    downlink_lifecycle::{
        map::StatefulMapDownlinkLifecycle, value::StatefulValueDownlinkLifecycle,
    },
    event_handler::{
        ActionContext, DownlinkSpawner, HandlerAction, HandlerFuture, Spawner, StepResult,
        WriteStream,
    },
    meta::AgentMetadata,
};

use super::{handlers::BoxDownlinkChannel, OpenMapDownlink, OpenValueDownlink};

struct TestAgent;

#[derive(Default)]
struct SpawnerInner {
    downlink: Option<(BoxDownlinkChannel<TestAgent>, WriteStream)>,
}

#[derive(Default)]
struct TestSpawner {
    futures: FuturesUnordered<HandlerFuture<TestAgent>>,
    inner: Arc<Mutex<SpawnerInner>>,
}

struct ContextInner {
    io: Option<(ByteWriter, ByteReader)>,
}

struct TestContext {
    expected_kind: DownlinkKind,
    inner: Arc<Mutex<ContextInner>>,
}

impl TestContext {
    fn new(expected_kind: DownlinkKind, io: (ByteWriter, ByteReader)) -> Self {
        TestContext {
            expected_kind,
            inner: Arc::new(Mutex::new(ContextInner { io: Some(io) })),
        }
    }
}

impl Spawner<TestAgent> for TestSpawner {
    fn spawn_suspend(&self, fut: HandlerFuture<TestAgent>) {
        self.futures.push(fut);
    }
}

impl DownlinkSpawner<TestAgent> for TestSpawner {
    fn spawn_downlink(
        &self,
        dl_channel: BoxDownlinkChannel<TestAgent>,
        dl_writer: WriteStream,
    ) -> Result<(), DownlinkRuntimeError> {
        let mut guard = self.inner.lock();
        assert!(guard.downlink.is_none());
        guard.downlink = Some((dl_channel, dl_writer));
        Ok(())
    }
}

const HOST: &str = "localhost";
const NODE: &str = "/node";
const LANE: &str = "lane";

impl AgentContext for TestContext {
    fn add_lane(
        &self,
        _name: &str,
        _uplink_kind: UplinkKind,
        _config: LaneConfig,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), AgentRuntimeError>> {
        panic!("Unexpected request to open a lane.")
    }

    fn open_downlink(
        &self,
        host: Option<&str>,
        node: &str,
        lane: &str,
        kind: DownlinkKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), DownlinkRuntimeError>> {
        assert_eq!(host, Some(HOST));
        assert_eq!(node, NODE);
        assert_eq!(lane, LANE);
        assert_eq!(kind, self.expected_kind);
        let io = self.inner.lock().io.take().expect("IO taken twice.");
        ready(Ok(io)).boxed()
    }

    fn open_store(
        &self,
        _name: &str,
        _kind: StoreKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), OpenStoreError>> {
        ready(Err(OpenStoreError::StoresNotSupported)).boxed()
    }
}

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);

const CONFIG: AgentConfig = AgentConfig::DEFAULT;
const NODE_URI: &str = "/node";

fn make_uri() -> RelativeUri {
    RelativeUri::try_from(NODE_URI).expect("Bad URI.")
}

fn make_meta(uri: &RelativeUri) -> AgentMetadata<'_> {
    AgentMetadata::new(uri, &CONFIG)
}

async fn run_all_and_check(
    mut spawner: TestSpawner,
    context: TestContext,
    meta: AgentMetadata<'_>,
    agent: &TestAgent,
) {
    while let Some(handler) = spawner.futures.next().await {
        let action_context = ActionContext::new(&spawner, &context, &spawner);
        run_handler(handler, action_context, agent, meta);
    }
    spawner
        .inner
        .lock()
        .downlink
        .take()
        .expect("Downlink was not registered.");
}

fn run_handler<H>(
    mut handler: H,
    action_context: ActionContext<'_, TestAgent>,
    agent: &TestAgent,
    meta: AgentMetadata<'_>,
) -> H::Completion
where
    H: HandlerAction<TestAgent>,
{
    loop {
        match handler.step(action_context, meta, agent) {
            StepResult::Continue { modified_lane } => {
                assert!(modified_lane.is_none());
            }
            StepResult::Fail(err) => panic!("{}", err),
            StepResult::Complete {
                modified_lane,
                result,
            } => {
                assert!(modified_lane.is_none());
                break result;
            }
        }
    }
}

#[tokio::test]
async fn open_value_downlink() {
    let uri = make_uri();
    let meta = make_meta(&uri);
    let lifecycle = StatefulValueDownlinkLifecycle::<TestAgent, _, i32>::new(());

    let handler = OpenValueDownlink::<i32, _>::new(
        Address::text(Some(HOST), NODE, LANE),
        lifecycle,
        ValueDownlinkConfig::default(),
    );

    let spawner = TestSpawner::default();
    let (in_tx, _in_rx) = byte_channel(BUFFER_SIZE);
    let (_out_tx, out_rx) = byte_channel(BUFFER_SIZE);
    let context = TestContext::new(DownlinkKind::Value, (in_tx, out_rx));

    let agent = TestAgent;
    let action_context = ActionContext::new(&spawner, &context, &spawner);
    let _handle = run_handler(handler, action_context, &agent, meta);

    run_all_and_check(spawner, context, meta, &agent).await;
}

#[tokio::test]
async fn open_map_downlink() {
    let uri = make_uri();
    let meta = make_meta(&uri);
    let lifecycle = StatefulMapDownlinkLifecycle::<TestAgent, _, i32, Text>::new(());

    let handler = OpenMapDownlink::<i32, Text, _>::new(
        Address::text(Some(HOST), NODE, LANE),
        lifecycle,
        MapDownlinkConfig::default(),
    );

    let spawner = TestSpawner::default();
    let (in_tx, _in_rx) = byte_channel(BUFFER_SIZE);
    let (_out_tx, out_rx) = byte_channel(BUFFER_SIZE);
    let context = TestContext::new(DownlinkKind::Map, (in_tx, out_rx));

    let agent = TestAgent;
    let action_context = ActionContext::new(&spawner, &context, &spawner);
    let _handle = run_handler(handler, action_context, &agent, meta);

    run_all_and_check(spawner, context, meta, &agent).await;
}
