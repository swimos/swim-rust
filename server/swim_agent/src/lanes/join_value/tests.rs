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

use std::{collections::HashMap, fmt::Debug, num::NonZeroUsize, sync::Arc};

use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use parking_lot::Mutex;
use swim_api::{
    agent::{AgentConfig, AgentContext, LaneConfig},
    downlink::DownlinkKind,
    error::{AgentRuntimeError, DownlinkRuntimeError, OpenStoreError},
    meta::lane::LaneKind,
    store::StoreKind,
};
use swim_model::{address::Address, Text};
use swim_utilities::{
    io::byte_channel::{byte_channel, ByteReader, ByteWriter},
    non_zero_usize,
    routing::route_uri::RouteUri,
};

use crate::{
    agent_model::downlink::handlers::BoxDownlinkChannel,
    event_handler::{
        ActionContext, DownlinkSpawner, EventHandlerError, HandlerAction, HandlerFuture,
        Modification, Spawner, StepResult, WriteStream,
    },
    item::MapItem,
    lanes::join_value::{
        default_lifecycle::DefaultJoinValueLifecycle, AddDownlinkAction, JoinValueLaneGet,
        JoinValueLaneGetMap,
    },
    meta::AgentMetadata,
    test_context::dummy_context,
};

use super::JoinValueLane;

const ID: u64 = 857383;

const K1: i32 = 5;
const K2: i32 = 78;
const K3: i32 = -4;

const ABSENT: i32 = 93;

const V1: &str = "first";
const V2: &str = "second";
const V3: &str = "third";
const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);

fn init() -> HashMap<i32, String> {
    [(K1, V1), (K2, V2), (K3, V3)]
        .into_iter()
        .map(|(k, v)| (k, v.to_string()))
        .collect()
}

fn make_lane(contents: impl IntoIterator<Item = (i32, String)>) -> JoinValueLane<i32, String> {
    let lane = JoinValueLane::new(ID);
    lane.init(contents.into_iter().collect());
    lane
}

#[test]
fn get_from_join_value_lane() {
    let lane = make_lane(init());

    let value = lane.get(&K1, |v| v.cloned());
    assert_eq!(value, Some(V1.to_string()));

    let value = lane.get(&ABSENT, |v| v.cloned());
    assert!(value.is_none());
}

#[test]
fn get_join_value_lane() {
    let lane = make_lane(init());

    let value = lane.get_map(Clone::clone);
    assert_eq!(value, init());
}

const CONFIG: AgentConfig = AgentConfig::DEFAULT;
const NODE_URI: &str = "/node";

fn make_uri() -> RouteUri {
    RouteUri::try_from(NODE_URI).expect("Bad URI.")
}

fn make_meta(uri: &RouteUri) -> AgentMetadata<'_> {
    AgentMetadata::new(uri, &CONFIG)
}

struct TestAgent {
    lane: JoinValueLane<i32, String>,
}

impl TestAgent {
    pub const LANE: fn(&TestAgent) -> &JoinValueLane<i32, String> = |agent| &agent.lane;

    fn with_init() -> Self {
        TestAgent {
            lane: make_lane(init()),
        }
    }
}

fn check_result<T: Eq + Debug>(
    result: StepResult<T>,
    written: bool,
    trigger_handler: bool,
    complete: Option<T>,
) {
    let expected_mod = if written {
        if trigger_handler {
            Some(Modification::of(ID))
        } else {
            Some(Modification::no_trigger(ID))
        }
    } else {
        None
    };
    match (result, complete) {
        (
            StepResult::Complete {
                modified_item,
                result,
            },
            Some(expected),
        ) => {
            assert_eq!(modified_item, expected_mod);
            assert_eq!(result, expected);
        }
        (StepResult::Continue { modified_item }, None) => {
            assert_eq!(modified_item, expected_mod);
        }
        ow => {
            panic!("Unexpected result: {:?}", ow);
        }
    }
}

#[test]
fn join_value_lane_get_event_handler() {
    let uri = make_uri();
    let meta = make_meta(&uri);
    let agent = TestAgent::with_init();

    let mut handler = JoinValueLaneGet::new(TestAgent::LANE, K1);

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &agent);
    check_result(result, false, false, Some(Some(V1.to_string())));

    let mut handler = JoinValueLaneGet::new(TestAgent::LANE, ABSENT);

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &agent);
    check_result(result, false, false, Some(None));

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &agent);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn join_value_lane_get_map_event_handler() {
    let uri = make_uri();
    let meta = make_meta(&uri);
    let agent = TestAgent::with_init();

    let mut handler = JoinValueLaneGetMap::new(TestAgent::LANE);

    let expected = init();

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &agent);
    check_result(result, false, false, Some(expected));

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &agent);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[derive(Default)]
struct Inner {
    downlink_channels: HashMap<Address<Text>, (ByteWriter, ByteReader)>,
    downlinks: Vec<(BoxDownlinkChannel<TestAgent>, WriteStream)>,
}

#[derive(Default)]
struct TestDownlinkContext {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct TestSpawner {
    futures: FuturesUnordered<HandlerFuture<TestAgent>>,
}

impl TestSpawner {
    async fn drain(
        &mut self,
        context: &TestDownlinkContext,
        meta: AgentMetadata<'_>,
        agent: &TestAgent,
    ) {
        let TestSpawner { futures } = self;
        let additional: FuturesUnordered<HandlerFuture<TestAgent>> = Default::default();
        let mut inits = HashMap::new();

        if !futures.is_empty() {
            while let Some(mut handler) = futures.next().await {
                let mut action_context =
                    ActionContext::new(&additional, context, context, &mut inits);
                loop {
                    match handler.step(&mut action_context, meta, agent) {
                        StepResult::Continue { .. } => {}
                        StepResult::Fail(err) => panic!("Handler failed: {:?}", err),
                        StepResult::Complete { .. } => break,
                    }
                }
            }
        }
        assert!(additional.is_empty());
        assert!(inits.is_empty());
    }
}

impl TestDownlinkContext {
    fn push_dl(&self, dl_channel: BoxDownlinkChannel<TestAgent>, dl_writer: WriteStream) {
        self.inner.lock().downlinks.push((dl_channel, dl_writer));
    }

    fn push_channels(&self, key: Address<Text>, io: (ByteWriter, ByteReader)) {
        self.inner.lock().downlink_channels.insert(key, io);
    }
}

impl DownlinkSpawner<TestAgent> for TestDownlinkContext {
    fn spawn_downlink(
        &self,
        dl_channel: BoxDownlinkChannel<TestAgent>,
        dl_writer: WriteStream,
    ) -> Result<(), DownlinkRuntimeError> {
        self.push_dl(dl_channel, dl_writer);
        Ok(())
    }
}

impl Spawner<TestAgent> for TestSpawner {
    fn spawn_suspend(&self, fut: HandlerFuture<TestAgent>) {
        self.futures.push(fut);
    }
}

impl AgentContext for TestDownlinkContext {
    fn add_lane(
        &self,
        _name: &str,
        _lane_kind: LaneKind,
        _config: LaneConfig,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), AgentRuntimeError>> {
        panic!("Unexpected new lane.");
    }

    fn add_store(
        &self,
        _name: &str,
        _kind: StoreKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), OpenStoreError>> {
        panic!("Unexpected new store.");
    }

    fn open_downlink(
        &self,
        host: Option<&str>,
        node: &str,
        lane: &str,
        kind: DownlinkKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), DownlinkRuntimeError>> {
        assert_eq!(kind, DownlinkKind::Value);
        let key = Address::text(host, node, lane);
        let (in_tx, in_rx) = byte_channel(BUFFER_SIZE);
        let (out_tx, out_rx) = byte_channel(BUFFER_SIZE);
        self.push_channels(key, (in_tx, out_rx));
        async move { Ok((out_tx, in_rx)) }.boxed()
    }
}

const REMOTE_NODE: &str = "/remote_node";
const REMOTE_LANE: &str = "remote_lane";

#[tokio::test]
async fn join_value_lane_add_downlinks_event_handler() {
    let uri = make_uri();
    let meta = make_meta(&uri);
    let agent = TestAgent::with_init();

    let address = Address::text(None, REMOTE_NODE, REMOTE_LANE);

    let mut handler = AddDownlinkAction::new(
        TestAgent::LANE,
        34,
        address.clone(),
        DefaultJoinValueLifecycle,
    );

    let context = TestDownlinkContext::default();
    let mut spawner = TestSpawner::default();
    let mut inits = HashMap::new();

    let mut action_context = ActionContext::new(&spawner, &context, &context, &mut inits);
    let result = handler.step(&mut action_context, meta, &agent);
    check_result(result, false, false, Some(()));

    let result = handler.step(&mut action_context, meta, &agent);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));

    drop(action_context);
    spawner.drain(&context, meta, &agent).await;

    let guard = context.inner.lock();
    let Inner {
        downlink_channels,
        downlinks,
    } = &*guard;
    assert_eq!(downlink_channels.len(), 1);
    assert!(downlink_channels.contains_key(&address));
    match downlinks.as_slice() {
        [(channel, _)] => {
            assert_eq!(channel.kind(), DownlinkKind::Event);
        }
        _ => panic!("Expected a single downlink."),
    }
}
