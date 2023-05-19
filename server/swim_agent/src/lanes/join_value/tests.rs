// Copyright 2015-2023 Swim Inc.
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

use std::{
    collections::HashMap,
    fmt::Debug,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt};
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
        ActionContext, BoxJoinValueInit, DownlinkSpawner, EventHandlerError, HandlerAction,
        Modification, StepResult, WriteStream,
    },
    item::{AgentItem, MapItem},
    lanes::join_value::{
        default_lifecycle::DefaultJoinValueLifecycle, AddDownlinkAction, JoinValueLaneGet,
        JoinValueLaneGetMap,
    },
    meta::AgentMetadata,
    test_context::{dummy_context, run_event_handlers, run_with_futures},
};

use super::{JoinValueAddDownlink, JoinValueLane, LifecycleInitializer};

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

fn make_meta<'a>(
    uri: &'a RouteUri,
    route_params: &'a HashMap<String, String>,
) -> AgentMetadata<'a> {
    AgentMetadata::new(uri, route_params, &CONFIG)
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
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
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
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
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

struct Inner<Agent> {
    downlink_channels: HashMap<Address<Text>, (ByteWriter, ByteReader)>,
    downlinks: Vec<(BoxDownlinkChannel<Agent>, WriteStream)>,
}

impl<Agent> Default for Inner<Agent> {
    fn default() -> Self {
        Self {
            downlink_channels: Default::default(),
            downlinks: Default::default(),
        }
    }
}

pub struct TestDownlinkContext<Agent> {
    inner: Arc<Mutex<Inner<Agent>>>,
}

impl<Agent> Default for TestDownlinkContext<Agent> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

impl<Agent> TestDownlinkContext<Agent> {
    pub fn push_dl(&self, dl_channel: BoxDownlinkChannel<Agent>, dl_writer: WriteStream) {
        self.inner.lock().downlinks.push((dl_channel, dl_writer));
    }

    pub fn push_channels(&self, key: Address<Text>, io: (ByteWriter, ByteReader)) {
        self.inner.lock().downlink_channels.insert(key, io);
    }

    pub fn take_channels(&self) -> HashMap<Address<Text>, (ByteWriter, ByteReader)> {
        let mut guard = self.inner.lock();
        std::mem::take(&mut guard.downlink_channels)
    }

    pub fn take_downlinks(&self) -> Vec<(BoxDownlinkChannel<Agent>, WriteStream)> {
        let mut guard = self.inner.lock();
        std::mem::take(&mut guard.downlinks)
    }
}

impl<Agent> DownlinkSpawner<Agent> for TestDownlinkContext<Agent> {
    fn spawn_downlink(
        &self,
        dl_channel: BoxDownlinkChannel<Agent>,
        dl_writer: WriteStream,
    ) -> Result<(), DownlinkRuntimeError> {
        self.push_dl(dl_channel, dl_writer);
        Ok(())
    }
}

impl<Agent> AgentContext for TestDownlinkContext<Agent> {
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
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::with_init();

    let address = Address::text(None, REMOTE_NODE, REMOTE_LANE);

    let mut handler = AddDownlinkAction::new(
        TestAgent::LANE,
        34,
        address.clone(),
        DefaultJoinValueLifecycle,
    );

    let context = TestDownlinkContext::default();
    let spawner = FuturesUnordered::new();
    let mut inits = HashMap::new();

    let mut action_context = ActionContext::new(&spawner, &context, &context, &mut inits);
    let result = handler.step(&mut action_context, meta, &agent);
    check_result(result, false, false, Some(()));

    let result = handler.step(&mut action_context, meta, &agent);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));

    run_event_handlers(&context, &context, &agent, meta, &mut inits, spawner).await;

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

fn register_lifecycle(
    action_context: &mut ActionContext<'_, TestAgent>,
    agent: &TestAgent,
    count: Arc<AtomicUsize>,
) {
    let lc = DefaultJoinValueLifecycle;
    let fac = move || {
        count.fetch_add(1, Ordering::Relaxed);
        lc
    };
    let init: BoxJoinValueInit<'static, TestAgent> =
        Box::new(LifecycleInitializer::new(TestAgent::LANE, fac));
    let lane_id = agent.lane.id();
    action_context.register_join_value_initializer(lane_id, init);
}

#[tokio::test]
async fn open_downlink_from_registered() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::with_init();

    let address = Address::text(None, REMOTE_NODE, REMOTE_LANE);

    let handler = JoinValueAddDownlink::new(TestAgent::LANE, 12, address.clone());

    let context = TestDownlinkContext::default();
    let mut inits = HashMap::new();

    let count = Arc::new(AtomicUsize::new(0));

    let spawner = FuturesUnordered::new();
    let mut action_context = ActionContext::new(&spawner, &context, &context, &mut inits);
    register_lifecycle(&mut action_context, &agent, count.clone());
    assert!(spawner.is_empty());

    run_with_futures(&context, &context, &agent, meta, &mut inits, handler).await;

    assert_eq!(count.load(Ordering::Relaxed), 1);
}
