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

use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use bytes::BytesMut;
use futures::stream::FuturesUnordered;
use swimos_api::{
    address::Address,
    agent::{AgentConfig, DownlinkKind},
};
use swimos_utilities::routing::RouteUri;

use crate::lanes::join_value::JoinValueRemoveDownlink;
use crate::{
    event_handler::{
        ActionContext, BoxJoinLaneInit, EventHandlerError, HandlerAction, Modification, StepResult,
    },
    item::{AgentItem, MapItem},
    lanes::{
        join::test_util::{TestDlContextInner, TestDownlinkContext},
        join_value::{
            default_lifecycle::DefaultJoinValueLifecycle, AddDownlinkAction, JoinValueLaneGet,
            JoinValueLaneGetMap, JoinValueLaneWithEntry,
        },
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

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, false, false, Some(Some(V1.to_string())));

    let mut handler = JoinValueLaneGet::new(TestAgent::LANE, ABSENT);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, false, false, Some(None));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn join_value_lane_with_entry_event_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::with_init();

    let mut handler =
        JoinValueLaneWithEntry::new(TestAgent::LANE, K1, |v: Option<&str>| v.map(str::to_owned));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, false, false, Some(Some(V1.to_string())));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));

    let mut handler = JoinValueLaneWithEntry::new(TestAgent::LANE, ABSENT, |v: Option<&str>| {
        v.map(str::to_owned)
    });

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, false, false, Some(None));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
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

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, false, false, Some(expected));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
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
    let mut ad_hoc_buffer = BytesMut::new();

    let mut action_context = ActionContext::new(
        &spawner,
        &context,
        &context,
        &context,
        &mut inits,
        &mut ad_hoc_buffer,
    );
    let result = handler.step(&mut action_context, meta, &agent);
    check_result(result, false, false, Some(()));

    let result = handler.step(&mut action_context, meta, &agent);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));

    run_event_handlers(
        &context,
        &context,
        &context,
        &agent,
        meta,
        &mut inits,
        &mut ad_hoc_buffer,
        spawner,
    )
    .await;

    let guard = context.inner.lock();
    let TestDlContextInner {
        downlink_channels,
        downlinks,
    } = &*guard;
    assert_eq!(downlink_channels.len(), 1);
    assert!(downlink_channels.contains_key(&address));
    match downlinks.as_slice() {
        [channel] => {
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
    let init: BoxJoinLaneInit<'static, TestAgent> =
        Box::new(LifecycleInitializer::new(TestAgent::LANE, fac));
    let lane_id = agent.lane.id();
    action_context.register_join_lane_initializer(lane_id, init);
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
    let mut ad_hoc_buffer = BytesMut::new();

    let count = Arc::new(AtomicUsize::new(0));

    let spawner = FuturesUnordered::new();
    let mut action_context = ActionContext::new(
        &spawner,
        &context,
        &context,
        &context,
        &mut inits,
        &mut ad_hoc_buffer,
    );
    register_lifecycle(&mut action_context, &agent, count.clone());
    assert!(spawner.is_empty());

    run_with_futures(
        &context,
        &context,
        &context,
        &agent,
        meta,
        &mut inits,
        &mut ad_hoc_buffer,
        handler,
    )
    .await;

    assert_eq!(count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn stop_downlink() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::with_init();

    let address = Address::text(None, REMOTE_NODE, REMOTE_LANE);

    let handler = JoinValueAddDownlink::new(TestAgent::LANE, 12, address.clone());

    let context = TestDownlinkContext::default();
    let mut inits = HashMap::new();
    let mut ad_hoc_buffer = BytesMut::new();

    let count = Arc::new(AtomicUsize::new(0));

    let spawner = FuturesUnordered::new();
    let mut action_context = ActionContext::new(
        &spawner,
        &context,
        &context,
        &context,
        &mut inits,
        &mut ad_hoc_buffer,
    );
    register_lifecycle(&mut action_context, &agent, count.clone());
    assert!(spawner.is_empty());

    run_with_futures(
        &context,
        &context,
        &context,
        &agent,
        meta,
        &mut inits,
        &mut ad_hoc_buffer,
        handler,
    )
    .await;

    assert_eq!(count.load(Ordering::Relaxed), 1);

    agent.lane.inner.update(12, "value".to_string());

    let mut stop_handler = JoinValueRemoveDownlink::new(TestAgent::LANE, 12);
    let result = stop_handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, false, false, Some(()));

    let guard = agent.lane.keys.borrow();
    assert!(guard.is_empty());
    assert!(agent.lane.inner.get(&12, |value| value.is_none()));
}
